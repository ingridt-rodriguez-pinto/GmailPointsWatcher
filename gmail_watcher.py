import os
import json
import re
import threading
import time
import imaplib
import email
import argparse
import logging
import asyncio
import csv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, ContextTypes, CallbackQueryHandler, CommandHandler, MessageHandler, filters
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8428323516:AAElzjmSPfUeCoTGKbm7wnDLSZ5ek1-6Gvc")
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "buglione2500@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "tsvk jljb torw blih")
CHAT_ID = int(os.getenv("CHAT_ID", "2"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "60"))
MSSQL_SERVER = os.getenv("MSSQL_SERVER", "DESKTOP-UQTBU3F\\MSSQLSERVER_2022")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "GlobalPointsWatcher")
MSSQL_USER = os.getenv("MSSQL_USER", "sa_giovanni")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "pruebabd")


def _fmt2(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def points_to_usd(points: float) -> float:
    return _fmt2(points / 100.0)


def calculate_total_points() -> float:
    if not os.path.exists("puntos_registros.json"):
        return 0.0
    with open("puntos_registros.json", "r", encoding="utf-8") as f:
        registros = json.load(f)
    total = 0.0
    for company, transactions in registros.items():
        for tx in transactions:
            total += float(tx.get("puntos", 0.0))
    return _fmt2(total)


def save_points_record(name: str, total_usd: float, points: float) -> None:
    registros: Dict[str, List[Dict[str, Any]]] = {}
    if os.path.exists("puntos_registros.json"):
        with open("puntos_registros.json", "r", encoding="utf-8") as f:
            registros = json.load(f)
    if name not in registros:
        registros[name] = []

    entry = {
        "total_usd": _fmt2(total_usd),
        "puntos": _fmt2(points),
    }
    registros[name].append(entry)

    with open("puntos_registros.json", "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False)


def extract_company_name(body: str) -> str:
    m = re.search(r"en\s(.+?)\s+PA\s+con tarjeta", body, re.IGNORECASE | re.DOTALL)
    if not m:
        raise ValueError("No se encontró el nombre del comercio en el correo")
    return m.group(1).strip()


def extract_usd_value(body: str) -> float:
    m = re.search(r"compra\s+de\s*\$([\d,.]+)", body, re.IGNORECASE)
    if not m:
        raise ValueError("No se encontró el monto en USD en el correo")
    usd_str = m.group(1).replace(",", "")
    return _fmt2(float(usd_str))

def _get_text_from_message(msg: Any) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                charset = part.get_content_charset() or "utf-8"
                payload = part.get_payload(decode=True)
                if payload is None:
                    return str(part.get_payload())
                return payload.decode(charset, errors="replace")
        return ""
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload is None:
            return str(msg.get_payload())
        return payload.decode(charset, errors="replace")

class GmailWatcherPython:
    def __init__(self) -> None:
        self.old_points_total = calculate_total_points()
        self.application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        self._tx_pending = {}
        self._pending_card_query = set()
        setup_logging()

    def start(self) -> None:
        self.application.add_handler(CallbackQueryHandler(self._on_recognize))
        self.application.add_handler(CommandHandler("puntos", self._cmd_puntos))
        self.application.add_handler(CommandHandler("start", self._cmd_start))
        self.application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self._on_text))
        try:
            db_ensure_recognition_table()
        except Exception:
            pass
        self.application.job_queue.run_repeating(self._email_job, interval=TIMEOUT_SECONDS, first=0)
        self.application.run_polling()

    async def _email_job(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        items = await asyncio.to_thread(self._process_emails)
        for it in items:
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Reconocida", callback_data=f"rec:{it['token']}:Y"),
                    InlineKeyboardButton("No reconocida", callback_data=f"rec:{it['token']}:N"),
                ]
            ])
            await context.bot.send_message(chat_id=CHAT_ID, text=it["text"], reply_markup=kb)

    def _process_emails(self) -> list:
        M = imaplib.IMAP4_SSL("imap.gmail.com")
        M.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        M.select("INBOX")

        status, data = M.search(None, '(UNSEEN FROM "contactenos@globalbank.com.pa" SUBJECT "CONFIRMACION DE TRANSACCION")')
        if status != "OK":
            M.logout()
            return []

        items_out = []
        ids = data[0].split()
        for num in ids:
            status, msg_data = M.fetch(num, "(RFC822)")
            if status != "OK":
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            body = _get_text_from_message(msg)

            try:
                company_name = extract_company_name(body)
                usd_value = extract_usd_value(body)
                card_last4 = extract_card_last4(body)
                m1, m2, token = self._calculate_store_and_messages(company_name, usd_value, card_last4)
                self._tx_pending[token] = {
                    "company": company_name,
                    "amount": usd_value,
                    "card": card_last4,
                }
                items_out.append({"text": m1, "token": token})
                items_out.append({"text": m2, "token": token})
            finally:
                M.store(num, "+FLAGS", "\\Seen")

        M.close()
        M.logout()
        return items_out

    def _calculate_store_and_messages(self, company_name: str, usd_value: float, card_last4: str):
        puntos = float(int(usd_value))
        save_points_record(company_name, usd_value, puntos)
        try:
            db_insert_transaction(company_name, "Global Bank", card_last4, usd_value, int(puntos), datetime.now())
        except Exception:
            pass
        new_total_points = calculate_total_points()
        usd_equiv = points_to_usd(new_total_points)
        m1 = f"{int(puntos)} puntos agregados por {company_name} (compra ${_fmt2(usd_value)})"
        m2 = f"Total puntos: {int(new_total_points)} | Equivalente: ${usd_equiv}"
        self.old_points_total = new_total_points
        token = f"{int(time.time())}-{hash(company_name+str(usd_value)+card_last4) & 0xffffffff}"
        return m1, m2, token

    async def _on_recognize(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        q = update.callback_query
        if not q or not q.data:
            return
        parts = q.data.split(":")
        if parts[0] == "rec" and len(parts) == 3:
            token = parts[1]
            ans = parts[2]
            tx = self._tx_pending.get(token)
            if not tx:
                await q.answer()
                return
            status = "RECONOCIDA" if ans == "Y" else "NO RECONOCIDA"
            logging.info(f"TX {token} {tx['company']} ${_fmt2(tx['amount'])} {tx['card']} status={status}")
            try:
                db_insert_recognition(token, tx['company'], tx['amount'], tx['card'], status, datetime.now())
            except Exception:
                pass
            await q.answer()
            await q.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(
                chat_id=q.message.chat_id,
                text=f"Transacción {tx['company']} ${_fmt2(tx['amount'])} terminación {tx['card']} marcada como {status}"
            )
            return
        if parts[0] == "menu" and len(parts) == 2:
            key = parts[1]
            await q.answer()
            if key == "puntos":
                await self._cmd_puntos(update, context)
            elif key == "totales":
                total_points = calculate_total_points()
                usd_equiv = points_to_usd(total_points)
                await context.bot.send_message(chat_id=q.message.chat_id, text=f"Total puntos: {int(total_points)} | Equivalente: ${usd_equiv}")
            elif key == "ayuda":
                await context.bot.send_message(chat_id=q.message.chat_id, text=self._help_text())
            elif key == "resumen":
                await self._cmd_resumen(update, context)

    async def _cmd_puntos(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        total_points = calculate_total_points()
        usd_equiv = points_to_usd(total_points)
        chat_id = update.effective_chat.id
        self._pending_card_query.add(chat_id)
        await context.bot.send_message(chat_id=chat_id, text=f"Total puntos: {int(total_points)} | Equivalente: ${usd_equiv}\nIngresa los últimos 4 dígitos de la tarjeta")

    async def _on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        if chat_id not in self._pending_card_query:
            return
        text = (update.message.text or "").strip()
        m = re.search(r"\b(\d{4})\b", text)
        if not m:
            await context.bot.send_message(chat_id=chat_id, text="Por favor, envía solo los últimos 4 dígitos")
            return
        last4 = m.group(1)
        pts = db_sum_points_by_card(last4)
        usd = points_to_usd(pts)
        await context.bot.send_message(chat_id=chat_id, text=f"Tarjeta terminación {last4}: {int(pts)} puntos | Equivalente: ${usd}")
        try:
            self._pending_card_query.remove(chat_id)
        except Exception:
            pass

    # No se requiere interacción por Telegram; los puntos son fijos (1 punto por $1).


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--totales", action="store_true")
    parser.add_argument("--enviar", action="store_true")
    parser.add_argument("--insert-demo", action="store_true")
    parser.add_argument("--demo-company", type=str)
    parser.add_argument("--demo-amount", type=float)
    parser.add_argument("--demo-card", type=str)
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--card", type=str)
    parser.add_argument("--recon-report", action="store_true")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--csv", type=str)
    args = parser.parse_args()

    if args.totales:
        total_points = calculate_total_points()
        usd_equiv = points_to_usd(total_points)
        print(f"Total puntos: {int(total_points)}")
        print(f"Equivalente en USD: ${usd_equiv}")
        if args.enviar and TELEGRAM_TOKEN and CHAT_ID:
            app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
            asyncio.run(app.bot.send_message(chat_id=CHAT_ID, text=f"Total puntos: {int(total_points)} | Equivalente: ${usd_equiv}"))
        return

    if args.insert_demo:
        company = args.demo_company or "COMERCIO"
        amount = float(args.demo_amount or 0)
        card = (args.demo_card or "0000")[:4]
        puntos = int(amount)
        save_points_record(company, amount, puntos)
        try:
            db_insert_transaction(company, "Global Bank", card, amount, int(puntos), datetime.now())
        except Exception:
            pass
        print("Insert demo ejecutado")
        return

    if args.backfill and args.card:
        card = args.card[:4]
        try:
            if os.path.exists("puntos_registros.json"):
                with open("puntos_registros.json", "r", encoding="utf-8") as f:
                    registros = json.load(f)
                for company, txs in registros.items():
                    for tx in txs:
                        amount = float(tx.get("total_usd", 0))
                        puntos = int(float(tx.get("puntos", 0)))
                        try:
                            db_insert_transaction(company, "Global Bank", card, amount, puntos, datetime.now())
                        except Exception:
                            pass
                print("Backfill completado")
        except Exception:
            print("Error en backfill")
        return

    if args.recon_report:
        y = args.year or datetime.now().year
        m = args.month or datetime.now().month
        rows = []
        conn = _db_connect()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT Token, Company, AmountUSD, CardLast4, Status, RecognizedAt
                    FROM dbo.TransactionRecognitions
                    WHERE YEAR(RecognizedAt)=? AND MONTH(RecognizedAt)=?
                    ORDER BY RecognizedAt DESC
                    """,
                    y,
                    m,
                )
                rows = cur.fetchall()
            except Exception:
                rows = []
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        print(f"Reconocimientos {y}-{m:02d}: {len(rows)}")
        for r in rows[:10]:
            print(f"{r[5]} | {r[1]} ${_fmt2(float(r[2]))} {r[3]} {r[4]} token={r[0]}")
        outcsv = args.csv or "recognitions_report.csv"
        try:
            with open(outcsv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Token","Company","AmountUSD","CardLast4","Status","RecognizedAt"])
                for r in rows:
                    w.writerow([r[0], r[1], float(r[2]), r[3], r[4], r[5]])
            print(f"CSV: {outcsv}")
        except Exception:
            pass
        return

    watcher = GmailWatcherPython()
    watcher.start()
def setup_logging() -> None:
    try:
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[
                logging.FileHandler(os.path.join("logs", "watcher.log"), encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )
    except Exception:
        logging.basicConfig(level=logging.INFO)

def extract_card_last4(body: str) -> str:
    m = re.search(r"terminaci[oó]n\s*(\d{4})", body, re.IGNORECASE)
    return m.group(1) if m else "0000"

def _db_connect():
    try:
        if all([MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD]):
            import pyodbc  # type: ignore
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USER};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes",
                timeout=5,
            )
            return conn
    except Exception as e:
        logging.warning(f"No se pudo conectar a SQL Server: {e}")
    return None


def db_insert_transaction(company_name: str, bank: str, card_last4: str, total_usd: float, points: int, dt: datetime) -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.Transactions(Company, Bank, CardLast4, AmountUSD, Points, TransactionAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            company_name,
            bank,
            card_last4,
            float(total_usd),
            int(points),
            dt,
        )
        conn.commit()
    except Exception as e:
        logging.error(f"Error insertando transacción en SQL Server: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_ensure_recognition_table() -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.TransactionRecognitions','U') IS NULL
            CREATE TABLE dbo.TransactionRecognitions(
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Token NVARCHAR(128) NOT NULL,
                Company NVARCHAR(256) NOT NULL,
                AmountUSD DECIMAL(18,2) NOT NULL,
                CardLast4 CHAR(4) NOT NULL,
                Status NVARCHAR(20) NOT NULL,
                RecognizedAt DATETIME NOT NULL
            )
            """
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_insert_recognition(token: str, company: str, amount_usd: float, card_last4: str, status: str, dt: datetime) -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.TransactionRecognitions(Token, Company, AmountUSD, CardLast4, Status, RecognizedAt)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            token,
            company,
            float(amount_usd),
            card_last4,
            status,
            dt,
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_sum_points_by_card(card_last4: str) -> int:
    conn = _db_connect()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(Points),0) FROM dbo.Transactions WHERE CardLast4=?", card_last4[:4])
        r = cur.fetchone()
        return int(r[0] or 0)
    except Exception:
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_monthly_summary(card_last4: str | None) -> Dict[str, Any]:
    y = datetime.now().year
    m = datetime.now().month
    conn = _db_connect()
    out = {"year": y, "month": m, "points": 0, "top_company": None}
    if not conn:
        return out
    try:
        cur = conn.cursor()
        if card_last4:
            cur.execute(
                """
                SELECT COALESCE(SUM(Points),0)
                FROM dbo.Transactions
                WHERE YEAR(TransactionAt)=? AND MONTH(TransactionAt)=? AND CardLast4=?
                """,
                y,
                m,
                card_last4[:4],
            )
            out["points"] = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                SELECT TOP 1 Company, SUM(Points) AS P
                FROM dbo.Transactions
                WHERE YEAR(TransactionAt)=? AND MONTH(TransactionAt)=? AND CardLast4=?
                GROUP BY Company
                ORDER BY P DESC
                """,
                y,
                m,
                card_last4[:4],
            )
        else:
            cur.execute(
                """
                SELECT COALESCE(SUM(Points),0)
                FROM dbo.Transactions
                WHERE YEAR(TransactionAt)=? AND MONTH(TransactionAt)=?
                """,
                y,
                m,
            )
            out["points"] = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                SELECT TOP 1 Company, SUM(Points) AS P
                FROM dbo.Transactions
                WHERE YEAR(TransactionAt)=? AND MONTH(TransactionAt)=?
                GROUP BY Company
                ORDER BY P DESC
                """,
                y,
                m,
            )
        r = cur.fetchone()
        out["top_company"] = r[0] if r else None
        return out
    except Exception:
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_ensure_monthly_summary_sp() -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE OR ALTER PROCEDURE dbo.sp_SendMonthlySummaryEmail
                @CardLast4   CHAR(4),
                @SendTo      NVARCHAR(256),
                @ProfileName NVARCHAR(128) = N'DefaultProfile'
            AS
            BEGIN
                SET NOCOUNT ON;
                DECLARE @Year INT = YEAR(GETDATE());
                DECLARE @Month INT = MONTH(GETDATE());
                DECLARE @TotalPoints INT = 0;
                DECLARE @TopCompany NVARCHAR(256) = NULL;
                SELECT @TotalPoints = COALESCE(SUM(Points),0)
                FROM dbo.Transactions
                WHERE YEAR(TransactionAt)=@Year AND MONTH(TransactionAt)=@Month AND CardLast4=@CardLast4;
                SELECT TOP 1 @TopCompany = Company
                FROM (
                    SELECT Company, SUM(Points) AS P
                    FROM dbo.Transactions
                    WHERE YEAR(TransactionAt)=@Year AND MONTH(TransactionAt)=@Month AND CardLast4=@CardLast4
                    GROUP BY Company
                ) T
                ORDER BY P DESC;
                DECLARE @MonthName NVARCHAR(20) = DATENAME(MONTH, DATEFROMPARTS(@Year, @Month, 1));
                DECLARE @Subject NVARCHAR(255) = N'Resumen puntos ' + @MonthName + N' ' + CONVERT(NVARCHAR(4), @Year);
                DECLARE @Body NVARCHAR(MAX) =
                    N'Resumen mensual ' + @MonthName + N' ' + CONVERT(NVARCHAR(4), @Year) + CHAR(13)+CHAR(10) +
                    N'Tarjeta terminación: ' + @CardLast4 + CHAR(13)+CHAR(10) +
                    N'Total puntos: ' + CONVERT(NVARCHAR(50), @TotalPoints) + CHAR(13)+CHAR(10) +
                    N'Equivalente USD: $' + CONVERT(NVARCHAR(50), CAST(CONVERT(DECIMAL(18,2), @TotalPoints/100.0) AS DECIMAL(18,2))) + CHAR(13)+CHAR(10) +
                    N'Comercio más consumido: ' + COALESCE(@TopCompany, N'–') + CHAR(13)+CHAR(10);
                EXEC msdb.dbo.sp_send_dbmail
                    @profile_name = @ProfileName,
                    @recipients   = @SendTo,
                    @subject      = @Subject,
                    @body         = @Body;
            END
            """
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_create_summary_job(card_last4: str, send_to: str, profile_name: str = 'DefaultProfile') -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT job_id FROM msdb.dbo.sysjobs WHERE name = 'GlobalPointsMonthlySummary'")
        r = cur.fetchone()
        if r:
            return
        cur.execute(
            """
            DECLARE @job_id UNIQUEIDENTIFIER;
            EXEC msdb.dbo.sp_add_job @job_name = N'GlobalPointsMonthlySummary', @enabled = 1, @job_id = @job_id OUTPUT;
            EXEC msdb.dbo.sp_add_jobstep @job_name = N'GlobalPointsMonthlySummary', @step_name = N'Send Summary', @subsystem = N'TSQL',
                @command = N'EXEC dbo.sp_SendMonthlySummaryEmail @CardLast4=''' + ? + ''', @SendTo=''' + ? + ''', @ProfileName=''' + ? + '''';
            EXEC msdb.dbo.sp_add_schedule @schedule_name = N'MonthlySummarySchedule', @freq_type = 16, @freq_interval = 1, @active_start_time = 80000;
            EXEC msdb.dbo.sp_attach_schedule @job_name = N'GlobalPointsMonthlySummary', @schedule_name = N'MonthlySummarySchedule';
            EXEC msdb.dbo.sp_add_jobserver @job_name = N'GlobalPointsMonthlySummary';
            """,
            card_last4[:4],
            send_to,
            profile_name,
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Consultar puntos", callback_data="menu:puntos")],
            [InlineKeyboardButton("Ver totales", callback_data="menu:totales")],
            [InlineKeyboardButton("Resumen mensual", callback_data="menu:resumen")],
            [InlineKeyboardButton("Ayuda", callback_data="menu:ayuda")],
        ])
        await context.bot.send_message(chat_id=chat_id, text=self._help_text(), reply_markup=kb)

    def _help_text(self) -> str:
        return (
            "Opciones:\n"
            "- /puntos: muestra total y solicita últimos 4 dígitos\n"
            "- /resumen: resumen mensual y top comercio\n"
            "- Botones Reconocida/No reconocida: marcan la transacción y se guardan\n"
            "- Ver totales: consulta tus puntos acumulados y equivalente USD\n"
            "- Reporte mensual CLI: --recon-report [--year --month --csv]"
        )

    async def _cmd_resumen(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        s = db_monthly_summary(None)
        txt = (
            f"Resumen {s['year']}-{s['month']:02d}:\n"
            f"Puntos: {int(s['points'])} | USD: ${points_to_usd(s['points'])}\n"
            f"Top comercio: {s['top_company'] or '–'}"
        )
        await context.bot.send_message(chat_id=chat_id, text=txt)
