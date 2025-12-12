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
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()

# Configuración de BD desde .env
MSSQL_SERVER = os.getenv("MSSQL_SERVER", rf"DESKTOP-UQTBU3F\MSSQLSERVER_2022")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "GlobalPointsWatcher")
MSSQL_USER = os.getenv("MSSQL_USER", "sa_giovanni")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "pruebabd")

# Valores por defecto para bootstrap (se migrarán a BD)
DEFAULT_TELEGRAM_TOKEN = "8428323516:AAElzjmSPfUeCoTGKbm7wnDLSZ5ek1-6Gvc"
DEFAULT_EMAIL_USERNAME = "buglione2500@gmail.com"
DEFAULT_EMAIL_PASSWORD = "tsvk jljb torw blih"
DEFAULT_CHAT_ID = "1943663667"
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "60"))

# Variables globales que se llenarán desde BD
TELEGRAM_TOKEN = None
EMAIL_USERNAME = None
EMAIL_PASSWORD = None
CHAT_ID = None

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

def db_ensure_parameters_table() -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Crear tabla si no existe
        cur.execute(
            """
            IF OBJECT_ID('dbo.Parameters','U') IS NULL
            CREATE TABLE dbo.Parameters(
                ParamKey NVARCHAR(50) PRIMARY KEY,
                ParamValue NVARCHAR(MAX)
            )
            """
        )
        conn.commit()
        
        # Insertar valores por defecto si no existen
        defaults = {
            "TELEGRAM_TOKEN": DEFAULT_TELEGRAM_TOKEN,
            "EMAIL_USERNAME": DEFAULT_EMAIL_USERNAME,
            "EMAIL_PASSWORD": DEFAULT_EMAIL_PASSWORD,
            "CHAT_ID": DEFAULT_CHAT_ID
        }
        
        for key, val in defaults.items():
            cur.execute("SELECT 1 FROM dbo.Parameters WHERE ParamKey = ?", key)
            if not cur.fetchone():
                cur.execute("INSERT INTO dbo.Parameters (ParamKey, ParamValue) VALUES (?, ?)", key, val)
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error asegurando tabla Parameters: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def load_config_from_db() -> bool:
    global TELEGRAM_TOKEN, EMAIL_USERNAME, EMAIL_PASSWORD, CHAT_ID
    conn = _db_connect()
    if not conn:
        logging.error("No se pudo conectar a BD para cargar configuración")
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT ParamKey, ParamValue FROM dbo.Parameters")
        rows = cur.fetchall()
        config = {row[0]: row[1] for row in rows}
        
        TELEGRAM_TOKEN = config.get("TELEGRAM_TOKEN")
        EMAIL_USERNAME = config.get("EMAIL_USERNAME")
        EMAIL_PASSWORD = config.get("EMAIL_PASSWORD")
        try:
            CHAT_ID = int(config.get("CHAT_ID", "0"))
        except:
            CHAT_ID = 0
            
        if not all([TELEGRAM_TOKEN, EMAIL_USERNAME, EMAIL_PASSWORD, CHAT_ID]):
             logging.warning("Faltan configuraciones en la tabla Parameters")
             
        return True
    except Exception as e:
        logging.error(f"Error cargando configuración: {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
        # Inicializar configuración
        db_ensure_parameters_table()
        if not load_config_from_db():
            logging.error("No se pudo cargar la configuración crítica. El bot puede fallar.")
        
        if TELEGRAM_TOKEN:
            self.application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        else:
            raise ValueError("TELEGRAM_TOKEN no encontrado en BD")

        self._tx_pending = {}
        self._pending_card_query = set()
        self._pending_points_action = {} # {chat_id: {"action": "add"|"redeem", "step": "card"|"amount"}}
        setup_logging()

    def start(self) -> None:
        if CHAT_ID == 1943663667:
            logging.warning("⚠️ Usando CHAT_ID por defecto (1943663667).")
        
        self.application.add_handler(CallbackQueryHandler(self._on_card_action, pattern=r"^card:"))
        self.application.add_handler(CallbackQueryHandler(self._on_recognize, pattern=r"^(rec|recdb):"))
        self.application.add_handler(CallbackQueryHandler(self._on_menu, pattern=r"^menu:"))
        self.application.add_handler(CommandHandler("puntos", self._cmd_puntos))
        self.application.add_handler(CommandHandler("start", self._cmd_start))
        self.application.add_handler(CommandHandler("status", self._cmd_status))
        self.application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self._on_text))
        try:
            db_ensure_recognition_table()
            db_ensure_unrecognized_table()
            db_ensure_monthly_summary_sp()
            db_ensure_user_cards_table()
        except Exception:
            pass
        self.application.job_queue.run_repeating(self._email_job, interval=TIMEOUT_SECONDS, first=0)
        self.application.run_polling()

    async def _email_job(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        items = await asyncio.to_thread(self._process_emails)
        for it in items:
            chat_id = it.get("chat_id", CHAT_ID)
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Reconocida", callback_data=f"rec:{it['token']}:Y"),
                    InlineKeyboardButton("No reconocida", callback_data=f"rec:{it['token']}:N"),
                ]
            ])
            await context.bot.send_message(chat_id=chat_id, text=it["text"], reply_markup=kb)

    def _process_emails(self) -> list:
        M = imaplib.IMAP4_SSL("imap.gmail.com")
        M.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        M.select("INBOX")

        status, data = M.search(None, '(UNSEEN FROM "contactenos@globalbank.com.pa" SUBJECT "CONFIRMACION DE TRANSACCION")')
        if status != "OK":
            M.logout()
            return []

        card_map = db_get_all_user_cards_map()
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
                
                target_chat_ids = card_map.get(card_last4, [CHAT_ID])
                for cid in target_chat_ids:
                    items_out.append({"text": m1, "token": token, "chat_id": cid})
                    items_out.append({"text": m2, "token": token, "chat_id": cid})
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
                if status == "NO RECONOCIDA":
                    db_insert_unrecognized(token, tx['company'], tx['amount'], tx['card'], datetime.now())
            except Exception:
                pass
            await q.answer()
            await q.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(
                chat_id=q.message.chat_id,
                text=f"Transacción {tx['company']} ${_fmt2(tx['amount'])} terminación {tx['card']} marcada como {status}"
            )
            return
        if parts[0] == "recdb" and len(parts) == 3:
            tx_id_str = parts[1]
            ans = parts[2]
            try:
                tx_id = int(tx_id_str)
                tx_data = db_get_transaction_by_id(tx_id)
                if not tx_data:
                     await q.answer("Transacción no encontrada")
                     return
                status = "RECONOCIDA" if ans == "Y" else "NO RECONOCIDA"
                # Generate a synthetic token or use "DB:{id}"
                token = f"DB:{tx_id}"
                db_insert_recognition(token, tx_data['company'], tx_data['amount'], tx_data['card'], status, datetime.now())
                if status == "NO RECONOCIDA":
                    db_insert_unrecognized(token, tx_data['company'], tx_data['amount'], tx_data['card'], datetime.now())
                await q.answer(f"Marcada como {status}")
                await q.edit_message_reply_markup(reply_markup=None)
                await context.bot.send_message(
                    chat_id=q.message.chat_id,
                    text=f"✅ Transacción {tx_data['company']} marcada como {status}"
                )
            except Exception:
                await q.answer("Error al procesar")
            return

    async def _on_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        q = update.callback_query
        if not q or not q.data:
            return
        parts = q.data.split(":")
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
            elif key == "status":
                await self._cmd_status(update, context)
            elif key == "historial":
                await self._cmd_historial(update, context)
            elif key == "gestionar":
                await self._cmd_gestionar_puntos_menu(update, context)
            elif key == "add_points":
                await self._init_points_action(update, context, "add")
            elif key == "redeem_points":
                await self._init_points_action(update, context, "redeem")
            elif key == "cards":
                await self._cmd_cards_menu(update, context)
            elif key == "add_card":
                await self._init_add_card(update, context)
            elif key == "list_cards":
                await self._cmd_list_cards(update, context)
            elif key == "email_report":
                await self._trigger_email_report(update, context)
            elif key == "start":
                await self._cmd_start(update, context)

    async def _trigger_email_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        await context.bot.send_message(chat_id=chat_id, text="⏳ Ejecutando job de reporte mensual...")
        success, msg = db_trigger_summary_job()
        emoji = "✅" if success else "❌"
        await context.bot.send_message(chat_id=chat_id, text=f"{emoji} {msg}")

    async def _cmd_cards_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Agregar Tarjeta", callback_data="menu:add_card")],
            [InlineKeyboardButton("📋 Listar Mis Tarjetas", callback_data="menu:list_cards")],
            [InlineKeyboardButton("🔙 Menú Principal", callback_data="menu:start")],
        ])
        await context.bot.send_message(chat_id=chat_id, text="💳 *Gestión de Tarjetas*", reply_markup=kb, parse_mode="Markdown")

    async def _init_add_card(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        self._pending_points_action[chat_id] = {"action": "add_card", "step": "card_number", "data": {}}
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data="menu:cards")]])
        await context.bot.send_message(chat_id=chat_id, text="💳 Ingresa los últimos 4 dígitos de la nueva tarjeta:", reply_markup=kb)

    async def _cmd_list_cards(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        cards = db_get_user_cards(chat_id)
        kb_rows = []
        if not cards:
             msg = "📭 No tienes tarjetas registradas."
        else:
             msg = "Selecciona una tarjeta para gestionar:"
             for c in cards:
                 label = f"💳 {c['card']}"
                 if c['alias']:
                     label += f" ({c['alias']})"
                 kb_rows.append([InlineKeyboardButton(label, callback_data=f"card:select:{c['id']}")])
        
        kb_rows.append([InlineKeyboardButton("🔙 Volver", callback_data="menu:cards")])
        kb = InlineKeyboardMarkup(kb_rows)
        await context.bot.send_message(chat_id=chat_id, text=msg, reply_markup=kb)

    async def _on_card_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        q = update.callback_query
        if not q or not q.data:
            return
        parts = q.data.split(":")
        # card:select:ID
        # card:edit:ID
        # card:delete:ID
        # card:confirm_delete:ID
        action = parts[1]
        card_id = int(parts[2])
        chat_id = q.message.chat_id
        
        if action == "select":
            await q.answer()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Editar", callback_data=f"card:edit:{card_id}")],
                [InlineKeyboardButton("🗑️ Eliminar", callback_data=f"card:delete:{card_id}")],
                [InlineKeyboardButton("🔙 Volver", callback_data="menu:list_cards")],
            ])
            await q.edit_message_text(text=f"Tarjeta seleccionada (ID {card_id}). ¿Qué deseas hacer?", reply_markup=kb)
        
        elif action == "query_points":
            card_last4 = parts[2]
            pts = db_sum_points_by_card(card_last4)
            usd = points_to_usd(pts)
            await q.answer()
            # Mostramos el resultado y volvemos a mostrar el menú de puntos
            await context.bot.send_message(chat_id=chat_id, text=f"💳 Tarjeta {card_last4}\n⭐ Puntos: {int(pts)}\n💵 Equivalente: ${usd}")
            await self._cmd_puntos(update, context)

        elif action == "delete":
            await q.answer()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Sí, Eliminar", callback_data=f"card:confirm_delete:{card_id}")],
                [InlineKeyboardButton("❌ Cancelar", callback_data="menu:list_cards")],
            ])
            await q.edit_message_text(text=f"¿Estás seguro de eliminar esta tarjeta?", reply_markup=kb)
        
        elif action == "save_new":
            card_val = str(parts[2]).zfill(4)
            if db_add_user_card(chat_id, card_val):
                await q.answer("Guardada")
                await q.edit_message_text(f"✅ Tarjeta {card_val} guardada correctamente.")
                if chat_id in self._pending_points_action:
                    del self._pending_points_action[chat_id]
                await self._cmd_list_cards(update, context)
            else:
                await q.answer("Error")
                await q.edit_message_text(f"❌ La tarjeta {card_val} ya existe o hubo un error.")
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("📋 Listar Tarjetas", callback_data="menu:list_cards")]])
                await context.bot.send_message(chat_id=chat_id, text="Intenta nuevamente o gestiona tus tarjetas.", reply_markup=kb)

        elif action == "retry_add":
            await q.answer()
            self._pending_points_action[chat_id] = {"action": "add_card", "step": "card_number", "data": {}}
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data="menu:cards")]])
            await q.edit_message_text(text="⌨️ Escribe nuevamente los 4 dígitos de la tarjeta:", reply_markup=kb)

        elif action == "confirm_delete":
            if db_delete_user_card(card_id):
                await q.answer("Tarjeta eliminada")
                await self._cmd_list_cards(update, context)
            else:
                await q.answer("Error al eliminar")

        elif action == "edit":
            await q.answer()
            self._pending_points_action[chat_id] = {"action": "edit_card", "step": "new_number", "data": {"id": card_id}}
            await context.bot.send_message(chat_id=chat_id, text="Ingresa los nuevos 4 dígitos para esta tarjeta:")

    async def _cmd_gestionar_puntos_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Agregar Puntos", callback_data="menu:add_points")],
            [InlineKeyboardButton("➖ Canjear Puntos", callback_data="menu:redeem_points")],
        ])
        await context.bot.send_message(chat_id=chat_id, text="🔢 *Gestión de Puntos*\nSelecciona una opción:", reply_markup=kb, parse_mode="Markdown")

    async def _init_points_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> None:
        chat_id = update.effective_chat.id
        self._pending_points_action[chat_id] = {"action": action, "step": "card", "data": {}}
        await context.bot.send_message(chat_id=chat_id, text="💳 Ingresa los últimos 4 dígitos de la tarjeta:")

    async def _cmd_puntos(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        total_points = calculate_total_points()
        usd_equiv = points_to_usd(total_points)
        
        cards = db_get_user_cards(chat_id)
        kb_rows = []
        for c in cards:
            label = f"💳 {c['card']}"
            if c['alias']:
                label += f" ({c['alias']})"
            kb_rows.append([InlineKeyboardButton(label, callback_data=f"card:query_points:{c['card']}")])
            
        kb_rows.append([InlineKeyboardButton("🔙 Menú Principal", callback_data="menu:start")])
        kb = InlineKeyboardMarkup(kb_rows)
        
        self._pending_card_query.add(chat_id)
        msg = (
            f"Total Puntos Globales: {int(total_points)} | ${usd_equiv}\n\n"
            "Selecciona una tarjeta para ver sus puntos o escribe los 4 dígitos:"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg, reply_markup=kb)

    async def _on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        text = (update.message.text or "").strip()

        # Handle Points Action (Add/Redeem/AddCard/EditCard)
        if chat_id in self._pending_points_action:
            state = self._pending_points_action[chat_id]
            
            # --- Gestión de Puntos (Add/Redeem) ---
            if state["step"] == "card":
                m = re.search(r"\b(\d{4})\b", text)
                if not m:
                    await context.bot.send_message(chat_id=chat_id, text="❌ Por favor, envía 4 dígitos válidos.")
                    return
                state["data"]["card"] = m.group(1)
                state["step"] = "amount"
                action_text = "agregar" if state["action"] == "add" else "canjear"
                await context.bot.send_message(chat_id=chat_id, text=f"💰 Ingresa la cantidad de puntos a {action_text}:")
                return

            elif state["step"] == "amount":
                try:
                    points = int(text)
                    if points <= 0:
                        raise ValueError
                    card = state["data"]["card"]
                    action = state["action"]
                    
                    final_points = points if action == "add" else -points
                    company = "Ajuste Manual: Agregar" if action == "add" else "Canje de Puntos"
                    
                    if action == "redeem":
                        current_pts = db_sum_points_by_card(card)
                        if current_pts < points:
                            await context.bot.send_message(chat_id=chat_id, text=f"❌ Saldo insuficiente. Tienes {current_pts} puntos.")
                            del self._pending_points_action[chat_id]
                            return

                    try:
                        db_insert_transaction(company, "Global Bank", card, 0.0, final_points, datetime.now())
                        emoji = "✅" if action == "add" else "🎁"
                        msg = f"{emoji} Operación exitosa.\nTarjeta: {card}\nPuntos: {final_points:+}\nNuevo saldo: {db_sum_points_by_card(card)}"
                        await context.bot.send_message(chat_id=chat_id, text=msg)
                    except Exception as e:
                         await context.bot.send_message(chat_id=chat_id, text="❌ Error al guardar en base de datos.")

                    del self._pending_points_action[chat_id]
                except ValueError:
                    await context.bot.send_message(chat_id=chat_id, text="❌ Ingresa un número entero válido mayor a 0.")
                return

            # --- Gestión de Tarjetas (Add/Edit) ---
            elif state["action"] == "add_card" and state["step"] == "card_number":
                m = re.search(r"(\d{4})", text)
                kb_cancel = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver a Tarjetas", callback_data="menu:cards")]])
                
                if not m:
                    await context.bot.send_message(chat_id=chat_id, text="❌ No detecté 4 dígitos. Por favor, escribe solo los últimos 4 números de la tarjeta.", reply_markup=kb_cancel)
                    return
                
                card = m.group(1)
                # Ask for confirmation
                kb_confirm = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Sí, Guardar", callback_data=f"card:save_new:{card}")],
                    [InlineKeyboardButton("🔄 No, Corregir", callback_data="card:retry_add:0")],
                    [InlineKeyboardButton("❌ Cancelar", callback_data="menu:cards")]
                ])
                await context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"¿Los dígitos **{card}** son correctos?", 
                    reply_markup=kb_confirm,
                    parse_mode="Markdown"
                )
                return
            
            elif state["action"] == "edit_card" and state["step"] == "new_number":
                m = re.search(r"\b(\d{4})\b", text)
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Volver a Tarjetas", callback_data="menu:cards")]])
                if not m:
                    await context.bot.send_message(chat_id=chat_id, text="❌ Por favor, envía 4 dígitos válidos.", reply_markup=kb)
                    return
                new_card = m.group(1)
                card_id = state["data"]["id"]
                if db_update_user_card(card_id, new_card):
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ Tarjeta actualizada a {new_card}.")
                    await self._cmd_list_cards(update, context)
                else:
                    await context.bot.send_message(chat_id=chat_id, text="❌ Error al actualizar tarjeta.", reply_markup=kb)
                del self._pending_points_action[chat_id]
                return
            
            # Si no coincide con ningún paso conocido, eliminamos el estado para evitar bloqueos
            del self._pending_points_action[chat_id]
            return

        if chat_id not in self._pending_card_query:
            # Si no estamos esperando un dato, mostramos el menú
            await self._cmd_start(update, context)
            return
        
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


    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        logging.info(f"Comando /start recibido del CHAT_ID: {chat_id}")
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Mis Puntos", callback_data="menu:puntos")],
            [InlineKeyboardButton("📜 Historial Reciente", callback_data="menu:historial")],
            [InlineKeyboardButton("💳 Mis Tarjetas", callback_data="menu:cards")],
            [InlineKeyboardButton("📜 Resumen Mensual", callback_data="menu:resumen")],
            [InlineKeyboardButton("📧 Reporte Email", callback_data="menu:email_report")],
            [InlineKeyboardButton("⚙️ Gestión Puntos", callback_data="menu:gestionar")],
            [InlineKeyboardButton("❓ Ayuda", callback_data="menu:ayuda")],
            [InlineKeyboardButton("🛠 Estado", callback_data="menu:status")],
        ])
        
        msg_text = "¡Hola! Soy tu asistente de Puntos Global Bank.\n¿Qué deseas hacer hoy?"
        if chat_id != CHAT_ID:
            msg_text += f"\n\n⚠️ *Nota:* Tu ID es `{chat_id}`. El bot está configurado para `{CHAT_ID}`. Actualiza la variable de entorno `CHAT_ID` para recibir notificaciones automáticas."

        await context.bot.send_message(
            chat_id=chat_id,
            text=msg_text,
            reply_markup=kb,
            parse_mode="Markdown"
        )

    def _help_text(self) -> str:
        return (
            "🤖 Ayuda del Bot\n\n"
            "💰 Mis Puntos: Consulta puntos acumulados por tarjeta.\n"
            "📜 Historial: Ver y validar tus últimas transacciones.\n"
            "📊 Resumen: Reporte mensual de actividad.\n"
            "✅ Validación: Marca transacciones como reconocidas/no reconocidas."
        )

    async def _cmd_resumen(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        s = db_monthly_summary(None)
        txt = (
            f"📅 *Resumen {s['year']}-{s['month']:02d}*\n"
            f"⭐ Puntos: {int(s['points'])}\n"
            f"💵 USD: ${points_to_usd(s['points'])}\n"
            f"🏆 Top comercio: {s['top_company'] or '–'}"
        )
        await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="Markdown")

    async def _cmd_historial(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        txs = db_get_recent_transactions_with_status(limit=5)
        if not txs:
            await context.bot.send_message(chat_id=chat_id, text="📭 No hay transacciones recientes.")
            return
        
        await context.bot.send_message(chat_id=chat_id, text="📜 *Últimas 5 transacciones:*", parse_mode="Markdown")
        
        for tx in txs:
            status_emoji = "✅" if tx['status'] == "RECONOCIDA" else ("❌" if tx['status'] == "NO RECONOCIDA" else "❓")
            date_str = tx['date'].strftime("%Y-%m-%d %H:%M")
            txt = (
                f"{date_str}\n"
                f"🛒 {tx['company']}\n"
                f"💰 ${tx['amount']} (Tarjeta {tx['card']})\n"
                f"Estado: {status_emoji} {tx['status'] or 'Pendiente'}"
            )
            
            kb = None
            if not tx['status']: 
                kb = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Sí", callback_data=f"recdb:{tx['id']}:Y"),
                        InlineKeyboardButton("❌ No", callback_data=f"recdb:{tx['id']}:N"),
                    ]
                ])
            
            await context.bot.send_message(chat_id=chat_id, text=txt, reply_markup=kb)

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        
        # Check DB
        db_ok = False
        try:
            conn = _db_connect()
            if conn:
                conn.close()
                db_ok = True
        except:
            pass
        
        # Check IMAP
        imap_ok = False
        try:
            M = imaplib.IMAP4_SSL("imap.gmail.com")
            M.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            M.logout()
            imap_ok = True
        except Exception as e:
            logging.error(f"IMAP Check failed: {e}")
            pass

        msg = (
            f"🛠 *Estado del Sistema*\n\n"
            f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"👤 Chat ID: `{chat_id}`\n"
            f"⚙️ Configurado: `{CHAT_ID}`\n"
            f"🗄 Base de Datos: {'✅ Conectado' if db_ok else '❌ Error'}\n"
            f"📧 Correo (IMAP): {'✅ Conectado' if imap_ok else '❌ Error'}\n"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")


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
    parser.add_argument("--create-job", action="store_true")
    parser.add_argument("--job-card", type=str)
    parser.add_argument("--job-email", type=str)
    args = parser.parse_args()

    if args.create_job and args.job_card and args.job_email:
        if db_create_summary_job(args.job_card, args.job_email):
            print(f"Job SQL Server creado para tarjeta {args.job_card} enviando a {args.job_email}")
        else:
            print("El Job ya existe o hubo un error al crearlo.")
        return

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
        return True
    except Exception:
        return False
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

def db_ensure_unrecognized_table() -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.UnrecognizedTransactions','U') IS NULL
            CREATE TABLE dbo.UnrecognizedTransactions(
            Id INT IDENTITY(1,1) PRIMARY KEY,
            Token NVARCHAR(128) NOT NULL,
            Company NVARCHAR(256) NOT NULL,
            AmountUSD DECIMAL(18,2) NOT NULL,
            CardLast4 CHAR(4) NOT NULL,
            ReportedAt DATETIME NOT NULL
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

def db_ensure_user_cards_table() -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.UserCards','U') IS NULL
            CREATE TABLE dbo.UserCards(
                Id INT IDENTITY(1,1) PRIMARY KEY,
                ChatId BIGINT NOT NULL,
                CardLast4 CHAR(4) NOT NULL,
                Alias NVARCHAR(50) NULL,
                CreatedAt DATETIME DEFAULT GETDATE()
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

def db_add_user_card(chat_id: int, card_last4: str, alias: str = None) -> bool:
    conn = _db_connect()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        # Check if exists
        cur.execute("SELECT 1 FROM dbo.UserCards WHERE ChatId=? AND CardLast4=?", chat_id, card_last4)
        if cur.fetchone():
            return False
        cur.execute(
            "INSERT INTO dbo.UserCards (ChatId, CardLast4, Alias) VALUES (?, ?, ?)",
            chat_id, card_last4, alias
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_get_user_cards(chat_id: int) -> List[Dict[str, Any]]:
    conn = _db_connect()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT Id, CardLast4, Alias FROM dbo.UserCards WHERE ChatId=?", chat_id)
        rows = cur.fetchall()
        return [{"id": r[0], "card": r[1], "alias": r[2]} for r in rows]
    except Exception:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_update_user_card(card_id: int, new_last4: str) -> bool:
    conn = _db_connect()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("UPDATE dbo.UserCards SET CardLast4=? WHERE Id=?", new_last4, card_id)
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_delete_user_card(card_id: int) -> bool:
    conn = _db_connect()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.UserCards WHERE Id=?", card_id)
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_get_all_user_cards_map() -> Dict[str, List[int]]:
    conn = _db_connect()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT CardLast4, ChatId FROM dbo.UserCards")
        rows = cur.fetchall()
        mapping = {}
        for r in rows:
            card = r[0]
            chat_id = r[1]
            if card not in mapping:
                mapping[card] = []
            mapping[card].append(chat_id)
        return mapping
    except Exception:
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_trigger_summary_job() -> Tuple[bool, str]:
    conn = _db_connect()
    if not conn:
        return False, "Error de conexión"
    try:
        cur = conn.cursor()
        # Verificar si existe el job "Enviar resumen mensual puntos" (Usuario) o "GlobalPointsMonthlySummary" (Bot)
        job_name = 'Enviar resumen mensual puntos'
        cur.execute("SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?", job_name)
        if not cur.fetchone():
            job_name = 'GlobalPointsMonthlySummary'
            cur.execute("SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?", job_name)
            if not cur.fetchone():
                return False, "No se encontró ningún Job de resumen mensual configurado."
            
        cur.execute("EXEC msdb.dbo.sp_start_job @job_name = ?", job_name)
        return True, f"Job '{job_name}' iniciado correctamente."
    except Exception as e:
        return False, str(e)
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_insert_unrecognized(token: str, company: str, amount_usd: float, card_last4: str, dt: datetime) -> None:
    conn = _db_connect()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.UnrecognizedTransactions(Token, Company, AmountUSD, CardLast4, ReportedAt)
            VALUES (?, ?, ?, ?, ?)
            """,
            token,
            company,
            float(amount_usd),
            card_last4,
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

def db_get_transaction_by_id(tx_id: int) -> Dict[str, Any] | None:
    conn = _db_connect()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT Company, AmountUSD, CardLast4
            FROM dbo.Transactions
            WHERE Id = ?
            """,
            tx_id,
        )
        r = cur.fetchone()
        if r:
            return {"company": r[0], "amount": float(r[1]), "card": r[2]}
        return None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_get_recent_transactions_with_status(limit: int = 5) -> List[Dict[str, Any]]:
    conn = _db_connect()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        # Use Outer Apply or Subquery to get status
        cur.execute(
            """
            SELECT TOP (?)
                T.Id, T.Company, T.AmountUSD, T.CardLast4, T.TransactionAt,
                (SELECT TOP 1 Status FROM dbo.TransactionRecognitions R
                 WHERE R.Company = T.Company
                   AND R.AmountUSD = T.AmountUSD
                   AND R.CardLast4 = T.CardLast4
                   AND ABS(DATEDIFF(minute, R.RecognizedAt, GETDATE())) < 43200 -- 30 days lookup window or similar? actually better just match fields
                 ORDER BY R.RecognizedAt DESC) as Status
            FROM dbo.Transactions T
            ORDER BY T.TransactionAt DESC
            """,
            limit,
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0],
                "company": r[1],
                "amount": float(r[2]),
                "card": r[3],
                "date": r[4],
                "status": r[5],
            })
        return out
    except Exception:
        return []
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
                -- Report on the previous month since this runs on the 1st
                DECLARE @Dt DATE = DATEADD(month, -1, GETDATE());
                DECLARE @Year INT = YEAR(@Dt);
                DECLARE @Month INT = MONTH(@Dt);
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
                    ORDER BY P DESC
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

def db_create_summary_job(card_last4: str, send_to: str, profile_name: str = 'DefaultProfile') -> bool:
    conn = _db_connect()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT job_id FROM msdb.dbo.sysjobs WHERE name = 'GlobalPointsMonthlySummary'")
        r = cur.fetchone()
        if r:
            logging.info("El Job 'GlobalPointsMonthlySummary' ya existe. Omitiendo creación.")
            return False
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
        return True
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
