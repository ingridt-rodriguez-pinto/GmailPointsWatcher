import pyodbc
import os
from dotenv import load_dotenv
from logger_helper import AppLogger

# Cargar variables de entorno
load_dotenv()

class GlobalPointsDB:
    def __init__(self):
        self.log = AppLogger("DB_Client")
        
        server = os.getenv("MSSQL_SERVER")
        database = os.getenv("MSSQL_DATABASE")
        user = os.getenv("MSSQL_USER")
        password = os.getenv("MSSQL_PASSWORD")

        if not all([server, database, user, password]):
            self.log.error("Faltan variables de entorno MSSQL_ en el archivo .env")
            raise ValueError("Configuración de base de datos incompleta.")

        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
        )

    def _get_cursor(self):
        try:
            conn = pyodbc.connect(self.connection_string, autocommit=False)
            return conn, conn.cursor()
        except Exception as e:
            self.log.error(f"Error conectando a SQL Server: {e}")
            raise

    def get_user_data_by_email(self, email):
        """Busca ID y ChatID dado un email."""
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            sql = """
            SELECT u.UserId, u.TelegramChatId 
            FROM dbo.AppUsers u
            INNER JOIN dbo.EmailCredentials c ON u.Id = c.AppUserId
            WHERE c.Email = ?
            """
            cursor.execute(sql, (email,))
            row = cursor.fetchone()
            if row:
                return {"user_id": row[0], "chat_id": row[1]}
            return None
        except Exception as e:
            self.log.error(f"Error buscando usuario: {e}")
            return None
        finally:
            if conn: conn.close()

    def register_user_credentials(self, telegram_chat_id, email, raw_password):
        """Registra usuario y contraseña usando el SP de registro"""
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            # Llamada al SP que maneja la encriptación y lógica IF/ELSE
            sql = "EXEC dbo.sp_RegisterUserCredentials @TelegramChatId = ?, @Email = ?, @RawPassword = ?"
            cursor.execute(sql, (telegram_chat_id, email, raw_password))
            conn.commit()
            self.log.info(f"Credenciales registradas para ChatID {telegram_chat_id}")
            return True
        except Exception as e:
            if conn: conn.rollback()
            self.log.error(f"Error en registro: {e}")
            return False
        finally:
            if conn: conn.close()

    def process_transaction(self, app_user_id, merchant_text, card_last4, bank_name, amount):
        """Procesa la transacción nueva."""
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            sql = """
            DECLARE @OutId INT, @OutAction VARCHAR(20), @OutMsg NVARCHAR(MAX);
            EXEC dbo.sp_InsertTransactionFromEmail
                @AppUserId = ?, @RawComercioTexto = ?, @CardLast4 = ?, 
                @BankName = ?, @AmountUSD = ?,
                @TransactionId = @OutId OUTPUT, @BotAction = @OutAction OUTPUT, @MessageText = @OutMsg OUTPUT;
            SELECT @OutId as id, @OutAction as action, @OutMsg as msg;
            """
            cursor.execute(sql, (app_user_id, merchant_text, card_last4, bank_name, amount))
            row = cursor.fetchone()
            conn.commit()
            if row:
                return {"transaction_id": row.id, "bot_action": row.action, "message": row.msg}
            return None
        except Exception as e:
            if conn: conn.rollback()
            self.log.error(f"Error procesando transacción: {e}")
            return None
        finally:
            if conn: conn.close()

    def complete_configuration(self, transaction_id, multiplier=None, category_name=None):
        """Actualiza la configuración (llamado por los botones del Bot)."""
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            sql = "EXEC dbo.sp_CompletarConfiguracion @TransactionId = ?, @SelectedMultiplier = ?, @SelectedCategoryName = ?"
            cursor.execute(sql, (transaction_id, multiplier, category_name))
            conn.commit()
            return True
        except Exception as e:
            if conn: conn.rollback()
            self.log.error(f"Error configurando: {e}")
            return False
        finally:
            if conn: conn.close()

    def get_recent_transactions(self, chat_id, limit=5):
        """Obtiene las últimas N transacciones para mostrarlas en Telegram."""
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            sql = """
            SELECT TOP (?) 
                 t.Id, 
                m.Name AS Comercio, 
                t.AmountUSD, 
                t.Points, 
                t.Multiplicador,
                t.TransactionAt
            FROM dbo.Transactions t
            INNER JOIN dbo.Comercio m ON t.ComercioId = m.Id
            INNER JOIN dbo.UserCards c ON t.UserCardId = c.Id
            INNER JOIN dbo.AppUsers u ON c.AppUserId = u.UserId
            WHERE u.TelegramChatId = ?
            ORDER BY t.TransactionAt DESC
            """
            cursor.execute(sql, (limit, chat_id))
            rows = cursor.fetchall()
            self.log.info(rows)
            results = []
            for r in rows:
                results.append({
                    "id": r.Id,
                    "comercio": r.Comercio,
                    "monto": float(r.AmountUSD),
                    "puntos": r.Points,
                    "multiplicador": float(r.Multiplicador) if r.Multiplicador else 1.0,
                    "fecha": r.TransactionAt.strftime("%d/%m %H:%M")
                })
            return results
        except Exception as e:
            self.log.error(f"Error obteniendo recientes: {e}")
            return []
        finally:
            if conn: conn.close()

    def get_user_cards(self, chat_id):
        """Lista las tarjetas del usuario."""
        """
        Lista las tarjetas del usuario traduciendo primero el ChatId a AppUserId.
        """
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            
            # Buscamos el ID real usando el ChatId de Telegram
            sql = """
            DECLARE @UID INT;
            SELECT @UID = UserId FROM dbo.AppUsers WHERE TelegramChatId = ?;
            
            IF @UID IS NOT NULL
                EXEC dbo.sp_ListUserCards @AppUserId = @UID;
            """
            
            cursor.execute(sql, (chat_id,))
            rows = cursor.fetchall()
            
            cards = []
            for r in rows:
                # Tu SP devuelve: Id, Bank, CardLast4, Alias, FechaRegistro
                cards.append({
                    "id": r.Id,
                    "banco": r.Bank,
                    "last4": r.CardLast4,
                    "alias": r.Alias,
                    "fecha": r.FechaRegistro,
                    # Como tu SP ya no devuelve 'IsActive', asumimos True o lo quitamos
                    "activa": True 
                })
            return cards
        except Exception as e:
            self.log.error(f"Error listando tarjetas: {e}")
            return []
        finally:
            if conn: conn.close()

    def get_monthly_summary(self, chat_id):
        """Obtiene estadísticas del mes actual."""
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            sql = "EXEC dbo.sp_MonthlyPointsSummary @TelegramChatId = ?"
            cursor.execute(sql, (chat_id,))
            row = cursor.fetchone()
            if row:
                return {
                    "total_usd": float(row.TotalUSD),
                    "total_points": row.TotalPoints,
                    "count": row.TxCount,
                    "top_category": row.TopCategory,
                    "month_name": row.MonthName
                }
            return None
        except Exception as e:
            self.log.error(f"Error en resumen mensual: {e}")
            return None
        finally:
            if conn: conn.close()

    def get_all_monitored_accounts(self):
        """
        Recupera TODOS los usuarios activos con sus credenciales desencriptadas.
        """
        conn, cursor = None, None
        try:
            conn, cursor = self._get_cursor()
            
            # Script SQL para desencriptar y leer
            sql = """
            OPEN SYMMETRIC KEY EmailCredsKey DECRYPTION BY CERTIFICATE EmailCredsCert;

            SELECT 
                u.UserId AS UserId,
                u.TelegramChatId,
                c.Email,
                CONVERT(NVARCHAR(MAX), DecryptByKey(c.PasswordEncrypted)) AS DecryptedPass
            FROM dbo.AppUsers u
            INNER JOIN dbo.EmailCredentials c ON u.UserId = c.AppUserId
            WHERE u.IsActive = 1;

            CLOSE SYMMETRIC KEY EmailCredsKey;
            """
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            accounts = []
            for r in rows:
                if r.Email and r.DecryptedPass:
                    accounts.append({
                        "user_id": r.UserId,
                        "chat_id": r.TelegramChatId,
                        "email": r.Email,
                        "password": r.DecryptedPass
                    })
            return accounts

        except Exception as e:
            self.log.error(f"Error obteniendo cuentas para monitorear: {e}")
            return []
        finally:
            if conn: conn.close()