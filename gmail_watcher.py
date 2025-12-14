import imaplib
import email
import re
import time
import os
import requests
from email.utils import parseaddr
from dotenv import load_dotenv

from db_client import GlobalPointsDB
from logger_helper import AppLogger

# Configuración
load_dotenv()
log = AppLogger("GmailWatcher")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
IMAP_SERVER = "imap.gmail.com"

# Lista blanca de remitentes (bancos)
ALLOWED_SENDERS = [
    "contactenos@globalbank.com.pa",
    "ingridt.r.pinto@gmail.com" 
]

def enviar_telegram(chat_id, mensaje, botones=None):
    """Envía notificación a Telegram."""
    if not chat_id or not TELEGRAM_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}
    if botones: payload["reply_markup"] = botones
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Error Telegram: {e}")

def crear_botones_configuracion(transaction_id, action_type):
    """Genera botones de configuración."""
    keyboard = {"inline_keyboard": []}
    if action_type in ['ASK_MULT', 'ASK_BOTH']:
        keyboard["inline_keyboard"].append([
            {"text": "1x", "callback_data": f"cfg|{transaction_id}|mult|1.0"},
            {"text": "2x", "callback_data": f"cfg|{transaction_id}|mult|2.0"},
            {"text": "3x", "callback_data": f"cfg|{transaction_id}|mult|3.0"}
        ])
    if action_type in ['ASK_CAT', 'ASK_BOTH']:
        keyboard["inline_keyboard"].append([
            {"text": "Comida", "callback_data": f"cfg|{transaction_id}|cat|Comida"},
            {"text": "Transporte", "callback_data": f"cfg|{transaction_id}|cat|Transporte"},
            {"text": "Super", "callback_data": f"cfg|{transaction_id}|cat|Supermercado"}
        ])
        keyboard["inline_keyboard"].append([
            {"text": "Servicios", "callback_data": f"cfg|{transaction_id}|cat|Servicios"},
            {"text": "General", "callback_data": f"cfg|{transaction_id}|cat|General"}
        ])
    return keyboard

def extraer_datos_regex(cuerpo_texto):
    """Extrae datos del correo."""
    texto = cuerpo_texto.replace('\r', '').replace('\n', ' ')
    
    patron_comercio = r" en\s+(.*?)\s+con tarjeta"
    patron_monto = r"\$\s?([\d\.,]+)"
    patron_tarjeta = r"terminaci[óo]n\s+(\w{4})"
    patron_banco = r"(Global Bank|Bac Credomatic|Banco General)"

    match_comercio = re.search(patron_comercio, texto, re.IGNORECASE)
    match_monto = re.search(patron_monto, texto)
    match_tarjeta = re.search(patron_tarjeta, texto, re.IGNORECASE)
    match_banco = re.search(patron_banco, texto, re.IGNORECASE)

    if match_comercio and match_monto and match_tarjeta:
        return {
            "comercio": match_comercio.group(1).strip(),
            "monto": float(match_monto.group(1).replace(',', '')),
            "last4": match_tarjeta.group(1),
            "banco": match_banco.group(1) if match_banco else "Global Bank"
        }
    return None

def procesar_cuenta(db, account):
    """
    Se conecta al Gmail de un usuario específico y procesa sus correos.
    dict {user_id, chat_id, email, password}
    """
    email_addr = account['email']
    pwd = account['password']
    
    try:
        
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(email_addr, pwd)
        mail.select("inbox")
        
        log.info('Login a correo existoso')
        # Buscar correos no leídos
        status, messages = mail.search(None, '(UNSEEN SUBJECT "CONFIRMACION")')
        email_ids = messages[0].split()

        if email_ids:
            log.info(f"[{email_addr}] Detectados {len(email_ids)} correos nuevos.")

        for e_id in email_ids:
            try:
                _, msg_data = mail.fetch(e_id, "(RFC822)")
                msg = email.message_from_bytes(msg_data[0][1])
                
                # Filtro Remitente
                remitente_raw = msg.get('From')
                remitente_email = parseaddr(remitente_raw)[1]
                
                es_valido = any(s.lower() in remitente_email.lower() for s in ALLOWED_SENDERS)
                if not es_valido:
                    continue

                # Extraer cuerpo
                cuerpo = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            cuerpo = part.get_payload(decode=True).decode(errors='ignore')
                            break
                else:
                    cuerpo = msg.get_payload(decode=True).decode(errors='ignore')

                # Procesar Datos
                datos = extraer_datos_regex(cuerpo)
                if datos:
                    log.info(f"[{email_addr}] Compra: {datos['comercio']} (${datos['monto']})")
                    
                    # Guardar en BD (Usamos el user_id de la cuenta actual)
                    res = db.process_transaction(
                        app_user_id=account['user_id'],
                        merchant_text=datos['comercio'],
                        card_last4=datos['last4'],
                        bank_name=datos['banco'],
                        amount=datos['monto']
                    )

                    if res:
                        botones = None
                        if res['bot_action'] != 'AUTO':
                            botones = crear_botones_configuracion(res['transaction_id'], res['bot_action'])
                        # Enviamos al Chat ID de esta cuenta
                        enviar_telegram(account['chat_id'], f"💳 {res['message']}", botones)

            except Exception as e:
                log.error(f"[{email_addr}] Error leyendo correo {e_id}: {e}")

        mail.close()
        mail.logout()

    except imaplib.IMAP4.error:
        log.warning(f"[{email_addr}] Error de autenticación. Posible contraseña cambiada.")
    except Exception as e:
        log.error(f"[{email_addr}] Error de conexión: {e}")

def main():
    log.info("Iniciando lector de correo...")
    
    try:
        db = GlobalPointsDB()
    except Exception as e:
        log.error(f"Error crítico conectando a BD: {e}")
        return

    while True:
        try:
            # 1. Obtener todas las cuentas a monitorear desde la BD
            cuentas = db.get_all_monitored_accounts()
            
            if not cuentas:
                log.info("No hay cuentas activas para monitorear. Esperando...")
            
            # 2. Iterar y procesar cada una
            for cuenta in cuentas:
                log.info(f"Revisando buzón de: {cuenta['email']}")
                procesar_cuenta(db, cuenta)
                
        except Exception as e:
            log.error(f"Error en el ciclo principal: {e}")
        
        time.sleep(45)

if __name__ == "__main__":
    main()