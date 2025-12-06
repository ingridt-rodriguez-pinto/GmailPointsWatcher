import os
import time
import requests

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8428323516:AAElzjmSPfUeCoTGKbm7wnDLSZ5ek1-6Gvc")

def get_updates() -> dict:
    r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", timeout=15)
    return r.json()


def send_message(chat_id: int, text: str) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
    except Exception:
        pass


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN no configurado")
    print("Envía un mensaje al bot y espera 10s...")
    for _ in range(10):
        data = get_updates()
        if isinstance(data, dict) and data.get("ok"):
            for u in data.get("result", []):
                msg = u.get("message") or u.get("edited_message")
                if msg and "chat" in msg:
                    cid = msg["chat"]["id"]
                    print(f"CHAT_ID: {cid}")
                    send_message(cid, f"CHAT_ID: {cid}")
                    return
        time.sleep(1)
    print("No se encontraron updates. Envía un mensaje al bot y vuelve a intentar.")

if __name__ == "__main__":
    main()
