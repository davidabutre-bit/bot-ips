import os
from dotenv import load_dotenv
from telethon import TelegramClient

# =========================
# CARREGA .ENV
# =========================

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

SOURCE_CHAT = os.getenv("SOURCE_CHAT")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")

# =========================
# VALIDAÇÃO
# =========================

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

# =========================
# CLIENT TELEGRAM
# =========================

client = TelegramClient(
    "coutips_ips_session",
    int(API_ID),
    API_HASH
)

# =========================
# START
# =========================

print("🚀 COUTIPS ONLINE - SCORE CONTEXTUAL ATIVO")
print("📊 Estratégias ativas: IPS FT | IPS HT | CHAMA 3.0 | ARCE HT")
print(f"📡 Canal destino: {TARGET_CHANNEL}")

client.start()

print("✅ TELEGRAM CONECTADO COM SUCESSO")

client.run_until_disconnected()