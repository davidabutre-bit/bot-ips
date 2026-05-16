from telethon.sync import TelegramClient

api_id = 36525640
api_hash = "25bfdf0065ba1025cd97c226076d69b6"

SESSION_NAME = "coutips_v2_session.session"

client = TelegramClient(
    SESSION_NAME,
    api_id,
    api_hash,
    device_model="COUTIPS VPS",
    system_version="ALFA 3.0",
    app_version="GOAT ENGINE",
)

client.start()

print("SESSION GERADA COM SUCESSO")
