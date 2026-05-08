from telethon.sync import TelegramClient

api_id = 36525640
api_hash = "25bfdf0065ba1025cd97c226076d69b6"

client = TelegramClient("coutips_ips_session", api_id, api_hash)

client.start()

print("SESSAO CRIADA COM SUCESSO")

client.disconnect()