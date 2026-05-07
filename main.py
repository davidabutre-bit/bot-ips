from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel
import os
import re

# =========================
# CONFIGURAÇÕES
# =========================

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

# SESSÃO REAL DA SUA CONTA TELEGRAM
SESSION_NAME = "corner_sessao"

# CANAL DESTINO
CANAL_DESTINO = "@CoutipsIPS"

# =========================
# CLIENT TELEGRAM
# =========================

client = TelegramClient(
    SESSION_NAME,
    API_ID,
    API_HASH
)

print("🚀 COUTIPS IPS ONLINE - USERBOT")
print(f"📤 Canal destino: {CANAL_DESTINO}")

# =========================
# FUNÇÃO SCORE HT
# =========================

def analisar_ht(msg):

    try:

        tempo_match = re.search(r"Tempo:\s*(\d+)", msg)
        if not tempo_match:
            return None

        tempo = int(tempo_match.group(1))

        if tempo < 25 or tempo > 45:
            return None

        ataques_10 = re.search(r"Ultimos 10':.*?(\d+).*?(\d+)", msg)

        if not ataques_10:
            return None

        casa10 = int(ataques_10.group(1))
        fora10 = int(ataques_10.group(2))

        score = max(casa10, fora10) * 7

        if score < 70:
            print(f"[HT] BLOQUEADO ({score}%)")
            return None

        jogo = re.search(r"Jogo:\s*(.+)", msg)
        resultado = re.search(r"Resultado:\s*(.+)", msg)

        jogo = jogo.group(1) if jogo else "Jogo"
        resultado = resultado.group(1) if resultado else "0x0"

        texto = f"""
📊 COUTIPS IPS HT

🏟 {jogo}

⏱ Tempo: {tempo}'
⚽ Placar: {resultado}

🎯 Chance de GOL: {score}%

✅ ENTRADA HT

📌 Mercado:
Over HT

📤 Canal oficial:
{CANAL_DESTINO}
"""

        return texto

    except Exception as e:
        print(f"ERRO HT: {e}")
        return None

# =========================
# FUNÇÃO SCORE FT
# =========================

def analisar_ft(msg):

    try:

        tempo_match = re.search(r"Tempo:\s*(\d+)", msg)
        if not tempo_match:
            return None

        tempo = int(tempo_match.group(1))

        if tempo < 65 or tempo > 75:
            return None

        ultimo_gol = re.search(r"Último golo:\s*(\d+)", msg)

        if ultimo_gol:

            minuto_gol = int(ultimo_gol.group(1))

            if minuto_gol >= 55:
                print("[FT] BLOQUEADO GOL RECENTE")
                return None

        ataques_10 = re.search(r"Ultimos 10':.*?(\d+).*?(\d+)", msg)

        if not ataques_10:
            return None

        casa10 = int(ataques_10.group(1))
        fora10 = int(ataques_10.group(2))

        score = max(casa10, fora10) * 8

        if score < 80:
            print(f"[FT] BLOQUEADO ({score}%)")
            return None

        jogo = re.search(r"Jogo:\s*(.+)", msg)
        resultado = re.search(r"Resultado:\s*(.+)", msg)
        competicao = re.search(r"Competição:\s*(.+)", msg)

        jogo = jogo.group(1) if jogo else "Jogo"
        resultado = resultado.group(1) if resultado else "0x0"
        competicao = competicao.group(1) if competicao else "Liga"

        texto = f"""
📊 COUTIPS IPS FT

🏟 {jogo}
🏆 {competicao}

⏱ Tempo: {tempo}'
⚽ Placar: {resultado}

🎯 Chance de GOL: {score}%

✅ ENTRADA PREMIUM

📌 Mercado:
Over 1.5 FT

📌 Odd mínima:
1.75+

⚠ Gestão:
1% da banca

🚨 APOSTE COM RESPONSABILIDADE
"""

        return texto

    except Exception as e:
        print(f"ERRO FT: {e}")
        return None

# =========================
# EVENTO NOVA MENSAGEM
# =========================

@client.on(events.NewMessage)
async def handler(event):

    try:

        texto = event.raw_text

        if "IPS PÓS 70 TESTE" in texto:

            print("📥 ALERTA FT RECEBIDO")

            resposta = analisar_ft(texto)

            if resposta:

                await client.send_message(
                    CANAL_DESTINO,
                    resposta
                )

                print("✅ FT ENVIADO")

        elif "BOT IPS HT TESTE" in texto:

            print("📥 ALERTA HT RECEBIDO")

            resposta = analisar_ht(texto)

            if resposta:

                await client.send_message(
                    CANAL_DESTINO,
                    resposta
                )

                print("✅ HT ENVIADO")

    except Exception as e:
        print(f"ERRO GERAL: {e}")

# =========================
# INICIAR BOT
# =========================

client.start()
client.run_until_disconnected()