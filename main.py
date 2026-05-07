import os
import re
from dotenv import load_dotenv
from telethon import TelegramClient, events

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")

client = TelegramClient(
    "coutips_ips_session",
    API_ID,
    API_HASH
).start(bot_token=BOT_TOKEN)


# =========================
# AUXILIARES
# =========================

def extract_value(pattern, text, default=0):

    match = re.search(pattern, text, re.MULTILINE)

    if match:
        try:
            return float(match.group(1).replace(",", "."))
        except:
            return default

    return default


def contains(text, word):
    return word.lower() in text.lower()


def extract_match_name(text):

    match = re.search(
        r"Jogo:\s*\*?(.*?)\*?(?:🏆|Competição:)",
        text,
        re.IGNORECASE | re.DOTALL
    )

    if match:
        return match.group(1).strip()

    return "Jogo"


def extract_bet365_link(text):

    links = re.findall(r"https://[^\s]+", text)

    for link in links:
        if "bet365" in link.lower():
            return link

    return ""


def extract_cornerpro_link(text):

    links = re.findall(r"https://[^\s]+", text)

    for link in links:
        if "cornerprobet" in link.lower():
            return link

    return ""


# =========================
# SCORE HT
# =========================

def calculate_ht_score(text):

    score = 0

    ult5 = extract_value(r"Ultimos 5':\s*(\d+)", text)
    ult10 = extract_value(r"Ultimos 10':\s*(\d+)", text)

    tempo = extract_value(r"Tempo:\s*(\d+)", text)

    if ult5 >= 5:
        score += 22

    if ult10 >= 8:
        score += 28

    if 22 <= tempo <= 35:
        score += 15

    if contains(text, "0 x 0"):
        score += 15

    elif contains(text, "0 x 1"):
        score += 12

    elif contains(text, "1 x 1"):
        score += 10

    elif contains(text, "2 x 0"):
        score -= 12

    elif contains(text, "3 x 0"):
        score -= 25

    if contains(text, "Último golo: 22"):
        score -= 20

    if contains(text, "Último golo: 23"):
        score -= 20

    if contains(text, "Último golo: 24"):
        score -= 20

    if contains(text, "Último golo: 25"):
        score -= 18

    if contains(text, "Último golo: 26"):
        score -= 15

    return max(0, min(score, 100))


# =========================
# SCORE FT
# =========================

def calculate_ft_score(text):

    score = 0

    ult5 = extract_value(r"Ultimos 5':\s*(\d+)", text)
    ult10 = extract_value(r"Ultimos 10':\s*(\d+)", text)

    tempo = extract_value(r"Tempo:\s*(\d+)", text)

    if ult5 >= 4:
        score += 20

    if ult10 >= 7:
        score += 28

    if 68 <= tempo <= 76:
        score += 15

    if contains(text, "1 x 1"):
        score += 20

    elif contains(text, "0 x 0"):
        score += 20

    elif contains(text, "2 x 1"):
        score += 15

    elif contains(text, "1 x 2"):
        score += 15

    elif contains(text, "2 x 0"):
        score -= 18

    elif contains(text, "3 x 0"):
        score -= 35

    elif contains(text, "4 x 1"):
        score -= 40

    if contains(text, "Último golo: 68"):
        score -= 20

    if contains(text, "Último golo: 69"):
        score -= 25

    if contains(text, "Último golo: 70"):
        score -= 30

    return max(0, min(score, 100))


# =========================
# MENSAGEM
# =========================

def build_message(game, score, mode, bet365_link, corner_link):

    verdict = "❌ NÃO ENTRAR"

    if score >= 85:
        verdict = "✅ ENTRADA PREMIUM"

    elif score >= 75:
        verdict = "✅ ENTRADA FORTE"

    elif score >= 68:
        verdict = "⚠️ ENTRADA MODERADA"

    return f"""
📊 COUTIPS IPS {mode}

🏟 {game}

🎯 Chance COUTIPS: {score}%

{verdict}

🔗 Bet365:
{bet365_link}

📈 CornerPro:
{corner_link}

⚠️ Gestão:
1% da banca.
"""


# =========================
# EVENTOS
# =========================

@client.on(events.NewMessage)
async def handler(event):

    text = event.raw_text

    if "BOT IPS HT" in text.upper():

        score = calculate_ht_score(text)

        if score >= 68:

            game = extract_match_name(text)

            bet365_link = extract_bet365_link(text)
            corner_link = extract_cornerpro_link(text)

            message = build_message(
                game,
                score,
                "HT",
                bet365_link,
                corner_link
            )

            await client.send_message(
                TARGET_CHANNEL,
                message
            )

            print(f"[HT] ENVIADO -> {game} ({score}%)")

        else:
            print(f"[HT] BLOQUEADO ({score}%)")


    elif "PÓS 70" in text.upper() or "POS 70" in text.upper():

        score = calculate_ft_score(text)

        if score >= 68:

            game = extract_match_name(text)

            bet365_link = extract_bet365_link(text)
            corner_link = extract_cornerpro_link(text)

            message = build_message(
                game,
                score,
                "FT",
                bet365_link,
                corner_link
            )

            await client.send_message(
                TARGET_CHANNEL,
                message
            )

            print(f"[FT] ENVIADO -> {game} ({score}%)")

        else:
            print(f"[FT] BLOQUEADO ({score}%)")


# =========================
# START
# =========================

print("🚀 COUTIPS IPS ONLINE...")
print(f"📤 Canal: {TARGET_CHANNEL}")

client.run_until_disconnected()