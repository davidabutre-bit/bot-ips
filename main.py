from telethon import TelegramClient, events
import os
import re
import time

# =========================
# CONFIGURAÇÕES
# =========================

API_ID_RAW = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")

if not API_ID_RAW or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado no Railway.")

API_ID = int(API_ID_RAW)

SESSION_NAME = "corner_sessao"
CANAL_DESTINO = os.getenv("TARGET_CHANNEL", "@CoutipsIPS")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

mensagens_processadas = {}
TEMPO_DUPLICADO_SEGUNDOS = 300


# =========================
# FUNÇÕES BÁSICAS
# =========================

def limpar(texto):
    if not texto:
        return ""
    return texto.replace("*", "").replace("\r", "").strip()


def normalizar(texto):
    return limpar(texto).lower()


def extrair_numero(pattern, texto, default=0):
    m = re.search(pattern, texto, re.I | re.S)
    if not m:
        return default
    try:
        return float(m.group(1).replace(",", "."))
    except Exception:
        return default


def extrair_jogo(texto):
    m = re.search(r"Jogo:\s*(.*?)(?:Competição:|🏆|$)", texto, re.I | re.S)
    return m.group(1).strip() if m else "Jogo não identificado"


def extrair_liga(texto):
    m = re.search(r"Competição:\s*(.*?)(?:Tempo:|🕛|$)", texto, re.I | re.S)
    return m.group(1).strip() if m else "Liga não identificada"


def extrair_tempo(texto):
    return int(extrair_numero(r"Tempo:\s*(\d+)", texto, 0))


def extrair_placar(texto):
    m = re.search(r"Resultado:\s*(\d+)\s*x\s*(\d+)", texto, re.I)
    if not m:
        return 0, 0, "0 x 0"
    casa = int(m.group(1))
    fora = int(m.group(2))
    return casa, fora, f"{casa} x {fora}"


def extrair_ultimos(texto, minutos):
    m = re.search(rf"Ultimos {minutos}':\s*(\d+).*?-\s*(\d+)", texto, re.I | re.S)
    if not m:
        return 0, 0
    return int(m.group(1)), int(m.group(2))


def extrair_total(nome, texto):
    m = re.search(rf"{nome}:\s*(\d+)\s*-\s*(\d+)", texto, re.I)
    if not m:
        return 0, 0
    return int(m.group(1)), int(m.group(2))


def extrair_ultimo_gol(texto):
    m = re.search(r"(?:Último|Ultimo) golo:\s*'?(\d+)", texto, re.I)
    return int(m.group(1)) if m else None


def extrair_bet365(texto):
    links = re.findall(r"https://[^\s*]+", texto)
    for link in links:
        if "bet365" in link.lower():
            return link
    return "Link não identificado"


def extrair_odds(texto):
    m = re.search(
        r"Odds 1x2 Pre-live:\s*([\d.]+)\s*/\s*([\d.]+)\s*/\s*([\d.]+)",
        texto,
        re.I
    )
    if not m:
        return 0.0, 0.0, 0.0
    return float(m.group(1)), float(m.group(2)), float(m.group(3))


def pressao_stats(texto):
    m = re.search(r"Índice de Pressão:(.*?)(?:Ataques:|https|$)", texto, re.I | re.S)
    if not m:
        m = re.search(r"Indice de Pressao:(.*?)(?:Ataques:|https|$)", texto, re.I | re.S)

    if not m:
        return {"p18": 0, "p20": 0, "p25": 0, "p30": 0, "max": 0}

    valores = []
    for parte in m.group(1).split(";"):
        nums = [x.strip() for x in parte.split(",") if x.strip()]
        if len(nums) == 2:
            try:
                valores.append(float(nums[0].replace(",", ".")))
                valores.append(float(nums[1].replace(",", ".")))
            except Exception:
                pass

    return {
        "p18": sum(1 for v in valores if v >= 18),
        "p20": sum(1 for v in valores if v >= 20),
        "p25": sum(1 for v in valores if v >= 25),
        "p30": sum(1 for v in valores if v >= 30),
        "max": max(valores) if valores else 0
    }


def bonus_liga(texto):
    liga = extrair_liga(texto).lower()

    boas = [
        "finland", "kolmonen", "kakkonen", "reserve", "res.",
        "u20", "u23", "women", "norway", "latvia", "estonia",
        "czech", "georgia"
    ]

    risco = [
        "algeria", "bolivia", "peru", "paraguay"
    ]

    if any(x in liga for x in boas):
        return 5

    if any(x in liga for x in risco):
        return -5

    return 0


def converter_probabilidade(score_bruto):
    if score_bruto <= 0:
        return 0

    if score_bruto < 40:
        return int(score_bruto)

    if score_bruto < 55:
        return int(45 + (score_bruto - 40) * 0.6)

    if score_bruto < 70:
        return int(56 + (score_bruto - 55) * 0.75)

    if score_bruto < 85:
        return int(68 + (score_bruto - 70) * 0.9)

    if score_bruto < 105:
        return int(82 + (score_bruto - 85) * 0.45)

    return 95


def mercado(casa, fora, modo):
    total = casa + fora
    return f"Over {total + 0.5:.1f} {modo}"


def odd_minima(tempo, modo):
    if modo == "HT":
        if tempo <= 30:
            return "1.75+"
        if tempo <= 36:
            return "1.60+"
        return "1.50+"

    if tempo <= 74:
        return "1.60+"
    if tempo <= 79:
        return "1.75+"
    if tempo <= 83:
        return "1.90+"
    return "2.00+"


def veredito(chance):
    if chance >= 85:
        return "✅ ENTRADA PREMIUM"
    if chance >= 75:
        return "✅ ENTRADA FORTE"
    if chance >= 68:
        return "⚠️ ENTRADA MODERADA"
    return "❌ NÃO ENTRAR"


def favorito_forte(texto):
    odd_casa, _, odd_fora = extrair_odds(texto)
    return (odd_casa and odd_casa <= 1.60) or (odd_fora and odd_fora <= 1.60)


def favorito_eh_casa(texto):
    odd_casa, _, odd_fora = extrair_odds(texto)
    if not odd_casa or not odd_fora:
        return None
    return odd_casa < odd_fora


# =========================
# SCORE HT OFICIAL
# =========================

def score_ht(texto):
    tempo = extrair_tempo(texto)
    casa, fora, _ = extrair_placar(texto)
    total_gols = casa + fora
    ultimo_gol = extrair_ultimo_gol(texto)

    u5_casa, u5_fora = extrair_ultimos(texto, 5)
    u10_casa, u10_fora = extrair_ultimos(texto, 10)

    ap_casa, ap_fora = extrair_total("Ataques Perigosos", texto)
    cantos_casa, cantos_fora = extrair_total("Cantos", texto)
    rb_casa, rb_fora = extrair_total("Remates Baliza", texto)
    verm_casa, verm_fora = extrair_total("Cartões vermelhos", texto)

    pressao = pressao_stats(texto)

    total_u5 = u5_casa + u5_fora
    total_u10 = u10_casa + u10_fora
    total_ap = ap_casa + ap_fora
    total_rb = rb_casa + rb_fora
    total_cantos = cantos_casa + cantos_fora

    score = 0

    if pressao["p20"] >= 3:
        score += 32
    elif pressao["p18"] >= 3:
        score += 20

    if pressao["p25"] >= 2:
        score += 12
    if pressao["p30"] >= 1:
        score += 8

    if total_u5 >= 8:
        score += 26
    elif total_u5 >= 6:
        score += 19
    elif total_u5 >= 4:
        score += 11

    if total_u10 >= 16:
        score += 21
    elif total_u10 >= 10:
        score += 15
    elif total_u10 >= 7:
        score += 9

    if 25 <= tempo <= 34:
        score += 13
    elif 35 <= tempo <= 40:
        score += 16
    elif 41 <= tempo <= 45:
        score += 8
    else:
        score -= 8

    if total_gols == 0:
        score += 15
    elif total_gols == 1:
        score += 11
    elif total_gols == 2:
        score -= 4
    else:
        score -= 16

    if total_rb >= 5:
        score += 10
    elif total_rb >= 3:
        score += 7

    if total_cantos >= 5:
        score += 5
    elif total_cantos >= 3:
        score += 3

    if total_ap >= 35:
        score += 8
    elif total_ap >= 25:
        score += 5

    if favorito_forte(texto):
        score += 10

    if verm_casa > 0 or verm_fora > 0:
        score += 12

    if ultimo_gol is not None:
        diff = tempo - ultimo_gol
        if 0 <= diff <= 3:
            score -= 25
        elif diff <= 6:
            score -= 12
        elif diff >= 12 and total_u10 >= 10:
            score += 5

    score += bonus_liga(texto)

    return converter_probabilidade(score)


# =========================
# SCORE FT OFICIAL
# =========================

def score_ft(texto):
    tempo = extrair_tempo(texto)
    casa, fora, _ = extrair_placar(texto)
    diff_gols = abs(casa - fora)
    ultimo_gol = extrair_ultimo_gol(texto)

    u5_casa, u5_fora = extrair_ultimos(texto, 5)
    u10_casa, u10_fora = extrair_ultimos(texto, 10)

    ap_casa, ap_fora = extrair_total("Ataques Perigosos", texto)
    cantos_casa, cantos_fora = extrair_total("Cantos", texto)
    rb_casa, rb_fora = extrair_total("Remates Baliza", texto)

    pressao = pressao_stats(texto)

    total_u5 = u5_casa + u5_fora
    total_u10 = u10_casa + u10_fora
    total_ap = ap_casa + ap_fora
    total_cantos = cantos_casa + cantos_fora
    total_rb = rb_casa + rb_fora

    score = 0

    if pressao["p20"] >= 3:
        score += 30
    elif pressao["p18"] >= 3:
        score += 18

    if pressao["p25"] >= 2:
        score += 10
    if pressao["p30"] >= 1:
        score += 7

    if total_u5 >= 12:
        score += 32
    elif total_u5 >= 8:
        score += 27
    elif total_u5 >= 6:
        score += 20
    elif total_u5 >= 4:
        score += 11

    if total_u10 >= 18:
        score += 28
    elif total_u10 >= 14:
        score += 24
    elif total_u10 >= 10:
        score += 17
    elif total_u10 >= 7:
        score += 9

    if 65 <= tempo <= 74:
        score += 16
    elif 75 <= tempo <= 79:
        score += 11
    elif 80 <= tempo <= 83:
        score += 5
    else:
        score -= 10

    if diff_gols == 0:
        score += 20
    elif diff_gols == 1:
        score += 16
    elif diff_gols == 2:
        if total_u10 >= 10:
            score += 7
        else:
            score -= 8
    else:
        if total_u10 >= 12 or total_u5 >= 7:
            score += 5
        else:
            score -= 20

    if ultimo_gol is not None:
        diff = tempo - ultimo_gol
        if 0 <= diff <= 3:
            score -= 30
        elif diff <= 6:
            score -= 17
        elif diff <= 8:
            score -= 8
        elif diff >= 15 and total_u10 >= 10:
            score += 6

    if total_ap >= 100:
        score += 11
    elif total_ap >= 70:
        score += 7
    elif total_ap >= 50:
        score += 4

    if total_cantos >= 10:
        score += 7
    elif total_cantos >= 6:
        score += 4

    if total_rb >= 8:
        score += 7
    elif total_rb >= 5:
        score += 5
    elif total_rb >= 3:
        score += 2

    # Correção importante:
    # favorito forte + 0x0/placar curto + pressão recente viva pós-70
    if favorito_forte(texto) and tempo >= 70 and diff_gols <= 1:
        if total_u5 >= 8 and total_u10 >= 14:
            score += 18
        elif total_u5 >= 6 and total_u10 >= 10:
            score += 12

    # Massacre vivo com placar alto não deve ser bloqueado automaticamente
    if diff_gols >= 3 and total_u10 >= 12:
        score += 8

    score += bonus_liga(texto)

    return converter_probabilidade(score)


# =========================
# MENSAGEM FINAL
# =========================

def montar_mensagem(texto, modo, chance):
    jogo = extrair_jogo(texto)
    liga = extrair_liga(texto)
    tempo = extrair_tempo(texto)
    casa, fora, placar = extrair_placar(texto)
    link = extrair_bet365(texto)

    return f"""
📊 COUTIPS IPS {modo}

🏟 {jogo}
🏆 {liga}

⏱ Tempo: {tempo}'
⚽ Placar: {placar}

🎯 Chance de GOL: {chance}%

{veredito(chance)}

📌 Mercado:
{mercado(casa, fora, modo)}

📌 Odd mínima:
{odd_minima(tempo, modo)}

🔗 Bet365:
{link}

⚠️ Gestão:
1% da banca

⛔ APOSTE COM RESPONSABILIDADE ⛔
""".strip()


def chave_mensagem(texto):
    jogo = extrair_jogo(texto)
    tempo = extrair_tempo(texto)
    casa, fora, _ = extrair_placar(texto)
    return f"{jogo}-{tempo}-{casa}-{fora}"


def limpar_duplicados_antigos():
    agora = time.time()
    antigos = [
        chave for chave, timestamp in mensagens_processadas.items()
        if agora - timestamp > TEMPO_DUPLICADO_SEGUNDOS
    ]
    for chave in antigos:
        del mensagens_processadas[chave]


# =========================
# HANDLER TELEGRAM
# =========================

@client.on(events.NewMessage)
async def handler(event):
    texto = limpar(event.raw_text)

    if not texto:
        return

    limpar_duplicados_antigos()

    chave = chave_mensagem(texto)

    if chave in mensagens_processadas:
        print(f"[DUPLICADO] Ignorado: {chave}")
        return

    modo = None

    if "BOT IPS HT TESTE" in texto or "ARCE HT" in texto:
        modo = "HT"
    elif "IPS PÓS 70 TESTE" in texto or "IPS POS 70 TESTE" in texto:
        modo = "FT"

    if not modo:
        return

    mensagens_processadas[chave] = time.time()

    try:
        if modo == "HT":
            print("📥 ALERTA HT RECEBIDO")
            chance = score_ht(texto)
        else:
            print("📥 ALERTA FT RECEBIDO")
            chance = score_ft(texto)

        jogo = extrair_jogo(texto)

        if chance >= 68:
            await client.send_message(CANAL_DESTINO, montar_mensagem(texto, modo, chance))
            print(f"✅ {modo} ENVIADO ({chance}%) | {jogo}")
        else:
            print(f"[{modo}] BLOQUEADO ({chance}%) | {jogo}")

    except Exception as e:
        print(f"[ERRO] Falha ao processar alerta: {e}")


print("🚀 COUTIPS IPS ONLINE - USERBOT OFICIAL")
print(f"📤 Canal destino: {CANAL_DESTINO}")

client.start()
client.run_until_disconnected()