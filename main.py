from telethon import TelegramClient, events
from dotenv import load_dotenv
import os
import re
import time

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
TARGET_CHANNEL = "@CoutipsIPS"

client = TelegramClient("coutips_ips_session", API_ID, API_HASH)

ultimos_jogos = {}

CORTES = {
    "IPS HT": 73,
    "IPS FT": 76,
    "CHAMA 3.0": 78,
    "ARCE HT": 75,
}

PRIORIDADE = {
    "ARCE HT": ["IPS HT"],
    "CHAMA 3.0": ["IPS FT"],
}

def limpar_texto(txt):
    return txt.replace("\n", " ").replace("  ", " ").strip()

def detectar_estrategia(texto):
    t = texto.upper()

    if "ARCE" in t:
        return "ARCE HT"

    if "CHAMA" in t:
        return "CHAMA 3.0"

    if "HT" in t or "1ºT" in t or "1T" in t:
        return "IPS HT"

    return "IPS FT"

def extrair_numero(pattern, texto, padrao=0):
    m = re.search(pattern, texto, re.IGNORECASE)

    if not m:
        return padrao

    try:
        return int(m.group(1))
    except:
        return padrao

def extrair_jogo(texto):
    m = re.search(r"Jogo:\s*(.+)", texto, re.IGNORECASE)

    if m:
        return m.group(1).split("\n")[0].strip()

    linhas = texto.splitlines()

    for linha in linhas:
        if " x " in linha.lower() or " vs " in linha.lower():
            return linha.strip()

    return "Jogo não identificado"

def calcular_score(texto, estrategia):
    tempo = extrair_numero(r"Tempo:\s*(\d+)", texto)

    cantos_total = 0
    remates_baliza_total = 0

    m_cantos = re.search(
        r"Cantos:\s*(\d+)\s*-\s*(\d+)",
        texto,
        re.IGNORECASE
    )

    if m_cantos:
        cantos_total = (
            int(m_cantos.group(1))
            + int(m_cantos.group(2))
        )

    m_rb = re.search(
        r"Remates Baliza:\s*(\d+)\s*-\s*(\d+)",
        texto,
        re.IGNORECASE
    )

    if m_rb:
        remates_baliza_total = (
            int(m_rb.group(1))
            + int(m_rb.group(2))
        )

    score = 60

    if tempo >= 25:
        score += 5

    if tempo >= 65:
        score += 7

    if tempo >= 78:
        score -= 6

    if tempo >= 84:
        score -= 10

    if cantos_total >= 6:
        score += 6

    if cantos_total >= 9:
        score += 5

    if remates_baliza_total >= 5:
        score += 8

    if remates_baliza_total >= 8:
        score += 6

    texto_upper = texto.upper()

    if "PRESSÃO" in texto_upper or "PRESSAO" in texto_upper:
        score += 6

    if "ATAQUES PERIGOSOS" in texto_upper:
        score += 6

    if "ÚLTIMO GOL" in texto_upper or "ULTIMO GOL" in texto_upper:
        score -= 3

    if estrategia == "IPS HT":
        score += 2

    elif estrategia == "IPS FT":
        score += 0

    elif estrategia == "CHAMA 3.0":
        score += 5

    elif estrategia == "ARCE HT":
        score += 4

    return max(0, min(score, 95))

def verificar_prioridade(jogo, estrategia):
    agora = time.time()

    for bot_premium, bots_bloqueados in PRIORIDADE.items():

        if estrategia in bots_bloqueados:

            chave_premium = f"{jogo}_{bot_premium}"

            if chave_premium in ultimos_jogos:

                if agora - ultimos_jogos[chave_premium] <= 600:
                    return False, f"Bloqueado por prioridade: {bot_premium}"

    chave_atual = f"{jogo}_{estrategia}"
    ultimos_jogos[chave_atual] = agora

    return True, "OK"

def montar_mensagem(jogo, estrategia, score):
    mercado = "Over 0.5 Gol"

    return f"""
🚀 ENTRADA COUTIPS

⚽ Jogo: {jogo}
📊 Estratégia: {estrategia}
🎯 Mercado: {mercado}

🔥 Chance COUTIPS de Gol: {score}%

✅ Entrada validada por leitura ofensiva contextual.

📌 Entrar somente se a odd estiver acima de 1.60.
💰 Gestão recomendada: 1% da banca.
⛔ Não entrar se saiu gol nos últimos 2–3 minutos.

COUTIPS — leitura ao vivo com pressão, contexto e disciplina.
"""

@client.on(events.NewMessage)
async def handler(event):

    texto = event.raw_text or ""

    if not texto.strip():
        return

    print("\n📩 NOVA MENSAGEM RECEBIDA:")
    print(texto)

    estrategia = detectar_estrategia(texto)
    jogo = extrair_jogo(texto)

    score = calcular_score(texto, estrategia)

    corte = CORTES.get(estrategia, 76)

    print(f"🎯 Estratégia detectada: {estrategia}")
    print(f"⚽ Jogo: {jogo}")
    print(f"📊 Score calculado: {score}%")
    print(f"📌 Corte mínimo: {corte}%")

    permitido, motivo = verificar_prioridade(
        jogo,
        estrategia
    )

    if not permitido:
        print(f"⛔ BLOQUEADO: {motivo}")
        return

    if score < corte:
        print("⛔ BLOQUEADO: score abaixo do corte")
        return

    mensagem_final = montar_mensagem(
        jogo,
        estrategia,
        score
    )

    await client.send_message(
        TARGET_CHANNEL,
        mensagem_final
    )

    print("✅ MENSAGEM ENVIADA PARA O CANAL")

print("🚀 COUTIPS ONLINE - SCORE CONTEXTUAL ATIVO")
print("📊 Estratégias ativas: IPS FT | IPS HT | CHAMA 3.0 | ARCE HT")
print(f"📡 Canal destino: {TARGET_CHANNEL}")

client.start()

print("✅ TELEGRAM CONECTADO COM SUCESSO")

client.run_until_disconnected()