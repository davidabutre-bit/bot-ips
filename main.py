from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import os
import re
import time

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
TARGET_CHANNEL = "@CoutipsIPS"

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

API_ID = int(API_ID)

client = TelegramClient("coutips_ips_session", API_ID, API_HASH)

CORTES = {
    "IPS HT": 70,
    "IPS FT": 71,
    "CHAMA 3.0": 74,
    "ARCE HT": 70,
}

PRIORIDADE = {
    "ARCE HT": ["IPS HT"],
    "CHAMA 3.0": ["IPS FT"],
}

COOLDOWN_SEGUNDOS = 600
CACHE_MAX_SEGUNDOS = 3600
ultimos_jogos = {}


def normalizar(texto):
    return texto.replace("*", "").replace("_", "").replace("**", "").strip()


def limpar_linha(texto):
    return re.sub(r"\s+", " ", texto).strip()


def pegar_numero(pattern, texto, padrao=0):
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return padrao
    try:
        return int(m.group(1))
    except Exception:
        return padrao


def pegar_par(pattern, texto):
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return 0, 0
    try:
        return int(m.group(1)), int(m.group(2))
    except Exception:
        return 0, 0


def extrair_link_bet365(texto):
    m = re.search(r"https?://(?:www\.)?bet365[^\s*]+", texto, re.IGNORECASE)
    if m:
        return m.group(0).strip()
    return ""


def detectar_estrategia(texto):
    t = texto.upper()

    if "ARCE" in t:
        return "ARCE HT"

    if "CHAMA" in t:
        return "CHAMA 3.0"

    if "HT" in t or "1ºT" in t or "1T" in t or "INTERVALO" in t:
        return "IPS HT"

    return "IPS FT"


def mensagem_valida(texto):
    t = texto.upper()
    if "ALERTA ESTRATÉGIA" not in t and "ALERTA ESTRATEGIA" not in t:
        return False
    if "JOGO:" not in t:
        return False
    if "TEMPO:" not in t:
        return False
    return True


def extrair_jogo(texto):
    texto = normalizar(texto)

    m = re.search(r"Jogo:\s*(.+)", texto, re.IGNORECASE)
    if m:
        linha = m.group(1).split("\n")[0]
        return limpar_linha(linha)

    for linha in texto.splitlines():
        if " x " in linha.lower() or " vs " in linha.lower():
            return limpar_linha(linha)

    return "Jogo não identificado"


def extrair_tempo(texto):
    return pegar_numero(r"Tempo:\s*(\d+)", texto, 0)


def extrair_resultado(texto):
    m = re.search(r"Resultado:\s*([0-9]+)\s*x\s*([0-9]+)", texto, re.IGNORECASE)
    if not m:
        return "Placar não identificado"
    return f"{m.group(1)} x {m.group(2)}"


def extrair_odds(texto):
    m = re.search(
        r"Odds.*?:\s*([0-9.]+)\s*/\s*([0-9.]+)\s*/\s*([0-9.]+)",
        texto,
        re.IGNORECASE,
    )
    if not m:
        return 0, 0, 0
    try:
        return float(m.group(1)), float(m.group(2)), float(m.group(3))
    except Exception:
        return 0, 0, 0


def extrair_metricas(texto):
    texto_limpo = normalizar(texto)

    tempo = extrair_tempo(texto_limpo)
    placar = extrair_resultado(texto_limpo)

    ataques_perigosos = pegar_par(r"Ataques Perigosos:\s*(\d+)\s*-\s*(\d+)", texto_limpo)
    ataques = pegar_par(r"Ataques:\s*(\d+)\s*-\s*(\d+)", texto_limpo)
    cantos = pegar_par(r"Cantos:\s*(\d+)\s*-\s*(\d+)", texto_limpo)
    posse = pegar_par(r"Posse bola:\s*(\d+)\s*-\s*(\d+)", texto_limpo)
    remates_baliza = pegar_par(r"Remates Baliza:\s*(\d+)\s*-\s*(\d+)", texto_limpo)
    remates_lado = pegar_par(r"Remates lado:\s*(\d+)\s*-\s*(\d+)", texto_limpo)
    vermelhos = pegar_par(r"Cartões vermelhos:\s*(\d+)\s*-\s*(\d+)", texto_limpo)

    odds = extrair_odds(texto_limpo)

    ultimos5 = pegar_par(r"Ultimos 5':\s*(\d+).*?-\s*(\d+)", texto_limpo)
    ultimos10 = pegar_par(r"Ultimos 10':\s*(\d+).*?-\s*(\d+)", texto_limpo)

    ultimo_gol = pegar_numero(r"Último golo:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Ultimo golo:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Último gol:\s*(\d+)", texto_limpo, 0)

    return {
        "tempo": tempo,
        "placar": placar,
        "ataques_perigosos": ataques_perigosos,
        "ataques": ataques,
        "cantos": cantos,
        "posse": posse,
        "remates_baliza": remates_baliza,
        "remates_lado": remates_lado,
        "vermelhos": vermelhos,
        "odds": odds,
        "ultimos5": ultimos5,
        "ultimos10": ultimos10,
        "ultimo_gol": ultimo_gol,
        "bet365": extrair_link_bet365(texto_limpo),
    }


def favorito_score(metricas):
    odd_casa, _, odd_fora = metricas["odds"]

    bonus = 0

    if odd_casa and odd_casa <= 1.30:
        bonus += 9
    elif odd_casa and odd_casa <= 1.45:
        bonus += 7
    elif odd_casa and odd_casa <= 1.55:
        bonus += 5

    if odd_fora and odd_fora <= 1.50:
        bonus += 7

    return bonus


def dominio_score(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    cantos_casa, cantos_fora = metricas["cantos"]
    posse_casa, posse_fora = metricas["posse"]

    score = 0

    if ap_casa + ap_fora >= 20:
        score += 5
    if abs(ap_casa - ap_fora) >= 8:
        score += 7
    if abs(ap_casa - ap_fora) >= 15:
        score += 5

    if rb_casa + rb_fora >= 4:
        score += 8
    if abs(rb_casa - rb_fora) >= 3:
        score += 6

    if cantos_casa + cantos_fora >= 5:
        score += 5
    if abs(cantos_casa - cantos_fora) >= 3:
        score += 4

    if abs(posse_casa - posse_fora) >= 12:
        score += 4

    return score


def momentum_score(metricas):
    u5_casa, u5_fora = metricas["ultimos5"]
    u10_casa, u10_fora = metricas["ultimos10"]

    score = 0

    if u5_casa + u5_fora >= 4:
        score += 5
    if u10_casa + u10_fora >= 8:
        score += 7

    if abs(u10_casa - u10_fora) >= 4:
        score += 5

    return score


def relogio_score(metricas, estrategia):
    tempo = metricas["tempo"]
    score = 0

    if estrategia in ["IPS HT", "ARCE HT"]:
        if 22 <= tempo <= 35:
            score += 8
        elif 36 <= tempo <= 45:
            score += 3
        elif tempo < 18:
            score -= 8

    else:
        if 65 <= tempo <= 76:
            score += 9
        elif 77 <= tempo <= 82:
            score += 3
        elif tempo >= 83:
            score -= 10

    return score


def risco_gol_recente(metricas):
    tempo = metricas["tempo"]
    ultimo_gol = metricas["ultimo_gol"]

    if ultimo_gol <= 0:
        return 0

    diferenca = tempo - ultimo_gol

    if diferenca <= 2:
        return -12
    if diferenca <= 5:
        return -6

    return 0


def fake_pressure_penalty(metricas):
    ap_total = sum(metricas["ataques_perigosos"])
    rb_total = sum(metricas["remates_baliza"])

    if ap_total >= 25 and rb_total <= 1:
        return -12

    if ap_total >= 18 and rb_total <= 0:
        return -15

    return 0


def calcular_score(texto, estrategia):
    metricas = extrair_metricas(texto)

    score = 50

    score += dominio_score(metricas)
    score += momentum_score(metricas)
    score += relogio_score(metricas, estrategia)
    score += risco_gol_recente(metricas)
    score += fake_pressure_penalty(metricas)

    if estrategia == "IPS HT":
        score += favorito_score(metricas) * 0.8
        score += 3

    elif estrategia == "IPS FT":
        score += 2

    elif estrategia == "CHAMA 3.0":
        score += favorito_score(metricas) * 0.7
        score += 8

    elif estrategia == "ARCE HT":
        score += favorito_score(metricas)
        score += 8

    return int(max(0, min(score, 95))), metricas


def limpar_cache():
    agora = time.time()
    for chave in list(ultimos_jogos.keys()):
        if agora - ultimos_jogos[chave] > CACHE_MAX_SEGUNDOS:
            del ultimos_jogos[chave]


def verificar_prioridade(jogo, estrategia):
    limpar_cache()
    agora = time.time()

    for bot_premium, bots_bloqueados in PRIORIDADE.items():
        if estrategia in bots_bloqueados:
            chave_premium = f"{jogo}_{bot_premium}"
            if chave_premium in ultimos_jogos:
                if agora - ultimos_jogos[chave_premium] <= COOLDOWN_SEGUNDOS:
                    return False, f"Bloqueado por prioridade: {bot_premium}"

    chave_atual = f"{jogo}_{estrategia}"

    if chave_atual in ultimos_jogos:
        if agora - ultimos_jogos[chave_atual] <= COOLDOWN_SEGUNDOS:
            return False, "Bloqueado por duplicidade do mesmo bot"

    ultimos_jogos[chave_atual] = agora

    return True, "OK"


def faixa_score(score, estrategia):
    if estrategia == "CHAMA 3.0":
        if score >= 92:
            return "DIAMANTE / ELITE"
        if score >= 85:
            return "PREMIUM"
        return "BOA"

    if estrategia == "ARCE HT":
        if score >= 89:
            return "ELITE HT"
        if score >= 82:
            return "PREMIUM HT"
        return "BOA HT"

    if estrategia == "IPS HT":
        if score >= 88:
            return "ELITE HT"
        if score >= 81:
            return "PREMIUM HT"
        return "BOA HT"

    if score >= 90:
        return "ELITE FT"
    if score >= 84:
        return "PREMIUM FT"
    return "BOA FT"


def montar_mensagem(jogo, estrategia, score, metricas):
    categoria = faixa_score(score, estrategia)

    link_bet365 = ""
    if metricas.get("bet365"):
        link_bet365 = f"\n🔗 Bet365: {metricas['bet365']}"

    return f"""🚀 ENTRADA COUTIPS

⚽ Jogo: {jogo}
⏱ Tempo: {metricas['tempo']}'
📊 Placar: {metricas['placar']}

🎯 Mercado: Over 0.5 Gol
📌 Estratégia: {estrategia}
🔥 Chance COUTIPS de Gol: {score}%
🏆 Classificação: {categoria}

✅ Leitura aprovada por pressão ofensiva contextual.
📈 Critérios: intensidade, domínio, finalização, relógio e contexto.

📌 Entrar somente se a odd estiver acima de 1.60.
💰 Gestão recomendada: 1% da banca.
⛔ Não entrar se saiu gol nos últimos 2–3 minutos.{link_bet365}

COUTIPS — leitura ao vivo com pressão, contexto e disciplina."""


@client.on(events.NewMessage(chats="CornerProBot"))
async def handler(event):
    if event.out:
        return

    texto = event.raw_text or ""

    if not texto.strip():
        return

    if not mensagem_valida(texto):
        print("ℹ️ Mensagem ignorada: não parece alerta CornerPro.")
        return

    try:
        print("\n📩 NOVA MENSAGEM RECEBIDA:")
        print(texto)

        estrategia = detectar_estrategia(texto)
        jogo = extrair_jogo(texto)

        score, metricas = calcular_score(texto, estrategia)
        corte = CORTES.get(estrategia, 71)

        print(f"🎯 Estratégia detectada: {estrategia}")
        print(f"⚽ Jogo: {jogo}")
        print(f"📊 Score calculado: {score}%")
        print(f"📌 Corte mínimo: {corte}%")

        permitido, motivo = verificar_prioridade(jogo, estrategia)

        if not permitido:
            print(f"⛔ BLOQUEADO: {motivo}")
            return

        if score < corte:
            print("⛔ BLOQUEADO: score abaixo do corte")
            return

        mensagem_final = montar_mensagem(jogo, estrategia, score, metricas)

        try:
            await client.send_message(TARGET_CHANNEL, mensagem_final)
            print("✅ MENSAGEM ENVIADA PARA O CANAL")

        except FloodWaitError as e:
            print(f"⛔ FLOOD WAIT: aguardar {e.seconds} segundos.")

        except Exception as e:
            print(f"❌ ERRO AO ENVIAR MENSAGEM: {e}")

    except Exception as e:
        print(f"❌ ERRO NO PROCESSAMENTO DO ALERTA: {e}")


print("🚀 COUTIPS ONLINE - SCORE CONTEXTUAL ATIVO")
print("📊 Estratégias ativas: IPS FT | IPS HT | CHAMA 3.0 | ARCE HT")
print(f"📡 Canal destino: {TARGET_CHANNEL}")

client.start()

print("✅ TELEGRAM CONECTADO COM SUCESSO")

client.run_until_disconnected()