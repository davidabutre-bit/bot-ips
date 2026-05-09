from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import os
import re
import time
import asyncio
import traceback

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "@CoutipsIPS")

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

API_ID = int(API_ID)

client = TelegramClient("coutips_ips_session", API_ID, API_HASH)

CORTES = {
    "HT_PREMIUM": 75,
    "HT_MODERADO": 73,
    "FT_PREMIUM": 78,
    "FT_MODERADO": 76,
}

PRIORIDADE_BOT = {
    "HT_PREMIUM": 4,
    "FT_PREMIUM": 4,
    "HT_MODERADO": 2,
    "FT_MODERADO": 2,
}

COOLDOWN_SEGUNDOS = 600
CACHE_MAX_SEGUNDOS = 3600
JANELA_DECISAO_SEGUNDOS = 4
INTERVALO_ENVIO_SEGUNDOS = 1.8
WATCHDOG_SEGUNDOS = 60

ultimos_enviados = {}
pendentes_por_jogo = {}
tarefas_decisao = {}
mensagens_processadas = {}
fila_envio = asyncio.Queue()
tarefa_envio = None


# =========================================================
# FUNÇÕES BASE
# =========================================================

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def normalizar(texto):
    return texto.replace("*", "").replace("_", "").replace("**", "").strip()


def limpar_linha(texto):
    return re.sub(r"\s+", " ", texto).strip()


def normalizar_chave_jogo(jogo):
    jogo = jogo.lower()
    jogo = re.sub(r"\([^)]*\)", "", jogo)
    jogo = re.sub(r"[^a-z0-9À-ÿ]+", " ", jogo)
    jogo = re.sub(r"\s+", " ", jogo).strip()
    return jogo


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


def pegar_float_par(pattern, texto):
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return 0.0, 0.0
    try:
        return float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))
    except Exception:
        return 0.0, 0.0


def extrair_link_bet365(texto):
    m = re.search(r"https?://(?:www\.)?bet365[^\s*]+", texto, re.IGNORECASE)
    if m:
        return m.group(0).strip()
    return ""


def limpar_memoria_interna():
    agora = time.time()

    for chave in list(ultimos_enviados.keys()):
        if agora - ultimos_enviados[chave] > CACHE_MAX_SEGUNDOS:
            del ultimos_enviados[chave]

    for chave in list(mensagens_processadas.keys()):
        if agora - mensagens_processadas[chave] > CACHE_MAX_SEGUNDOS:
            del mensagens_processadas[chave]

    for chave in list(pendentes_por_jogo.keys()):
        alertas = pendentes_por_jogo.get(chave, [])
        if not alertas:
            pendentes_por_jogo.pop(chave, None)
            continue

        mais_recente = max(a.get("recebido_em", 0) for a in alertas)
        if agora - mais_recente > 120:
            pendentes_por_jogo.pop(chave, None)
            tarefas_decisao.pop(chave, None)


# =========================================================
# PARSER
# =========================================================

def detectar_estrategia(texto):
    t = texto.upper()

    if "HT_PREMIUM" in t or "HT_PREMIUN" in t or "ARCE" in t:
        return "HT_PREMIUM"

    if "HT_MODERADO" in t or "COLTHT" in t or "IPS HT" in t:
        return "HT_MODERADO"

    if "FT_PREMIUM" in t or "FT_PREMIUN" in t or "CHAMA" in t:
        return "FT_PREMIUM"

    if "FT_MODERADO" in t or "IPS FT" in t or "PÓS-70" in t or "POS-70" in t:
        return "FT_MODERADO"

    if "1ºT" in t or "1T" in t or "INTERVALO" in t:
        return "HT_MODERADO"

    return "FT_MODERADO"


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


def extrair_gols_placar(placar):
    m = re.search(r"([0-9]+)\s*x\s*([0-9]+)", placar)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def mercado_dinamico(placar):
    gols_casa, gols_fora = extrair_gols_placar(placar)

    if gols_casa is None or gols_fora is None:
        return "Over 0.5 Gol"

    total = gols_casa + gols_fora
    linha = total + 0.5
    return f"Over {linha:.1f} Gol"


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

    ultimos5 = pegar_par(
        r"(?:Ultimos|Últimos)\s*5['’]?:\s*(\d+)\s*\([^)]*\)\s*-\s*(\d+)",
        texto_limpo,
    )
    ultimos10 = pegar_par(
        r"(?:Ultimos|Últimos)\s*10['’]?:\s*(\d+)\s*\([^)]*\)\s*-\s*(\d+)",
        texto_limpo,
    )

    ultimo_gol = pegar_numero(r"Último golo:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Ultimo golo:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Último gol:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Ultimo gol:\s*(\d+)", texto_limpo, 0)

    chance_golo = pegar_par(r"Chance de Golo:Casa=(\d+)\s*/\s*Fora=(\d+)", texto_limpo)
    heatmap = pegar_par(r"heatmapFull:Casa=(\d+)\s*/\s*Fora=(\d+)", texto_limpo)
    xg = pegar_float_par(r"xg:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", texto_limpo)
    xgl = pegar_float_par(r"xgl:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", texto_limpo)

    mercado = mercado_dinamico(placar)

    return {
        "tempo": tempo,
        "placar": placar,
        "mercado": mercado,
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
        "chance_golo": chance_golo,
        "heatmap": heatmap,
        "xg": xg,
        "xgl": xgl,
        "bet365": extrair_link_bet365(texto_limpo),
    }


# =========================================================
# SCORE COUTIPS
# =========================================================

def favorito_score(metricas):
    odd_casa, _, odd_fora = metricas["odds"]

    bonus = 0

    if odd_casa and odd_casa <= 1.30:
        bonus += 8
    elif odd_casa and odd_casa <= 1.45:
        bonus += 6
    elif odd_casa and odd_casa <= 1.60:
        bonus += 4

    if odd_fora and odd_fora <= 1.45:
        bonus += 6
    elif odd_fora and odd_fora <= 1.60:
        bonus += 4

    return bonus


def dominio_score(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    cantos_casa, cantos_fora = metricas["cantos"]
    ataques_casa, ataques_fora = metricas["ataques"]
    heat_casa, heat_fora = metricas.get("heatmap", (0, 0))
    chance_casa, chance_fora = metricas.get("chance_golo", (0, 0))
    xg_casa, xg_fora = metricas.get("xg", (0.0, 0.0))

    ap_total = ap_casa + ap_fora
    rb_total = rb_casa + rb_fora
    cantos_total = cantos_casa + cantos_fora
    ataques_total = ataques_casa + ataques_fora
    xg_total = xg_casa + xg_fora

    score = 0

    if ap_total >= 18:
        score += 3
    if ap_total >= 25:
        score += 3
    if ap_total >= 35:
        score += 3

    if abs(ap_casa - ap_fora) >= 10:
        score += 4
    if abs(ap_casa - ap_fora) >= 18:
        score += 4

    if ataques_total >= 70:
        score += 2
    if ataques_total >= 95:
        score += 2

    if rb_total >= 3:
        score += 4
    if rb_total >= 5:
        score += 4

    if abs(rb_casa - rb_fora) >= 3:
        score += 4

    if cantos_total >= 5:
        score += 3
    if cantos_total >= 8:
        score += 2

    if abs(cantos_casa - cantos_fora) >= 3:
        score += 2

    if abs(heat_casa - heat_fora) >= 25:
        score += 3

    if abs(chance_casa - chance_fora) >= 5:
        score += 4

    if xg_total >= 0.55:
        score += 3
    if xg_total >= 1.00:
        score += 3

    return score


def momentum_score(metricas):
    u5_casa, u5_fora = metricas["ultimos5"]
    u10_casa, u10_fora = metricas["ultimos10"]

    u5_total = u5_casa + u5_fora
    u10_total = u10_casa + u10_fora

    score = 0

    if u5_total >= 3:
        score += 3
    if u5_total >= 5:
        score += 3

    if u10_total >= 7:
        score += 4
    if u10_total >= 10:
        score += 3

    if abs(u10_casa - u10_fora) >= 4:
        score += 3

    if abs(u5_casa - u5_fora) >= 3:
        score += 2

    return score


def relogio_score(metricas, estrategia):
    tempo = metricas["tempo"]

    if estrategia in ["HT_PREMIUM", "HT_MODERADO"]:
        if 25 <= tempo <= 35:
            return 8
        if 36 <= tempo <= 42:
            return 4
        if 18 <= tempo <= 24:
            return 2
        if tempo < 18:
            return -10
        return -4

    if estrategia in ["FT_PREMIUM", "FT_MODERADO"]:
        if 68 <= tempo <= 76:
            return 8
        if 77 <= tempo <= 80:
            return 3
        if 81 <= tempo <= 83:
            return -4
        if tempo >= 84:
            return -12
        if tempo < 65:
            return -6
        return 2

    return 0


def risco_gol_recente(metricas):
    tempo = metricas["tempo"]
    ultimo_gol = metricas["ultimo_gol"]

    if ultimo_gol <= 0:
        return 0

    diferenca = tempo - ultimo_gol

    if diferenca < 0:
        return -10

    if diferenca <= 2:
        return -18

    if diferenca <= 5:
        return -9

    return 0


def fake_pressure_penalty(metricas):
    ap_total = sum(metricas["ataques_perigosos"])
    rb_total = sum(metricas["remates_baliza"])
    remates_lado_total = sum(metricas["remates_lado"])
    u5_total = sum(metricas["ultimos5"])
    u10_total = sum(metricas["ultimos10"])
    xg_total = sum(metricas.get("xg", (0.0, 0.0)))

    penalidade = 0

    if ap_total >= 25 and rb_total <= 1:
        penalidade -= 10

    if ap_total >= 18 and rb_total == 0:
        penalidade -= 12

    if ap_total >= 25 and remates_lado_total <= 2 and rb_total <= 1:
        penalidade -= 6

    if u10_total <= 3 and u5_total <= 1:
        penalidade -= 7

    if ap_total >= 25 and xg_total <= 0.20:
        penalidade -= 8

    return penalidade


def score_por_tipo_bot(metricas, estrategia):
    score = 42

    score += dominio_score(metricas)
    score += momentum_score(metricas)
    score += relogio_score(metricas, estrategia)
    score += risco_gol_recente(metricas)
    score += fake_pressure_penalty(metricas)

    fav = favorito_score(metricas)

    if estrategia == "HT_PREMIUM":
        score += fav
        score += 7

    elif estrategia == "HT_MODERADO":
        score += int(fav * 0.7)
        score += 4

    elif estrategia == "FT_PREMIUM":
        score += int(fav * 0.6)
        score += 7

    elif estrategia == "FT_MODERADO":
        score += int(fav * 0.4)
        score += 4

    return score


def ajuste_confianca(metricas, estrategia, score):
    rb_total = sum(metricas["remates_baliza"])
    ap_total = sum(metricas["ataques_perigosos"])
    u10_total = sum(metricas["ultimos10"])
    xg_total = sum(metricas.get("xg", (0.0, 0.0)))

    if score >= 88 and rb_total <= 1:
        score -= 8

    if estrategia in ["FT_PREMIUM", "FT_MODERADO"]:
        if metricas["tempo"] >= 81 and sum(metricas["ultimos5"]) <= 2:
            score -= 8
        if rb_total == 0:
            score -= 7

    if estrategia in ["HT_PREMIUM", "FT_PREMIUM"]:
        if ap_total < 18:
            score -= 5
        if u10_total < 6:
            score -= 5

    if estrategia in ["HT_MODERADO", "FT_MODERADO"]:
        if u10_total <= 3:
            score -= 6

    if xg_total <= 0.15 and rb_total <= 1:
        score -= 7

    return score


def calcular_forca_premium(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    ataques_casa, ataques_fora = metricas["ataques"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    u5_casa, u5_fora = metricas["ultimos5"]
    u10_casa, u10_fora = metricas["ultimos10"]
    chance_casa, chance_fora = metricas.get("chance_golo", (0, 0))
    heat_casa, heat_fora = metricas.get("heatmap", (0, 0))
    xg_casa, xg_fora = metricas.get("xg", (0.0, 0.0))

    pontos = 0

    if abs(ap_casa - ap_fora) >= 14:
        pontos += 1

    if abs(ataques_casa - ataques_fora) >= 18:
        pontos += 1

    if abs(rb_casa - rb_fora) >= 3 and (rb_casa + rb_fora) >= 3:
        pontos += 1

    if abs(u10_casa - u10_fora) >= 6 and (u10_casa + u10_fora) >= 8:
        pontos += 1

    if abs(u5_casa - u5_fora) >= 4:
        pontos += 1

    if abs(chance_casa - chance_fora) >= 5:
        pontos += 1

    if (xg_casa + xg_fora) >= 0.55 and abs(xg_casa - xg_fora) >= 0.35:
        pontos += 1

    if abs(heat_casa - heat_fora) >= 22:
        pontos += 1

    return pontos


def teto_contextual(metricas, estrategia, score):
    tempo = metricas["tempo"]
    placar = metricas["placar"]

    ap_casa, ap_fora = metricas["ataques_perigosos"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    cantos_casa, cantos_fora = metricas["cantos"]
    u10_casa, u10_fora = metricas["ultimos10"]
    xg_casa, xg_fora = metricas.get("xg", (0.0, 0.0))

    rb_total = rb_casa + rb_fora
    u10_total = u10_casa + u10_fora
    xg_total = xg_casa + xg_fora

    dif_ap = abs(ap_casa - ap_fora)
    dif_rb = abs(rb_casa - rb_fora)
    dif_cantos = abs(cantos_casa - cantos_fora)

    gols_casa, gols_fora = extrair_gols_placar(placar)
    diferenca_placar = 0

    if gols_casa is not None and gols_fora is not None:
        diferenca_placar = abs(gols_casa - gols_fora)

    ultimo_gol = metricas["ultimo_gol"]
    minutos_desde_gol = 999

    if ultimo_gol > 0:
        minutos_desde_gol = tempo - ultimo_gol

    forca_premium = calcular_forca_premium(metricas)

    premium_real = forca_premium >= 5 and minutos_desde_gol > 3
    massacre_forte = forca_premium >= 4
    jogo_bom = u10_total >= 8 or dif_ap >= 10 or rb_total >= 3 or xg_total >= 0.45

    if minutos_desde_gol < 0:
        score = min(score, 82)
    elif minutos_desde_gol <= 2:
        score = min(score, 78)
    elif minutos_desde_gol <= 5:
        score = min(score, 84)

    if rb_total <= 1:
        score = min(score, 82)

    if dif_ap <= 5 and dif_rb <= 1 and dif_cantos <= 1:
        score = min(score, 81)

    if estrategia == "HT_MODERADO":
        if premium_real:
            score = min(score, 87)
        elif jogo_bom:
            score = min(score, 84)
        else:
            score = min(score, 79)

    elif estrategia == "FT_MODERADO":
        if diferenca_placar >= 3:
            score = min(score, 82)
        elif tempo >= 80:
            score = min(score, 81)
        elif premium_real:
            score = min(score, 86)
        elif jogo_bom:
            score = min(score, 84)
        else:
            score = min(score, 79)

    elif estrategia == "HT_PREMIUM":
        if premium_real:
            score = min(score, 94)
        elif massacre_forte:
            score = min(score, 90)
        else:
            score = min(score, 86)

    elif estrategia == "FT_PREMIUM":
        if premium_real and tempo <= 80 and diferenca_placar <= 2:
            score = min(score, 94)
        elif premium_real:
            score = min(score, 89)
        elif jogo_bom:
            score = min(score, 86)
        else:
            score = min(score, 82)

    return score


def calcular_score(texto, estrategia):
    metricas = extrair_metricas(texto)

    score = score_por_tipo_bot(metricas, estrategia)
    score = ajuste_confianca(metricas, estrategia, score)
    score = teto_contextual(metricas, estrategia, score)

    score = int(max(0, min(score, 95)))

    return score, metricas


# =========================================================
# CACHE / PRIORIDADE / DECISÃO
# =========================================================

def melhor_alerta(alertas):
    return sorted(
        alertas,
        key=lambda a: (
            PRIORIDADE_BOT.get(a["estrategia"], 0),
            a["score"],
            a["metricas"]["tempo"],
        ),
        reverse=True,
    )[0]


def ja_enviado_recentemente(chave_jogo):
    limpar_memoria_interna()
    agora = time.time()

    if chave_jogo in ultimos_enviados:
        if agora - ultimos_enviados[chave_jogo] <= COOLDOWN_SEGUNDOS:
            return True

    return False


def marcar_enviado(chave_jogo):
    ultimos_enviados[chave_jogo] = time.time()


# =========================================================
# MENSAGEM FINAL
# =========================================================

def faixa_score(score, estrategia):
    if estrategia == "HT_PREMIUM":
        if score >= 90:
            return "ELITE HT"
        if score >= 82:
            return "PREMIUM HT"
        return "BOA HT"

    if estrategia == "HT_MODERADO":
        if score >= 86:
            return "MUITO FORTE HT"
        if score >= 81:
            return "FORTE HT"
        return "BOA HT"

    if estrategia == "FT_PREMIUM":
        if score >= 92:
            return "DIAMANTE / ELITE"
        if score >= 85:
            return "PREMIUM FT"
        return "BOA FT"

    if estrategia == "FT_MODERADO":
        if score >= 86:
            return "MUITO FORTE FT"
        if score >= 81:
            return "FORTE FT"
        return "BOA FT"

    return "BOA"


def nome_publico_estrategia(estrategia):
    nomes = {
        "HT_PREMIUM": "HT PREMIUM",
        "HT_MODERADO": "HT MODERADO",
        "FT_PREMIUM": "FT PREMIUM",
        "FT_MODERADO": "FT MODERADO",
    }

    return nomes.get(estrategia, estrategia)


def montar_mensagem(jogo, estrategia, score, metricas):
    categoria = faixa_score(score, estrategia)
    nome_estrategia = nome_publico_estrategia(estrategia)

    link_bet365 = ""
    if metricas.get("bet365"):
        link_bet365 = f"\n🔗 Bet365: {metricas['bet365']}"

    return f"""🚀 ENTRADA COUTIPS

⚽ Jogo: {jogo}
⏱ Tempo: {metricas['tempo']}'
📊 Placar: {metricas['placar']}

🎯 Mercado: {metricas['mercado']}
📌 Estratégia: {nome_estrategia}
🔥 Chance COUTIPS de Gol: {score}%
🏆 Classificação: {categoria}

✅ Leitura aprovada por pressão ofensiva contextual.
📈 Critérios: intensidade, domínio, finalização, relógio e contexto.

📌 Entrar somente se a odd estiver acima de 1.60.
💰 Gestão recomendada: 1% da banca.
⛔ Não entrar se saiu gol nos últimos 2–3 minutos.{link_bet365}

COUTIPS — leitura ao vivo com pressão, contexto e disciplina."""


# =========================================================
# ENVIO COM FILA
# =========================================================

async def trabalhador_envio():
    log("📤 Fila de envio iniciada.")

    while True:
        alerta = await fila_envio.get()

        try:
            mensagem = montar_mensagem(
                alerta["jogo"],
                alerta["estrategia"],
                alerta["score"],
                alerta["metricas"],
            )

            await client.send_message(TARGET_CHANNEL, mensagem)
            marcar_enviado(alerta["chave_jogo"])

            log(
                f"✅ ENVIADO | {alerta['estrategia']} | "
                f"{alerta['score']}% | {alerta['jogo']}"
            )

            await asyncio.sleep(INTERVALO_ENVIO_SEGUNDOS)

        except FloodWaitError as e:
            log(f"⛔ FLOOD WAIT: aguardando {e.seconds} segundos.")
            await asyncio.sleep(e.seconds + 1)

            try:
                mensagem = montar_mensagem(
                    alerta["jogo"],
                    alerta["estrategia"],
                    alerta["score"],
                    alerta["metricas"],
                )

                await client.send_message(TARGET_CHANNEL, mensagem)
                marcar_enviado(alerta["chave_jogo"])

                log(
                    f"✅ ENVIADO APÓS FLOODWAIT | {alerta['estrategia']} | "
                    f"{alerta['score']}% | {alerta['jogo']}"
                )

            except Exception as e2:
                log(f"❌ ERRO APÓS FLOODWAIT: {e2}")
                log(traceback.format_exc())

        except Exception as e:
            log(f"❌ ERRO AO ENVIAR MENSAGEM: {e}")
            log(traceback.format_exc())

        finally:
            fila_envio.task_done()


async def decidir_e_enviar(chave_jogo):
    try:
        await asyncio.sleep(JANELA_DECISAO_SEGUNDOS)

        alertas = pendentes_por_jogo.pop(chave_jogo, [])
        tarefas_decisao.pop(chave_jogo, None)

        if not alertas:
            log(f"ℹ️ Nenhum alerta pendente para {chave_jogo}")
            return

        escolhido = melhor_alerta(alertas)

        if ja_enviado_recentemente(chave_jogo):
            log(
                f"⛔ BLOQUEADO POR COOLDOWN | {escolhido['estrategia']} | "
                f"{escolhido['jogo']}"
            )
            return

        if len(alertas) > 1:
            estrategias = ", ".join([a["estrategia"] for a in alertas])
            log(
                f"🏆 PRIORIDADE APLICADA | Escolhido: {escolhido['estrategia']} | "
                f"Recebidos: {estrategias} | Jogo: {escolhido['jogo']}"
            )

        await fila_envio.put(escolhido)

    except Exception as e:
        log(f"❌ ERRO NA JANELA DE DECISÃO: {e}")
        log(traceback.format_exc())


async def watchdog_envio():
    global tarefa_envio

    while True:
        await asyncio.sleep(WATCHDOG_SEGUNDOS)

        try:
            limpar_memoria_interna()

            if tarefa_envio is None or tarefa_envio.done() or tarefa_envio.cancelled():
                log("⚠️ Watchdog: fila de envio parada. Reiniciando trabalhador.")
                tarefa_envio = asyncio.create_task(trabalhador_envio())

            log(
                f"🩺 WATCHDOG OK | fila={fila_envio.qsize()} | "
                f"pendentes={len(pendentes_por_jogo)} | cache={len(ultimos_enviados)}"
            )

        except Exception as e:
            log(f"❌ ERRO NO WATCHDOG: {e}")
            log(traceback.format_exc())


# =========================================================
# PROCESSAMENTO DE EVENTOS
# =========================================================

async def processar_evento(event, origem="nova"):
    if event.out:
        return

    texto = event.raw_text or ""

    log(f"📥 EVENTO RECEBIDO DO TELEGRAM | origem={origem}")

    if not texto.strip():
        log("ℹ️ Evento ignorado: texto vazio.")
        return

    if not mensagem_valida(texto):
        log("ℹ️ Mensagem ignorada: não parece alerta CornerPro.")
        return

    try:
        msg_id = getattr(event.message, "id", None)
        chat_id = getattr(event.message, "chat_id", None)

        chave_msg = f"{chat_id}_{msg_id}_{origem}"

        if chave_msg in mensagens_processadas:
            log(f"ℹ️ Evento duplicado ignorado | msg={chave_msg}")
            return

        mensagens_processadas[chave_msg] = time.time()

        estrategia = detectar_estrategia(texto)
        jogo = extrair_jogo(texto)
        chave_jogo = normalizar_chave_jogo(jogo)

        score, metricas = calcular_score(texto, estrategia)
        corte = CORTES.get(estrategia, 76)

        log(
            f"📊 PROCESSADO | {estrategia} | {score}% | "
            f"Corte {corte}% | {jogo} | {metricas['tempo']}' | "
            f"{metricas['placar']} | {metricas['mercado']}"
        )

        if score < corte:
            log(
                f"⛔ BLOQUEADO POR SCORE | {estrategia} | "
                f"{score}% < {corte}% | {jogo}"
            )
            return

        alerta = {
            "jogo": jogo,
            "chave_jogo": chave_jogo,
            "estrategia": estrategia,
            "score": score,
            "metricas": metricas,
            "texto_original": texto,
            "recebido_em": time.time(),
        }

        if chave_jogo not in pendentes_por_jogo:
            pendentes_por_jogo[chave_jogo] = []

        pendentes_por_jogo[chave_jogo].append(alerta)

        log(
            f"⏳ ALERTA EM JANELA DE DECISÃO | {estrategia} | "
            f"{score}% | aguardando {JANELA_DECISAO_SEGUNDOS}s | {jogo}"
        )

        if chave_jogo not in tarefas_decisao:
            tarefas_decisao[chave_jogo] = asyncio.create_task(decidir_e_enviar(chave_jogo))

    except Exception as e:
        log(f"❌ ERRO NO PROCESSAMENTO DO ALERTA: {e}")
        log(traceback.format_exc())


@client.on(events.NewMessage)
async def handler_nova_mensagem(event):
    await processar_evento(event, origem="nova")


@client.on(events.MessageEdited)
async def handler_mensagem_editada(event):
    await processar_evento(event, origem="editada")


# =========================================================
# START
# =========================================================

async def main():
    global tarefa_envio

    log("🚀 COUTIPS ONLINE - SCORE CONTEXTUAL ATIVO")
    log("📊 Estratégias ativas: HT_PREMIUM | HT_MODERADO | FT_PREMIUM | FT_MODERADO")
    log(f"📡 Canal destino: {TARGET_CHANNEL}")
    log(f"⏳ Janela de decisão por jogo: {JANELA_DECISAO_SEGUNDOS}s")
    log("⚠️ Confirme no Railway que existe apenas 1 instância/replica ativa.")

    await client.start()

    log("✅ TELEGRAM CONECTADO COM SUCESSO")

    tarefa_envio = asyncio.create_task(trabalhador_envio())
    asyncio.create_task(watchdog_envio())

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        log("🛑 Bot encerrado manualmente.")
    except Exception as e:
        log(f"❌ ERRO FATAL NO BOT: {e}")
        log(traceback.format_exc())