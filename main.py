from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import os
import re
import time
import asyncio
import traceback
import html
import unicodedata

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "@CoutipsIPS")
CORNERS_CHANNEL = os.getenv("CORNERS_CHANNEL", "@Goat_Bot01")
CONFIRMATION_CHANNEL = os.getenv("CONFIRMATION_CHANNEL", "@ALFA_CON")

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

API_ID = int(API_ID)
client = TelegramClient("coutips_v2_session", API_ID, API_HASH)

# =========================================================
# CONFIGURAÇÃO OFICIAL ATUAL
# =========================================================
# Fase atual: SOMENTE GOLS.
# Cantos continuam calculados internamente para log/contexto, mas NÃO são enviados.
# Corte principal separado por tempo de jogo.
# HT precisa ser mais raro/limpo; FT aceita mais caos emocional.
CORTE_GOL = int(os.getenv("CORTE_GOL", "85"))
CORTE_GOL_HT = int(os.getenv("CORTE_GOL_HT", "87"))
CORTE_GOL_FT = int(os.getenv("CORTE_GOL_FT", "82"))

# Confirmação usa o mesmo funil dos bots principais, mas aceita corte menor
# porque aparece em minuto mais caótico: fim do HT ou reta final do FT.
CORTE_CONFIRMACAO_GOL_HT = int(os.getenv("CORTE_CONFIRMACAO_GOL_HT", "82"))
CORTE_CONFIRMACAO_GOL_FT = int(os.getenv("CORTE_CONFIRMACAO_GOL_FT", "82"))
CORTE_CONFIRMACAO_GOL = int(os.getenv("CORTE_CONFIRMACAO_GOL", str(CORTE_CONFIRMACAO_GOL_HT)))

# Alertas 80+ entram na janela de decisão como OBSERVAÇÃO interna.
# O envio final continua respeitando o corte real: HT 87 / FT 82 / Confirmação HT 85 / Confirmação FT 82.
CORTE_OBSERVACAO_JANELA = int(os.getenv("CORTE_OBSERVACAO_JANELA", "80"))
CORTE_CANTO = 999

# Memória longa apenas para bots de confirmação.
# Não interfere no score dos bots principais.
MEMORIA_CONFIRMACAO_SEGUNDOS = int(os.getenv("MEMORIA_CONFIRMACAO_SEGUNDOS", "900"))

COOLDOWN_SEGUNDOS = int(os.getenv("COOLDOWN_SEGUNDOS", "600"))
CACHE_MAX_SEGUNDOS = int(os.getenv("CACHE_MAX_SEGUNDOS", "3600"))
JANELA_DECISAO_SEGUNDOS = float(os.getenv("JANELA_DECISAO_SEGUNDOS", "8"))
INTERVALO_ENVIO_SEGUNDOS = float(os.getenv("INTERVALO_ENVIO_SEGUNDOS", "5"))
CONFIRMACAO_DELTA_FORTE = int(os.getenv("CONFIRMACAO_DELTA_FORTE", "8"))
CONFIRMACAO_SCORE_MINIMO = int(os.getenv("CONFIRMACAO_SCORE_MINIMO", str(CORTE_CONFIRMACAO_GOL_HT)))
WATCHDOG_SEGUNDOS = int(os.getenv("WATCHDOG_SEGUNDOS", "60"))

ultimas_leituras_por_jogo = {}
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


def remover_acentos(texto):
    texto = str(texto or "")
    texto = unicodedata.normalize("NFD", texto)
    return "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")


def normalizar(texto):
    return str(texto or "").replace("*", "").replace("_", "").replace("**", "").strip()


def limpar_linha(texto):
    return re.sub(r"\s+", " ", str(texto or "")).strip()


def normalizar_chave_jogo(jogo):
    jogo = remover_acentos(str(jogo or "").lower())
    jogo = re.sub(r"\([^)]*\)", "", jogo)
    jogo = re.sub(r"[^a-z0-9]+", " ", jogo)
    return re.sub(r"\s+", " ", jogo).strip()


def pegar_numero(pattern, texto, padrao=0):
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return padrao
    try:
        return int(m.group(1))
    except Exception:
        return padrao


def pegar_float(pattern, texto, padrao=0.0):
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return padrao
    try:
        return float(m.group(1).replace(",", "."))
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
    return m.group(0).strip() if m else ""


def bloquear_categoria_base(texto):
    t = remover_acentos(texto).upper()
    for padrao in [
        r"\bU19\b", r"\bU20\b",
        r"\bSUB[-\s]?19\b", r"\bSUB[-\s]?20\b",
        r"\bUNDER[-\s]?19\b", r"\bUNDER[-\s]?20\b",
    ]:
        if re.search(padrao, t):
            return True
    return False


def limpar_memoria_interna():
    agora = time.time()

    for cache in [ultimos_enviados, mensagens_processadas, ultimas_leituras_por_jogo]:
        for chave in list(cache.keys()):
            valor = cache.get(chave, {})
            ts = valor.get("recebido_em", 0) if isinstance(valor, dict) else valor
            if agora - ts > CACHE_MAX_SEGUNDOS:
                cache.pop(chave, None)

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
# PARSER / ESTRATÉGIAS
# =========================================================

def detectar_estrategia(texto):
    t = remover_acentos(texto).upper()
    t = re.sub(r"\s+", " ", t)

    if "BOT_HT CONFIRMACAO" in t:
        return "ALFA_HT_CONFIRMACAO"
    if "BOT_FT CONFIRMACAO" in t:
        return "ALFA_FT_CONFIRMACAO"

    if "ARCE_HT" in t or "ARCE HT" in t or " ARCE " in t:
        return "ARCE_HT"
    if "CHAMA_FT" in t or "CHAMA FT" in t or " CHAMA " in t:
        return "CHAMA_FT"

    if "BOT_HT" in t or "HT_PREMIUM" in t or "HT_PREMIUN" in t or "HT_MODERADO" in t or "IPS HT" in t:
        return "ALFA_HT"

    if "BOT_FT" in t or "FT_PREMIUM" in t or "FT_PREMIUN" in t or "FT_MODERADO" in t or "IPS FT" in t or "POS-70" in t or "PÓS-70" in t:
        return "ALFA_FT"

    if "1ºT" in t or "1T" in t or "INTERVALO" in t:
        return "ALFA_HT"

    return "ALFA_FT"


def eh_ht(estrategia):
    return estrategia in ["ALFA_HT", "ALFA_HT_CONFIRMACAO", "ARCE_HT"]


def eh_ft(estrategia):
    return estrategia in ["ALFA_FT", "ALFA_FT_CONFIRMACAO", "CHAMA_FT"]


def eh_confirmacao(estrategia):
    return estrategia in ["ALFA_HT_CONFIRMACAO", "ALFA_FT_CONFIRMACAO"]


def mensagem_valida(texto):
    t = remover_acentos(texto).upper()
    return "ALERTA ESTRATEGIA" in t and "JOGO:" in t and "TEMPO:" in t


def extrair_jogo(texto):
    texto = normalizar(texto)
    m = re.search(r"Jogo:\s*(.+)", texto, re.IGNORECASE)
    if m:
        return limpar_linha(m.group(1).split("\n")[0])

    for linha in texto.splitlines():
        if " x " in linha.lower() or " vs " in linha.lower():
            return limpar_linha(linha)

    return "Jogo não identificado"


def extrair_competicao(texto):
    m = re.search(r"Competição:\s*(.+)", texto, re.IGNORECASE)
    return limpar_linha(m.group(1).split("\n")[0]) if m else ""


def extrair_tempo(texto):
    return pegar_numero(r"Tempo:\s*(\d+)", texto, 0)


def extrair_resultado(texto):
    m = re.search(r"Resultado:\s*([0-9]+)\s*x\s*([0-9]+)", texto, re.IGNORECASE)
    return f"{m.group(1)} x {m.group(2)}" if m else "Placar não identificado"


def extrair_gols_placar(placar):
    m = re.search(r"([0-9]+)\s*x\s*([0-9]+)", str(placar or ""))
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def mercado_dinamico(placar):
    gc, gf = extrair_gols_placar(placar)
    if gc is None:
        return "Over 0.5 Gol"
    return f"Over {gc + gf + 0.5:.1f} Gol"


def mercado_cantos_dinamico(metricas):
    return "Over Cantos"


def extrair_odds(texto):
    m = re.search(r"Odds.*?:\s*([0-9.]+)\s*/\s*([0-9.]+)\s*/\s*([0-9.]+)", texto, re.IGNORECASE)
    if not m:
        return 0, 0, 0
    try:
        return float(m.group(1)), float(m.group(2)), float(m.group(3))
    except Exception:
        return 0, 0, 0


def extrair_ultimo_gol_lado(texto):
    t = remover_acentos(texto).upper()
    for p in [
        r"ULTIMO\s+GOLO:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)",
        r"ULTIMO\s+GOL:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)",
    ]:
        m = re.search(p, t, re.IGNORECASE)
        if m:
            return "CASA" if m.group(1).upper() in ["CASA", "HOME"] else "FORA"
    return "DESCONHECIDO"


def extrair_ultimos_cantos_lados(texto):
    t = remover_acentos(texto).upper()
    m = re.search(r"ULTIMOS\s+CANTOS:\s*(.+)", t, re.IGNORECASE)
    if not m:
        return []

    linha = m.group(1).split("\n")[0]
    eventos = []
    for minuto, lado in re.findall(r"(\d+)\s*['’]?\s*(CASA|FORA|HOME|AWAY)", linha):
        eventos.append((int(minuto), "CASA" if lado in ["CASA", "HOME"] else "FORA"))
    return eventos


def extrair_pressao_alfa(texto):
    base = normalizar(texto)
    sem = remover_acentos(base)
    m = re.search(r"Índice de Pressão:(.+)", base, re.IGNORECASE | re.DOTALL)
    if not m:
        m = re.search(r"Indice de Pressao:(.+)", sem, re.IGNORECASE | re.DOTALL)

    if not m:
        return {
            "ip_pico_casa": 0.0,
            "ip_pico_fora": 0.0,
            "ip_consec_10_casa": 0,
            "ip_consec_10_fora": 0,
            "ip_consec_15_casa": 0,
            "ip_consec_15_fora": 0,
            "ip_consec_18_casa": 0,
            "ip_consec_18_fora": 0,
            "ip_consec_22_casa": 0,
            "ip_consec_22_fora": 0,
        }

    bloco = m.group(1).split("https://")[0]
    casa_vals, fora_vals = [], []

    for seg in bloco.split(";"):
        nums = re.findall(r"\d+(?:[.,]\d+)?", seg)
        if len(nums) == 2:
            try:
                casa_vals.append(float(nums[0].replace(",", ".")))
                fora_vals.append(float(nums[1].replace(",", ".")))
            except Exception:
                pass

    def consec(vals, limite):
        atual = maior = 0
        for v in vals:
            if v >= limite:
                atual += 1
                maior = max(maior, atual)
            else:
                atual = 0
        return maior

    return {
        "ip_pico_casa": max(casa_vals) if casa_vals else 0.0,
        "ip_pico_fora": max(fora_vals) if fora_vals else 0.0,
        "ip_consec_10_casa": consec(casa_vals, 10),
        "ip_consec_10_fora": consec(fora_vals, 10),
        "ip_consec_15_casa": consec(casa_vals, 15),
        "ip_consec_15_fora": consec(fora_vals, 15),
        "ip_consec_18_casa": consec(casa_vals, 18),
        "ip_consec_18_fora": consec(fora_vals, 18),
        "ip_consec_22_casa": consec(casa_vals, 22),
        "ip_consec_22_fora": consec(fora_vals, 22),
    }


def extrair_metricas(texto):
    tl = normalizar(texto)
    tempo = extrair_tempo(tl)
    placar = extrair_resultado(tl)

    rda = pegar_par(r"R\.\s*Dentro\s*Área:Casa=(\d+)\s*/\s*Fora=(\d+)", tl)
    if rda == (0, 0):
        rda = pegar_par(r"R\.\s*Dentro\s*Area:Casa=(\d+)\s*/\s*Fora=(\d+)", tl)

    ultimo_gol = (
        pegar_numero(r"Último golo:\s*(\d+)", tl, 0)
        or pegar_numero(r"Ultimo golo:\s*(\d+)", tl, 0)
        or pegar_numero(r"Último gol:\s*(\d+)", tl, 0)
        or pegar_numero(r"Ultimo gol:\s*(\d+)", tl, 0)
    )

    xg = pegar_float_par(r"xg:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", tl)
    if xg == (0.0, 0.0):
        total = pegar_float(r"\bxg:\s*([0-9.,]+)", tl, 0.0)
        xg = (total / 2, total / 2)

    xgl = pegar_float_par(r"xgl:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", tl)
    if xgl == (0.0, 0.0):
        total = pegar_float(r"\bxgl:\s*([0-9.,]+)", tl, 0.0)
        xgl = (total / 2, total / 2)

    xgi = pegar_float_par(r"xgi:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", tl)
    if xgi == (0.0, 0.0):
        total = pegar_float(r"\bxgi:\s*([0-9.,]+)", tl, 0.0)
        xgi = (total / 2, total / 2)

    return {
        "tempo": tempo,
        "placar": placar,
        "mercado": mercado_dinamico(placar),
        "competicao": extrair_competicao(tl),
        "ataques_perigosos": pegar_par(r"Ataques Perigosos:\s*(\d+)\s*-\s*(\d+)", tl),
        "ataques": pegar_par(r"Ataques:\s*(\d+)\s*-\s*(\d+)", tl),
        "cantos": pegar_par(r"Cantos:\s*(\d+)\s*-\s*(\d+)", tl),
        "ultimos_cantos_lados": extrair_ultimos_cantos_lados(tl),
        "posse": pegar_par(r"Posse bola:\s*(\d+)\s*-\s*(\d+)", tl),
        "remates_baliza": pegar_par(r"Remates Baliza:\s*(\d+)\s*-\s*(\d+)", tl),
        "remates_lado": pegar_par(r"Remates lado:\s*(\d+)\s*-\s*(\d+)", tl),
        "remates_dentro_area": rda,
        "vermelhos": pegar_par(r"Cartões vermelhos:\s*(\d+)\s*-\s*(\d+)", tl),
        "odds": extrair_odds(tl),
        "ultimos5": pegar_par(r"(?:Ultimos|Últimos)\s*5['’]?:\s*(\d+)\s*\([^)]*\)\s*-\s*(\d+)", tl),
        "ultimos10": pegar_par(r"(?:Ultimos|Últimos)\s*10['’]?:\s*(\d+)\s*\([^)]*\)\s*-\s*(\d+)", tl),
        "ultimo_gol": ultimo_gol,
        "ultimo_gol_lado": extrair_ultimo_gol_lado(tl),
        "chance_golo": pegar_par(r"Chance de Golo:Casa=(\d+)\s*/\s*Fora=(\d+)", tl),
        "heatmap": pegar_par(r"heatmapFull:Casa=(\d+)\s*/\s*Fora=(\d+)", tl),
        "heatmap_middle": pegar_par(r"heatmapMiddle:Casa=(\d+)\s*/\s*Fora=(\d+)", tl),
        "xg": xg,
        "xgl": xgl,
        "xgi": xgi,
        "pressao_alfa": extrair_pressao_alfa(tl),
        "bet365": extrair_link_bet365(tl),
    }


# =========================================================
# LIGAS / CONTEXTO
# =========================================================

def classificar_liga(competicao):
    c = remover_acentos(competicao).lower()

    ligas_premium = [
        "england premier league", "france ligue 1", "italy serie a",
        "spain la liga", "germany bundesliga", "denmark superliga",
        "netherlands eredivisie", "belgium pro league",
        "czech republic fortuna liga", "england league two",
        "switzerland", "portugal liga", "usa nwsl", "nwsl",
    ]

    ligas_moderadas = [
        "australia", "india", "china", "norway", "sweden",
        "finland", "japan", "singapore", "hong kong", "macao",
        "estonia", "poland", "lithuania", "brazil brasileiro women",
        "denmark first division", "korea republic k-league", "k-league",
    ]

    # Ligas estruturalmente mais under/truncadas.
    # Não bloqueiam sozinhas, mas exigem muito mais prova de gol.
    ligas_under = [
        "argentina", "colombia", "peru", "bolivia", "paraguay", "paraguai",
        "ecuador", "chile", "uruguay", "uruguai", "venezuela",
        "greece", "turkey", "turquia", "romania", "algeria", "iraq",
    ]

    ligas_perigosas = [
        "romania 3", "new zealand regional", "mongolia", "jordan",
    ]

    if any(x in c for x in ligas_under):
        return "UNDER"
    if any(x in c for x in ligas_perigosas):
        return "PERIGOSA"
    if any(x in c for x in ligas_premium):
        return "PREMIUM"
    if any(x in c for x in ligas_moderadas):
        return "MODERADA"

    return "NEUTRA"


def liga_score(metricas, mercado="gol"):
    liga = classificar_liga(metricas.get("competicao", ""))

    if mercado == "canto":
        if liga == "PREMIUM":
            return 3
        if liga == "MODERADA":
            return 1
        if liga == "PERIGOSA":
            return -3
        return 0

    if liga == "PREMIUM":
        return 4
    if liga == "MODERADA":
        return 0
    if liga == "UNDER":
        return -8
    if liga == "PERIGOSA":
        return -10
    return 0


def lado_favorito(metricas):
    oc, _, of = metricas["odds"]

    if not oc or not of:
        return "DESCONHECIDO", 0

    if oc < of:
        return "CASA", oc

    if of < oc:
        return "FORA", of

    return "EQUILIBRADO", oc


def lado_zebra(metricas):
    fav, _ = lado_favorito(metricas)
    if fav == "CASA":
        return "FORA"
    if fav == "FORA":
        return "CASA"
    return "DESCONHECIDO"


def lado_dominante(metricas):
    apc, apf = metricas["ataques_perigosos"]
    u5c, u5f = metricas["ultimos5"]
    u10c, u10f = metricas["ultimos10"]
    rbc, rbf = metricas["remates_baliza"]
    rlc, rlf = metricas["remates_lado"]
    rdac, rdaf = metricas.get("remates_dentro_area", (0, 0))
    cc, cf = metricas["cantos"]
    hc, hf = metricas.get("heatmap", (0, 0))
    ch_c, ch_f = metricas.get("chance_golo", (0, 0))
    xgc, xgf = metricas.get("xg", (0.0, 0.0))
    p = metricas.get("pressao_alfa", {})

    casa = (
        apc * 1.2 + u5c * 2 + u10c * 1.4 + rbc * 3.8
        + rdac * 2.2 + rlc * 1.2 + cc * 0.7 + ch_c * 1.1
        + xgc * 8 + hc * 0.1
        + p.get("ip_pico_casa", 0) * 0.45
        + p.get("ip_consec_18_casa", 0) * 2
        + p.get("ip_consec_22_casa", 0) * 3
    )

    fora = (
        apf * 1.2 + u5f * 2 + u10f * 1.4 + rbf * 3.8
        + rdaf * 2.2 + rlf * 1.2 + cf * 0.7 + ch_f * 1.1
        + xgf * 8 + hf * 0.1
        + p.get("ip_pico_fora", 0) * 0.45
        + p.get("ip_consec_18_fora", 0) * 2
        + p.get("ip_consec_22_fora", 0) * 3
    )

    dif = casa - fora

    if dif >= 8:
        return "CASA", dif
    if dif <= -8:
        return "FORA", abs(dif)

    return "EQUILIBRADO", abs(dif)


def lado_sofrendo_pressao(metricas):
    dom, _ = lado_dominante(metricas)
    if dom == "CASA":
        return "FORA"
    if dom == "FORA":
        return "CASA"
    return "DESCONHECIDO"


def lado_com_vermelho(metricas):
    vc, vf = metricas.get("vermelhos", (0, 0))
    if vc > vf:
        return "CASA"
    if vf > vc:
        return "FORA"
    if vc > 0 and vf > 0:
        return "AMBOS"
    return "NENHUM"


def pressao_viva(metricas):
    u5 = sum(metricas["ultimos5"])
    u10 = sum(metricas["ultimos10"])
    rb = sum(metricas["remates_baliza"])
    rl = sum(metricas["remates_lado"])
    rda = sum(metricas.get("remates_dentro_area", (0, 0)))
    ap = sum(metricas["ataques_perigosos"])
    xg = sum(metricas.get("xg", (0.0, 0.0)))
    p = metricas.get("pressao_alfa", {})

    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))
    ip18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    ip22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))

    return (
        ip22 >= 2
        or ip18 >= 3
        or (ip_pico >= 22 and (u5 >= 2 or u10 >= 5))
        or u5 >= 5
        or (u10 >= 9 and u5 >= 3)
        or (ap >= 30 and (rb >= 3 or rda >= 3 or rl >= 7 or xg >= 0.55))
    )


def tempo_operacional_valido(metricas, estrategia):
    tempo = metricas.get("tempo", 0)

    if eh_ht(estrategia):
        return tempo <= 37

    if eh_ft(estrategia):
        return tempo <= 82

    return True


def corte_gol_estrategia(estrategia):
    if estrategia == "ALFA_HT_CONFIRMACAO":
        return CORTE_CONFIRMACAO_GOL_HT
    if estrategia == "ALFA_FT_CONFIRMACAO":
        return CORTE_CONFIRMACAO_GOL_FT
    if eh_ht(estrategia):
        return CORTE_GOL_HT
    if eh_ft(estrategia):
        return CORTE_GOL_FT
    return CORTE_GOL


def bonus_bot_confianca(estrategia):
    # ARCE e CHAMA são bots de maior confiança operacional.
    # O bônus ajuda jogos bons desses bots a não morrerem por detalhe,
    # mas entra antes das travas finais para não liberar jogo ruim/fake pressure.
    if estrategia in ["ARCE_HT", "CHAMA_FT"]:
        return 3
    return 0


def valor_lado(metricas, campo, lado, padrao=(0, 0)):
    valores = metricas.get(campo, padrao)
    if not isinstance(valores, tuple) or len(valores) < 2:
        return 0
    return valores[0] if lado == "CASA" else valores[1] if lado == "FORA" else 0


def soma_lados(metricas, campo, padrao=(0, 0)):
    valores = metricas.get(campo, padrao)
    if not isinstance(valores, tuple) or len(valores) < 2:
        return 0
    return valores[0] + valores[1]


def lado_perdendo(metricas):
    gc, gf = extrair_gols_placar(metricas.get("placar", ""))
    if gc is None:
        return "DESCONHECIDO"
    if gc < gf:
        return "CASA"
    if gf < gc:
        return "FORA"
    return "EMPATE"


def lado_vencendo(metricas):
    gc, gf = extrair_gols_placar(metricas.get("placar", ""))
    if gc is None:
        return "DESCONHECIDO"
    if gc > gf:
        return "CASA"
    if gf > gc:
        return "FORA"
    return "EMPATE"


def gol_empate_ft_com_virada_potencial(metricas, lado_gol, dom=None, fav=None):
    """Exceção institucional para confirmação FT.

    Se o time que vinha pressionando marcou nos últimos 5 minutos,
    normalmente a pressão foi premiada e a confirmação deve ser cancelada.

    Exceção apenas no FT: quando esse gol foi empate, e o time ainda tem perfil
    claro de buscar a virada nos minutos finais — favorito visível ou domínio
    numérico muito forte, com produção real.
    """
    if lado_gol not in ["CASA", "FORA"]:
        return False

    gc, gf = extrair_gols_placar(metricas.get("placar", ""))
    if gc is None or gc != gf:
        return False

    # Se o placar atual está empatado e o lado marcou, então antes do gol
    # esse lado estava perdendo por 1. É o cenário de empate tardio.
    if lado_gol == "CASA" and gc <= 0:
        return False
    if lado_gol == "FORA" and gf <= 0:
        return False

    fav = fav or lado_favorito(metricas)[0]
    _, odd_fav = lado_favorito(metricas)
    dom = dom or lado_dominante(metricas)[0]
    dados = dados_lado(metricas, lado_gol)
    p = metricas.get("pressao_alfa", {})

    ip_pico_lado = p.get("ip_pico_casa", 0) if lado_gol == "CASA" else p.get("ip_pico_fora", 0)
    c18_lado = p.get("ip_consec_18_casa", 0) if lado_gol == "CASA" else p.get("ip_consec_18_fora", 0)
    c22_lado = p.get("ip_consec_22_casa", 0) if lado_gol == "CASA" else p.get("ip_consec_22_fora", 0)

    favorito_visivel = fav == lado_gol and odd_fav and odd_fav <= 1.75
    dominio_em_campo = dom == lado_gol and (
        dados["u5"] >= 3
        or dados["u10"] >= 7
        or dados["rb"] >= 2
        or dados["rda"] >= 2
        or dados["cantos"] >= 2
        or dados["xg"] >= 0.45
        or ip_pico_lado >= 24
        or c18_lado >= 3
        or c22_lado >= 2
    )
    producao_real = (
        dados["rb"] >= 1
        or dados["remates"] >= 4
        or dados["rda"] >= 2
        or dados["cantos"] >= 2
        or dados["xg"] >= 0.35
    )

    return producao_real and (favorito_visivel or dominio_em_campo)


def dados_lado(metricas, lado):
    rb = valor_lado(metricas, "remates_baliza", lado)
    rl = valor_lado(metricas, "remates_lado", lado)
    rda = valor_lado(metricas, "remates_dentro_area", lado)
    cantos = valor_lado(metricas, "cantos", lado)
    u5 = valor_lado(metricas, "ultimos5", lado)
    u10 = valor_lado(metricas, "ultimos10", lado)
    ap = valor_lado(metricas, "ataques_perigosos", lado)
    xg = valor_lado(metricas, "xg", lado, (0.0, 0.0))
    return {
        "rb": rb,
        "rl": rl,
        "remates": rb + rl,
        "rda": rda,
        "cantos": cantos,
        "u5": u5,
        "u10": u10,
        "ap": ap,
        "xg": xg,
    }


def consequencia_ofensiva_total(metricas, estrategia):
    rb = soma_lados(metricas, "remates_baliza")
    rl = soma_lados(metricas, "remates_lado")
    rda = soma_lados(metricas, "remates_dentro_area")
    cantos = soma_lados(metricas, "cantos")
    xg = soma_lados(metricas, "xg", (0.0, 0.0))

    if eh_ht(estrategia):
        return rb >= 1 or rb + rl >= 4 or rda >= 2 or cantos >= 2 or xg >= 0.30

    return rb >= 1 or rb + rl >= 4 or rda >= 2 or cantos >= 2 or xg >= 0.35


def lado_pressao_principal(metricas, estrategia):
    """Define o lado que precisa provar consequência ofensiva.

    - Em FT com placar de 2+ gols, o lado que perde precisa mostrar vida.
    - Em jogo apertado, usamos o lado dominante.
    - Se o domínio está equilibrado, usamos o favorito; se não houver favorito claro, fica desconhecido.
    """
    if eh_ft(estrategia):
        gc, gf = extrair_gols_placar(metricas.get("placar", ""))
        if gc is not None and abs(gc - gf) >= 2:
            perdendo = lado_perdendo(metricas)
            if perdendo in ["CASA", "FORA"]:
                return perdendo

    dom, _ = lado_dominante(metricas)
    if dom in ["CASA", "FORA"]:
        return dom

    fav, _ = lado_favorito(metricas)
    if fav in ["CASA", "FORA"]:
        return fav

    return "DESCONHECIDO"


def consequencia_ofensiva_lado(metricas, estrategia, lado):
    if lado not in ["CASA", "FORA"]:
        return consequencia_ofensiva_total(metricas, estrategia)

    dados = dados_lado(metricas, lado)

    if eh_ht(estrategia):
        return (
            dados["rb"] >= 1
            or dados["remates"] >= 3
            or dados["rda"] >= 1
            or dados["cantos"] >= 1
            or dados["xg"] >= 0.22
        )

    return (
        dados["rb"] >= 1
        or dados["remates"] >= 3
        or dados["rda"] >= 2
        or dados["cantos"] >= 2
        or dados["xg"] >= 0.28
    )


def pressao_sustentada_nivel(metricas, estrategia):
    p = metricas.get("pressao_alfa", {})
    c10 = max(p.get("ip_consec_10_casa", 0), p.get("ip_consec_10_fora", 0))
    c15 = max(p.get("ip_consec_15_casa", 0), p.get("ip_consec_15_fora", 0))
    c18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    c22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))
    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))

    if ip_pico >= 35 and c22 >= 2:
        return "COLAPSO"
    if ip_pico >= 30:
        return "ELITE"
    if eh_ht(estrategia):
        if c22 >= 3 or c18 >= 4:
            return "FORTE"
        if c15 >= 5:
            return "OBSERVACAO"
    else:
        if c22 >= 5 or c18 >= 5:
            return "FORTE"
        if c15 >= 5:
            return "OBSERVACAO"
    if c10 >= 5:
        return "VIVA_BAIXA"
    return "FRACA"


def pressao_recente_caiu(metricas):
    u5 = soma_lados(metricas, "ultimos5")
    u10 = soma_lados(metricas, "ultimos10")
    p = metricas.get("pressao_alfa", {})
    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))

    # Se teve pico alto, mas os últimos 5/10 não sustentam volume, é sinal de pressão antiga.
    return ip_pico >= 22 and (u5 <= 3 or u10 <= 6)




# =========================================================
# FUNIL INSTITUCIONAL ANTI-FAKE PRESSURE
# =========================================================

def perfil_origem_estrategia(estrategia):
    if estrategia in ["ARCE_HT", "CHAMA_FT"]:
        return "RADAR_CONFIAVEL"
    if eh_confirmacao(estrategia):
        return "CONFIRMACAO"
    return "RADAR_ABERTO"


def lado_alvo_contextual(metricas, estrategia):
    """Define quem precisa provar perigo real.

    Esta função evita o erro antigo de somar estatística dos dois lados.
    O score só deve respeitar a pressão do lado que realmente carrega o contexto.
    """
    dom, dif_dom = lado_dominante(metricas)
    fav, odd_fav = lado_favorito(metricas)
    perdendo = lado_perdendo(metricas)
    vencendo = lado_vencendo(metricas)

    # Em FT, se o favorito está perdendo ou empatando e domina, ele é o lado-alvo.
    if eh_ft(estrategia) and fav in ["CASA", "FORA"]:
        if perdendo == fav and dom == fav:
            return fav, "FAVORITO_PERDENDO_E_PRESSIONANDO"
        if vencendo == "EMPATE" and dom == fav:
            return fav, "FAVORITO_EMPATANDO_E_PRESSIONANDO"

    # Em qualquer tempo, domínio claro é a melhor referência.
    if dom in ["CASA", "FORA"] and dif_dom >= 8:
        return dom, "LADO_DOMINANTE_EM_CAMPO"

    # Se não houver domínio claro, usa favorito apenas como fallback, nunca como passe livre.
    if fav in ["CASA", "FORA"] and odd_fav and odd_fav <= 1.70:
        return fav, "FAVORITO_ESTRUTURAL_FALLBACK"

    return "DESCONHECIDO", "SEM_LADO_ALVO_CLARO"


def valores_pressao_lado(metricas, lado):
    p = metricas.get("pressao_alfa", {})
    if lado == "CASA":
        return {
            "ip_pico": p.get("ip_pico_casa", 0),
            "c10": p.get("ip_consec_10_casa", 0),
            "c15": p.get("ip_consec_15_casa", 0),
            "c18": p.get("ip_consec_18_casa", 0),
            "c22": p.get("ip_consec_22_casa", 0),
        }
    if lado == "FORA":
        return {
            "ip_pico": p.get("ip_pico_fora", 0),
            "c10": p.get("ip_consec_10_fora", 0),
            "c15": p.get("ip_consec_15_fora", 0),
            "c18": p.get("ip_consec_18_fora", 0),
            "c22": p.get("ip_consec_22_fora", 0),
        }
    return {"ip_pico": 0, "c10": 0, "c15": 0, "c18": 0, "c22": 0}


def qualidade_pressao_lado(metricas, estrategia, lado):
    """Mede a pressão do lado certo, não do jogo inteiro."""
    dados = dados_lado(metricas, lado)
    ip = valores_pressao_lado(metricas, lado)
    chance = valor_lado(metricas, "chance_golo", lado)
    heat = valor_lado(metricas, "heatmap", lado)

    pressao_recente = 0
    if dados["u5"] >= 3:
        pressao_recente += 1
    if dados["u5"] >= 5:
        pressao_recente += 1
    if dados["u10"] >= 6:
        pressao_recente += 1
    if dados["u10"] >= 10:
        pressao_recente += 1

    continuidade = 0
    if ip["c10"] >= 5:
        continuidade += 1
    if ip["c15"] >= 4:
        continuidade += 1
    if ip["c18"] >= 3:
        continuidade += 1
    if ip["c22"] >= 2:
        continuidade += 1
    if ip["ip_pico"] >= 24 and dados["u10"] >= 5:
        continuidade += 1

    consequencia = 0
    if dados["rb"] >= 1:
        consequencia += 1
    if dados["rb"] >= 2:
        consequencia += 1
    if dados["remates"] >= 4:
        consequencia += 1
    if dados["rda"] >= 2:
        consequencia += 1
    if dados["cantos"] >= 2:
        consequencia += 1
    if dados["xg"] >= 0.35:
        consequencia += 1
    if chance >= 8:
        consequencia += 1

    territorio = 0
    if dados["ap"] >= 12:
        territorio += 1
    if dados["ap"] >= 20:
        territorio += 1
    if heat >= 60:
        territorio += 1

    return {
        "dados": dados,
        "ip": ip,
        "chance": chance,
        "pressao_recente": pressao_recente,
        "continuidade": continuidade,
        "consequencia": consequencia,
        "territorio": territorio,
    }


def contexto_emocional_vivo(metricas, estrategia, lado):
    gc, gf = extrair_gols_placar(metricas.get("placar", ""))
    if gc is None:
        return True, "PLACAR_DESCONHECIDO"

    gols_total = gc + gf
    dif = abs(gc - gf)
    fav, _ = lado_favorito(metricas)
    perdendo = lado_perdendo(metricas)
    vencendo = lado_vencendo(metricas)

    if eh_ht(estrategia):
        # HT ainda pode estar vivo mesmo com 1x0/0x1, mas 3+ gols cedo exige muita cautela.
        if gols_total >= 3 and dif >= 2:
            return False, "HT_PLACAR_JA_ABRIU_DEMAIS"
        return True, "HT_CONTEXTO_VIVO"

    # FT: placar apertado, empate ou favorito perdendo é o melhor cenário.
    if dif <= 1:
        return True, "FT_PLACAR_ABERTO"
    if fav in ["CASA", "FORA"] and perdendo == fav:
        return True, "FAVORITO_ATRAS_DO_PLACAR"

    # Massacre só continua vivo se a pressão atual do próprio dominante segue extrema.
    q = qualidade_pressao_lado(metricas, estrategia, lado)
    if dif >= 2 and lado == vencendo:
        massacre_continua = (
            q["pressao_recente"] >= 2
            and q["continuidade"] >= 2
            and q["consequencia"] >= 2
            and q["dados"]["u5"] >= 4
        )
        if massacre_continua:
            return True, "MASSACRE_CONTINUA_VIVO"
        return False, "PLACAR_RESOLVIDO_SEM_FOME"

    return dif <= 2, "CONTEXTO_NEUTRO"


def trava_pos_gol_institucional(metricas, estrategia, lado):
    tempo = metricas.get("tempo", 0)
    ultimo = metricas.get("ultimo_gol", 0)
    lado_gol = metricas.get("ultimo_gol_lado", "DESCONHECIDO")

    if not ultimo or ultimo <= 0:
        return False, 0, "SEM_GOL_RECENTE_INSTITUCIONAL"

    minutos = tempo - ultimo
    if minutos < 0:
        return True, 70, "GOL_INCONSISTENTE_BLOQUEIO"

    if minutos > 8:
        return False, 0, "GOL_NAO_RECENTE"

    q = qualidade_pressao_lado(metricas, estrategia, lado)
    dom, _ = lado_dominante(metricas)
    fav, _ = lado_favorito(metricas)
    pressionado = lado_sofrendo_pressao(metricas)
    zebra = lado_zebra(metricas)

    pressao_extrema_pos_gol = (
        q["pressao_recente"] >= 2
        and q["continuidade"] >= 2
        and q["consequencia"] >= 2
        and q["dados"]["u5"] >= 4
    )

    # Confirmação existe para isso: se o dominante marcou recente, a entrada principal não deve ir direto.
    if lado_gol == lado and minutos <= 5:
        if pressao_extrema_pos_gol and eh_confirmacao(estrategia):
            return False, 0, "CONFIRMACAO_VALIDOU_MASSACRE_POS_GOL"
        if pressao_extrema_pos_gol:
            return True, 83, "MASSACRE_POS_GOL_AGUARDAR_CONFIRMACAO"
        return True, 76, "PRESSAO_PREMIADA_BLOQUEIO"

    # Zebra/time pressionado marcando contra o fluxo abre o jogo.
    if lado_gol in [pressionado, zebra] and lado == dom and minutos <= 8:
        return False, 0, "GOL_CONTRA_FLUXO_ABRIU_JOGO"

    # Gol de outro lado sem clareza: cautela.
    if minutos <= 3:
        return True, 82, "GOL_MUITO_RECENTE_CAUTELA"

    return False, 0, "GOL_RECENTE_SEM_TRAVA"


def funil_institucional_gol(metricas, estrategia, chave_jogo):
    """Funil antes do score.

    Retorna se o jogo está aprovado para pontuar de verdade.
    Caso bloqueado, retorna teto máximo para impedir score inflado.
    """
    perfil = perfil_origem_estrategia(estrategia)
    lado, motivo_lado = lado_alvo_contextual(metricas, estrategia)
    metricas["perfil_origem"] = perfil
    metricas["lado_alvo"] = lado
    metricas["motivo_lado_alvo"] = motivo_lado

    if lado not in ["CASA", "FORA"]:
        return False, 72, "SEM_LADO_ALVO_CLARO"

    q = qualidade_pressao_lado(metricas, estrategia, lado)
    metricas["qualidade_lado_alvo"] = q

    # Validações obrigatórias.
    tem_pressao = q["pressao_recente"] >= 1
    tem_continuidade = q["continuidade"] >= 1
    tem_consequencia = q["consequencia"] >= 1
    emocional_vivo, motivo_emocional = contexto_emocional_vivo(metricas, estrategia, lado)
    metricas["motivo_emocional"] = motivo_emocional

    validacoes = sum([tem_pressao, tem_continuidade, tem_consequencia, emocional_vivo])
    metricas["validacoes_funil"] = validacoes

    # Bots abertos precisam provar mais. CHAMA/ARCE já vêm pré-filtrados, mas não têm passe livre.
    minimo_validacoes = 4 if perfil == "RADAR_ABERTO" else 3
    if validacoes < minimo_validacoes:
        return False, 74 if perfil == "RADAR_ABERTO" else 79, "FALHOU_VALIDACOES_OBRIGATORIAS"

    # Travas anti-fake pressure.
    dados = q["dados"]
    ip = q["ip"]
    chance = q["chance"]

    if not tem_consequencia:
        return False, 76, "SEM_CONSEQUENCIA_OFENSIVA_DO_LADO_CERTO"

    if ip["ip_pico"] >= 22 and dados["rb"] == 0 and dados["remates"] <= 2 and dados["xg"] < 0.30:
        return False, 76, "IP_ALTO_SEM_FINALIZACAO"

    if dados["ap"] >= 20 and dados["rb"] == 0 and dados["rda"] == 0 and dados["xg"] < 0.30:
        return False, 76, "ATAQUE_PERIGOSO_SEM_AREA"

    if q["territorio"] >= 2 and q["consequencia"] <= 1 and dados["xg"] < 0.35:
        return False, 78, "TERRITORIO_SEM_RUPTURA"

    if q["continuidade"] == 0 and dados["u5"] <= 2:
        return False, 75, "PRESSAO_ANTIGA_SEM_CONTINUIDADE"

    if not emocional_vivo:
        return False, 78, motivo_emocional

    bloqueia_gol, teto_gol, motivo_gol = trava_pos_gol_institucional(metricas, estrategia, lado)
    metricas["motivo_pos_gol_institucional"] = motivo_gol
    if bloqueia_gol:
        return False, teto_gol, motivo_gol

    # Para bots normais, exigir anomalia de valor; não basta parecer razoável.
    if perfil == "RADAR_ABERTO":
        anomalia = (
            q["pressao_recente"] >= 2
            and q["continuidade"] >= 2
            and q["consequencia"] >= 2
            and (chance >= 8 or dados["u10"] >= 8 or dados["rb"] >= 2 or dados["remates"] >= 5)
        )
        if not anomalia:
            return False, 82, "BOT_ABERTO_SEM_ANOMALIA_PREMIUM"

    # Confirmação precisa provar manutenção/melhora real.
    if eh_confirmacao(estrategia):
        d, md = score_delta_confirmacao(metricas, estrategia, chave_jogo)
        metricas["delta_confirmacao_funil"] = d
        metricas["motivo_delta_contextual"] = md
        if md not in ["CONFIRMACAO_MELHOROU"] and metricas.get("motivo_pos_gol_institucional") != "CONFIRMACAO_VALIDOU_MASSACRE_POS_GOL":
            return False, 82, "CONFIRMACAO_SEM_MELHORA_REAL"

    return True, 99, "FUNIL_APROVADO"


def score_classificacao_institucional(metricas, estrategia):
    """Pontuação limitada após o funil aprovar.

    Aqui não há soma livre. A nota classifica qualidade, não cria aprovação do nada.
    """
    perfil = metricas.get("perfil_origem", perfil_origem_estrategia(estrategia))
    lado = metricas.get("lado_alvo", "DESCONHECIDO")
    q = metricas.get("qualidade_lado_alvo") or qualidade_pressao_lado(metricas, estrategia, lado)
    dados = q["dados"]
    ip = q["ip"]
    chance = q["chance"]

    if perfil == "RADAR_CONFIAVEL":
       score = 70

elif perfil == "CONFIRMACAO":
       score = 72
else:
       score = 66

# Pressão recente e continuidade valem mais que acumulado.
score += min(q["pressao_recente"] * 2, 6)
score += min(q["continuidade"] * 2, 6)
score += min(q["consequencia"] * 2, 8)

    if dados["u5"] >= 6:
        score += 3
    if dados["u10"] >= 10:
        score += 3
    if ip["c22"] >= 3:
        score += 4
    if ip["ip_pico"] >= 30 and ip["c18"] >= 3:
        score += 3
    if chance >= 10:
        score += 4
    elif chance >= 8:
        score += 2
    if dados["rb"] >= 3:
        score += 3
    if dados["xg"] >= 0.60:
        score += 3

    # Contexto emocional/valor.
    fav, odd_fav = lado_favorito(metricas)
    perdendo = lado_perdendo(metricas)
    vencendo = lado_vencendo(metricas)
    if lado == fav and perdendo == fav:
        score += 5
    elif lado == fav and vencendo == "EMPATE":
        score += 3

    # Odds parelhas com domínio real = edge de mercado; favorito baixo não dá bônus direto.
    oc, oe, of = metricas.get("odds", (0, 0, 0))
    if oc and of:
        odds_parelhas = min(oc, of) >= 1.65 and max(oc, of) <= 4.5
        if odds_parelhas and perfil == "RADAR_ABERTO" and q["pressao_recente"] >= 2 and q["consequencia"] >= 2:
            score += 5
            metricas["valor_mercado_contextual"] = "ODDS_PARELHAS_COM_DOMINIO_AO_VIVO"
        else:
            metricas["valor_mercado_contextual"] = "SEM_BONUS_ODD"

    # Liga e relógio entram como ajuste, não como aprovação.
    score += liga_score(metricas, "gol")
    score += int(relogio_score(metricas, estrategia) * 0.5)

    # Score de confiança separado.
    confianca = 50
    confianca += q["pressao_recente"] * 8
    confianca += q["continuidade"] * 7
    confianca += q["consequencia"] * 8
    if metricas.get("motivo_emocional") in ["FT_PLACAR_ABERTO", "FAVORITO_ATRAS_DO_PLACAR", "HT_CONTEXTO_VIVO"]:
        confianca += 8
    if metricas.get("motivo_pos_gol_institucional") in ["PRESSAO_PREMIADA_BLOQUEIO", "MASSACRE_POS_GOL_AGUARDAR_CONFIRMACAO"]:
        confianca -= 20
    metricas["score_confianca_contextual"] = int(max(0, min(confianca, 99)))

    # Classificação institucional: 93+ é raro.
    if score >= 93:
        absurdo = (
            q["pressao_recente"] >= 3
            and q["continuidade"] >= 3
            and q["consequencia"] >= 3
            and (dados["rb"] >= 3 or chance >= 12 or dados["xg"] >= 0.75)
        )
        if not absurdo:
            score = 92

    # Bots abertos só chegam alto com anomalia verdadeira.
    if perfil == "RADAR_ABERTO" and score >= 90:
        if not (q["pressao_recente"] >= 2 and q["continuidade"] >= 2 and q["consequencia"] >= 3):
            score = 87

    return int(max(0, min(score, 99)))


# =========================================================
# SCORE DE GOL
# =========================================================

def favorito_score(metricas):
    bonus = 0

    for odd in [metricas["odds"][0], metricas["odds"][2]]:
        if not odd:
            continue

        if odd <= 1.35:
            bonus += 10
        elif odd <= 1.50:
            bonus += 7
        elif odd <= 1.70:
            bonus += 4
        elif odd <= 2.00:
            bonus += 1

    return min(bonus, 12)


def score_padrao_alfa(metricas, estrategia):
    p = metricas.get("pressao_alfa", {})

    ch_c, ch_f = metricas.get("chance_golo", (0, 0))
    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))
    c15 = max(p.get("ip_consec_15_casa", 0), p.get("ip_consec_15_fora", 0))
    c18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    c22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))
    chance_max = max(ch_c, ch_f)

    dom, _ = lado_dominante(metricas)
    lado_chance = "CASA" if ch_c >= ch_f else "FORA"
    extra = 3 if dom == lado_chance and dom != "EQUILIBRADO" else 0

    if c15 < 5:
        if ip_pico >= 22 and chance_max >= 10:
            return 8 + extra, "ALFA_OBSERVACAO_SEM_CONTINUIDADE"
        return 0, "SEM_PADRAO_ALFA"

    if eh_ft(estrategia):
        if c22 >= 3 and chance_max >= 14:
            return 30 + extra, "ALFA_FT_DIAMANTE"
        if c18 >= 3 and chance_max >= 10:
            return 26 + extra, "ALFA_FT_FORTE"
        if chance_max >= 8:
            return 20 + extra, "ALFA_FT_BASE"
        return 12 + extra, "ALFA_FT_PRESSAO_SEM_CHANCE"

    if eh_ht(estrategia):
        if c22 >= 3 and chance_max >= 10:
            return 30 + extra, "ALFA_HT_DIAMANTE"
        if c18 >= 3 and chance_max >= 6:
            return 26 + extra, "ALFA_HT_FORTE"
        if chance_max >= 5:
            return 18 + extra, "ALFA_HT_BASE"
        return 10 + extra, "ALFA_HT_PRESSAO_SEM_CHANCE"

    return 0, "SEM_PADRAO_ALFA"


def dominio_score_gol(metricas):
    apc, apf = metricas["ataques_perigosos"]
    rbc, rbf = metricas["remates_baliza"]
    rlc, rlf = metricas["remates_lado"]
    rdac, rdaf = metricas.get("remates_dentro_area", (0, 0))
    cc, cf = metricas["cantos"]
    atc, atf = metricas["ataques"]
    hc, hf = metricas.get("heatmap", (0, 0))
    chc, chf = metricas.get("chance_golo", (0, 0))
    xgc, xgf = metricas.get("xg", (0, 0))
    xic, xif = metricas.get("xgi", (0, 0))
    p = metricas.get("pressao_alfa", {})

    score = 0
    ap = apc + apf
    rb = rbc + rbf
    rl = rlc + rlf
    rda = rdac + rdaf
    cant = cc + cf
    at = atc + atf
    xg = xgc + xgf
    xgi = xic + xif

    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))
    ip18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    ip22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))

    if ip_pico >= 18:
        score += 3
    if ip_pico >= 22:
        score += 4
    if ip18 >= 3:
        score += 4
    if ip22 >= 3:
        score += 5

    if ap >= 18:
        score += 3
    if ap >= 25:
        score += 3
    if ap >= 35:
        score += 3

    if abs(apc - apf) >= 10:
        score += 4
    if abs(apc - apf) >= 18:
        score += 3

    if at >= 70:
        score += 2
    if at >= 95:
        score += 2

    if rb >= 2:
        score += 4
    if rb >= 3:
        score += 4
    if rb >= 5:
        score += 4

    if rda >= 2:
        score += 3
    if rda >= 4:
        score += 4

    if rl >= 5:
        score += 2
    if rl >= 8:
        score += 2

    if abs(rbc - rbf) >= 3 and rb >= 3:
        score += 3

    if cant >= 5:
        score += 1
    if cant >= 8:
        score += 1

    if abs(hc - hf) >= 25:
        score += 2

    if abs(chc - chf) >= 5:
        score += 3

    if xg >= 0.35:
        score += 2
    if xg >= 0.55:
        score += 3
    if xg >= 1:
        score += 3

    if xgi >= 1.5:
        score += 2
    if xgi >= 2.3:
        score += 2

    return score


def momentum_score(metricas):
    u5c, u5f = metricas["ultimos5"]
    u10c, u10f = metricas["ultimos10"]

    u5 = u5c + u5f
    u10 = u10c + u10f
    score = 0

    if u5 >= 3:
        score += 3
    if u5 >= 5:
        score += 4
    if u5 >= 8:
        score += 2

    if u10 >= 7:
        score += 4
    if u10 >= 10:
        score += 3
    if u10 >= 14:
        score += 2

    if abs(u10c - u10f) >= 4:
        score += 3
    if abs(u5c - u5f) >= 3:
        score += 2

    return score


def relogio_score(metricas, estrategia):
    t = metricas["tempo"]

    if eh_ht(estrategia):
        if 18 <= t <= 30:
            return 8
        if 31 <= t <= 36:
            return 5
        if 37 <= t <= 38:
            return -2
        if t < 18:
            return -10
        return -6

    if eh_ft(estrategia):
        if 65 <= t <= 75:
            return 8
        if 76 <= t <= 81:
            return 5
        if 82 <= t <= 83:
            return -4
        if t >= 84:
            return -14
        if t < 63:
            return -6
        return 2

    return 0


def ajuste_vermelho_contextual(metricas):
    liga = classificar_liga(metricas.get("competicao", ""))
    vermelho = lado_com_vermelho(metricas)
    dom, _ = lado_dominante(metricas)
    fav, _ = lado_favorito(metricas)
    zebra = lado_zebra(metricas)

    if vermelho in ["NENHUM", "AMBOS"] or dom == "EQUILIBRADO":
        return 0, "SEM_VERMELHO_RELEVANTE"

    peso = 1.25 if liga == "PREMIUM" else 0.55 if liga == "PERIGOSA" else 1

    if vermelho != dom and pressao_viva(metricas):
        bonus = int(7 * peso)
        if dom == zebra:
            bonus += int(3 * peso)
        if vermelho == fav:
            bonus += int(2 * peso)
        return bonus, "VERMELHO_ABRIU_PRESSAO"

    if vermelho == dom:
        return -6, "VERMELHO_NO_LADO_PRESSAO"

    return 0, "SEM_AJUSTE_VERMELHO"


def ajuste_gol_recente_contextual(metricas, estrategia):
    tempo = metricas["tempo"]
    ultimo_gol = metricas["ultimo_gol"]
    lado = metricas.get("ultimo_gol_lado", "DESCONHECIDO")

    if ultimo_gol <= 0:
        return 0, "SEM_GOL_RECENTE"

    minutos = tempo - ultimo_gol

    if minutos < 0:
        return -14, "GOL_INCONSISTENTE"

    dom, _ = lado_dominante(metricas)
    pressionado = lado_sofrendo_pressao(metricas)
    fav, _ = lado_favorito(metricas)
    zebra = lado_zebra(metricas)
    vivo = pressao_viva(metricas)
    lado_pressao = lado_pressao_principal(metricas, estrategia)

    # Confirmação com gol recente: regra central.
    # Se o time que justificava a pressão/necessidade marcou nos últimos 5 min,
    # a pressão foi premiada e a entrada tardia perde valor.
    # HT: trava na risca.
    # FT: exceção somente para gol de empate com tendência real de virada.
    if eh_confirmacao(estrategia) and minutos <= 5:
        if lado != "DESCONHECIDO":
            gol_do_lado_que_pressao = lado in [dom, lado_pressao]

            if gol_do_lado_que_pressao:
                if eh_ft(estrategia) and gol_empate_ft_com_virada_potencial(metricas, lado, dom=dom, fav=fav):
                    return 10, "GOL_EMPATE_FT_ABRIU_VIRADA"
                return -40, "TRAVA_CONFIRMACAO_PRESSAO_PREMIADA_5M"

            if lado == pressionado and dom != "EQUILIBRADO" and vivo:
                return 14, "GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado == zebra and dom == fav and vivo:
                return 16, "GOL_ZEBRA_CONTRA_FLUXO"

        return -22, "TRAVA_CONFIRMACAO_GOL_RECENTE_5M"

    if minutos <= 2:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo:
                return 10, "GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado == zebra and dom == fav and vivo:
                return 12, "GOL_ZEBRA_CONTRA_FLUXO"
            if lado == dom:
                return -16, "PRESSAO_PREMIADA_RECENTE"
        return -8, "GOL_RECENTE_SEM_CONFIRMACAO"

    if minutos <= 5:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo:
                return 8, "GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado == zebra and dom == fav and vivo:
                return 10, "GOL_ZEBRA_CONTRA_FLUXO"
            if lado == dom and not vivo:
                return -12, "PRESSAO_PREMIADA_MORREU"
            if lado == dom and vivo:
                return -5, "PRESSAO_PREMIADA_MAS_VIVA"
        return -4, "GOL_RECENTE_CAUTELA"

    if minutos <= 10:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo:
                return 5, "GOL_CONTRA_FLUXO_CONTEXTO_BOM"
            if lado == zebra and dom == fav and vivo:
                return 7, "GOL_ZEBRA_ABRIU_JOGO"
            if lado == fav and dom != lado and vivo:
                return 3, "FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA"
            if lado == dom and not vivo:
                return -6, "GOL_MATOU_RITMO"

    if minutos > 10 and lado != "DESCONHECIDO" and dom != lado and vivo:
        return 3, "PRESSAO_POS_GOL_VALIDADA"

    return 0, "GOL_ANTIGO_OK"

def fake_pressure_penalty_gol(metricas):
    ap = sum(metricas["ataques_perigosos"])
    rb = sum(metricas["remates_baliza"])
    rl = sum(metricas["remates_lado"])
    rda = sum(metricas.get("remates_dentro_area", (0, 0)))
    u5 = sum(metricas["ultimos5"])
    u10 = sum(metricas["ultimos10"])
    xg = sum(metricas.get("xg", (0, 0)))
    xgi = sum(metricas.get("xgi", (0, 0)))
    cant = sum(metricas["cantos"])
    hc, hf = metricas.get("heatmap", (0, 0))
    padrao = metricas.get("padrao_alfa", "")

    pen = 0

    if ap >= 25 and rb <= 1:
        pen -= 8
    if ap >= 18 and rb == 0:
        pen -= 12
    if ap >= 25 and rl <= 2 and rb <= 1:
        pen -= 6
    if u10 <= 3 and u5 <= 1:
        pen -= 8
    if ap >= 25 and xg <= 0.20:
        pen -= 8
    if ap >= 25 and xg <= 0.35 and xgi <= 0.60 and rb <= 1:
        pen -= 6
    if cant >= 4 and rb <= 1 and xg <= 0.35:
        pen -= 6
    if rda == 0 and rb <= 1 and ap >= 20:
        pen -= 5

    # Fake pressure sofisticada: território/heatmap/ataques sem ruptura.
    if ap >= 35 and rb == 0 and rda == 0 and xg < 0.35:
        pen -= 12
    if abs(hc - hf) >= 18 and rb <= 1 and rda == 0 and xg < 0.35:
        pen -= 6
    if u5 <= 2 and u10 <= 6 and ap >= 30 and rb <= 1:
        pen -= 6

    if padrao in ["ALFA_FT_DIAMANTE", "ALFA_HT_DIAMANTE", "ALFA_FT_FORTE", "ALFA_HT_FORTE", "ALFA_FT_BASE", "ALFA_HT_BASE"]:
        pen = int(pen * 0.55)

    return pen


def score_delta_confirmacao(metricas, estrategia, chave_jogo):
    if not eh_confirmacao(estrategia):
        return 0, "SEM_DELTA"

    leitura_anterior = ultimas_leituras_por_jogo.get(chave_jogo, {})
    ant = leitura_anterior.get("metricas", {})
    if not ant:
        return 0, "SEM_LEITURA_ANTERIOR"

    idade = time.time() - leitura_anterior.get("recebido_em", 0)
    if idade > MEMORIA_CONFIRMACAO_SEGUNDOS:
        return 0, "LEITURA_ANTERIOR_EXPIRADA"

    u5_atual = soma_lados(metricas, "ultimos5")
    u10_atual = soma_lados(metricas, "ultimos10")
    u5_ant = soma_lados(ant, "ultimos5")
    u10_ant = soma_lados(ant, "ultimos10")

    rb_atual = soma_lados(metricas, "remates_baliza")
    rb_ant = soma_lados(ant, "remates_baliza")
    rda_atual = soma_lados(metricas, "remates_dentro_area")
    rda_ant = soma_lados(ant, "remates_dentro_area")
    cantos_atual = soma_lados(metricas, "cantos")
    cantos_ant = soma_lados(ant, "cantos")

    p = metricas.get("pressao_alfa", {})
    pa = ant.get("pressao_alfa", {})
    nivel_atual = pressao_sustentada_nivel(metricas, estrategia)
    ip_pico_atual = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))
    ip_pico_ant = max(pa.get("ip_pico_casa", 0), pa.get("ip_pico_fora", 0))
    c18_atual = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    c18_ant = max(pa.get("ip_consec_18_casa", 0), pa.get("ip_consec_18_fora", 0))
    c22_atual = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))
    c22_ant = max(pa.get("ip_consec_22_casa", 0), pa.get("ip_consec_22_fora", 0))

    score = 0

    # Confirmação institucional: não basta o acumulado subir.
    # Precisa existir melhora real no RECENTE ou produção ofensiva nova com pressão ainda viva.
    recente_melhorou = u5_atual >= u5_ant + 2 or u10_atual >= u10_ant + 3
    pressao_manteve_forte = nivel_atual in ["FORTE", "ELITE", "COLAPSO"] and u10_atual >= 7
    pressao_subiu = ip_pico_atual >= ip_pico_ant + 4 or c18_atual > c18_ant or c22_atual > c22_ant
    producao_subiu = rb_atual > rb_ant or rda_atual > rda_ant or cantos_atual > cantos_ant
    producao_do_lado_certo = consequencia_ofensiva_lado(metricas, estrategia, lado_pressao_principal(metricas, estrategia))

    if recente_melhorou:
        score += 5
    if pressao_subiu:
        score += 3
    if pressao_manteve_forte:
        score += 3
    if producao_subiu and (u5_atual >= 3 or u10_atual >= 7):
        score += 4
    if producao_do_lado_certo:
        score += 2

    if u5_atual <= max(1, u5_ant - 2):
        score -= 5
    if u10_atual <= max(1, u10_ant - 3):
        score -= 5
    if not producao_do_lado_certo and not pressao_subiu:
        score -= 3

    if score >= 8 and (recente_melhorou or pressao_subiu or (producao_subiu and pressao_manteve_forte)):
        return score, "CONFIRMACAO_MELHOROU"

    if score <= -4:
        return score, "CONFIRMACAO_PIOROU"

    return score, "CONFIRMACAO_ESTAVEL"


def ajuste_confianca_gol(metricas, estrategia, score):
    rb = sum(metricas["remates_baliza"])
    rda = sum(metricas.get("remates_dentro_area", (0, 0)))
    ap = sum(metricas["ataques_perigosos"])
    u10 = sum(metricas["ultimos10"])
    u5 = sum(metricas["ultimos5"])
    xg = sum(metricas.get("xg", (0, 0)))
    liga = classificar_liga(metricas.get("competicao", ""))
    padrao = metricas.get("padrao_alfa", "")

    red = 0.55 if padrao in ["ALFA_FT_DIAMANTE", "ALFA_HT_DIAMANTE", "ALFA_FT_FORTE", "ALFA_HT_FORTE", "ALFA_FT_BASE", "ALFA_HT_BASE"] else 1

    if score >= 88 and rb <= 1:
        score -= int(10 * red)
    if score >= 85 and rb <= 1 and rda <= 1:
        score -= int(7 * red)

    if eh_ft(estrategia) and metricas["tempo"] >= 81 and u5 <= 2:
        score -= 8
    if eh_ft(estrategia) and rb == 0:
        score -= int(8 * red)

    if ap < 18:
        score -= 5
    if u10 < 5:
        score -= 5
    if xg <= 0.15 and rb <= 1:
        score -= int(8 * red)

    if liga == "UNDER" and score >= 84:
        score -= 6

    if liga == "PERIGOSA" and score >= 84:
        score -= 8

    return score


def calcular_forca_premium_gol(metricas):
    apc, apf = metricas["ataques_perigosos"]
    atc, atf = metricas["ataques"]
    rbc, rbf = metricas["remates_baliza"]
    rdac, rdaf = metricas.get("remates_dentro_area", (0, 0))
    u5c, u5f = metricas["ultimos5"]
    u10c, u10f = metricas["ultimos10"]
    chc, chf = metricas.get("chance_golo", (0, 0))
    hc, hf = metricas.get("heatmap", (0, 0))
    xgc, xgf = metricas.get("xg", (0, 0))
    p = metricas.get("pressao_alfa", {})

    pts = 0

    if max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0)) >= 3:
        pts += 2
    if max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0)) >= 3:
        pts += 1
    if abs(apc - apf) >= 14:
        pts += 1
    if abs(atc - atf) >= 18:
        pts += 1
    if abs(rbc - rbf) >= 3 and rbc + rbf >= 3:
        pts += 1
    if rdac + rdaf >= 3:
        pts += 1
    if abs(u10c - u10f) >= 6 and u10c + u10f >= 8:
        pts += 1
    if abs(u5c - u5f) >= 4:
        pts += 1
    if abs(chc - chf) >= 5:
        pts += 1
    if xgc + xgf >= 0.55:
        pts += 1
    if abs(hc - hf) >= 22:
        pts += 1

    return pts


def modelo_herta_aprovado(metricas, estrategia):
    p = metricas.get("pressao_alfa", {})

    c15 = max(p.get("ip_consec_15_casa", 0), p.get("ip_consec_15_fora", 0))
    c18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    c22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))
    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))

    u5c, u5f = metricas.get("ultimos5", (0, 0))
    u10c, u10f = metricas.get("ultimos10", (0, 0))
    u5 = u5c + u5f
    u10 = u10c + u10f

    apc, apf = metricas.get("ataques_perigosos", (0, 0))
    rbc, rbf = metricas.get("remates_baliza", (0, 0))
    rlc, rlf = metricas.get("remates_lado", (0, 0))
    rdac, rdaf = metricas.get("remates_dentro_area", (0, 0))
    chc, chf = metricas.get("chance_golo", (0, 0))
    cc, cf = metricas.get("cantos", (0, 0))
    xgc, xgf = metricas.get("xg", (0.0, 0.0))

    rb = rbc + rbf
    rl = rlc + rlf
    rda = rdac + rdaf
    chance_max = max(chc, chf)
    xg = xgc + xgf
    dif_ap = abs(apc - apf)
    dif_u5 = abs(u5c - u5f)
    dif_u10 = abs(u10c - u10f)
    cant = cc + cf

    dom, _ = lado_dominante(metricas)

    placar_casa, placar_fora = extrair_gols_placar(metricas.get("placar", ""))
    empate_ou_apertado = True
    if placar_casa is not None:
        empate_ou_apertado = abs(placar_casa - placar_fora) <= 1

    pressao_sustentada = c15 >= 5
    pressao_alta = c18 >= 3 or c22 >= 2 or ip_pico >= 24
    recente_vivo = (u5 >= 5 and u10 >= 8) or (eh_ht(estrategia) and u5 >= 4 and u10 >= 7)
    direcional = dif_u5 >= 4 or dif_u10 >= 6 or dif_ap >= 12 or dom in ["CASA", "FORA"]
    producao = rb >= 3 or chance_max >= 10 or rda >= 3 or rl >= 7 or xg >= 0.50 or cant >= 5
    contexto = empate_ou_apertado or metricas.get("motivo_gol_contextual") in [
        "GOL_ZEBRA_CONTRA_FLUXO",
        "GOL_TIME_PRESSIONADO_ABRIU_JOGO",
        "GOL_CONTRA_FLUXO_CONTEXTO_BOM",
        "FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA",
    ]

    if eh_ft(estrategia):
        return pressao_sustentada and recente_vivo and direcional and producao and contexto

    if eh_ht(estrategia):
        return pressao_sustentada and (recente_vivo or pressao_alta) and (direcional or producao) and producao

    return False


def aplicar_travas_herta(metricas, estrategia, score):
    p = metricas.get("pressao_alfa", {})

    c15 = max(p.get("ip_consec_15_casa", 0), p.get("ip_consec_15_fora", 0))
    u5 = sum(metricas.get("ultimos5", (0, 0)))
    u10 = sum(metricas.get("ultimos10", (0, 0)))
    rb = sum(metricas.get("remates_baliza", (0, 0)))
    rda = sum(metricas.get("remates_dentro_area", (0, 0)))
    xg = sum(metricas.get("xg", (0.0, 0.0)))
    chance_max = max(metricas.get("chance_golo", (0, 0)))
    motivo = metricas.get("motivo_gol_contextual", "")

    if not modelo_herta_aprovado(metricas, estrategia):
        score = min(score, 84)

    if c15 < 5:
        score = min(score, 82)

    if eh_ft(estrategia) and (u5 < 5 or u10 < 8):
        score = min(score, 83)

    if rb <= 1 and chance_max < 10 and xg < 0.45:
        score = min(score, 82)

    if rb == 0 and rda <= 1:
        score = min(score, 78)

    if motivo in ["PRESSAO_PREMIADA_RECENTE", "PRESSAO_PREMIADA_MORREU", "GOL_MATOU_RITMO"]:
        score = min(score, 78)

    if score >= 92:
        if not (modelo_herta_aprovado(metricas, estrategia) and (u5 >= 7 or chance_max >= 14 or rb >= 5 or xg >= 0.75)):
            score = min(score, 91)

    return score


def aplicar_travas_ht_elite(metricas, estrategia, score):
    if not eh_ht(estrategia):
        return score

    tempo = metricas.get("tempo", 0)
    p = metricas.get("pressao_alfa", {})

    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))
    c15 = max(p.get("ip_consec_15_casa", 0), p.get("ip_consec_15_fora", 0))
    c18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
    c22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))

    rb_total = sum(metricas.get("remates_baliza", (0, 0)))
    rl_total = sum(metricas.get("remates_lado", (0, 0)))
    remates_total = rb_total + rl_total
    rda_total = sum(metricas.get("remates_dentro_area", (0, 0)))
    cantos_total = sum(metricas.get("cantos", (0, 0)))
    u5_total = sum(metricas.get("ultimos5", (0, 0)))
    u10_total = sum(metricas.get("ultimos10", (0, 0)))
    ap_casa, ap_fora = metricas.get("ataques_perigosos", (0, 0))
    dif_ap = abs(ap_casa - ap_fora)
    xg_total = sum(metricas.get("xg", (0.0, 0.0)))
    chance_max = max(metricas.get("chance_golo", (0, 0)))

    odds_validas = [o for o in metricas.get("odds", (0, 0, 0)) if o and o > 0]
    odd_favorito = min(odds_validas, default=99)

    super_favorito = odd_favorito <= 1.60

    finalizacao_real = (
        rb_total >= 1
        or remates_total >= 4
        or xg_total >= 0.35
        or rda_total >= 2
    )

    finalizacao_forte_ht = (
        rb_total >= 2
        or remates_total >= 6
        or xg_total >= 0.50
        or rda_total >= 3
    )

    pressao_sustentada = (
        c15 >= 5
        or c18 >= 4
        or c22 >= 2
    )

    pressao_cruzeiro = (
        ip_pico >= 30
        and c18 >= 4
        and c15 >= 5
        and u10_total >= 7
        and (
            rb_total >= 2
            or remates_total >= 5
            or cantos_total >= 4
            or xg_total >= 0.45
        )
    )

    colapso_extremo = (
        ip_pico >= 35
        and c22 >= 2
        and u5_total >= 4
        and finalizacao_real
    )

    dominio_real = (
        dif_ap >= 10
        or cantos_total >= 4
        or u10_total >= 8
        or chance_max >= 8
    )

    # HT sem ameaça real não pode virar elite.
    if rb_total == 0 and remates_total <= 2 and xg_total < 0.30:
        score = min(score, 84)

    # HT com pressão bonita, mas sem finalização suficiente.
    if score >= 88 and not finalizacao_real:
        score = min(score, 86)

    # Antes dos 22 minutos, só deixa subir muito se for absurdo.
    if tempo < 22 and not colapso_extremo:
        score = min(score, 86)

    # Antes dos 25 minutos, precisa de finalização + pressão sustentada.
    if tempo < 25 and not (pressao_sustentada and finalizacao_real):
        score = min(score, 88)

    # HT 90+ só com super favorito dominante ou padrão Cruzeiro/colapso.
    if score >= 90:
        libera_90 = (
            (super_favorito and pressao_sustentada and finalizacao_real and dominio_real)
            or pressao_cruzeiro
            or colapso_extremo
        )

        if not libera_90:
            score = min(score, 89)

    # HT 92+ só no padrão Cruzeiro/colapso.
    if score >= 92:
        libera_92 = (
            pressao_cruzeiro
            or colapso_extremo
            or (super_favorito and pressao_sustentada and finalizacao_forte_ht and dominio_real)
        )

        if not libera_92:
            score = min(score, 91)

    # Teto máximo realista para HT.
    if score >= 96:
        score = min(score, 96)

    return score


def aplicar_trava_consequencia_ofensiva(metricas, estrategia, score):
    nivel = pressao_sustentada_nivel(metricas, estrategia)
    lado_principal = lado_pressao_principal(metricas, estrategia)
    tem_consequencia_total = consequencia_ofensiva_total(metricas, estrategia)
    tem_consequencia_lado = consequencia_ofensiva_lado(metricas, estrategia, lado_principal)

    rb = soma_lados(metricas, "remates_baliza")
    rl = soma_lados(metricas, "remates_lado")
    rda = soma_lados(metricas, "remates_dentro_area")
    cantos = soma_lados(metricas, "cantos")
    xg = soma_lados(metricas, "xg", (0.0, 0.0))

    metricas["nivel_pressao_sustentada"] = nivel
    metricas["lado_pressao_principal"] = lado_principal
    metricas["tem_consequencia_ofensiva"] = tem_consequencia_total
    metricas["tem_consequencia_lado_certo"] = tem_consequencia_lado

    # Sem consequência em lugar nenhum: trava dura.
    if not tem_consequencia_total:
        return min(score, 84 if eh_ht(estrategia) else 82)

    # Consequência existe no jogo, mas não no lado que está pressionando/precisa do gol.
    # Isso evita misturar IP de um lado com remate do outro.
    if not tem_consequencia_lado:
        return min(score, 85 if eh_ht(estrategia) else 81)

    # IP forte com pouca consequência ainda não é elite.
    if nivel in ["FORTE", "ELITE", "COLAPSO"]:
        if rb == 0 and rda == 0 and cantos == 0 and xg < 0.35:
            return min(score, 82 if eh_ft(estrategia) else 84)
        if score >= 88 and rb == 0 and (rl < 4 or xg < 0.35):
            return min(score, 86)

    return score


def aplicar_trava_liga_under(metricas, estrategia, score):
    liga = classificar_liga(metricas.get("competicao", ""))
    if liga not in ["UNDER", "PERIGOSA"]:
        return score

    nivel = metricas.get("nivel_pressao_sustentada") or pressao_sustentada_nivel(metricas, estrategia)
    tem_consequencia = metricas.get("tem_consequencia_ofensiva", consequencia_ofensiva_total(metricas, estrategia))
    u5 = soma_lados(metricas, "ultimos5")
    u10 = soma_lados(metricas, "ultimos10")
    rb = soma_lados(metricas, "remates_baliza")
    rda = soma_lados(metricas, "remates_dentro_area")
    xg = soma_lados(metricas, "xg", (0.0, 0.0))
    gc, gf = extrair_gols_placar(metricas.get("placar", ""))
    aberto = True if gc is None else abs(gc - gf) <= 1

    prova_forte = (
        nivel in ["FORTE", "ELITE", "COLAPSO"]
        and tem_consequencia
        and u5 >= 4
        and u10 >= 8
        and (rb >= 2 or rda >= 3 or xg >= 0.55)
        and aberto
    )

    if not prova_forte:
        return min(score, 82 if eh_ft(estrategia) else 84)

    # Mesmo quando passa, liga under não deve virar diamante fácil.
    return min(score, 88 if eh_ft(estrategia) else 89)


def aplicar_trava_ft_jogo_morto(metricas, estrategia, score):
    if not eh_ft(estrategia):
        return score

    gc, gf = extrair_gols_placar(metricas.get("placar", ""))
    if gc is None:
        return score

    dif = abs(gc - gf)
    perdendo = lado_perdendo(metricas)
    vencendo = lado_vencendo(metricas)
    motivo = metricas.get("motivo_gol_contextual", "")

    if dif < 2 or perdendo in ["EMPATE", "DESCONHECIDO"]:
        return score

    dados_perdendo = dados_lado(metricas, perdendo)
    dados_vencendo = dados_lado(metricas, vencendo)

    perdedor_vivo = (
        dados_perdendo["u5"] >= 3
        or dados_perdendo["u10"] >= 6
        or dados_perdendo["rb"] >= 1
        or dados_perdendo["rda"] >= 2
        or dados_perdendo["cantos"] >= 2
        or dados_perdendo["xg"] >= 0.35
    )

    perdedor_convertivel = (
        dados_perdendo["rb"] >= 1
        or dados_perdendo["remates"] >= 4
        or dados_perdendo["rda"] >= 2
        or dados_perdendo["cantos"] >= 2
        or dados_perdendo["xg"] >= 0.35
    )

    # 2x0/3x0 confortável: se quem precisa do gol não cria perigo real, trava.
    if not perdedor_vivo or not perdedor_convertivel:
        return min(score, 80)

    # Se o perdedor tem só circulação/posse sem chute/área, ainda é fake pressure.
    if dados_perdendo["rb"] == 0 and dados_perdendo["rda"] == 0 and dados_perdendo["xg"] < 0.30:
        return min(score, 81)

    # Se o time que vence ainda concentra a pressão, é mais controle que caos.
    if dados_vencendo["u5"] > dados_perdendo["u5"] and dados_perdendo["rb"] == 0 and dados_perdendo["rda"] == 0:
        return min(score, 81)

    # Exceção: gol contra fluxo pode abrir o jogo, mas mesmo assim não vira diamante automático.
    if motivo in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO", "GOL_CONTRA_FLUXO_CONTEXTO_BOM", "GOL_ZEBRA_ABRIU_JOGO"]:
        return min(score, 90)

    return score


def aplicar_trava_pressao_antiga(metricas, estrategia, score):
    if pressao_recente_caiu(metricas):
        if eh_ht(estrategia):
            return min(score, 85)
        return min(score, 82)
    return score


def teto_contextual_gol(metricas, estrategia, score):
    tempo = metricas["tempo"]
    placar = metricas["placar"]

    apc, apf = metricas["ataques_perigosos"]
    rbc, rbf = metricas["remates_baliza"]
    cc, cf = metricas["cantos"]
    u10c, u10f = metricas["ultimos10"]
    xgc, xgf = metricas.get("xg", (0, 0))
    rdac, rdaf = metricas.get("remates_dentro_area", (0, 0))

    rb = rbc + rbf
    rda = rdac + rdaf
    u10 = u10c + u10f
    xg = xgc + xgf
    dif_ap = abs(apc - apf)
    dif_rb = abs(rbc - rbf)
    dif_c = abs(cc - cf)

    gc, gf = extrair_gols_placar(placar)
    dif_placar = abs(gc - gf) if gc is not None else 0

    ug = metricas["ultimo_gol"]
    minutos = tempo - ug if ug > 0 else 999

    forca = calcular_forca_premium_gol(metricas)
    motivo = metricas.get("motivo_gol_contextual", "")
    liga = classificar_liga(metricas.get("competicao", ""))
    padrao = metricas.get("padrao_alfa", "")
    delta = metricas.get("motivo_delta_contextual", "")

    premium = forca >= 5 and minutos > 2
    bom = u10 >= 8 or dif_ap >= 10 or rb >= 3 or xg >= 0.45 or rda >= 3
    forte = padrao in ["ALFA_FT_DIAMANTE", "ALFA_HT_DIAMANTE", "ALFA_FT_FORTE", "ALFA_HT_FORTE", "ALFA_FT_BASE", "ALFA_HT_BASE"]

    if minutos < 0:
        score = min(score, 75)

    elif minutos <= 2:
        if motivo in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO"]:
            score = min(score, 92)
        elif motivo in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA_5M", "TRAVA_CONFIRMACAO_GOL_RECENTE_5M", "TRAVA_CONFIRMACAO_PRESSAO_PREMIADA"]:
            score = min(score, 72)
        elif motivo == "PRESSAO_PREMIADA_RECENTE":
            score = min(score, 76)
        else:
            score = min(score, 78)

    elif minutos <= 5:
        if motivo in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO"]:
            score = min(score, 92)
        elif motivo in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA_5M", "TRAVA_CONFIRMACAO_GOL_RECENTE_5M"]:
            score = min(score, 72)
        elif motivo in ["PRESSAO_PREMIADA_MORREU", "PRESSAO_PREMIADA_RECENTE"]:
            score = min(score, 78)
        elif motivo == "PRESSAO_PREMIADA_MAS_VIVA":
            score = min(score, 84)
        else:
            score = min(score, 82)

    if rb <= 1:
        score = min(score, 84 if forte else 82)

    if rb == 0 and rda <= 1:
        score = min(score, 80 if forte else 78)

    if dif_ap <= 5 and dif_rb <= 1 and dif_c <= 1:
        score = min(score, 83 if forte else 81)

    if xg <= 0.25 and rb <= 1:
        score = min(score, 80 if forte else 78)

    if liga == "UNDER":
        score = min(score, 86 if (premium or forte) else 82)

    if liga == "PERIGOSA":
        score = min(score, 84 if (premium or forte) else 80)

    if delta == "CONFIRMACAO_PIOROU":
        score = min(score, 78)

    if eh_confirmacao(estrategia):
        if premium and delta == "CONFIRMACAO_MELHOROU":
            score = min(score, 94)
        elif premium or forte:
            score = min(score, 90)
        elif bom:
            score = min(score, 86)
        else:
            score = min(score, 80)
    else:
        if premium or forte:
            score = min(score, 92)
        elif bom:
            score = min(score, 87)
        else:
            score = min(score, 81)

    if eh_ft(estrategia) and dif_placar >= 3 and not pressao_viva(metricas):
        score = min(score, 78)

    return score


def score_gol(metricas, estrategia, chave_jogo):
    """Score institucional por funil.

    Ordem oficial:
    RADAR -> VALIDACAO -> TRAVAS -> CLASSIFICACAO.
    O score não pode aprovar jogo ruim por soma livre.
    """
    metricas["lado_favorito"] = lado_favorito(metricas)[0]
    metricas["lado_zebra"] = lado_zebra(metricas)
    metricas["lado_dominante"] = lado_dominante(metricas)[0]
    metricas["lado_vermelho"] = lado_com_vermelho(metricas)
    metricas["nivel_pressao_sustentada"] = pressao_sustentada_nivel(metricas, estrategia)

    # Mantém compatibilidade de logs/mensagens antigas.
    _, padrao = score_padrao_alfa(metricas, estrategia)
    metricas["padrao_alfa"] = padrao

    aprovado_funil, teto_funil, motivo_funil = funil_institucional_gol(metricas, estrategia, chave_jogo)
    metricas["funil_aprovado"] = aprovado_funil
    metricas["motivo_funil"] = motivo_funil

    # Contextos antigos continuam preenchidos para compatibilidade, mas não comandam aprovação.
    ag, mg = ajuste_gol_recente_contextual(metricas, estrategia)
    metricas["motivo_gol_contextual"] = mg
    av, mv = ajuste_vermelho_contextual(metricas)
    metricas["motivo_vermelho_contextual"] = mv
    if "motivo_delta_contextual" not in metricas:
        d, md = score_delta_confirmacao(metricas, estrategia, chave_jogo)
        metricas["motivo_delta_contextual"] = md
    metricas["bonus_bot_confianca"] = 0
    metricas["tem_consequencia_ofensiva"] = consequencia_ofensiva_lado(metricas, estrategia, metricas.get("lado_alvo", "DESCONHECIDO"))

    if not aprovado_funil:
        # Bloqueio institucional:
        # não importa a estatística bonita, o jogo não provou vida real.
        #
        # Importante:
        # se o motivo for "gol recente do dominante/favorito", a entrada principal
        # NÃO pode passar só porque o teto ficou acima do corte. O jogo fica salvo
        # em memória para possível confirmação posterior, mas não vai ao canal principal.
        motivo_bloqueio = str(motivo_funil or "")
        if motivo_bloqueio in [
            "MASSACRE_POS_GOL_AGUARDAR_CONFIRMACAO",
            "PRESSAO_PREMIADA_BLOQUEIO",
            "GOL_MUITO_RECENTE_CAUTELA",
            "GOL_INCONSISTENTE_BLOQUEIO",
        ]:
            score = 79
        elif eh_confirmacao(estrategia):
            score = min(teto_funil, 81)
        else:
            score = min(teto_funil, 79 if perfil_origem_estrategia(estrategia) == "RADAR_ABERTO" else 81)

        if not tempo_operacional_valido(metricas, estrategia):
            score = min(score, 72)

        return int(max(0, min(score, 99))), metricas

    score = score_classificacao_institucional(metricas, estrategia)

    # Travas finais mantidas como proteção extra. Elas só reduzem, nunca aprovam.
    score = aplicar_trava_consequencia_ofensiva(metricas, estrategia, score)
    score = aplicar_trava_pressao_antiga(metricas, estrategia, score)
    score = aplicar_trava_ft_jogo_morto(metricas, estrategia, score)
    score = aplicar_trava_liga_under(metricas, estrategia, score)
    score = aplicar_travas_ht_elite(metricas, estrategia, score)

    if not tempo_operacional_valido(metricas, estrategia):
        score = min(score, 72)

    return int(max(0, min(score, 99))), metricas

# =========================================================
# CANTO MANTIDO APENAS PARA LOG INTERNO
# =========================================================

def score_canto(metricas, estrategia, chave_jogo):
    return 0


def tipo_pressao(metricas, gol, canto, estrategia=None):
    corte = corte_gol_estrategia(estrategia or "")
    if gol >= corte:
        return "GOL"
    if pressao_viva(metricas):
        return "OBSERVACAO"
    return "BLOQUEIO"


def calcular_scores(texto, estrategia, chave_jogo):
    metricas = extrair_metricas(texto)
    gol, metricas = score_gol(metricas, estrategia, chave_jogo)
    canto = score_canto(metricas, estrategia, chave_jogo)
    tipo = tipo_pressao(metricas, gol, canto, estrategia)

    metricas["score_gol"] = gol
    metricas["score_canto"] = canto
    metricas["tipo_pressao"] = tipo

    return gol, canto, tipo, metricas


# =========================================================
# PRIORIDADE / CONFIRMAÇÃO / DECISÃO
# =========================================================

def prioridade_estrategia(estrategia):
    if estrategia in ["ARCE_HT", "CHAMA_FT"]:
        return 3
    if estrategia in ["ALFA_HT", "ALFA_FT"]:
        return 2
    if estrategia in ["ALFA_HT_CONFIRMACAO", "ALFA_FT_CONFIRMACAO"]:
        return 1
    return 0


def alerta_passou_corte(alerta):
    estrategia = alerta.get("estrategia", "")
    return alerta.get("score_gol", 0) >= corte_gol_estrategia(estrategia)


def prioridade_alerta(alerta):
    estrategia = alerta.get("estrategia", "")
    score = alerta.get("score_gol", 0)
    corte = corte_gol_estrategia(estrategia)

    # Ordem institucional:
    # 1) primeiro quem realmente passou o funil/corte;
    # 2) depois quem ficou mais acima do próprio corte;
    # 3) depois score bruto;
    # 4) ARCE/CHAMA entram como desempate/peso de confiança, não como passe livre;
    # 5) por último o minuto.
    return (
        1 if score >= corte else 0,
        score - corte,
        score,
        prioridade_estrategia(estrategia),
        alerta.get("metricas", {}).get("tempo", 0),
    )


def confirmacao_melhorou_forte(alerta):
    if not eh_confirmacao(alerta.get("estrategia", "")):
        return False

    metricas = alerta.get("metricas", {})
    delta_ctx = metricas.get("motivo_delta_contextual", "")
    motivo_gol = metricas.get("motivo_gol_contextual", "")
    motivo_pos_gol = metricas.get("motivo_pos_gol_institucional", "")
    score_gol = alerta.get("score_gol", 0)

    # Regra oficial:
    # confirmação forte pode voltar ao CANAL PRINCIPAL quando o jogo que foi
    # abortado por gol recente continuou amassando depois do gol.
    corte = corte_gol_estrategia(alerta.get("estrategia", ""))

    if motivo_pos_gol == "CONFIRMACAO_VALIDOU_MASSACRE_POS_GOL" and score_gol >= corte:
        return True

    # Exceção oficial:
    # se o time pressionado/zebra marcou contra o fluxo,
    # o jogo abriu e pode ir ao canal principal.
    if motivo_gol in [
        "GOL_TIME_PRESSIONADO_ABRIU_JOGO",
        "GOL_ZEBRA_CONTRA_FLUXO",
        "GOL_CONTRA_FLUXO_CONTEXTO_BOM",
        "GOL_ZEBRA_ABRIU_JOGO",
        "GOL_EMPATE_FT_ABRIU_VIRADA",
    ] and score_gol >= corte:
        return True

    # Confirmação normal só vai ao principal se realmente melhorou.
    # Não usamos corte fixo de HT aqui para não matar confirmação FT boa.
    if delta_ctx == "CONFIRMACAO_MELHOROU" and score_gol >= corte:
        return True

    return False


def melhor_alerta(alertas):
    # Não escolher ARCE/CHAMA cegamente se outro alerta passou melhor pelo funil.
    # ARCE/CHAMA já recebem bônus no score e ainda servem como desempate.
    return sorted(alertas, key=prioridade_alerta, reverse=True)[0]


def ja_enviado_recentemente(chave_envio):
    limpar_memoria_interna()
    agora = time.time()
    return chave_envio in ultimos_enviados and agora - ultimos_enviados[chave_envio] <= COOLDOWN_SEGUNDOS


def marcar_enviado(chave_envio):
    ultimos_enviados[chave_envio] = time.time()


def salvar_ultima_leitura(chave_jogo, alerta):
    ultimas_leituras_por_jogo[chave_jogo] = {
        "estrategia": alerta["estrategia"],
        "metricas": alerta["metricas"],
        "score_gol": alerta.get("score_gol", 0),
        "score_canto": alerta.get("score_canto", 0),
        "tipo_pressao": alerta.get("tipo_pressao", "BLOQUEIO"),
        "recebido_em": time.time(),
    }


# =========================================================
# MENSAGENS
# =========================================================

def faixa_publica(score):
    if score >= 92:
        return f"💎 ALFA — ENTRADA DIAMANTE ({score}%)"
    if score >= 85:
        return f"🔥 ALFA — ENTRADA VALIDADA ({score}%)"
    if score >= 80:
        return f"✅ ALFA — ENTRADA APROVADA ({score}%)"
    return "⚠️ ALFA — OBSERVAÇÃO INTERNA"


def nome_publico_bot(estrategia):
    return {
        "ALFA_HT": "ALFA HT",
        "ALFA_HT_CONFIRMACAO": "ALFA HT CONFIRMAÇÃO",
        "ARCE_HT": "ALFA HT",
        "ALFA_FT": "ALFA FT",
        "ALFA_FT_CONFIRMACAO": "ALFA FT CONFIRMAÇÃO",
        "CHAMA_FT": "ALFA FT",
    }.get(estrategia, estrategia)


def texto_liga_publico(metricas):
    liga = classificar_liga(metricas.get("competicao", ""))
    if liga == "PREMIUM":
        return "🟢 Liga Premium"
    if liga == "MODERADA":
        return "🟡 Liga Moderada"
    if liga == "UNDER":
        return "🟠 Liga Under"
    if liga == "PERIGOSA":
        return "🔴 Liga Perigosa"
    return "⚪ Liga Neutra"


def texto_contexto_gol(metricas):
    motivo = metricas.get("motivo_gol_contextual", "")
    verm = metricas.get("motivo_vermelho_contextual", "")
    delta = metricas.get("motivo_delta_contextual", "")
    padrao = metricas.get("padrao_alfa", "")

    textos = []

    if padrao in ["ALFA_FT_DIAMANTE", "ALFA_HT_DIAMANTE", "ALFA_FT_FORTE", "ALFA_HT_FORTE"]:
        textos.append("🧠 Padrão ALFA: IP sustentado + Chance de Gol dentro da base histórica.")
    elif padrao in ["ALFA_FT_BASE", "ALFA_HT_BASE", "ALFA_FT_PRESSAO_SEM_CHANCE", "ALFA_HT_PRESSAO_SEM_CHANCE", "ALFA_OBSERVACAO_SEM_CONTINUIDADE"]:
        textos.append("⚠️ Padrão ALFA: contexto forte, mas com ressalva em algum ponto da régua.")

    if motivo in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO"]:
        textos.append("🔥 Contexto extra: gol contra o fluxo aumentou a urgência ofensiva.")
    elif motivo == "GOL_CONTRA_FLUXO_CONTEXTO_BOM":
        textos.append("🔥 Contexto extra: time pressionado marcou, mas o jogo segue aberto.")
    elif motivo == "PRESSAO_PREMIADA_MAS_VIVA":
        textos.append("⚠️ Contexto: pressão já foi premiada, mas segue viva.")
    elif motivo == "FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA":
        textos.append("🔥 Contexto extra: gol anterior não matou o jogo; adversário segue pressionando.")
    elif motivo == "GOL_EMPATE_FT_ABRIU_VIRADA":
        textos.append("🔥 Contexto extra: empate tardio com pressão viva manteve cenário de virada.")
    elif motivo in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA_5M", "TRAVA_CONFIRMACAO_GOL_RECENTE_5M"]:
        textos.append("⛔ Confirmação travada/rebaixada por gol recente.")

    if delta == "CONFIRMACAO_MELHOROU":
        textos.append("📈 Confirmação: pressão aumentou em relação ao primeiro gatilho.")
    elif delta == "CONFIRMACAO_PIOROU":
        textos.append("⚠️ Confirmação: pressão caiu em relação ao primeiro gatilho.")

    if verm == "VERMELHO_ABRIU_PRESSAO":
        textos.append("🟥 Contexto extra: superioridade numérica reforça a pressão ofensiva.")

    if metricas.get("bonus_bot_confianca", 0) > 0:
        textos.append("⭐ Bot de confiança: ARCE/CHAMA reforçou a leitura do sinal.")

    return "\n" + "\n".join(textos) if textos else ""


def montar_mensagem_gol(jogo, estrategia, score, metricas):
    ctx = texto_contexto_gol(metricas)
    link = f"\n🔗 Bet365: {html.escape(metricas['bet365'])}" if metricas.get("bet365") else ""

    nome_bot = html.escape(nome_publico_bot(estrategia))
    titulo = "🔁 ENTRADA CONFIRMADA" if eh_confirmacao(estrategia) else "🔥 ENTRADA VALIDADA"
    motivo_pos_gol = metricas.get("motivo_pos_gol_institucional", "")
    if motivo_pos_gol == "CONFIRMACAO_VALIDOU_MASSACRE_POS_GOL":
        titulo = "🔁 ENTRADA CONFIRMADA"

    leitura_base = "Pressão viva, domínio ofensivo e contexto aberto."
    if motivo_pos_gol == "CONFIRMACAO_VALIDOU_MASSACRE_POS_GOL":
        leitura_base = "Gol recente já foi absorvido. O favorito/dominante continuou pressionando forte."
    elif metricas.get("motivo_funil") == "FUNIL_APROVADO":
        leitura_base = "Jogo passou pelo funil ALFA: pressão, validação, travas e classificação."

    return f"""{titulo}

⚽ <b>COUTIPS {nome_bot}</b>

🏟 <b>Jogo:</b> {html.escape(str(jogo))}
⏱ <b>Minuto:</b> {metricas['tempo']}'
⚽ <b>Placar:</b> {html.escape(str(metricas['placar']))}

📊 <b>Chance ALFA:</b> {score}%
🎯 <b>Entrada:</b> {html.escape(str(metricas['mercado']))}
💰 <b>Odd mínima:</b> 1.60

🧠 <b>Leitura:</b>
{html.escape(leitura_base)}{ctx}

📌 <b>Gestão:</b>
Entrada padrão — 1% da banca.
Evitar entrada se sair gol antes de apostar.{link}

COUTIPS — leitura ao vivo com pressão, contexto e disciplina."""
def montar_mensagem_canto(jogo, estrategia, score, metricas):
    # Mantido apenas para evitar erro caso algum item antigo esteja na fila.
    return ""


# =========================================================
# FILA / ENVIO
# =========================================================

async def trabalhador_envio():
    log("📤 Fila de envio iniciada.")

    while True:
        item = await fila_envio.get()

        try:
            alerta = item["alerta"]
            mercado = item["mercado"]
            destino_override = item.get("destino")

            if mercado == "GOL":
                mensagem = montar_mensagem_gol(
                    alerta["jogo"],
                    alerta["estrategia"],
                    alerta["score_gol"],
                    alerta["metricas"],
                )
                destino = destino_override or TARGET_CHANNEL
                score_log = alerta["score_gol"]
            else:
                log(f"⚠️ Mercado ignorado na fase atual: {mercado}")
                continue

            await client.send_message(destino, mensagem, parse_mode="html")
            marcar_enviado(item["chave_envio"])

            log(
                f"✅ ENVIADO {mercado} | {alerta['estrategia']} | "
                f"{score_log}% | {alerta['jogo']} | canal={destino}"
            )

            await asyncio.sleep(INTERVALO_ENVIO_SEGUNDOS)

        except FloodWaitError as e:
            log(f"⛔ FLOOD WAIT: aguardando {e.seconds} segundos.")
            await asyncio.sleep(e.seconds + 1)

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
        confirmacoes = [a for a in alertas if eh_confirmacao(a.get("estrategia", ""))]

        mercados = []

        # FASE ATUAL: SOMENTE GOL
        corte_escolhido = corte_gol_estrategia(escolhido["estrategia"])
        if escolhido["score_gol"] >= corte_escolhido:
            mercados.append("GOL")

        if len(alertas) > 1:
            log(
                f"🏆 PRIORIDADE APLICADA | Escolhido: {escolhido['estrategia']} | "
                f"Recebidos: {', '.join(a['estrategia'] for a in alertas)} | Jogo: {escolhido['jogo']}"
            )

        if mercados:
            if eh_confirmacao(escolhido["estrategia"]) and not confirmacao_melhorou_forte(escolhido):
                log(
                    f"🟡 CONFIRMAÇÃO SEM MELHORA FORTE | Não vai ao canal principal | "
                    f"{escolhido['estrategia']} | Gol={escolhido['score_gol']}% | {escolhido['jogo']}"
                )
            else:
                for mercado in mercados:
                    chave_envio = f"{chave_jogo}_{mercado}_MAIN"

                    if ja_enviado_recentemente(chave_envio):
                        log(f"⛔ BLOQUEADO POR COOLDOWN {mercado} | {escolhido['jogo']}")
                        continue

                    await fila_envio.put({
                        "alerta": escolhido,
                        "mercado": mercado,
                        "chave_envio": chave_envio,
                    })
        else:
            log(f"⛔ BLOQUEADO FINAL | Gol={escolhido['score_gol']}% | {escolhido['jogo']}")

        # Confirmações: somente GOL.
        # Se melhorou forte ou gol contra fluxo, vai ao principal.
        # Caso contrário, se passar corte técnico, vai para canal técnico.
        for conf in confirmacoes:
            if conf is escolhido and eh_confirmacao(escolhido["estrategia"]) and confirmacao_melhorou_forte(conf):
                continue

            conf_mercados = []

            corte_conf = corte_gol_estrategia(conf["estrategia"])
            if conf["score_gol"] >= corte_conf:
                conf_mercados.append("GOL")

            if not conf_mercados:
                continue

            destino = TARGET_CHANNEL if confirmacao_melhorou_forte(conf) else CONFIRMATION_CHANNEL
            tipo_destino = "PRINCIPAL" if destino == TARGET_CHANNEL else "TÉCNICO"

            for mercado in conf_mercados:
                chave_envio = f"{chave_jogo}_{mercado}_{conf['estrategia']}_{tipo_destino}"

                if ja_enviado_recentemente(chave_envio):
                    continue

                await fila_envio.put({
                    "alerta": conf,
                    "mercado": mercado,
                    "chave_envio": chave_envio,
                    "destino": destino,
                })

                log(
                    f"📌 CONFIRMAÇÃO PARA CANAL {tipo_destino} | {conf['estrategia']} | "
                    f"{mercado} | Gol={conf['score_gol']}% | {conf['jogo']}"
                )

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
                f"pendentes={len(pendentes_por_jogo)} | cache={len(ultimos_enviados)} | "
                f"leituras={len(ultimas_leituras_por_jogo)}"
            )

        except Exception as e:
            log(f"❌ ERRO NO WATCHDOG: {e}")
            log(traceback.format_exc())


# =========================================================
# EVENTOS
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

    if bloquear_categoria_base(texto):
        log("⛔ BLOQUEADO CATEGORIA BASE | Sub-19/Sub-20 não permitido")
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

        sg, sc, tipo, metricas = calcular_scores(texto, estrategia, chave_jogo)

        log(
            f"📊 PROCESSADO | {estrategia} | Gol={sg}% | Canto={sc}% | Tipo={tipo} | "
            f"{jogo} | {metricas['tempo']}' | {metricas['placar']} | {metricas['mercado']} | "
            f"Liga={classificar_liga(metricas.get('competicao', ''))} | "
            f"PadraoALFA={metricas.get('padrao_alfa', '')} | "
            f"IP={metricas.get('pressao_alfa', {})} | "
            f"Fav={metricas.get('lado_favorito')} | Dom={metricas.get('lado_dominante')} | "
            f"Gol={metricas.get('ultimo_gol')} {metricas.get('ultimo_gol_lado')} | "
            f"GolCtx={metricas.get('motivo_gol_contextual', '')} | "
            f"Delta={metricas.get('motivo_delta_contextual', '')} | "
            f"Vermelho={metricas.get('lado_vermelho')} | "
            f"VermCtx={metricas.get('motivo_vermelho_contextual', '')} | "
            f"NivelPressao={metricas.get('nivel_pressao_sustentada', '')} | "
            f"Consequencia={metricas.get('tem_consequencia_ofensiva', '')}"
        )

        alerta = {
            "jogo": jogo,
            "chave_jogo": chave_jogo,
            "estrategia": estrategia,
            "score_gol": sg,
            "score_canto": sc,
            "tipo_pressao": tipo,
            "metricas": metricas,
            "texto_original": texto,
            "recebido_em": time.time(),
        }

        salvar_ultima_leitura(chave_jogo, alerta)

        corte_evento = corte_gol_estrategia(estrategia)
        if sg < CORTE_OBSERVACAO_JANELA:
            log(
                f"⛔ BLOQUEADO POR SCORE | {estrategia} | "
                f"Gol={sg}% < observação {CORTE_OBSERVACAO_JANELA}% | {jogo}"
            )
            return

        if sg < corte_evento:
            log(
                f"🟡 OBSERVAÇÃO EM JANELA | {estrategia} | "
                f"Gol={sg}% abaixo do corte real {corte_evento}% | {jogo}"
            )

        pendentes_por_jogo.setdefault(chave_jogo, []).append(alerta)

        log(
            f"⏳ ALERTA EM JANELA DE DECISÃO | {estrategia} | "
            f"Gol={sg}% | aguardando {JANELA_DECISAO_SEGUNDOS}s | {jogo}"
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

    log("🚀 COUTIPS / ALFA ONLINE - SOMENTE GOLS ATIVO")
    log("📊 Estratégias ativas: ALFA_HT | ALFA_HT CONFIRMAÇÃO | ALFA_FT | ALFA_FT CONFIRMAÇÃO | ARCE_HT | CHAMA_FT")
    log(f"⚽ Canal gols: {TARGET_CHANNEL}")
    log(f"🧪 Canal técnico de confirmação: {CONFIRMATION_CHANNEL}")
    log(f"🎯 Cortes gol: HT={CORTE_GOL_HT}% | FT={CORTE_GOL_FT}% | Conf HT={CORTE_CONFIRMACAO_GOL_HT}% | Conf FT={CORTE_CONFIRMACAO_GOL_FT}% | Cantos desativados")
    log(f"⏳ Janela de decisão por jogo: {JANELA_DECISAO_SEGUNDOS}s")
    log(f"📤 Intervalo entre envios: {INTERVALO_ENVIO_SEGUNDOS}s")
    log("⚠️ Confirme no Railway que existe apenas 1 instância/replica ativa.")
    log("🧠 Score ALFA: funil anti-fake pressure, HT 87+, FT 82+, ARCE/CHAMA +3, liga under pesada, consequência ofensiva do lado certo e confirmação forte pode voltar ao canal principal.")

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
