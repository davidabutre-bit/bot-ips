
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

# Canal de gols
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "@CoutipsIPS")

# Canal de cantos / GOAT CORNERS
CORNERS_CHANNEL = os.getenv("CORNERS_CHANNEL", "@Goat_Bot01")

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

API_ID = int(API_ID)

client = TelegramClient("coutips_ips_session", API_ID, API_HASH)

# =========================================================
# CONFIGURAÇÃO PRINCIPAL
# =========================================================

# Score mínimo para enviar ao canal
CORTE_GOL = int(os.getenv("CORTE_GOL", "75"))
CORTE_CANTO = int(os.getenv("CORTE_CANTO", "75"))

# Corte interno para não transformar qualquer pressão territorial em sinal
CORTE_HIBRIDO_GOL = int(os.getenv("CORTE_HIBRIDO_GOL", "75"))
CORTE_HIBRIDO_CANTO = int(os.getenv("CORTE_HIBRIDO_CANTO", "75"))

COOLDOWN_SEGUNDOS = int(os.getenv("COOLDOWN_SEGUNDOS", "600"))
CACHE_MAX_SEGUNDOS = int(os.getenv("CACHE_MAX_SEGUNDOS", "3600"))
JANELA_DECISAO_SEGUNDOS = float(os.getenv("JANELA_DECISAO_SEGUNDOS", "4"))
INTERVALO_ENVIO_SEGUNDOS = float(os.getenv("INTERVALO_ENVIO_SEGUNDOS", "1.8"))
WATCHDOG_SEGUNDOS = int(os.getenv("WATCHDOG_SEGUNDOS", "60"))

# Guarda última leitura do mesmo jogo para comparar BOT_HT -> BOT_HT CONFIRMAÇÃO
# e BOT_FT -> BOT_FT CONFIRMAÇÃO.
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
    texto = "".join(ch for ch in texto if unicodedata.category(ch) != "Mn")
    return texto


def normalizar(texto):
    return str(texto or "").replace("*", "").replace("_", "").replace("**", "").strip()


def limpar_linha(texto):
    return re.sub(r"\s+", " ", str(texto or "")).strip()


def normalizar_chave_jogo(jogo):
    jogo = remover_acentos(str(jogo or "").lower())
    jogo = re.sub(r"\([^)]*\)", "", jogo)
    jogo = re.sub(r"[^a-z0-9]+", " ", jogo)
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
    if m:
        return m.group(0).strip()
    return ""


def bloquear_categoria_base(texto):
    t = remover_acentos(texto).upper()

    padroes_bloqueados = [
        r"\bU19\b",
        r"\bU20\b",
        r"\bSUB[-\s]?19\b",
        r"\bSUB[-\s]?20\b",
        r"\bUNDER[-\s]?19\b",
        r"\bUNDER[-\s]?20\b",
    ]

    for padrao in padroes_bloqueados:
        if re.search(padrao, t):
            return True

    return False


def limpar_memoria_interna():
    agora = time.time()

    for chave in list(ultimos_enviados.keys()):
        if agora - ultimos_enviados[chave] > CACHE_MAX_SEGUNDOS:
            del ultimos_enviados[chave]

    for chave in list(mensagens_processadas.keys()):
        if agora - mensagens_processadas[chave] > CACHE_MAX_SEGUNDOS:
            del mensagens_processadas[chave]

    for chave in list(ultimas_leituras_por_jogo.keys()):
        leitura = ultimas_leituras_por_jogo.get(chave, {})
        if agora - leitura.get("recebido_em", 0) > CACHE_MAX_SEGUNDOS:
            del ultimas_leituras_por_jogo[chave]

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

    # Novos bots oficiais
    if "BOT_HT CONFIRMACAO" in t:
        return "BOT_HT_CONFIRMACAO"
    if "BOT_FT CONFIRMACAO" in t:
        return "BOT_FT_CONFIRMACAO"
    if "BOT_HT" in t:
        return "BOT_HT"
    if "BOT_FT" in t:
        return "BOT_FT"

    # Compatibilidade com nomes antigos
    if "HT_PREMIUM" in t or "HT_PREMIUN" in t or "ARCE" in t:
        return "BOT_HT"
    if "HT_MODERADO" in t or "COLTHT" in t or "IPS HT" in t:
        return "BOT_HT"
    if "FT_PREMIUM" in t or "FT_PREMIUN" in t or "CHAMA" in t:
        return "BOT_FT"
    if "FT_MODERADO" in t or "IPS FT" in t or "POS-70" in t or "PÓS-70" in t:
        return "BOT_FT"

    if "1ºT" in t or "1T" in t or "INTERVALO" in t:
        return "BOT_HT"

    return "BOT_FT"


def eh_ht(estrategia):
    return estrategia in ["BOT_HT", "BOT_HT_CONFIRMACAO"]


def eh_ft(estrategia):
    return estrategia in ["BOT_FT", "BOT_FT_CONFIRMACAO"]


def eh_confirmacao(estrategia):
    return estrategia in ["BOT_HT_CONFIRMACAO", "BOT_FT_CONFIRMACAO"]


def mensagem_valida(texto):
    t = remover_acentos(texto).upper()

    if "ALERTA ESTRATEGIA" not in t:
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


def extrair_competicao(texto):
    m = re.search(r"Competição:\s*(.+)", texto, re.IGNORECASE)
    if m:
        return limpar_linha(m.group(1).split("\n")[0])
    return ""


def extrair_tempo(texto):
    return pegar_numero(r"Tempo:\s*(\d+)", texto, 0)


def extrair_resultado(texto):
    m = re.search(r"Resultado:\s*([0-9]+)\s*x\s*([0-9]+)", texto, re.IGNORECASE)
    if not m:
        return "Placar não identificado"
    return f"{m.group(1)} x {m.group(2)}"


def extrair_gols_placar(placar):
    m = re.search(r"([0-9]+)\s*x\s*([0-9]+)", str(placar or ""))
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


def mercado_cantos_dinamico(metricas):
    cantos_casa, cantos_fora = metricas.get("cantos", (0, 0))
    total = cantos_casa + cantos_fora
    linha = total + 0.5
    return f"Mais {linha:.1f} Escanteios"


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


def extrair_ultimo_gol_lado(texto):
    """
    Lê formatos como:
    Último golo: 55' Casa
    Último golo: 55’ Fora
    Ultimo gol: 55 Casa
    """
    t = remover_acentos(texto).upper()

    padroes = [
        r"ULTIMO\s+GOLO:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)",
        r"ULTIMO\s+GOL:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)",
    ]

    for p in padroes:
        m = re.search(p, t, re.IGNORECASE)
        if m:
            lado = m.group(1).upper()
            if lado in ["CASA", "HOME"]:
                return "CASA"
            if lado in ["FORA", "AWAY"]:
                return "FORA"

    return "DESCONHECIDO"


def extrair_ultimos_cantos_lados(texto):
    """
    Lê algo como:
    Últimos cantos: 66' Fora,63' Fora
    Retorna lista de tuplas: [(66, "FORA"), (63, "FORA")]
    """
    t = remover_acentos(texto).upper()
    m = re.search(r"ULTIMOS\s+CANTOS:\s*(.+)", t, re.IGNORECASE)
    if not m:
        return []

    linha = m.group(1).split("\n")[0]
    eventos = []

    for minuto, lado in re.findall(r"(\d+)\s*['’]?\s*(CASA|FORA|HOME|AWAY)", linha):
        lado_final = "CASA" if lado in ["CASA", "HOME"] else "FORA"
        eventos.append((int(minuto), lado_final))

    return eventos


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

    remates_dentro_area = pegar_par(r"R\.\s*Dentro\s*Área:Casa=(\d+)\s*/\s*Fora=(\d+)", texto_limpo)
    if remates_dentro_area == (0, 0):
        remates_dentro_area = pegar_par(r"R\.\s*Dentro\s*Area:Casa=(\d+)\s*/\s*Fora=(\d+)", texto_limpo)

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
    heatmap_middle = pegar_par(r"heatmapMiddle:Casa=(\d+)\s*/\s*Fora=(\d+)", texto_limpo)

    # CornerPro às vezes envia xg:0.48 e às vezes xg:Casa=... / Fora=...
    xg_par = pegar_float_par(r"xg:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", texto_limpo)
    if xg_par == (0.0, 0.0):
        xg_total = pegar_float(r"\bxg:\s*([0-9.,]+)", texto_limpo, 0.0)
        xg_par = (xg_total / 2, xg_total / 2)

    xgl_par = pegar_float_par(r"xgl:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", texto_limpo)
    if xgl_par == (0.0, 0.0):
        xgl_total = pegar_float(r"\bxgl:\s*([0-9.,]+)", texto_limpo, 0.0)
        xgl_par = (xgl_total / 2, xgl_total / 2)

    xgi_par = pegar_float_par(r"xgi:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", texto_limpo)
    if xgi_par == (0.0, 0.0):
        xgi_total = pegar_float(r"\bxgi:\s*([0-9.,]+)", texto_limpo, 0.0)
        xgi_par = (xgi_total / 2, xgi_total / 2)

    mercado = mercado_dinamico(placar)

    return {
        "tempo": tempo,
        "placar": placar,
        "mercado": mercado,
        "competicao": extrair_competicao(texto_limpo),
        "ataques_perigosos": ataques_perigosos,
        "ataques": ataques,
        "cantos": cantos,
        "ultimos_cantos_lados": extrair_ultimos_cantos_lados(texto_limpo),
        "posse": posse,
        "remates_baliza": remates_baliza,
        "remates_lado": remates_lado,
        "remates_dentro_area": remates_dentro_area,
        "vermelhos": vermelhos,
        "odds": odds,
        "ultimos5": ultimos5,
        "ultimos10": ultimos10,
        "ultimo_gol": ultimo_gol,
        "ultimo_gol_lado": extrair_ultimo_gol_lado(texto_limpo),
        "chance_golo": chance_golo,
        "heatmap": heatmap,
        "heatmap_middle": heatmap_middle,
        "xg": xg_par,
        "xgl": xgl_par,
        "xgi": xgi_par,
        "bet365": extrair_link_bet365(texto_limpo),
    }


# =========================================================
# LIGAS
# =========================================================

def classificar_liga(competicao):
    c = remover_acentos(competicao).lower()

    # Ajustável com os CSVs históricos.
    ligas_premium = [
        "england premier league 2",
        "republic of ireland premier",
        "bulgaria second league",
        "paraguay division profesional reserva",
        "mls",
        "usa mls",
        "england league two",
        "malta first division",
        "nigeria npfl",
        "panama liga prom",
        "chile primera b",
        "poland 2 liga",
        "qatar qsl cup",
        "norway 3",
        "georgia",
        "crystalbet",
        "switzerland challenge",
        "portugal liga portugal",
        "italy serie a",
        "denmark superliga",
    ]

    ligas_moderadas = [
        "india", "indian", "china", "norway", "sweden",
        "finland", "japan", "south africa", "andorra",
        "germany", "spain la liga 2", "latvia", "lithuania",
        "brazil brasileiro women", "portugal taca revelacao",
        "saudi arabia pro league", "england championship",
    ]

    ligas_perigosas = [
        "algeria", "romania 3", "new zealand regional",
        "greece", "colombia", "argentina primera c",
        "iraq", "iraqi", "bolivia", "peru",
    ]

    for termo in ligas_perigosas:
        if termo in c:
            return "PERIGOSA"

    for termo in ligas_premium:
        if termo in c:
            return "PREMIUM"

    for termo in ligas_moderadas:
        if termo in c:
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
    if liga == "PERIGOSA":
        return -7
    return 0


# =========================================================
# LEITURA CONTEXTUAL
# =========================================================

def lado_favorito(metricas):
    odd_casa, _, odd_fora = metricas["odds"]

    if not odd_casa or not odd_fora:
        return "DESCONHECIDO", 0

    if odd_casa < odd_fora:
        return "CASA", odd_casa

    if odd_fora < odd_casa:
        return "FORA", odd_fora

    return "EQUILIBRADO", odd_casa


def lado_zebra(metricas):
    favorito, _ = lado_favorito(metricas)
    if favorito == "CASA":
        return "FORA"
    if favorito == "FORA":
        return "CASA"
    return "DESCONHECIDO"


def lado_dominante(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    u5_casa, u5_fora = metricas["ultimos5"]
    u10_casa, u10_fora = metricas["ultimos10"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    rl_casa, rl_fora = metricas["remates_lado"]
    rda_casa, rda_fora = metricas.get("remates_dentro_area", (0, 0))
    cantos_casa, cantos_fora = metricas["cantos"]
    heat_casa, heat_fora = metricas.get("heatmap", (0, 0))
    chance_casa, chance_fora = metricas.get("chance_golo", (0, 0))
    xg_casa, xg_fora = metricas.get("xg", (0.0, 0.0))

    casa = 0
    fora = 0

    casa += ap_casa * 1.2
    fora += ap_fora * 1.2

    casa += u5_casa * 2.0
    fora += u5_fora * 2.0

    casa += u10_casa * 1.4
    fora += u10_fora * 1.4

    casa += rb_casa * 3.8
    fora += rb_fora * 3.8

    casa += rda_casa * 2.2
    fora += rda_fora * 2.2

    casa += rl_casa * 1.2
    fora += rl_fora * 1.2

    casa += cantos_casa * 0.7
    fora += cantos_fora * 0.7

    casa += chance_casa * 1.1
    fora += chance_fora * 1.1

    casa += xg_casa * 8
    fora += xg_fora * 8

    if heat_casa > 0 or heat_fora > 0:
        casa += heat_casa * 0.10
        fora += heat_fora * 0.10

    diferenca = casa - fora

    if diferenca >= 8:
        return "CASA", diferenca
    if diferenca <= -8:
        return "FORA", abs(diferenca)

    return "EQUILIBRADO", abs(diferenca)


def lado_sofrendo_pressao(metricas):
    dominante, _ = lado_dominante(metricas)

    if dominante == "CASA":
        return "FORA"
    if dominante == "FORA":
        return "CASA"

    return "DESCONHECIDO"


def pressao_viva(metricas):
    u5_total = sum(metricas["ultimos5"])
    u10_total = sum(metricas["ultimos10"])
    rb_total = sum(metricas["remates_baliza"])
    rl_total = sum(metricas["remates_lado"])
    rda_total = sum(metricas.get("remates_dentro_area", (0, 0)))
    ap_total = sum(metricas["ataques_perigosos"])
    xg_total = sum(metricas.get("xg", (0.0, 0.0)))

    if u5_total >= 5:
        return True

    if u10_total >= 9 and u5_total >= 3:
        return True

    if ap_total >= 30 and (rb_total >= 3 or rda_total >= 3 or rl_total >= 7 or xg_total >= 0.55):
        return True

    return False


def lado_com_vermelho(metricas):
    vermelho_casa, vermelho_fora = metricas.get("vermelhos", (0, 0))

    if vermelho_casa > vermelho_fora:
        return "CASA"
    if vermelho_fora > vermelho_casa:
        return "FORA"
    if vermelho_casa > 0 and vermelho_fora > 0:
        return "AMBOS"

    return "NENHUM"


def tempo_operacional_valido(metricas, estrategia):
    tempo = metricas.get("tempo", 0)

    # Regra operacional definida:
    # HT precisa chegar até 36/37 no máximo.
    # FT confirmação até 81/82 no máximo.
    if eh_ht(estrategia):
        return tempo <= 37

    if eh_ft(estrategia):
        return tempo <= 82

    return True


# =========================================================
# SCORE DE GOL
# =========================================================

def favorito_score(metricas):
    odd_casa, _, odd_fora = metricas["odds"]

    bonus = 0

    # Favoritismo é multiplicador, não filtro duro.
    for odd in [odd_casa, odd_fora]:
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


def dominio_score_gol(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    rl_casa, rl_fora = metricas["remates_lado"]
    rda_casa, rda_fora = metricas.get("remates_dentro_area", (0, 0))
    cantos_casa, cantos_fora = metricas["cantos"]
    ataques_casa, ataques_fora = metricas["ataques"]
    heat_casa, heat_fora = metricas.get("heatmap", (0, 0))
    chance_casa, chance_fora = metricas.get("chance_golo", (0, 0))
    xg_casa, xg_fora = metricas.get("xg", (0.0, 0.0))
    xgi_casa, xgi_fora = metricas.get("xgi", (0.0, 0.0))

    ap_total = ap_casa + ap_fora
    rb_total = rb_casa + rb_fora
    rl_total = rl_casa + rl_fora
    rda_total = rda_casa + rda_fora
    cantos_total = cantos_casa + cantos_fora
    ataques_total = ataques_casa + ataques_fora
    xg_total = xg_casa + xg_fora
    xgi_total = xgi_casa + xgi_fora

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
        score += 3

    if ataques_total >= 70:
        score += 2
    if ataques_total >= 95:
        score += 2

    # Conversão real pesa mais que volume bruto.
    if rb_total >= 2:
        score += 4
    if rb_total >= 3:
        score += 4
    if rb_total >= 5:
        score += 4

    if rda_total >= 2:
        score += 3
    if rda_total >= 4:
        score += 4

    if rl_total >= 5:
        score += 2
    if rl_total >= 8:
        score += 2

    if abs(rb_casa - rb_fora) >= 3 and rb_total >= 3:
        score += 3

    # Cantos ajudam, mas não mandam no score de gol.
    if cantos_total >= 5:
        score += 1
    if cantos_total >= 8:
        score += 1

    if abs(heat_casa - heat_fora) >= 25:
        score += 2

    if abs(chance_casa - chance_fora) >= 5:
        score += 3

    if xg_total >= 0.35:
        score += 2
    if xg_total >= 0.55:
        score += 3
    if xg_total >= 1.00:
        score += 3

    if xgi_total >= 1.50:
        score += 2
    if xgi_total >= 2.30:
        score += 2

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
        score += 4
    if u5_total >= 8:
        score += 2

    if u10_total >= 7:
        score += 4
    if u10_total >= 10:
        score += 3
    if u10_total >= 14:
        score += 2

    if abs(u10_casa - u10_fora) >= 4:
        score += 3

    if abs(u5_casa - u5_fora) >= 3:
        score += 2

    return score


def relogio_score(metricas, estrategia):
    tempo = metricas["tempo"]

    if eh_ht(estrategia):
        if 20 <= tempo <= 30:
            return 8
        if 31 <= tempo <= 36:
            return 5
        if 37 <= tempo <= 38:
            return -2
        if tempo < 18:
            return -10
        return -6

    if eh_ft(estrategia):
        if 65 <= tempo <= 75:
            return 8
        if 76 <= tempo <= 81:
            return 5
        if 82 <= tempo <= 83:
            return -4
        if tempo >= 84:
            return -14
        if tempo < 63:
            return -6
        return 2

    return 0


def ajuste_vermelho_contextual(metricas):
    liga = classificar_liga(metricas.get("competicao", ""))
    vermelho = lado_com_vermelho(metricas)
    dominante, _ = lado_dominante(metricas)
    favorito, _ = lado_favorito(metricas)
    zebra = lado_zebra(metricas)
    vivo = pressao_viva(metricas)

    if vermelho in ["NENHUM", "AMBOS"] or dominante == "EQUILIBRADO":
        return 0, "SEM_VERMELHO_RELEVANTE"

    peso_liga = 1

    if liga == "PREMIUM":
        peso_liga = 1.25
    elif liga == "MODERADA":
        peso_liga = 1.0
    elif liga == "PERIGOSA":
        peso_liga = 0.55

    if vermelho != dominante and vivo:
        bonus = int(7 * peso_liga)

        if dominante == zebra:
            bonus += int(3 * peso_liga)

        if vermelho == favorito:
            bonus += int(2 * peso_liga)

        return bonus, "VERMELHO_ABRIU_PRESSAO"

    if vermelho == dominante:
        return -6, "VERMELHO_NO_LADO_PRESSAO"

    return 0, "SEM_AJUSTE_VERMELHO"


def ajuste_gol_recente_contextual(metricas, estrategia):
    tempo = metricas["tempo"]
    ultimo_gol = metricas["ultimo_gol"]
    lado_gol = metricas.get("ultimo_gol_lado", "DESCONHECIDO")

    if ultimo_gol <= 0:
        return 0, "SEM_GOL_RECENTE"

    minutos = tempo - ultimo_gol

    if minutos < 0:
        return -14, "GOL_INCONSISTENTE"

    dominante, _ = lado_dominante(metricas)
    pressionado = lado_sofrendo_pressao(metricas)
    favorito, _ = lado_favorito(metricas)
    zebra = lado_zebra(metricas)
    vivo = pressao_viva(metricas)

    # Regra nova e obrigatória:
    # Em bot de confirmação, gol nos últimos 0-2 min do lado que estava macetando
    # bloqueia/rebaixa forte. Não confundir pressão premiada com continuidade pós-gol.
    if eh_confirmacao(estrategia) and minutos <= 2:
        if lado_gol != "DESCONHECIDO":
            if lado_gol == pressionado and dominante != "EQUILIBRADO" and vivo:
                return 12, "GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado_gol == zebra and dominante == favorito and vivo:
                return 14, "GOL_ZEBRA_CONTRA_FLUXO"
            if lado_gol == dominante:
                return -30, "TRAVA_CONFIRMACAO_PRESSAO_PREMIADA"
        return -18, "TRAVA_CONFIRMACAO_GOL_RECENTE"

    if minutos <= 2:
        if lado_gol != "DESCONHECIDO":
            if lado_gol == pressionado and dominante != "EQUILIBRADO" and vivo:
                return 10, "GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado_gol == zebra and dominante == favorito and vivo:
                return 12, "GOL_ZEBRA_CONTRA_FLUXO"
            if lado_gol == dominante:
                return -16, "PRESSAO_PREMIADA_RECENTE"
        return -8, "GOL_RECENTE_SEM_CONFIRMACAO"

    if minutos <= 5:
        if lado_gol != "DESCONHECIDO":
            if lado_gol == pressionado and dominante != "EQUILIBRADO" and vivo:
                return 8, "GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado_gol == zebra and dominante == favorito and vivo:
                return 10, "GOL_ZEBRA_CONTRA_FLUXO"
            if lado_gol == dominante and not vivo:
                return -12, "PRESSAO_PREMIADA_MORREU"
            if lado_gol == dominante and vivo:
                return -5, "PRESSAO_PREMIADA_MAS_VIVA"
        return -4, "GOL_RECENTE_CAUTELA"

    if minutos <= 10:
        if lado_gol != "DESCONHECIDO":
            if lado_gol == pressionado and dominante != "EQUILIBRADO" and vivo:
                return 5, "GOL_CONTRA_FLUXO_CONTEXTO_BOM"
            if lado_gol == zebra and dominante == favorito and vivo:
                return 7, "GOL_ZEBRA_ABRIU_JOGO"
            if lado_gol == favorito and dominante != lado_gol and vivo:
                return 3, "FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA"
            if lado_gol == dominante and not vivo:
                return -6, "GOL_MATOU_RITMO"

        return 0, "GOL_ANTIGO_OK"

    if minutos > 10:
        if lado_gol != "DESCONHECIDO":
            if dominante != lado_gol and vivo:
                return 3, "PRESSAO_POS_GOL_VALIDADA"

        return 0, "GOL_ANTIGO_OK"

    return 0, "GOL_ANTIGO_OK"


def fake_pressure_penalty_gol(metricas):
    ap_total = sum(metricas["ataques_perigosos"])
    rb_total = sum(metricas["remates_baliza"])
    remates_lado_total = sum(metricas["remates_lado"])
    rda_total = sum(metricas.get("remates_dentro_area", (0, 0)))
    u5_total = sum(metricas["ultimos5"])
    u10_total = sum(metricas["ultimos10"])
    xg_total = sum(metricas.get("xg", (0.0, 0.0)))
    xgi_total = sum(metricas.get("xgi", (0.0, 0.0)))
    cantos_total = sum(metricas["cantos"])
    vermelho = lado_com_vermelho(metricas)
    dominante, _ = lado_dominante(metricas)

    penalidade = 0

    # Pressão bonita, mas sem finalização limpa.
    if ap_total >= 25 and rb_total <= 1:
        penalidade -= 8

    if ap_total >= 18 and rb_total == 0:
        penalidade -= 12

    if ap_total >= 25 and remates_lado_total <= 2 and rb_total <= 1:
        penalidade -= 6

    if u10_total <= 3 and u5_total <= 1:
        penalidade -= 8

    if ap_total >= 25 and xg_total <= 0.20:
        penalidade -= 8

    if ap_total >= 25 and xg_total <= 0.35 and xgi_total <= 0.60 and rb_total <= 1:
        penalidade -= 6

    # Pressão de canto não deve inflar score de gol.
    if cantos_total >= 4 and rb_total <= 1 and xg_total <= 0.35:
        penalidade -= 6

    if rda_total == 0 and rb_total <= 1 and ap_total >= 20:
        penalidade -= 5

    if vermelho != "NENHUM" and dominante != "EQUILIBRADO" and vermelho != dominante and penalidade < 0:
        penalidade = int(penalidade * 0.55)

    return penalidade


def score_delta_confirmacao(metricas, estrategia, chave_jogo):
    if not eh_confirmacao(estrategia):
        return 0, "SEM_DELTA"

    anterior = ultimas_leituras_por_jogo.get(chave_jogo)
    if not anterior:
        return 0, "SEM_LEITURA_ANTERIOR"

    ant = anterior.get("metricas", {})
    if not ant:
        return 0, "SEM_LEITURA_ANTERIOR"

    score = 0

    u5_atual = sum(metricas.get("ultimos5", (0, 0)))
    u5_ant = sum(ant.get("ultimos5", (0, 0)))
    u10_atual = sum(metricas.get("ultimos10", (0, 0)))
    u10_ant = sum(ant.get("ultimos10", (0, 0)))
    ap_atual = sum(metricas.get("ataques_perigosos", (0, 0)))
    ap_ant = sum(ant.get("ataques_perigosos", (0, 0)))
    rb_atual = sum(metricas.get("remates_baliza", (0, 0)))
    rb_ant = sum(ant.get("remates_baliza", (0, 0)))
    cantos_atual = sum(metricas.get("cantos", (0, 0)))
    cantos_ant = sum(ant.get("cantos", (0, 0)))

    if u5_atual > u5_ant:
        score += 4
    if u10_atual > u10_ant:
        score += 3
    if ap_atual > ap_ant:
        score += 3
    if rb_atual > rb_ant:
        score += 4
    if cantos_atual > cantos_ant:
        score += 2

    if u5_atual < max(1, u5_ant - 2):
        score -= 4
    if u10_atual < max(1, u10_ant - 3):
        score -= 4

    if score >= 8:
        return score, "CONFIRMACAO_MELHOROU"
    if score <= -4:
        return score, "CONFIRMACAO_PIOROU"
    return score, "CONFIRMACAO_ESTAVEL"


def score_gol(metricas, estrategia, chave_jogo):
    score = 42

    score += dominio_score_gol(metricas)
    score += momentum_score(metricas)
    score += relogio_score(metricas, estrategia)
    score += fake_pressure_penalty_gol(metricas)
    score += liga_score(metricas, "gol")

    ajuste_gol, motivo_gol = ajuste_gol_recente_contextual(metricas, estrategia)
    score += ajuste_gol
    metricas["motivo_gol_contextual"] = motivo_gol

    ajuste_vermelho, motivo_vermelho = ajuste_vermelho_contextual(metricas)
    score += ajuste_vermelho
    metricas["motivo_vermelho_contextual"] = motivo_vermelho

    delta, motivo_delta = score_delta_confirmacao(metricas, estrategia, chave_jogo)
    score += delta
    metricas["motivo_delta_contextual"] = motivo_delta

    metricas["lado_favorito"] = lado_favorito(metricas)[0]
    metricas["lado_zebra"] = lado_zebra(metricas)
    metricas["lado_dominante"] = lado_dominante(metricas)[0]
    metricas["lado_vermelho"] = lado_com_vermelho(metricas)

    fav = favorito_score(metricas)

    if eh_confirmacao(estrategia):
        score += int(fav * 0.65)
        score += 4
    else:
        score += int(fav * 0.75)
        score += 5

    score = ajuste_confianca_gol(metricas, estrategia, score)
    score = teto_contextual_gol(metricas, estrategia, score)

    if not tempo_operacional_valido(metricas, estrategia):
        score = min(score, 72)

    return int(max(0, min(score, 99))), metricas


def ajuste_confianca_gol(metricas, estrategia, score):
    rb_total = sum(metricas["remates_baliza"])
    rda_total = sum(metricas.get("remates_dentro_area", (0, 0)))
    ap_total = sum(metricas["ataques_perigosos"])
    u10_total = sum(metricas["ultimos10"])
    u5_total = sum(metricas["ultimos5"])
    xg_total = sum(metricas.get("xg", (0.0, 0.0)))
    liga = classificar_liga(metricas.get("competicao", ""))

    if score >= 88 and rb_total <= 1:
        score -= 10

    if score >= 85 and rb_total <= 1 and rda_total <= 1:
        score -= 7

    if eh_ft(estrategia):
        if metricas["tempo"] >= 81 and u5_total <= 2:
            score -= 8
        if rb_total == 0:
            score -= 8

    if ap_total < 18:
        score -= 5
    if u10_total < 5:
        score -= 5

    if xg_total <= 0.15 and rb_total <= 1:
        score -= 8

    if liga == "PERIGOSA" and score >= 86:
        score -= 4

    return score


def calcular_forca_premium_gol(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    ataques_casa, ataques_fora = metricas["ataques"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    rda_casa, rda_fora = metricas.get("remates_dentro_area", (0, 0))
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

    if (rda_casa + rda_fora) >= 3:
        pontos += 1

    if abs(u10_casa - u10_fora) >= 6 and (u10_casa + u10_fora) >= 8:
        pontos += 1

    if abs(u5_casa - u5_fora) >= 4:
        pontos += 1

    if abs(chance_casa - chance_fora) >= 5:
        pontos += 1

    if (xg_casa + xg_fora) >= 0.55:
        pontos += 1

    if abs(heat_casa - heat_fora) >= 22:
        pontos += 1

    return pontos


def teto_contextual_gol(metricas, estrategia, score):
    tempo = metricas["tempo"]
    placar = metricas["placar"]

    ap_casa, ap_fora = metricas["ataques_perigosos"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    cantos_casa, cantos_fora = metricas["cantos"]
    u10_casa, u10_fora = metricas["ultimos10"]
    xg_casa, xg_fora = metricas.get("xg", (0.0, 0.0))
    rda_casa, rda_fora = metricas.get("remates_dentro_area", (0, 0))

    rb_total = rb_casa + rb_fora
    rda_total = rda_casa + rda_fora
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

    forca_premium = calcular_forca_premium_gol(metricas)
    motivo_gol = metricas.get("motivo_gol_contextual", "")
    motivo_vermelho = metricas.get("motivo_vermelho_contextual", "")
    motivo_delta = metricas.get("motivo_delta_contextual", "")
    liga = classificar_liga(metricas.get("competicao", ""))

    premium_real = forca_premium >= 5 and minutos_desde_gol > 2
    jogo_bom = u10_total >= 8 or dif_ap >= 10 or rb_total >= 3 or xg_total >= 0.45 or rda_total >= 3

    if minutos_desde_gol < 0:
        score = min(score, 75)

    elif minutos_desde_gol <= 2:
        if motivo_gol in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO"]:
            score = min(score, 92)
        elif motivo_gol == "TRAVA_CONFIRMACAO_PRESSAO_PREMIADA":
            score = min(score, 72)
        elif motivo_gol == "PRESSAO_PREMIADA_RECENTE":
            score = min(score, 76)
        else:
            score = min(score, 78)

    elif minutos_desde_gol <= 5:
        if motivo_gol in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO"]:
            score = min(score, 92)
        elif motivo_gol in ["PRESSAO_PREMIADA_MORREU", "PRESSAO_PREMIADA_RECENTE"]:
            score = min(score, 78)
        elif motivo_gol == "PRESSAO_PREMIADA_MAS_VIVA":
            score = min(score, 84)
        else:
            score = min(score, 82)

    # Tetos anti-inflação.
    if rb_total <= 1:
        score = min(score, 82)

    if rb_total == 0 and rda_total <= 1:
        score = min(score, 78)

    if dif_ap <= 5 and dif_rb <= 1 and dif_cantos <= 1:
        score = min(score, 81)

    if xg_total <= 0.25 and rb_total <= 1:
        score = min(score, 78)

    if liga == "PERIGOSA":
        if not premium_real:
            score = min(score, 80)
        else:
            score = min(score, 84)

    if motivo_vermelho == "VERMELHO_ABRIU_PRESSAO" and liga in ["PREMIUM", "MODERADA"]:
        if eh_ft(estrategia):
            score = min(score + 2, 90)

    if motivo_delta == "CONFIRMACAO_PIOROU":
        score = min(score, 78)

    if eh_confirmacao(estrategia):
        if premium_real and motivo_delta == "CONFIRMACAO_MELHOROU":
            score = min(score, 94)
        elif premium_real:
            score = min(score, 90)
        elif jogo_bom:
            score = min(score, 86)
        else:
            score = min(score, 80)
    else:
        if premium_real:
            score = min(score, 92)
        elif jogo_bom:
            score = min(score, 87)
        else:
            score = min(score, 81)

    if eh_ft(estrategia) and diferenca_placar >= 3:
        score = min(score, 78)

    return score


# =========================================================
# SCORE DE CANTOS / GOAT CORNERS
# =========================================================

def pressao_lateral_score(metricas):
    ap_casa, ap_fora = metricas["ataques_perigosos"]
    ataques_casa, ataques_fora = metricas["ataques"]
    cantos_casa, cantos_fora = metricas["cantos"]
    u5_casa, u5_fora = metricas["ultimos5"]
    u10_casa, u10_fora = metricas["ultimos10"]
    rb_casa, rb_fora = metricas["remates_baliza"]
    rl_casa, rl_fora = metricas["remates_lado"]
    rda_casa, rda_fora = metricas.get("remates_dentro_area", (0, 0))
    heat_casa, heat_fora = metricas.get("heatmap", (0, 0))
    heat_mid_casa, heat_mid_fora = metricas.get("heatmap_middle", (0, 0))

    ap_total = ap_casa + ap_fora
    ataques_total = ataques_casa + ataques_fora
    cantos_total = cantos_casa + cantos_fora
    u5_total = u5_casa + u5_fora
    u10_total = u10_casa + u10_fora
    rb_total = rb_casa + rb_fora
    rl_total = rl_casa + rl_fora
    rda_total = rda_casa + rda_fora

    score = 0

    if ap_total >= 20:
        score += 5
    if ap_total >= 30:
        score += 4
    if ap_total >= 40:
        score += 3

    if ataques_total >= 50:
        score += 3
    if ataques_total >= 75:
        score += 2

    if u5_total >= 4:
        score += 4
    if u5_total >= 7:
        score += 3

    if u10_total >= 8:
        score += 4
    if u10_total >= 12:
        score += 3

    if cantos_total >= 2:
        score += 3
    if cantos_total >= 4:
        score += 4
    if cantos_total >= 6:
        score += 3

    if abs(cantos_casa - cantos_fora) >= 2:
        score += 2

    # Remate para fora e baixa conversão limpa ajudam cantos.
    if rl_total >= 4:
        score += 3
    if rl_total >= 7:
        score += 3

    if rb_total <= 2 and ap_total >= 20:
        score += 4

    if rda_total <= 2 and ap_total >= 20:
        score += 2

    if abs(heat_casa - heat_fora) >= 18:
        score += 3

    # Heatmap middle baixo em relação ao full sugere lateralidade.
    if max(heat_casa, heat_fora) >= 55 and max(heat_mid_casa, heat_mid_fora) <= 45:
        score += 4

    return score


def score_canto(metricas, estrategia, chave_jogo):
    score = 40

    score += pressao_lateral_score(metricas)
    score += int(momentum_score(metricas) * 0.85)
    score += liga_score(metricas, "canto")
    score += relogio_score(metricas, estrategia)

    # Cantos gostam de jogo vivo, mas não necessariamente de finalização limpa.
    rb_total = sum(metricas["remates_baliza"])
    rda_total = sum(metricas.get("remates_dentro_area", (0, 0)))
    ap_total = sum(metricas["ataques_perigosos"])
    cantos_total = sum(metricas["cantos"])
    u5_total = sum(metricas["ultimos5"])
    u10_total = sum(metricas["ultimos10"])

    if ap_total >= 25 and cantos_total >= 3 and rb_total <= 2:
        score += 6

    if u5_total >= 5 and cantos_total >= 2:
        score += 4

    if u10_total >= 9 and cantos_total >= 3:
        score += 4

    # Se é pressão de gol muito limpa, canto perde um pouco de prioridade, mas não bloqueia.
    if rb_total >= 5 and rda_total >= 4:
        score -= 4

    # Gol recente do time que pressionava pode matar canto também.
    ajuste_gol, motivo_gol = ajuste_gol_recente_contextual(metricas, estrategia)
    metricas["motivo_canto_gol_contextual"] = motivo_gol

    if motivo_gol in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA", "PRESSAO_PREMIADA_RECENTE", "PRESSAO_PREMIADA_MORREU"]:
        score -= 8
    elif motivo_gol in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO", "GOL_ZEBRA_ABRIU_JOGO"]:
        score += 4

    delta, motivo_delta = score_delta_confirmacao(metricas, estrategia, chave_jogo)
    metricas["motivo_canto_delta_contextual"] = motivo_delta
    if motivo_delta == "CONFIRMACAO_MELHOROU":
        score += 5
    elif motivo_delta == "CONFIRMACAO_PIOROU":
        score -= 5

    score = teto_contextual_canto(metricas, estrategia, score)

    if not tempo_operacional_valido(metricas, estrategia):
        score = min(score, 72)

    return int(max(0, min(score, 99)))


def teto_contextual_canto(metricas, estrategia, score):
    ap_total = sum(metricas["ataques_perigosos"])
    cantos_total = sum(metricas["cantos"])
    u5_total = sum(metricas["ultimos5"])
    u10_total = sum(metricas["ultimos10"])
    rb_total = sum(metricas["remates_baliza"])
    rl_total = sum(metricas["remates_lado"])

    motivo_gol = metricas.get("motivo_canto_gol_contextual", "")
    liga = classificar_liga(metricas.get("competicao", ""))

    if ap_total < 16:
        score = min(score, 74)

    if cantos_total == 0 and u5_total < 5:
        score = min(score, 74)

    if cantos_total <= 1 and u10_total < 7:
        score = min(score, 76)

    if rl_total <= 2 and rb_total <= 1 and cantos_total <= 1:
        score = min(score, 76)

    if motivo_gol in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA", "PRESSAO_PREMIADA_MORREU"]:
        score = min(score, 78)

    if liga == "PERIGOSA" and score >= 85:
        score = min(score, 82)

    if eh_confirmacao(estrategia):
        # confirmação pode subir se o canto/território continuou.
        if metricas.get("motivo_canto_delta_contextual") == "CONFIRMACAO_MELHOROU":
            score = min(score, 94)
        else:
            score = min(score, 90)
    else:
        score = min(score, 92)

    return score


def tipo_pressao(metricas, score_gol_final, score_canto_final):
    if score_gol_final >= CORTE_GOL and score_canto_final >= CORTE_CANTO:
        if abs(score_gol_final - score_canto_final) <= 8:
            return "HIBRIDA"
        if score_gol_final > score_canto_final:
            return "GOL_COM_CANTO"
        return "CANTO_COM_GOL"

    if score_gol_final >= CORTE_GOL:
        return "GOL"

    if score_canto_final >= CORTE_CANTO:
        return "CANTO"

    if pressao_viva(metricas):
        return "OBSERVACAO"

    return "BLOQUEIO"


def calcular_scores(texto, estrategia, chave_jogo):
    metricas = extrair_metricas(texto)

    gol, metricas = score_gol(metricas, estrategia, chave_jogo)
    canto = score_canto(metricas, estrategia, chave_jogo)
    tipo = tipo_pressao(metricas, gol, canto)

    metricas["score_gol"] = gol
    metricas["score_canto"] = canto
    metricas["tipo_pressao"] = tipo

    return gol, canto, tipo, metricas


# =========================================================
# CACHE / PRIORIDADE / DECISÃO
# =========================================================

def prioridade_alerta(alerta):
    # prioridade dentro da janela de decisão:
    # confirmação > maior score do mercado > tempo
    estrategia = alerta["estrategia"]
    score_max = max(alerta.get("score_gol", 0), alerta.get("score_canto", 0))
    return (
        1 if eh_confirmacao(estrategia) else 0,
        score_max,
        alerta["metricas"]["tempo"],
    )


def melhor_alerta(alertas):
    return sorted(alertas, key=prioridade_alerta, reverse=True)[0]


def ja_enviado_recentemente(chave_envio):
    limpar_memoria_interna()
    agora = time.time()

    if chave_envio in ultimos_enviados:
        if agora - ultimos_enviados[chave_envio] <= COOLDOWN_SEGUNDOS:
            return True

    return False


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
# MENSAGENS FINAIS
# =========================================================

def faixa_publica(score):
    if score >= 98:
        return "👑 ENTRADA LENDÁRIA 👑"
    if score >= 89:
        return "💎 ENTRADA DIAMANTE 💎"
    if score >= 81:
        return "🔥 ENTRADA ELITE 🔥"
    if score >= 75:
        return "🚨 ENTRADA FORTE 🚨"
    return "⚠️ OBSERVAÇÃO"


def nome_publico_bot(estrategia):
    nomes = {
        "BOT_HT": "PULSO HT",
        "BOT_HT_CONFIRMACAO": "COLAPSO HT",
        "BOT_FT": "PULSO FT",
        "BOT_FT_CONFIRMACAO": "COLAPSO FT",
    }
    return nomes.get(estrategia, estrategia)


def texto_liga_publico(metricas):
    liga = classificar_liga(metricas.get("competicao", ""))

    if liga == "PREMIUM":
        return "🟢 Liga Premium"
    if liga == "MODERADA":
        return "🟡 Liga Moderada"
    if liga == "PERIGOSA":
        return "🔴 Liga Perigosa"
    return "⚪ Liga Neutra"


def texto_contexto_gol(metricas):
    motivo = metricas.get("motivo_gol_contextual", "")
    motivo_vermelho = metricas.get("motivo_vermelho_contextual", "")
    motivo_delta = metricas.get("motivo_delta_contextual", "")

    textos = []

    if motivo in ["GOL_ZEBRA_CONTRA_FLUXO", "GOL_TIME_PRESSIONADO_ABRIU_JOGO"]:
        textos.append("🔥 Contexto extra: gol contra o fluxo aumentou a urgência ofensiva.")
    elif motivo == "GOL_CONTRA_FLUXO_CONTEXTO_BOM":
        textos.append("🔥 Contexto extra: time pressionado marcou, mas o jogo segue aberto.")
    elif motivo == "PRESSAO_PREMIADA_MAS_VIVA":
        textos.append("⚠️ Contexto: pressão já foi premiada, mas segue viva.")
    elif motivo == "FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA":
        textos.append("🔥 Contexto extra: gol anterior não matou o jogo; adversário segue pressionando.")

    if motivo_delta == "CONFIRMACAO_MELHOROU":
        textos.append("📈 Confirmação: pressão aumentou em relação ao primeiro gatilho.")
    elif motivo_delta == "CONFIRMACAO_PIOROU":
        textos.append("⚠️ Confirmação: pressão caiu em relação ao primeiro gatilho.")

    if motivo_vermelho == "VERMELHO_ABRIU_PRESSAO":
        textos.append("🟥 Contexto extra: superioridade numérica reforça a pressão ofensiva.")

    if not textos:
        return ""

    return "\n" + "\n".join(textos)


def texto_contexto_canto(metricas):
    motivo_delta = metricas.get("motivo_canto_delta_contextual", "")
    textos = []

    if motivo_delta == "CONFIRMACAO_MELHOROU":
        textos.append("📈 Confirmação: pressão territorial aumentou em relação ao primeiro gatilho.")

    cantos_casa, cantos_fora = metricas.get("cantos", (0, 0))
    ultimos_cantos = metricas.get("ultimos_cantos_lados", [])

    if cantos_casa + cantos_fora >= 4:
        textos.append("🚩 Volume de cantos já ativo no jogo.")

    if ultimos_cantos:
        ultimo = ultimos_cantos[0]
        textos.append(f"🚩 Último canto: {ultimo[0]}' {ultimo[1].title()}.")

    if not textos:
        return ""

    return "\n" + "\n".join(textos)


def montar_mensagem_gol(jogo, estrategia, score, metricas):
    categoria = faixa_publica(score)
    nome_bot = nome_publico_bot(estrategia)

    jogo_safe = html.escape(str(jogo))
    placar_safe = html.escape(str(metricas["placar"]))
    mercado_safe = html.escape(str(metricas["mercado"]))
    categoria_safe = html.escape(str(categoria))
    liga_safe = html.escape(texto_liga_publico(metricas))
    bot_safe = html.escape(nome_bot)

    contexto_gol = texto_contexto_gol(metricas)

    link_bet365 = ""
    if metricas.get("bet365"):
        link_bet365 = f"\n🔗 Bet365: {html.escape(metricas['bet365'])}"

    return f"""{categoria_safe}

⚽ <b>COUTIPS {bot_safe}</b>

🏟 Jogo: {jogo_safe}
🕛 Tempo: {metricas['tempo']}'
📊 Placar: {placar_safe}

🎯 Mercado: {mercado_safe}
📈 Chance COUTIPS de Gol: <b>{score}%</b>
{liga_safe}

🔥 Leitura COUTIPS:
Pressão ofensiva viva, contexto favorável e leitura voltada para novo gol.{contexto_gol}

📌 Entrar somente se a odd estiver acima de 1.60.
💰 Gestão recomendada: 1% da banca.
⚠️ Confirmar mercado e odd ao vivo antes da entrada.{link_bet365}

COUTIPS — leitura ao vivo com pressão, contexto e disciplina."""


def montar_mensagem_canto(jogo, estrategia, score, metricas):
    categoria = faixa_publica(score)
    nome_bot = nome_publico_bot(estrategia)

    jogo_safe = html.escape(str(jogo))
    placar_safe = html.escape(str(metricas["placar"]))
    mercado_cantos = html.escape(mercado_cantos_dinamico(metricas))
    liga_safe = html.escape(texto_liga_publico(metricas))
    bot_safe = html.escape(nome_bot)

    contexto_canto = texto_contexto_canto(metricas)

    link_bet365 = ""
    if metricas.get("bet365"):
        link_bet365 = f"\n🔗 Bet365: {html.escape(metricas['bet365'])}"

    return f"""🐐<b>GOAT CORNERS</b> 🚩

{html.escape(categoria)}

🏟 Jogo: {jogo_safe}
🕛 Tempo: {metricas['tempo']}'
⚽ Placar: {placar_safe}

🚩 Mercado: {mercado_cantos}
📊 Pressão para Canto: <b>{score}%</b>
📌 Bot: {bot_safe}
{liga_safe}

🔥 Leitura GOAT:
Pressão territorial forte, ataques constantes e tendência de bola travada/cantos.{contexto_canto}

⚠️ Entrada somente se a linha e a odd ainda fizerem sentido.
💰 Gestão: 1% da banca.
🔞 Jogue com responsabilidade.{link_bet365}"""


# =========================================================
# ENVIO COM FILA
# =========================================================

async def trabalhador_envio():
    log("📤 Fila de envio iniciada.")

    while True:
        item = await fila_envio.get()

        try:
            alerta = item["alerta"]
            mercado = item["mercado"]

            if mercado == "GOL":
                mensagem = montar_mensagem_gol(
                    alerta["jogo"],
                    alerta["estrategia"],
                    alerta["score_gol"],
                    alerta["metricas"],
                )
                destino = TARGET_CHANNEL
                score_log = alerta["score_gol"]

            elif mercado == "CANTO":
                mensagem = montar_mensagem_canto(
                    alerta["jogo"],
                    alerta["estrategia"],
                    alerta["score_canto"],
                    alerta["metricas"],
                )
                destino = CORNERS_CHANNEL
                score_log = alerta["score_canto"]

            else:
                log(f"⚠️ Mercado desconhecido na fila: {mercado}")
                fila_envio.task_done()
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

            try:
                alerta = item["alerta"]
                mercado = item["mercado"]

                if mercado == "GOL":
                    mensagem = montar_mensagem_gol(alerta["jogo"], alerta["estrategia"], alerta["score_gol"], alerta["metricas"])
                    destino = TARGET_CHANNEL
                else:
                    mensagem = montar_mensagem_canto(alerta["jogo"], alerta["estrategia"], alerta["score_canto"], alerta["metricas"])
                    destino = CORNERS_CHANNEL

                await client.send_message(destino, mensagem, parse_mode="html")
                marcar_enviado(item["chave_envio"])

                log(f"✅ ENVIADO APÓS FLOODWAIT | {mercado} | {alerta['jogo']}")

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

        mercados_para_enviar = []

        if escolhido["score_gol"] >= CORTE_GOL:
            mercados_para_enviar.append("GOL")

        if escolhido["score_canto"] >= CORTE_CANTO:
            mercados_para_enviar.append("CANTO")

        if not mercados_para_enviar:
            log(
                f"⛔ BLOQUEADO FINAL | Gol={escolhido['score_gol']}% "
                f"Canto={escolhido['score_canto']}% | {escolhido['jogo']}"
            )
            return

        if len(alertas) > 1:
            estrategias = ", ".join([a["estrategia"] for a in alertas])
            log(
                f"🏆 PRIORIDADE APLICADA | Escolhido: {escolhido['estrategia']} | "
                f"Recebidos: {estrategias} | Jogo: {escolhido['jogo']}"
            )

        for mercado in mercados_para_enviar:
            chave_envio = f"{chave_jogo}_{mercado}"

            if ja_enviado_recentemente(chave_envio):
                log(f"⛔ BLOQUEADO POR COOLDOWN {mercado} | {escolhido['jogo']}")
                continue

            await fila_envio.put(
                {
                    "alerta": escolhido,
                    "mercado": mercado,
                    "chave_envio": chave_envio,
                }
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

        score_gol_final, score_canto_final, tipo, metricas = calcular_scores(texto, estrategia, chave_jogo)

        log(
            f"📊 PROCESSADO | {estrategia} | Gol={score_gol_final}% | "
            f"Canto={score_canto_final}% | Tipo={tipo} | {jogo} | {metricas['tempo']}' | "
            f"{metricas['placar']} | {metricas['mercado']} | "
            f"Liga={classificar_liga(metricas.get('competicao', ''))} | "
            f"Fav={metricas.get('lado_favorito')} | Dom={metricas.get('lado_dominante')} | "
            f"Gol={metricas.get('ultimo_gol')} {metricas.get('ultimo_gol_lado')} | "
            f"GolCtx={metricas.get('motivo_gol_contextual', '')} | "
            f"Delta={metricas.get('motivo_delta_contextual', '')} | "
            f"Vermelho={metricas.get('lado_vermelho')} | "
            f"VermCtx={metricas.get('motivo_vermelho_contextual', '')}"
        )

        # Salva leitura mesmo que bloqueie, para futura confirmação comparar.
        alerta_base = {
            "jogo": jogo,
            "chave_jogo": chave_jogo,
            "estrategia": estrategia,
            "score_gol": score_gol_final,
            "score_canto": score_canto_final,
            "tipo_pressao": tipo,
            "metricas": metricas,
            "texto_original": texto,
            "recebido_em": time.time(),
        }
        salvar_ultima_leitura(chave_jogo, alerta_base)

        if score_gol_final < CORTE_GOL and score_canto_final < CORTE_CANTO:
            log(
                f"⛔ BLOQUEADO POR SCORE | {estrategia} | "
                f"Gol={score_gol_final}% < {CORTE_GOL}% | "
                f"Canto={score_canto_final}% < {CORTE_CANTO}% | {jogo}"
            )
            return

        if chave_jogo not in pendentes_por_jogo:
            pendentes_por_jogo[chave_jogo] = []

        pendentes_por_jogo[chave_jogo].append(alerta_base)

        log(
            f"⏳ ALERTA EM JANELA DE DECISÃO | {estrategia} | "
            f"Gol={score_gol_final}% | Canto={score_canto_final}% | "
            f"aguardando {JANELA_DECISAO_SEGUNDOS}s | {jogo}"
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

    log("🚀 COUTIPS / GOAT ONLINE - SCORE CONTEXTUAL 3.0 ATIVO")
    log("📊 Estratégias ativas: BOT_HT | BOT_HT CONFIRMAÇÃO | BOT_FT | BOT_FT CONFIRMAÇÃO")
    log(f"⚽ Canal gols: {TARGET_CHANNEL}")
    log(f"🚩 Canal cantos: {CORNERS_CHANNEL}")
    log(f"🎯 Corte gol: {CORTE_GOL}% | Corte canto: {CORTE_CANTO}%")
    log(f"⏳ Janela de decisão por jogo: {JANELA_DECISAO_SEGUNDOS}s")
    log("⚠️ Confirme no Railway que existe apenas 1 instância/replica ativa.")
    log("🧠 Score 3.0: gol/canto/híbrido, delta de confirmação, gol recente, liga e pressão convertível.")

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
