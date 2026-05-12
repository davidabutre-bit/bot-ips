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

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

API_ID = int(API_ID)
client = TelegramClient("coutips_ips_session", API_ID, API_HASH)

CORTE_GOL = int(os.getenv("CORTE_GOL", "75"))
CORTE_CANTO = int(os.getenv("CORTE_CANTO", "75"))
COOLDOWN_SEGUNDOS = int(os.getenv("COOLDOWN_SEGUNDOS", "600"))
CACHE_MAX_SEGUNDOS = int(os.getenv("CACHE_MAX_SEGUNDOS", "3600"))
JANELA_DECISAO_SEGUNDOS = float(os.getenv("JANELA_DECISAO_SEGUNDOS", "4"))
INTERVALO_ENVIO_SEGUNDOS = float(os.getenv("INTERVALO_ENVIO_SEGUNDOS", "1.8"))
WATCHDOG_SEGUNDOS = int(os.getenv("WATCHDOG_SEGUNDOS", "60"))

ultimas_leituras_por_jogo = {}
ultimos_enviados = {}
pendentes_por_jogo = {}
tarefas_decisao = {}
mensagens_processadas = {}
fila_envio = asyncio.Queue()
tarefa_envio = None


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
    for padrao in [r"\bU19\b", r"\bU20\b", r"\bSUB[-\s]?19\b", r"\bSUB[-\s]?20\b", r"\bUNDER[-\s]?19\b", r"\bUNDER[-\s]?20\b"]:
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
    cc, cf = metricas.get("cantos", (0, 0))
    return f"Mais {cc + cf + 0.5:.1f} Escanteios"


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
    for p in [r"ULTIMO\s+GOLO:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)", r"ULTIMO\s+GOL:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)"]:
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
        return {"ip_pico_casa": 0.0, "ip_pico_fora": 0.0, "ip_consec_18_casa": 0, "ip_consec_18_fora": 0, "ip_consec_22_casa": 0, "ip_consec_22_fora": 0}
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
    ultimo_gol = pegar_numero(r"Último golo:\s*(\d+)", tl, 0) or pegar_numero(r"Ultimo golo:\s*(\d+)", tl, 0) or pegar_numero(r"Último gol:\s*(\d+)", tl, 0) or pegar_numero(r"Ultimo gol:\s*(\d+)", tl, 0)
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


def classificar_liga(competicao):
    c = remover_acentos(competicao).lower()
    ligas_premium = ["england premier league", "france ligue 1", "italy serie a", "spain la liga", "germany bundesliga", "denmark superliga", "netherlands eredivisie", "belgium pro league", "czech republic fortuna liga", "england league two", "switzerland", "portugal liga"]
    ligas_moderadas = ["australia", "india", "china", "norway", "sweden", "finland", "japan", "singapore", "hong kong", "macao", "estonia", "poland", "lithuania", "brazil brasileiro women", "denmark first division"]
    ligas_perigosas = ["algeria", "romania 3", "new zealand regional", "greece", "colombia", "argentina primera c", "iraq", "bolivia", "peru", "ecuador", "chile", "mongolia", "jordan"]
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
        return 3 if liga == "PREMIUM" else 1 if liga == "MODERADA" else -3 if liga == "PERIGOSA" else 0
    return 4 if liga == "PREMIUM" else 0 if liga == "MODERADA" else -7 if liga == "PERIGOSA" else 0


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
    return "FORA" if fav == "CASA" else "CASA" if fav == "FORA" else "DESCONHECIDO"


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
    casa = apc*1.2 + u5c*2 + u10c*1.4 + rbc*3.8 + rdac*2.2 + rlc*1.2 + cc*.7 + ch_c*1.1 + xgc*8 + hc*.1 + p.get("ip_pico_casa",0)*.45 + p.get("ip_consec_18_casa",0)*2 + p.get("ip_consec_22_casa",0)*3
    fora = apf*1.2 + u5f*2 + u10f*1.4 + rbf*3.8 + rdaf*2.2 + rlf*1.2 + cf*.7 + ch_f*1.1 + xgf*8 + hf*.1 + p.get("ip_pico_fora",0)*.45 + p.get("ip_consec_18_fora",0)*2 + p.get("ip_consec_22_fora",0)*3
    dif = casa - fora
    if dif >= 8:
        return "CASA", dif
    if dif <= -8:
        return "FORA", abs(dif)
    return "EQUILIBRADO", abs(dif)


def lado_sofrendo_pressao(metricas):
    dom, _ = lado_dominante(metricas)
    return "FORA" if dom == "CASA" else "CASA" if dom == "FORA" else "DESCONHECIDO"


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
    u5 = sum(metricas["ultimos5"]); u10 = sum(metricas["ultimos10"]); rb = sum(metricas["remates_baliza"]); rl = sum(metricas["remates_lado"]); rda = sum(metricas.get("remates_dentro_area", (0,0))); ap = sum(metricas["ataques_perigosos"]); xg = sum(metricas.get("xg", (0,0)))
    p = metricas.get("pressao_alfa", {})
    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0)); ip18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0)); ip22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0))
    return ip22 >= 2 or ip18 >= 3 or (ip_pico >= 22 and (u5 >= 2 or u10 >= 5)) or u5 >= 5 or (u10 >= 9 and u5 >= 3) or (ap >= 30 and (rb >= 3 or rda >= 3 or rl >= 7 or xg >= .55))


def tempo_operacional_valido(metricas, estrategia):
    tempo = metricas.get("tempo", 0)
    if eh_ht(estrategia):
        return tempo <= 37
    if eh_ft(estrategia):
        return tempo <= 82
    return True


def favorito_score(metricas):
    bonus = 0
    for odd in [metricas["odds"][0], metricas["odds"][2]]:
        if not odd:
            continue
        if odd <= 1.35: bonus += 10
        elif odd <= 1.50: bonus += 7
        elif odd <= 1.70: bonus += 4
        elif odd <= 2.00: bonus += 1
    return min(bonus, 12)


def score_padrao_alfa(metricas, estrategia):
    p = metricas.get("pressao_alfa", {})
    ch_c, ch_f = metricas.get("chance_golo", (0, 0))
    ip_pico = max(p.get("ip_pico_casa", 0), p.get("ip_pico_fora", 0))
    chance_max = max(ch_c, ch_f)
    dom, _ = lado_dominante(metricas)
    lado_chance = "CASA" if ch_c >= ch_f else "FORA"
    extra = 3 if dom == lado_chance and dom != "EQUILIBRADO" else 0
    if eh_ft(estrategia):
        c22 = max(p.get("ip_consec_22_casa", 0), p.get("ip_consec_22_fora", 0)); c18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
        if c22 >= 3 and chance_max >= 10: return 26 + extra, "ALFA_FT_FORTE"
        if ip_pico >= 22 and chance_max >= 10: return 20 + extra, "ALFA_FT_QUASE_FORTE"
        if c18 >= 3 and chance_max >= 8: return 15 + extra, "ALFA_FT_RESSALVA"
        if ip_pico >= 18 and chance_max >= 8: return 10 + extra, "ALFA_FT_OBSERVACAO"
    if eh_ht(estrategia):
        c18 = max(p.get("ip_consec_18_casa", 0), p.get("ip_consec_18_fora", 0))
        if c18 >= 3 and chance_max >= 6: return 26 + extra, "ALFA_HT_FORTE"
        if ip_pico >= 18 and chance_max >= 6: return 20 + extra, "ALFA_HT_QUASE_FORTE"
        if ip_pico >= 15 and chance_max >= 5: return 11 + extra, "ALFA_HT_RESSALVA"
    return 0, "SEM_PADRAO_ALFA"


def dominio_score_gol(metricas):
    apc, apf = metricas["ataques_perigosos"]; rbc, rbf = metricas["remates_baliza"]; rlc, rlf = metricas["remates_lado"]; rdac, rdaf = metricas.get("remates_dentro_area", (0,0)); cc, cf = metricas["cantos"]; atc, atf = metricas["ataques"]; hc, hf = metricas.get("heatmap", (0,0)); chc, chf = metricas.get("chance_golo", (0,0)); xgc, xgf = metricas.get("xg", (0,0)); xic, xif = metricas.get("xgi", (0,0)); p = metricas.get("pressao_alfa", {})
    score = 0; ap = apc+apf; rb = rbc+rbf; rl = rlc+rlf; rda = rdac+rdaf; cant = cc+cf; at = atc+atf; xg = xgc+xgf; xgi = xic+xif
    ip_pico=max(p.get("ip_pico_casa",0),p.get("ip_pico_fora",0)); ip18=max(p.get("ip_consec_18_casa",0),p.get("ip_consec_18_fora",0)); ip22=max(p.get("ip_consec_22_casa",0),p.get("ip_consec_22_fora",0))
    score += 3 if ip_pico >= 18 else 0; score += 4 if ip_pico >= 22 else 0; score += 4 if ip18 >= 3 else 0; score += 5 if ip22 >= 3 else 0
    score += 3 if ap >= 18 else 0; score += 3 if ap >= 25 else 0; score += 3 if ap >= 35 else 0; score += 4 if abs(apc-apf) >= 10 else 0; score += 3 if abs(apc-apf) >= 18 else 0
    score += 2 if at >= 70 else 0; score += 2 if at >= 95 else 0
    score += 4 if rb >= 2 else 0; score += 4 if rb >= 3 else 0; score += 4 if rb >= 5 else 0
    score += 3 if rda >= 2 else 0; score += 4 if rda >= 4 else 0; score += 2 if rl >= 5 else 0; score += 2 if rl >= 8 else 0; score += 3 if abs(rbc-rbf) >= 3 and rb >= 3 else 0
    score += 1 if cant >= 5 else 0; score += 1 if cant >= 8 else 0; score += 2 if abs(hc-hf) >= 25 else 0; score += 3 if abs(chc-chf) >= 5 else 0
    score += 2 if xg >= .35 else 0; score += 3 if xg >= .55 else 0; score += 3 if xg >= 1 else 0; score += 2 if xgi >= 1.5 else 0; score += 2 if xgi >= 2.3 else 0
    return score


def momentum_score(metricas):
    u5c,u5f=metricas["ultimos5"]; u10c,u10f=metricas["ultimos10"]; u5=u5c+u5f; u10=u10c+u10f; score=0
    score += 3 if u5 >= 3 else 0; score += 4 if u5 >= 5 else 0; score += 2 if u5 >= 8 else 0; score += 4 if u10 >= 7 else 0; score += 3 if u10 >= 10 else 0; score += 2 if u10 >= 14 else 0; score += 3 if abs(u10c-u10f) >= 4 else 0; score += 2 if abs(u5c-u5f) >= 3 else 0
    return score


def relogio_score(metricas, estrategia):
    t = metricas["tempo"]
    if eh_ht(estrategia):
        if 18 <= t <= 30: return 8
        if 31 <= t <= 36: return 5
        if 37 <= t <= 38: return -2
        if t < 18: return -10
        return -6
    if eh_ft(estrategia):
        if 65 <= t <= 75: return 8
        if 76 <= t <= 81: return 5
        if 82 <= t <= 83: return -4
        if t >= 84: return -14
        if t < 63: return -6
        return 2
    return 0


def ajuste_vermelho_contextual(metricas):
    liga=classificar_liga(metricas.get("competicao","")); vermelho=lado_com_vermelho(metricas); dom,_=lado_dominante(metricas); fav,_=lado_favorito(metricas); zebra=lado_zebra(metricas)
    if vermelho in ["NENHUM","AMBOS"] or dom == "EQUILIBRADO": return 0,"SEM_VERMELHO_RELEVANTE"
    peso = 1.25 if liga == "PREMIUM" else .55 if liga == "PERIGOSA" else 1
    if vermelho != dom and pressao_viva(metricas):
        bonus = int(7*peso) + (int(3*peso) if dom == zebra else 0) + (int(2*peso) if vermelho == fav else 0)
        return bonus,"VERMELHO_ABRIU_PRESSAO"
    if vermelho == dom: return -6,"VERMELHO_NO_LADO_PRESSAO"
    return 0,"SEM_AJUSTE_VERMELHO"


def ajuste_gol_recente_contextual(metricas, estrategia):
    tempo=metricas["tempo"]; ug=metricas["ultimo_gol"]; lado=metricas.get("ultimo_gol_lado","DESCONHECIDO")
    if ug <= 0: return 0,"SEM_GOL_RECENTE"
    minutos=tempo-ug
    if minutos < 0: return -14,"GOL_INCONSISTENTE"
    dom,_=lado_dominante(metricas); pressionado=lado_sofrendo_pressao(metricas); fav,_=lado_favorito(metricas); zebra=lado_zebra(metricas); vivo=pressao_viva(metricas)
    if eh_confirmacao(estrategia) and minutos <= 2:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo: return 12,"GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado == zebra and dom == fav and vivo: return 14,"GOL_ZEBRA_CONTRA_FLUXO"
            if lado == dom: return -30,"TRAVA_CONFIRMACAO_PRESSAO_PREMIADA"
        return -18,"TRAVA_CONFIRMACAO_GOL_RECENTE"
    if minutos <= 2:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo: return 10,"GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado == zebra and dom == fav and vivo: return 12,"GOL_ZEBRA_CONTRA_FLUXO"
            if lado == dom: return -16,"PRESSAO_PREMIADA_RECENTE"
        return -8,"GOL_RECENTE_SEM_CONFIRMACAO"
    if minutos <= 5:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo: return 8,"GOL_TIME_PRESSIONADO_ABRIU_JOGO"
            if lado == zebra and dom == fav and vivo: return 10,"GOL_ZEBRA_CONTRA_FLUXO"
            if lado == dom and not vivo: return -12,"PRESSAO_PREMIADA_MORREU"
            if lado == dom and vivo: return -5,"PRESSAO_PREMIADA_MAS_VIVA"
        return -4,"GOL_RECENTE_CAUTELA"
    if minutos <= 10:
        if lado != "DESCONHECIDO":
            if lado == pressionado and dom != "EQUILIBRADO" and vivo: return 5,"GOL_CONTRA_FLUXO_CONTEXTO_BOM"
            if lado == zebra and dom == fav and vivo: return 7,"GOL_ZEBRA_ABRIU_JOGO"
            if lado == fav and dom != lado and vivo: return 3,"FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA"
            if lado == dom and not vivo: return -6,"GOL_MATOU_RITMO"
    if minutos > 10 and lado != "DESCONHECIDO" and dom != lado and vivo: return 3,"PRESSAO_POS_GOL_VALIDADA"
    return 0,"GOL_ANTIGO_OK"


def fake_pressure_penalty_gol(metricas):
    ap=sum(metricas["ataques_perigosos"]); rb=sum(metricas["remates_baliza"]); rl=sum(metricas["remates_lado"]); rda=sum(metricas.get("remates_dentro_area",(0,0))); u5=sum(metricas["ultimos5"]); u10=sum(metricas["ultimos10"]); xg=sum(metricas.get("xg",(0,0))); xgi=sum(metricas.get("xgi",(0,0))); cant=sum(metricas["cantos"]); padrao=metricas.get("padrao_alfa","")
    pen=0
    if ap >= 25 and rb <= 1: pen -= 8
    if ap >= 18 and rb == 0: pen -= 12
    if ap >= 25 and rl <= 2 and rb <= 1: pen -= 6
    if u10 <= 3 and u5 <= 1: pen -= 8
    if ap >= 25 and xg <= .20: pen -= 8
    if ap >= 25 and xg <= .35 and xgi <= .60 and rb <= 1: pen -= 6
    if cant >= 4 and rb <= 1 and xg <= .35: pen -= 6
    if rda == 0 and rb <= 1 and ap >= 20: pen -= 5
    if padrao in ["ALFA_FT_FORTE","ALFA_HT_FORTE","ALFA_FT_QUASE_FORTE","ALFA_HT_QUASE_FORTE"]: pen = int(pen*.55)
    return pen


def score_delta_confirmacao(metricas, estrategia, chave_jogo):
    if not eh_confirmacao(estrategia): return 0,"SEM_DELTA"
    ant = ultimas_leituras_por_jogo.get(chave_jogo, {}).get("metricas", {})
    if not ant: return 0,"SEM_LEITURA_ANTERIOR"
    score=0
    for campo,peso in [("ultimos5",4),("ultimos10",3),("ataques_perigosos",3),("remates_baliza",4),("cantos",2)]:
        if sum(metricas.get(campo,(0,0))) > sum(ant.get(campo,(0,0))): score += peso
    if sum(metricas.get("ultimos5",(0,0))) < max(1, sum(ant.get("ultimos5",(0,0))) - 2): score -= 4
    if sum(metricas.get("ultimos10",(0,0))) < max(1, sum(ant.get("ultimos10",(0,0))) - 3): score -= 4
    if score >= 8: return score,"CONFIRMACAO_MELHOROU"
    if score <= -4: return score,"CONFIRMACAO_PIOROU"
    return score,"CONFIRMACAO_ESTAVEL"


def ajuste_confianca_gol(metricas, estrategia, score):
    rb=sum(metricas["remates_baliza"]); rda=sum(metricas.get("remates_dentro_area",(0,0))); ap=sum(metricas["ataques_perigosos"]); u10=sum(metricas["ultimos10"]); u5=sum(metricas["ultimos5"]); xg=sum(metricas.get("xg",(0,0))); liga=classificar_liga(metricas.get("competicao","")); padrao=metricas.get("padrao_alfa","")
    red=.55 if padrao in ["ALFA_FT_FORTE","ALFA_HT_FORTE","ALFA_FT_QUASE_FORTE","ALFA_HT_QUASE_FORTE"] else 1
    if score >= 88 and rb <= 1: score -= int(10*red)
    if score >= 85 and rb <= 1 and rda <= 1: score -= int(7*red)
    if eh_ft(estrategia) and metricas["tempo"] >= 81 and u5 <= 2: score -= 8
    if eh_ft(estrategia) and rb == 0: score -= int(8*red)
    if ap < 18: score -= 5
    if u10 < 5: score -= 5
    if xg <= .15 and rb <= 1: score -= int(8*red)
    if liga == "PERIGOSA" and score >= 86: score -= 4
    return score


def calcular_forca_premium_gol(metricas):
    apc,apf=metricas["ataques_perigosos"]; atc,atf=metricas["ataques"]; rbc,rbf=metricas["remates_baliza"]; rdac,rdaf=metricas.get("remates_dentro_area",(0,0)); u5c,u5f=metricas["ultimos5"]; u10c,u10f=metricas["ultimos10"]; chc,chf=metricas.get("chance_golo",(0,0)); hc,hf=metricas.get("heatmap",(0,0)); xgc,xgf=metricas.get("xg",(0,0)); p=metricas.get("pressao_alfa",{})
    pts=0
    pts += 2 if max(p.get("ip_consec_22_casa",0),p.get("ip_consec_22_fora",0)) >= 3 else 0
    pts += 1 if max(p.get("ip_consec_18_casa",0),p.get("ip_consec_18_fora",0)) >= 3 else 0
    pts += 1 if abs(apc-apf) >= 14 else 0; pts += 1 if abs(atc-atf) >= 18 else 0; pts += 1 if abs(rbc-rbf) >= 3 and rbc+rbf >= 3 else 0; pts += 1 if rdac+rdaf >= 3 else 0; pts += 1 if abs(u10c-u10f) >= 6 and u10c+u10f >= 8 else 0; pts += 1 if abs(u5c-u5f) >= 4 else 0; pts += 1 if abs(chc-chf) >= 5 else 0; pts += 1 if xgc+xgf >= .55 else 0; pts += 1 if abs(hc-hf) >= 22 else 0
    return pts


def teto_contextual_gol(metricas, estrategia, score):
    tempo=metricas["tempo"]; placar=metricas["placar"]; apc,apf=metricas["ataques_perigosos"]; rbc,rbf=metricas["remates_baliza"]; cc,cf=metricas["cantos"]; u10c,u10f=metricas["ultimos10"]; xgc,xgf=metricas.get("xg",(0,0)); rdac,rdaf=metricas.get("remates_dentro_area",(0,0)); rb=rbc+rbf; rda=rdac+rdaf; u10=u10c+u10f; xg=xgc+xgf; dif_ap=abs(apc-apf); dif_rb=abs(rbc-rbf); dif_c=abs(cc-cf); gc,gf=extrair_gols_placar(placar); dif_placar=abs(gc-gf) if gc is not None else 0; ug=metricas["ultimo_gol"]; minutos=tempo-ug if ug>0 else 999; forca=calcular_forca_premium_gol(metricas); motivo=metricas.get("motivo_gol_contextual",""); liga=classificar_liga(metricas.get("competicao","")); padrao=metricas.get("padrao_alfa",""); delta=metricas.get("motivo_delta_contextual","")
    premium = forca >= 5 and minutos > 2; bom = u10 >= 8 or dif_ap >= 10 or rb >= 3 or xg >= .45 or rda >= 3; forte = padrao in ["ALFA_FT_FORTE","ALFA_HT_FORTE","ALFA_FT_QUASE_FORTE","ALFA_HT_QUASE_FORTE"]
    if minutos < 0: score=min(score,75)
    elif minutos <= 2:
        if motivo in ["GOL_ZEBRA_CONTRA_FLUXO","GOL_TIME_PRESSIONADO_ABRIU_JOGO"]: score=min(score,92)
        elif motivo == "TRAVA_CONFIRMACAO_PRESSAO_PREMIADA": score=min(score,72)
        elif motivo == "PRESSAO_PREMIADA_RECENTE": score=min(score,76)
        else: score=min(score,78)
    elif minutos <= 5:
        if motivo in ["GOL_ZEBRA_CONTRA_FLUXO","GOL_TIME_PRESSIONADO_ABRIU_JOGO"]: score=min(score,92)
        elif motivo in ["PRESSAO_PREMIADA_MORREU","PRESSAO_PREMIADA_RECENTE"]: score=min(score,78)
        elif motivo == "PRESSAO_PREMIADA_MAS_VIVA": score=min(score,84)
        else: score=min(score,82)
    if rb <= 1: score=min(score, 84 if forte else 82)
    if rb == 0 and rda <= 1: score=min(score, 80 if forte else 78)
    if dif_ap <= 5 and dif_rb <= 1 and dif_c <= 1: score=min(score, 83 if forte else 81)
    if xg <= .25 and rb <= 1: score=min(score, 80 if forte else 78)
    if liga == "PERIGOSA": score=min(score, 84 if (premium or forte) else 80)
    if delta == "CONFIRMACAO_PIOROU": score=min(score,78)
    if eh_confirmacao(estrategia): score=min(score, 94 if premium and delta == "CONFIRMACAO_MELHOROU" else 90 if premium or forte else 86 if bom else 80)
    else: score=min(score, 92 if premium or forte else 87 if bom else 81)
    if eh_ft(estrategia) and dif_placar >= 3 and not pressao_viva(metricas): score=min(score,78)
    return score


def score_gol(metricas, estrategia, chave_jogo):
    score=42
    bonus,padrao=score_padrao_alfa(metricas, estrategia); score += bonus; metricas["padrao_alfa"] = padrao
    score += dominio_score_gol(metricas) + momentum_score(metricas) + relogio_score(metricas, estrategia) + fake_pressure_penalty_gol(metricas) + liga_score(metricas,"gol")
    ag,mg=ajuste_gol_recente_contextual(metricas, estrategia); score += ag; metricas["motivo_gol_contextual"] = mg
    av,mv=ajuste_vermelho_contextual(metricas); score += av; metricas["motivo_vermelho_contextual"] = mv
    d,md=score_delta_confirmacao(metricas, estrategia, chave_jogo); score += d; metricas["motivo_delta_contextual"] = md
    metricas["lado_favorito"] = lado_favorito(metricas)[0]; metricas["lado_zebra"] = lado_zebra(metricas); metricas["lado_dominante"] = lado_dominante(metricas)[0]; metricas["lado_vermelho"] = lado_com_vermelho(metricas)
    score += int(favorito_score(metricas) * (.65 if eh_confirmacao(estrategia) else .75)) + (4 if eh_confirmacao(estrategia) else 5)
    score = ajuste_confianca_gol(metricas, estrategia, score); score = teto_contextual_gol(metricas, estrategia, score)
    if not tempo_operacional_valido(metricas, estrategia): score=min(score,72)
    return int(max(0,min(score,99))), metricas


def pressao_lateral_score(metricas):
    ap=sum(metricas["ataques_perigosos"]); at=sum(metricas["ataques"]); cant=sum(metricas["cantos"]); u5=sum(metricas["ultimos5"]); u10=sum(metricas["ultimos10"]); rb=sum(metricas["remates_baliza"]); rl=sum(metricas["remates_lado"]); rda=sum(metricas.get("remates_dentro_area",(0,0))); hc,hf=metricas.get("heatmap",(0,0)); hmc,hmf=metricas.get("heatmap_middle",(0,0)); p=metricas.get("pressao_alfa",{})
    score=0; ip=max(p.get("ip_pico_casa",0),p.get("ip_pico_fora",0))
    score += 3 if ip >= 18 else 0; score += 3 if ip >= 22 else 0; score += 5 if ap >= 20 else 0; score += 4 if ap >= 30 else 0; score += 3 if ap >= 40 else 0; score += 3 if at >= 50 else 0; score += 2 if at >= 75 else 0; score += 4 if u5 >= 4 else 0; score += 3 if u5 >= 7 else 0; score += 4 if u10 >= 8 else 0; score += 3 if u10 >= 12 else 0; score += 3 if cant >= 2 else 0; score += 4 if cant >= 4 else 0; score += 3 if cant >= 6 else 0; score += 2 if abs(metricas["cantos"][0]-metricas["cantos"][1]) >= 2 else 0; score += 3 if rl >= 4 else 0; score += 3 if rl >= 7 else 0; score += 4 if rb <= 2 and ap >= 20 else 0; score += 2 if rda <= 2 and ap >= 20 else 0; score += 3 if abs(hc-hf) >= 18 else 0; score += 4 if max(hc,hf) >= 55 and max(hmc,hmf) <= 45 else 0
    return score


def teto_contextual_canto(metricas, estrategia, score):
    ap=sum(metricas["ataques_perigosos"]); cant=sum(metricas["cantos"]); u5=sum(metricas["ultimos5"]); u10=sum(metricas["ultimos10"]); rb=sum(metricas["remates_baliza"]); rl=sum(metricas["remates_lado"]); motivo=metricas.get("motivo_canto_gol_contextual",""); liga=classificar_liga(metricas.get("competicao",""))
    if ap < 16: score=min(score,74)
    if cant == 0 and u5 < 5: score=min(score,74)
    if cant <= 1 and u10 < 7: score=min(score,76)
    if rl <= 2 and rb <= 1 and cant <= 1: score=min(score,76)
    if motivo in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA","PRESSAO_PREMIADA_MORREU"]: score=min(score,78)
    if liga == "PERIGOSA" and score >= 85: score=min(score,82)
    score=min(score,94 if eh_confirmacao(estrategia) and metricas.get("motivo_canto_delta_contextual") == "CONFIRMACAO_MELHOROU" else 90 if eh_confirmacao(estrategia) else 92)
    return score


def score_canto(metricas, estrategia, chave_jogo):
    score=40 + pressao_lateral_score(metricas) + int(momentum_score(metricas)*.85) + liga_score(metricas,"canto") + relogio_score(metricas, estrategia)
    rb=sum(metricas["remates_baliza"]); rda=sum(metricas.get("remates_dentro_area",(0,0))); ap=sum(metricas["ataques_perigosos"]); cant=sum(metricas["cantos"]); u5=sum(metricas["ultimos5"]); u10=sum(metricas["ultimos10"])
    if ap >= 25 and cant >= 3 and rb <= 2: score += 6
    if u5 >= 5 and cant >= 2: score += 4
    if u10 >= 9 and cant >= 3: score += 4
    if rb >= 5 and rda >= 4: score -= 4
    _, motivo=ajuste_gol_recente_contextual(metricas, estrategia); metricas["motivo_canto_gol_contextual"] = motivo
    if motivo in ["TRAVA_CONFIRMACAO_PRESSAO_PREMIADA","PRESSAO_PREMIADA_RECENTE","PRESSAO_PREMIADA_MORREU"]: score -= 8
    elif motivo in ["GOL_ZEBRA_CONTRA_FLUXO","GOL_TIME_PRESSIONADO_ABRIU_JOGO","GOL_ZEBRA_ABRIU_JOGO"]: score += 4
    d,md=score_delta_confirmacao(metricas, estrategia, chave_jogo); metricas["motivo_canto_delta_contextual"] = md
    score += 5 if md == "CONFIRMACAO_MELHOROU" else -5 if md == "CONFIRMACAO_PIOROU" else 0
    score=teto_contextual_canto(metricas, estrategia, score)
    if not tempo_operacional_valido(metricas, estrategia): score=min(score,72)
    return int(max(0,min(score,99)))


def tipo_pressao(metricas, gol, canto):
    if gol >= CORTE_GOL and canto >= CORTE_CANTO: return "HIBRIDA" if abs(gol-canto) <= 8 else "GOL_COM_CANTO" if gol > canto else "CANTO_COM_GOL"
    if gol >= CORTE_GOL: return "GOL"
    if canto >= CORTE_CANTO: return "CANTO"
    if pressao_viva(metricas): return "OBSERVACAO"
    return "BLOQUEIO"


def calcular_scores(texto, estrategia, chave_jogo):
    metricas=extrair_metricas(texto)
    gol,metricas=score_gol(metricas, estrategia, chave_jogo)
    canto=score_canto(metricas, estrategia, chave_jogo)
    tipo=tipo_pressao(metricas,gol,canto)
    metricas["score_gol"]=gol; metricas["score_canto"]=canto; metricas["tipo_pressao"]=tipo
    return gol,canto,tipo,metricas


def prioridade_alerta(alerta):
    return (1 if eh_confirmacao(alerta["estrategia"]) else 0, max(alerta.get("score_gol",0), alerta.get("score_canto",0)), alerta["metricas"]["tempo"])


def melhor_alerta(alertas):
    return sorted(alertas, key=prioridade_alerta, reverse=True)[0]


def ja_enviado_recentemente(chave_envio):
    limpar_memoria_interna(); agora=time.time()
    return chave_envio in ultimos_enviados and agora - ultimos_enviados[chave_envio] <= COOLDOWN_SEGUNDOS


def marcar_enviado(chave_envio):
    ultimos_enviados[chave_envio]=time.time()


def salvar_ultima_leitura(chave_jogo, alerta):
    ultimas_leituras_por_jogo[chave_jogo] = {"estrategia": alerta["estrategia"], "metricas": alerta["metricas"], "score_gol": alerta.get("score_gol",0), "score_canto": alerta.get("score_canto",0), "tipo_pressao": alerta.get("tipo_pressao","BLOQUEIO"), "recebido_em": time.time()}


def faixa_publica(score):
    if score >= 98: return "👑 ENTRADA LENDÁRIA 👑"
    if score >= 89: return "💎 ENTRADA DIAMANTE 💎"
    if score >= 81: return "🔥 ENTRADA ELITE 🔥"
    if score >= 75: return "🚨 ENTRADA FORTE 🚨"
    return "⚠️ OBSERVAÇÃO"


def nome_publico_bot(estrategia):
    return {"ALFA_HT":"ALFA HT", "ALFA_HT_CONFIRMACAO":"ALFA HT CONFIRMAÇÃO", "ARCE_HT":"ALFA HT / ARCE", "ALFA_FT":"ALFA FT", "ALFA_FT_CONFIRMACAO":"ALFA FT CONFIRMAÇÃO", "CHAMA_FT":"ALFA FT / CHAMA"}.get(estrategia, estrategia)


def texto_liga_publico(metricas):
    liga=classificar_liga(metricas.get("competicao",""))
    return "🟢 Liga Premium" if liga == "PREMIUM" else "🟡 Liga Moderada" if liga == "MODERADA" else "🔴 Liga Perigosa" if liga == "PERIGOSA" else "⚪ Liga Neutra"


def texto_contexto_gol(metricas):
    motivo=metricas.get("motivo_gol_contextual",""); verm=metricas.get("motivo_vermelho_contextual",""); delta=metricas.get("motivo_delta_contextual",""); padrao=metricas.get("padrao_alfa",""); textos=[]
    if padrao in ["ALFA_FT_FORTE","ALFA_HT_FORTE"]: textos.append("🧠 Padrão ALFA: IP sustentado + Chance de Gol dentro da base histórica.")
    elif padrao in ["ALFA_FT_QUASE_FORTE","ALFA_HT_QUASE_FORTE","ALFA_FT_RESSALVA","ALFA_HT_RESSALVA"]: textos.append("⚠️ Padrão ALFA: contexto forte, mas com ressalva em algum ponto da régua.")
    if motivo in ["GOL_ZEBRA_CONTRA_FLUXO","GOL_TIME_PRESSIONADO_ABRIU_JOGO"]: textos.append("🔥 Contexto extra: gol contra o fluxo aumentou a urgência ofensiva.")
    elif motivo == "GOL_CONTRA_FLUXO_CONTEXTO_BOM": textos.append("🔥 Contexto extra: time pressionado marcou, mas o jogo segue aberto.")
    elif motivo == "PRESSAO_PREMIADA_MAS_VIVA": textos.append("⚠️ Contexto: pressão já foi premiada, mas segue viva.")
    elif motivo == "FAVORITO_MARCOU_MAS_OPONENTE_PRESSIONA": textos.append("🔥 Contexto extra: gol anterior não matou o jogo; adversário segue pressionando.")
    if delta == "CONFIRMACAO_MELHOROU": textos.append("📈 Confirmação: pressão aumentou em relação ao primeiro gatilho.")
    elif delta == "CONFIRMACAO_PIOROU": textos.append("⚠️ Confirmação: pressão caiu em relação ao primeiro gatilho.")
    if verm == "VERMELHO_ABRIU_PRESSAO": textos.append("🟥 Contexto extra: superioridade numérica reforça a pressão ofensiva.")
    return "\n" + "\n".join(textos) if textos else ""


def texto_contexto_canto(metricas):
    textos=[]; cant=sum(metricas.get("cantos",(0,0))); ult=metricas.get("ultimos_cantos_lados",[])
    if metricas.get("motivo_canto_delta_contextual") == "CONFIRMACAO_MELHOROU": textos.append("📈 Confirmação: pressão territorial aumentou em relação ao primeiro gatilho.")
    if cant >= 4: textos.append("🚩 Volume de cantos já ativo no jogo.")
    if ult: textos.append(f"🚩 Último canto: {ult[0][0]}' {ult[0][1].title()}.")
    return "\n" + "\n".join(textos) if textos else ""


def montar_mensagem_gol(jogo, estrategia, score, metricas):
    ctx=texto_contexto_gol(metricas); link=f"\n🔗 Bet365: {html.escape(metricas['bet365'])}" if metricas.get("bet365") else ""
    return f"""{html.escape(faixa_publica(score))}

⚽ <b>COUTIPS {html.escape(nome_publico_bot(estrategia))}</b>

🏟 Jogo: {html.escape(str(jogo))}
🕛 Tempo: {metricas['tempo']}'
📊 Placar: {html.escape(str(metricas['placar']))}

🎯 Mercado: {html.escape(str(metricas['mercado']))}
📈 Chance COUTIPS de Gol: <b>{score}%</b>
{html.escape(texto_liga_publico(metricas))}

🔥 Leitura ALFA:
Pressão ofensiva viva, padrão contextual aprovado e cenário voltado para novo gol.{ctx}

📌 Entrar somente se a odd estiver acima de 1.60.
💰 Gestão recomendada: 1% da banca.
⚠️ Confirmar mercado e odd ao vivo antes da entrada.{link}

COUTIPS — leitura ao vivo com pressão, contexto e disciplina."""


def montar_mensagem_canto(jogo, estrategia, score, metricas):
    ctx=texto_contexto_canto(metricas); link=f"\n🔗 Bet365: {html.escape(metricas['bet365'])}" if metricas.get("bet365") else ""
    return f"""🐐<b>GOAT CORNERS</b> 🚩

{html.escape(faixa_publica(score))}

🏟 Jogo: {html.escape(str(jogo))}
🕛 Tempo: {metricas['tempo']}'
⚽ Placar: {html.escape(str(metricas['placar']))}

🚩 Mercado: {html.escape(mercado_cantos_dinamico(metricas))}
📊 Pressão para Canto: <b>{score}%</b>
📌 Bot: {html.escape(nome_publico_bot(estrategia))}
{html.escape(texto_liga_publico(metricas))}

🔥 Leitura GOAT:
Pressão territorial forte, ataques constantes e tendência de bola travada/cantos.{ctx}

⚠️ Entrada somente se a linha e a odd ainda fizerem sentido.
💰 Gestão: 1% da banca.
🔞 Jogue com responsabilidade.{link}"""


async def trabalhador_envio():
    log("📤 Fila de envio iniciada.")
    while True:
        item = await fila_envio.get()
        try:
            alerta=item["alerta"]; mercado=item["mercado"]
            if mercado == "GOL":
                mensagem=montar_mensagem_gol(alerta["jogo"], alerta["estrategia"], alerta["score_gol"], alerta["metricas"]); destino=TARGET_CHANNEL; score_log=alerta["score_gol"]
            elif mercado == "CANTO":
                mensagem=montar_mensagem_canto(alerta["jogo"], alerta["estrategia"], alerta["score_canto"], alerta["metricas"]); destino=CORNERS_CHANNEL; score_log=alerta["score_canto"]
            else:
                log(f"⚠️ Mercado desconhecido na fila: {mercado}"); fila_envio.task_done(); continue
            await client.send_message(destino, mensagem, parse_mode="html")
            marcar_enviado(item["chave_envio"])
            log(f"✅ ENVIADO {mercado} | {alerta['estrategia']} | {score_log}% | {alerta['jogo']} | canal={destino}")
            await asyncio.sleep(INTERVALO_ENVIO_SEGUNDOS)
        except FloodWaitError as e:
            log(f"⛔ FLOOD WAIT: aguardando {e.seconds} segundos."); await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            log(f"❌ ERRO AO ENVIAR MENSAGEM: {e}"); log(traceback.format_exc())
        finally:
            fila_envio.task_done()


async def decidir_e_enviar(chave_jogo):
    try:
        await asyncio.sleep(JANELA_DECISAO_SEGUNDOS)
        alertas=pendentes_por_jogo.pop(chave_jogo, []); tarefas_decisao.pop(chave_jogo, None)
        if not alertas:
            log(f"ℹ️ Nenhum alerta pendente para {chave_jogo}"); return
        escolhido=melhor_alerta(alertas); mercados=[]
        if escolhido["score_gol"] >= CORTE_GOL: mercados.append("GOL")
        if escolhido["score_canto"] >= CORTE_CANTO: mercados.append("CANTO")
        if not mercados:
            log(f"⛔ BLOQUEADO FINAL | Gol={escolhido['score_gol']}% Canto={escolhido['score_canto']}% | {escolhido['jogo']}"); return
        if len(alertas) > 1:
            log(f"🏆 PRIORIDADE APLICADA | Escolhido: {escolhido['estrategia']} | Recebidos: {', '.join(a['estrategia'] for a in alertas)} | Jogo: {escolhido['jogo']}")
        for mercado in mercados:
            chave_envio=f"{chave_jogo}_{mercado}"
            if ja_enviado_recentemente(chave_envio):
                log(f"⛔ BLOQUEADO POR COOLDOWN {mercado} | {escolhido['jogo']}"); continue
            await fila_envio.put({"alerta": escolhido, "mercado": mercado, "chave_envio": chave_envio})
    except Exception as e:
        log(f"❌ ERRO NA JANELA DE DECISÃO: {e}"); log(traceback.format_exc())


async def watchdog_envio():
    global tarefa_envio
    while True:
        await asyncio.sleep(WATCHDOG_SEGUNDOS)
        try:
            limpar_memoria_interna()
            if tarefa_envio is None or tarefa_envio.done() or tarefa_envio.cancelled():
                log("⚠️ Watchdog: fila de envio parada. Reiniciando trabalhador."); tarefa_envio=asyncio.create_task(trabalhador_envio())
            log(f"🩺 WATCHDOG OK | fila={fila_envio.qsize()} | pendentes={len(pendentes_por_jogo)} | cache={len(ultimos_enviados)} | leituras={len(ultimas_leituras_por_jogo)}")
        except Exception as e:
            log(f"❌ ERRO NO WATCHDOG: {e}"); log(traceback.format_exc())


async def processar_evento(event, origem="nova"):
    if event.out: return
    texto=event.raw_text or ""
    log(f"📥 EVENTO RECEBIDO DO TELEGRAM | origem={origem}")
    if not texto.strip(): log("ℹ️ Evento ignorado: texto vazio."); return
    if not mensagem_valida(texto): log("ℹ️ Mensagem ignorada: não parece alerta CornerPro."); return
    if bloquear_categoria_base(texto): log("⛔ BLOQUEADO CATEGORIA BASE | Sub-19/Sub-20 não permitido"); return
    try:
        msg_id=getattr(event.message,"id",None); chat_id=getattr(event.message,"chat_id",None); chave_msg=f"{chat_id}_{msg_id}_{origem}"
        if chave_msg in mensagens_processadas: log(f"ℹ️ Evento duplicado ignorado | msg={chave_msg}"); return
        mensagens_processadas[chave_msg]=time.time()
        estrategia=detectar_estrategia(texto); jogo=extrair_jogo(texto); chave_jogo=normalizar_chave_jogo(jogo)
        sg,sc,tipo,metricas=calcular_scores(texto, estrategia, chave_jogo)
        log(f"📊 PROCESSADO | {estrategia} | Gol={sg}% | Canto={sc}% | Tipo={tipo} | {jogo} | {metricas['tempo']}' | {metricas['placar']} | {metricas['mercado']} | Liga={classificar_liga(metricas.get('competicao',''))} | PadraoALFA={metricas.get('padrao_alfa','')} | IP={metricas.get('pressao_alfa',{})} | Fav={metricas.get('lado_favorito')} | Dom={metricas.get('lado_dominante')} | Gol={metricas.get('ultimo_gol')} {metricas.get('ultimo_gol_lado')} | GolCtx={metricas.get('motivo_gol_contextual','')} | Delta={metricas.get('motivo_delta_contextual','')} | Vermelho={metricas.get('lado_vermelho')} | VermCtx={metricas.get('motivo_vermelho_contextual','')}")
        alerta={"jogo": jogo, "chave_jogo": chave_jogo, "estrategia": estrategia, "score_gol": sg, "score_canto": sc, "tipo_pressao": tipo, "metricas": metricas, "texto_original": texto, "recebido_em": time.time()}
        salvar_ultima_leitura(chave_jogo, alerta)
        if sg < CORTE_GOL and sc < CORTE_CANTO:
            log(f"⛔ BLOQUEADO POR SCORE | {estrategia} | Gol={sg}% < {CORTE_GOL}% | Canto={sc}% < {CORTE_CANTO}% | {jogo}"); return
        pendentes_por_jogo.setdefault(chave_jogo, []).append(alerta)
        log(f"⏳ ALERTA EM JANELA DE DECISÃO | {estrategia} | Gol={sg}% | Canto={sc}% | aguardando {JANELA_DECISAO_SEGUNDOS}s | {jogo}")
        if chave_jogo not in tarefas_decisao:
            tarefas_decisao[chave_jogo]=asyncio.create_task(decidir_e_enviar(chave_jogo))
    except Exception as e:
        log(f"❌ ERRO NO PROCESSAMENTO DO ALERTA: {e}"); log(traceback.format_exc())


@client.on(events.NewMessage)
async def handler_nova_mensagem(event):
    await processar_evento(event, origem="nova")


@client.on(events.MessageEdited)
async def handler_mensagem_editada(event):
    await processar_evento(event, origem="editada")


async def main():
    global tarefa_envio
    log("🚀 COUTIPS / ALFA / GOAT ONLINE - SCORE ALFA CONTEXTUAL ATIVO")
    log("📊 Estratégias ativas: ALFA_HT | ALFA_HT CONFIRMAÇÃO | ALFA_FT | ALFA_FT CONFIRMAÇÃO | ARCE_HT | CHAMA_FT")
    log(f"⚽ Canal gols: {TARGET_CHANNEL}")
    log(f"🚩 Canal cantos: {CORNERS_CHANNEL}")
    log(f"🎯 Corte gol: {CORTE_GOL}% | Corte canto: {CORTE_CANTO}%")
    log(f"⏳ Janela de decisão por jogo: {JANELA_DECISAO_SEGUNDOS}s")
    log("⚠️ Confirme no Railway que existe apenas 1 instância/replica ativa.")
    log("🧠 Score ALFA: IP sustentado, Chance de Gol, favorito em campo, ARCE/CHAMA no radar, gol recente e pressão convertível.")
    await client.start()
    log("✅ TELEGRAM CONECTADO COM SUCESSO")
    tarefa_envio=asyncio.create_task(trabalhador_envio())
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
