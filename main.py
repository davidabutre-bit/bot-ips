# -*- coding: utf-8 -*-
"""
COUTIPS / ALFA — GOAT V005 DEFINITIVO
HT com 3 portas + FT com funções no nível global

HT:
- Porta 1: Massacre Total (92%)
- Porta 2: Domínio Convertível (87%)
- Porta 3: Super Favorito em Crise (86%)

FT: funções no nível global (correção de escopo)
CORREÇÃO: MASSACRE_CONTINUA_MESMO_PLACAR_ABERTO só libera lado PERDENDO

CORREÇÕES APLICADAS:
- Bug do INTERVALO removido
- ip_lado com underscore corrigido
- titulo_periodo() usa minuto como autoridade
- Duplicação removida
- Funções FT movidas para nível global (fix NameError em produção)
- contexto_emocional_vivo_ft: placar aberto só libera lado perdendo

Start command: python main.py
"""

from __future__ import annotations

import asyncio
import csv
import html
import logging
import os
import re
import time
import traceback
import unicodedata
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

try:
    from telethon.errors.common import TypeNotFoundError
except Exception:
    class TypeNotFoundError(Exception):
        pass


# =========================================================
# VERSÃO / CONFIGURAÇÃO BASE
# =========================================================

VERSAO_COUTIPS = "ALFA_COUTIPS_2026_05_30_HT_3_PORTAS_V005_DEFINITIVO"

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
if not API_ID_RAW or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado no Railway/.env.")
try:
    API_ID = int(API_ID_RAW)
except ValueError as exc:
    raise RuntimeError("API_ID inválido — precisa ser número inteiro.") from exc

SESSION_NAME = os.getenv("SESSION_NAME", "coutips_v2_session")

TARGET_CHANNEL = (
    os.getenv("TARGET_CHANNEL")
    or os.getenv("TARGET_CHANNEL_GOLS")
    or "@CoutipsIPS"
)
CONFIRMATION_CHANNEL = (
    os.getenv("CONFIRMATION_CHANNEL")
    or os.getenv("TARGET_CHANNEL_CONFIRMACAO")
    or "@ALFA_CON"
)
CORNERS_CHANNEL = (
    os.getenv("CORNERS_CHANNEL")
    or os.getenv("TARGET_CHANNEL_CANTOS")
    or "@Goat_Bot01"
)

AUDIT_HT_OK = os.getenv("AUDIT_HT_OK", "")
AUDIT_HT_NO = os.getenv("AUDIT_HT_NO", "")
AUDIT_FT_OK = os.getenv("AUDIT_FT_OK", "")
AUDIT_FT_NO = os.getenv("AUDIT_FT_NO", "")

MODO_TESTE = os.getenv("MODO_TESTE", "false").lower() == "true"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_HABILITADO = os.getenv("OPENAI_HABILITADO", "true").lower() == "true"
OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))

# Cortes HT - três portas
CORTE_HT_MASSACRE = int(os.getenv("CORTE_HT_MASSACRE", "92"))
CORTE_HT_CONVERTIVEL = int(os.getenv("CORTE_HT_CONVERTIVEL", "87"))
CORTE_HT_CRISE = int(os.getenv("CORTE_HT_CRISE", "86"))
CORTE_GOL_FT = int(os.getenv("CORTE_GOL_FT", "83"))
CORTE_CONFIRMACAO_GOL_HT = int(os.getenv("CORTE_CONFIRMACAO_GOL_HT", "85"))
CORTE_CONFIRMACAO_GOL_FT = int(os.getenv("CORTE_CONFIRMACAO_GOL_FT", "80"))

COOLDOWN_SEGUNDOS = int(os.getenv("COOLDOWN_SEGUNDOS", "600"))
CACHE_MAX_SEGUNDOS = int(os.getenv("CACHE_MAX_SEGUNDOS", "3600"))
CACHE_MAX_ENTRADAS = int(os.getenv("CACHE_MAX_ENTRADAS", "800"))
JANELA_DECISAO_SEGUNDOS = float(os.getenv("JANELA_DECISAO_SEGUNDOS", "8"))
INTERVALO_ENVIO_SEGUNDOS = float(os.getenv("INTERVALO_ENVIO_SEGUNDOS", "5"))
WATCHDOG_SEGUNDOS = int(os.getenv("WATCHDOG_SEGUNDOS", "60"))
CSV_PATH = Path(os.getenv("CSV_PATH", "auditoria_alfa.csv"))

ODD_MINIMA_CLIENTE = os.getenv("ODD_MINIMA_CLIENTE", "1.65")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


# =========================================================
# ESTADO GLOBAL
# =========================================================

fila_envio: asyncio.Queue = asyncio.Queue(maxsize=200)
tarefa_envio: Optional[asyncio.Task] = None
ultimos_enviados: Dict[str, Dict[str, Any]] = {}
mensagens_processadas: Dict[str, float] = {}
ultimas_leituras_por_jogo: Dict[str, Dict[str, Any]] = {}
pendentes_por_jogo: Dict[str, List[Dict[str, Any]]] = {}
tarefas_decisao: Dict[str, asyncio.Task] = {}
locks_por_jogo: Dict[str, asyncio.Lock] = {}


# =========================================================
# LOG / UTILITÁRIOS
# =========================================================

def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)
    if msg.startswith("❌"):
        logging.error(msg)
    elif msg.startswith(("⚠️", "⛔", "🟡")):
        logging.warning(msg)
    else:
        logging.info(msg)


def logar_versao_inicial() -> None:
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log(f"🚀 VERSAO_COUTIPS_ATIVA = {VERSAO_COUTIPS}")
    log("✅ GOAT V005 DEFINITIVO — HT 3 portas + FT funções globais")
    log("🛡️ HT: Massacre (92%) | Domínio Convertível (87%) | Super Favorito Crise (86%)")
    log("🛡️ FT: placar aberto só libera lado PERDENDO (fix MASSACRE_CONTINUA)")
    log(f"📊 HT Massacre={CORTE_HT_MASSACRE}% | Convertível={CORTE_HT_CONVERTIVEL}% | Crise={CORTE_HT_CRISE}% | FT={CORTE_GOL_FT}%")
    log(f"📤 Canal gols: {TARGET_CHANNEL} | 🧪 Confirmação: {CONFIRMATION_CHANNEL}")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


def remover_acentos(texto: Any) -> str:
    txt = str(texto or "")
    txt = unicodedata.normalize("NFD", txt)
    return "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")


def normalizar(texto: Any) -> str:
    return str(texto or "").replace("*", "").replace("_", "").replace("**", "").strip()


def limpar_linha(texto: Any) -> str:
    return re.sub(r"\s+", " ", str(texto or "")).strip()


def normalizar_chave_jogo(jogo: str) -> str:
    j = remover_acentos(jogo).lower()
    j = re.sub(r"\([^)]*\)", "", j)
    j = re.sub(r"[^a-z0-9]+", " ", j)
    return re.sub(r"\s+", " ", j).strip()


def pegar_numero(pattern: str, texto: str, padrao: int = 0) -> int:
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return padrao
    try:
        return int(m.group(1))
    except Exception:
        return padrao


def pegar_float(pattern: str, texto: str, padrao: float = 0.0) -> float:
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return padrao
    try:
        return float(m.group(1).replace(",", "."))
    except Exception:
        return padrao


def pegar_par(pattern: str, texto: str) -> Tuple[int, int]:
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return (0, 0)
    try:
        return int(m.group(1)), int(m.group(2))
    except Exception:
        return (0, 0)


def pegar_float_par(pattern: str, texto: str) -> Tuple[float, float]:
    m = re.search(pattern, texto, re.IGNORECASE)
    if not m:
        return (0.0, 0.0)
    try:
        return float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))
    except Exception:
        return (0.0, 0.0)


def clamp(valor: float, minimo: int = 0, maximo: int = 100) -> int:
    return max(minimo, min(maximo, int(round(valor))))


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def lock_jogo(chave: str) -> asyncio.Lock:
    if chave not in locks_por_jogo:
        locks_por_jogo[chave] = asyncio.Lock()
    return locks_por_jogo[chave]


def limpar_memoria_interna() -> None:
    agora = time.time()
    for cache in (ultimos_enviados, mensagens_processadas, ultimas_leituras_por_jogo):
        for k in list(cache.keys()):
            valor = cache.get(k)
            if isinstance(valor, dict):
                ts = float(valor.get("recebido_em", 0) or 0)
            else:
                ts = float(valor or 0)
            if agora - ts > CACHE_MAX_SEGUNDOS:
                cache.pop(k, None)
        if len(cache) > CACHE_MAX_ENTRADAS:
            ordenadas = sorted(
                cache.keys(),
                key=lambda x: cache[x].get("recebido_em", 0) if isinstance(cache[x], dict) else cache[x]
            )
            for k in ordenadas[: len(cache) - CACHE_MAX_ENTRADAS]:
                cache.pop(k, None)

    for k in list(pendentes_por_jogo.keys()):
        alertas = pendentes_por_jogo.get(k, [])
        if not alertas:
            pendentes_por_jogo.pop(k, None)
            tarefas_decisao.pop(k, None)
            locks_por_jogo.pop(k, None)
            continue
        recente = max(a.get("recebido_em", 0) for a in alertas)
        if agora - recente > 180:
            pendentes_por_jogo.pop(k, None)
            tarefas_decisao.pop(k, None)
            locks_por_jogo.pop(k, None)


# =========================================================
# MODELOS DE DADOS
# =========================================================

@dataclass
class Metricas:
    jogo: str = "Jogo não identificado"
    competicao: str = ""
    estrategia: str = "ALFA_FT"
    tempo: int = 0
    placar: str = "0 x 0"
    mercado: str = "Over 0.5 Gol"
    ataques_perigosos: Tuple[int, int] = (0, 0)
    ataques: Tuple[int, int] = (0, 0)
    cantos: Tuple[int, int] = (0, 0)
    posse: Tuple[int, int] = (0, 0)
    remates_baliza: Tuple[int, int] = (0, 0)
    remates_lado: Tuple[int, int] = (0, 0)
    remates_dentro_area: Tuple[int, int] = (0, 0)
    vermelhos: Tuple[int, int] = (0, 0)
    ultimos5: Tuple[int, int] = (0, 0)
    ultimos10: Tuple[int, int] = (0, 0)
    odds: Tuple[float, float, float] = (0.0, 0.0, 0.0)
    ultimo_gol: int = 0
    ultimo_gol_lado: str = "DESCONHECIDO"
    ultimos_cantos_lados: List[Tuple[int, str]] = field(default_factory=list)
    chance_golo: Tuple[int, int] = (0, 0)
    xg: Tuple[float, float] = (0.0, 0.0)
    xgl: Tuple[float, float] = (0.0, 0.0)
    xgi: Tuple[float, float] = (0.0, 0.0)
    avgxg: Tuple[float, float] = (0.0, 0.0)
    pressao_alfa: Dict[str, float] = field(default_factory=dict)
    bet365: str = ""
    cornerpro: str = ""
    texto_bruto: str = ""

    liga: str = "NEUTRA"
    lado_favorito: str = "DESCONHECIDO"
    odd_favorito: float = 0.0
    lado_zebra: str = "DESCONHECIDO"
    lado_dominante: str = "EQUILIBRADO"
    lado_pressionante: str = "DESCONHECIDO"
    valor_pos_evento_classe: str = "SEM_VALOR_ESPECIAL"
    valor_pos_evento_motivo: str = ""
    protecao_ia_ativa: bool = False
    parser_confianca: int = 0
    ht_score_nivel: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DecisaoPython:
    score: int
    aprovado_pre_ia: bool
    status: str
    motivo: str
    detalhes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DecisaoIA:
    decisao: str
    confianca_original: int
    confianca_corrigida: int
    motivo: str
    protecao_ativa: bool
    protecao_motivo: str


@dataclass
class Alerta:
    texto: str
    metricas: Metricas
    chave_jogo: str
    recebido_em: float
    origem: str = "telegram"


# =========================================================
# DETECÇÃO / PARSER
# =========================================================

def detectar_estrategia(texto: str) -> str:
    t = remover_acentos(texto).upper()
    t = re.sub(r"\s+", " ", t)

    if "BOT_HT CONFIRMACAO" in t or "HT CONFIRMACAO" in t or "HT CONFIRMAÇÃO" in t or "ALFA HT CONFIRMACAO" in t:
        return "ALFA_HT_CONFIRMACAO"
    if "BOT_FT CONFIRMACAO" in t or "FT CONFIRMACAO" in t or "FT CONFIRMAÇÃO" in t or "ALFA FT CONFIRMACAO" in t:
        return "ALFA_FT_CONFIRMACAO"

    if "ARCE_HT" in t or "ARCE HT" in t or " ARCE " in t:
        return "ARCE_HT"
    if "CHAMA_FT" in t or "CHAMA FT" in t or " CHAMA " in t:
        return "CHAMA_FT"

    if (
        "BOT_HT" in t or "BOT HT" in t or "HT_PREMIUM" in t or "HT PREMIUM" in t
        or "HT_PREMIUN" in t or "HT_MODERADO" in t or "HT MODERADO" in t
        or "IPS HT" in t or "ALFA HT" in t
        or "PRIMEIRO TEMPO" in t or "1ºT" in t or "1T" in t
    ):
        return "ALFA_HT"

    if (
        "BOT_FT" in t or "BOT FT" in t or "FT_PREMIUM" in t or "FT PREMIUM" in t
        or "FT_PREMIUN" in t or "FT_MODERADO" in t or "FT MODERADO" in t
        or "IPS FT" in t or "POS-70" in t or "POS 70" in t or "PÓS-70" in t
        or "ALFA FT" in t or "SEGUNDO TEMPO" in t or "2ºT" in t or "2T" in t
    ):
        return "ALFA_FT"

    # REMOVIDO: bug do INTERVALO que convertia FT em HT
    return "ALFA_FT"


def eh_ht(estrategia: str) -> bool:
    return estrategia in {"ALFA_HT", "ALFA_HT_CONFIRMACAO", "ARCE_HT"}


def eh_ft(estrategia: str) -> bool:
    return estrategia in {"ALFA_FT", "ALFA_FT_CONFIRMACAO", "CHAMA_FT"}


def eh_confirmacao(estrategia: str) -> bool:
    return estrategia in {"ALFA_HT_CONFIRMACAO", "ALFA_FT_CONFIRMACAO"}


def mensagem_valida(texto: str) -> bool:
    t = remover_acentos(texto).upper()
    return "ALERTA ESTRATEGIA" in t and "JOGO:" in t and "TEMPO:" in t


def extrair_jogo(texto: str) -> str:
    t = normalizar(texto)
    m = re.search(r"Jogo:\s*(.+)", t, re.IGNORECASE)
    if m:
        return limpar_linha(m.group(1).split("\n")[0])
    for linha in t.splitlines():
        if " x " in linha.lower() or " vs " in linha.lower():
            return limpar_linha(linha)
    return "Jogo não identificado"


def extrair_competicao(texto: str) -> str:
    m = re.search(r"Competição:\s*(.+)", normalizar(texto), re.IGNORECASE)
    return limpar_linha(m.group(1).split("\n")[0]) if m else ""


def extrair_tempo(texto: str) -> int:
    return pegar_numero(r"Tempo:\s*(\d+)", texto, 0)


def extrair_resultado(texto: str) -> str:
    m = re.search(r"Resultado:\s*([0-9]+)\s*x\s*([0-9]+)", texto, re.IGNORECASE)
    if m:
        return f"{m.group(1)} x {m.group(2)}"
    return "0 x 0"


def extrair_gols_placar(placar: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.search(r"([0-9]+)\s*x\s*([0-9]+)", str(placar or ""))
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def mercado_dinamico(placar: str) -> str:
    gc, gf = extrair_gols_placar(placar)
    if gc is None or gf is None:
        return "Over 0.5 Gol"
    return f"Over {gc + gf + 0.5:.1f} Gol"


def extrair_odds(texto: str) -> Tuple[float, float, float]:
    m = re.search(r"Odds.*?:\s*([0-9.,]+)\s*/\s*([0-9.,]+)\s*/\s*([0-9.,]+)", texto, re.IGNORECASE)
    if not m:
        return (0.0, 0.0, 0.0)
    try:
        return tuple(float(x.replace(",", ".")) for x in m.groups())
    except Exception:
        return (0.0, 0.0, 0.0)


def extrair_ultimo_gol_lado(texto: str) -> str:
    t = remover_acentos(texto).upper()
    for padrao in (
        r"ULTIMO\s+GOLO:\s*\d+\s*['']?\s*(CASA|FORA|HOME|AWAY)",
        r"ULTIMO\s+GOL:\s*\d+\s*['']?\s*(CASA|FORA|HOME|AWAY)",
    ):
        m = re.search(padrao, t)
        if m:
            return "CASA" if m.group(1) in {"CASA", "HOME"} else "FORA"
    return "DESCONHECIDO"


def extrair_ultimo_gol_minuto(texto: str) -> int:
    t = remover_acentos(texto)
    return (
        pegar_numero(r"Ultimo\s+golo:\s*(\d+)", t, 0)
        or pegar_numero(r"Ultimo\s+gol:\s*(\d+)", t, 0)
        or pegar_numero(r"Último\s+golo:\s*(\d+)", texto, 0)
        or pegar_numero(r"Último\s+gol:\s*(\d+)", texto, 0)
    )


def extrair_ultimos_cantos_lados(texto: str) -> List[Tuple[int, str]]:
    t = remover_acentos(texto).upper()
    m = re.search(r"ULTIMOS\s+CANTOS:\s*(.+)", t)
    if not m:
        return []
    linha = m.group(1).split("\n")[0]
    eventos: List[Tuple[int, str]] = []
    for minuto, lado in re.findall(r"(\d+)\s*['']?\s*(CASA|FORA|HOME|AWAY)", linha):
        eventos.append((int(minuto), "CASA" if lado in {"CASA", "HOME"} else "FORA"))
    return eventos


def extrair_links(texto: str) -> Tuple[str, str]:
    bet365 = ""
    corner = ""
    for link in re.findall(r"https?://[^\s*]+", texto, re.IGNORECASE):
        if "bet365" in link.lower() and not bet365:
            bet365 = link.strip()
        if "cornerprobet" in link.lower() and "/analysis/" in link.lower() and not corner:
            corner = link.strip()
    return bet365, corner


def extrair_pressao_alfa(texto: str) -> Dict[str, float]:
    base = normalizar(texto)
    sem = remover_acentos(base)
    m = re.search(r"Índice de Pressão:(.+)", base, re.IGNORECASE | re.DOTALL)
    if not m:
        m = re.search(r"Indice de Pressao:(.+)", sem, re.IGNORECASE | re.DOTALL)
    if not m:
        return _pressao_vazia()

    bloco = m.group(1).split("https://")[0]
    casa_vals: List[float] = []
    fora_vals: List[float] = []
    for seg in bloco.split(";"):
        nums = re.findall(r"\d+(?:[.,]\d+)?", seg)
        if len(nums) == 2:
            try:
                casa_vals.append(float(nums[0].replace(",", ".")))
                fora_vals.append(float(nums[1].replace(",", ".")))
            except Exception:
                continue

    def consec(vals: List[float], limite: float) -> int:
        atual = maior = 0
        for v in vals:
            if v >= limite:
                atual += 1
                maior = max(maior, atual)
            else:
                atual = 0
        return maior

    def media(vals: List[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    return {
        "ip_pico_casa": max(casa_vals) if casa_vals else 0.0,
        "ip_pico_fora": max(fora_vals) if fora_vals else 0.0,
        "ip_media_casa": media(casa_vals),
        "ip_media_fora": media(fora_vals),
        "ip_consec_10_casa": consec(casa_vals, 10),
        "ip_consec_10_fora": consec(fora_vals, 10),
        "ip_consec_15_casa": consec(casa_vals, 15),
        "ip_consec_15_fora": consec(fora_vals, 15),
        "ip_consec_18_casa": consec(casa_vals, 18),
        "ip_consec_18_fora": consec(fora_vals, 18),
        "ip_consec_22_casa": consec(casa_vals, 22),
        "ip_consec_22_fora": consec(fora_vals, 22),
    }


def _pressao_vazia() -> Dict[str, float]:
    return {
        "ip_pico_casa": 0.0, "ip_pico_fora": 0.0,
        "ip_media_casa": 0.0, "ip_media_fora": 0.0,
        "ip_consec_10_casa": 0, "ip_consec_10_fora": 0,
        "ip_consec_15_casa": 0, "ip_consec_15_fora": 0,
        "ip_consec_18_casa": 0, "ip_consec_18_fora": 0,
        "ip_consec_22_casa": 0, "ip_consec_22_fora": 0,
    }


def calcular_confianca_parser(m: Metricas) -> int:
    score = 0
    if m.ataques_perigosos != (0, 0):
        score += 1
    if m.remates_baliza != (0, 0):
        score += 1
    if m.ultimos5 != (0, 0):
        score += 1
    if m.ultimos10 != (0, 0):
        score += 1
    if m.xg != (0.0, 0.0):
        score += 1
    if m.chance_golo != (0, 0):
        score += 1
    if m.odds != (0.0, 0.0, 0.0):
        score += 1
    if m.pressao_alfa and (
        m.pressao_alfa.get("ip_pico_casa", 0) > 0
        or m.pressao_alfa.get("ip_pico_fora", 0) > 0
    ):
        score += 1
    return score


def extrair_metricas(texto: str) -> Metricas:
    tl = normalizar(texto)
    estrategia = detectar_estrategia(tl)
    placar = extrair_resultado(tl)
    bet365, corner = extrair_links(tl)

    rda = pegar_par(r"R\.\s*Dentro\s*Área:Casa=(\d+)\s*/\s*Fora=(\d+)", tl)
    if rda == (0, 0):
        rda = pegar_par(r"R\.\s*Dentro\s*Area:Casa=(\d+)\s*/\s*Fora=(\d+)", remover_acentos(tl))

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

    m = Metricas(
        jogo=extrair_jogo(tl),
        competicao=extrair_competicao(tl),
        estrategia=estrategia,
        tempo=extrair_tempo(tl),
        placar=placar,
        mercado=mercado_dinamico(placar),
        ataques_perigosos=pegar_par(r"Ataques Perigosos:\s*(\d+)\s*-\s*(\d+)", tl),
        ataques=pegar_par(r"Ataques:\s*(\d+)\s*-\s*(\d+)", tl),
        cantos=pegar_par(r"Cantos:\s*(\d+)\s*-\s*(\d+)", tl),
        posse=pegar_par(r"Posse bola:\s*(\d+)\s*-\s*(\d+)", tl),
        remates_baliza=pegar_par(r"Remates Baliza:\s*(\d+)\s*-\s*(\d+)", tl),
        remates_lado=pegar_par(r"Remates lado:\s*(\d+)\s*-\s*(\d+)", tl),
        remates_dentro_area=rda,
        vermelhos=pegar_par(r"Cartões vermelhos:\s*(\d+)\s*-\s*(\d+)", tl),
        ultimos5=pegar_par(r"(?:Ultimos|Últimos)\s*5['']?:\s*(-?\d+)\s*\([^)]*\)\s*-\s*(-?\d+)", tl),
        ultimos10=pegar_par(r"(?:Ultimos|Últimos)\s*10['']?:\s*(-?\d+)\s*\([^)]*\)\s*-\s*(-?\d+)", tl),
        odds=extrair_odds(tl),
        ultimo_gol=extrair_ultimo_gol_minuto(tl),
        ultimo_gol_lado=extrair_ultimo_gol_lado(tl),
        ultimos_cantos_lados=extrair_ultimos_cantos_lados(tl),
        chance_golo=pegar_par(r"Chance de Golo:Casa=(\d+)\s*/\s*Fora=(\d+)", tl),
        xg=xg,
        xgl=xgl,
        xgi=xgi,
        avgxg=pegar_float_par(r"avgXGaFavor:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", tl),
        pressao_alfa=extrair_pressao_alfa(tl),
        bet365=bet365,
        cornerpro=corner,
        texto_bruto=texto,
    )
    preencher_contexto_calculado(m)
    m.parser_confianca = calcular_confianca_parser(m)
    if m.parser_confianca <= 2:
        log(f"🔴 PARSER_CRITICO | confianca={m.parser_confianca}/8 | {m.jogo} | {m.tempo}'")
    elif m.parser_confianca <= 4:
        log(f"⚠️ PARSER_ALERTA | confianca={m.parser_confianca}/8 | {m.jogo} | {m.tempo}'")
    return m


# =========================================================
# LIGAS
# =========================================================

LIGAS_PREMIUM = {
    "england premier", "england championship", "germany", "france ligue",
    "usa", "mls", "australia", "netherlands", "belgium", "switzerland",
    "champions league", "europa league", "conference league", "women", "womens",
    "feminino", "femenino", "feminine", "frauen", "naiset", "kvinder", "w "
}
LIGAS_MODERADAS = {
    "brazil", "brasil", "spain", "italy serie b", "turkey", "saudi", "uae",
    "qatar", "japan", "china", "norway", "sweden", "denmark", "finland",
    "poland", "czech", "scotland", "greece", "korea", "faroe", "iceland",
    "latvia", "lithuania", "estonia", "austria", "switzerland 2", "georgia",
}
LIGAS_UNDER = {
    "argentina primera", "colombia primera", "peru liga", "chile primera",
    "uruguay primera", "ecuador", "bolivia", "paraguay", "venezuela", "italy serie a",
    "nigeria", "ghana", "kenya", "tanzania", "egypt", "morocco", "algeria",
}
LIGAS_PERIGOSAS = {
    "mongolia", "myanmar", "cambodia", "laos", "bhutan", "san marino",
    "andorra", "gibraltar", "kosovo", "moldova", "armenia", "azerbaijan",
}

_TIMES_ELITE_TRANSICAO = {
    "atletico madrid", "atletico de madrid", "real madrid", "barcelona",
    "paris saint-germain", "psg", "bayern munich", "bayer leverkusen",
    "borussia dortmund", "rb leipzig", "eintracht frankfurt",
    "manchester city", "liverpool", "arsenal", "chelsea",
    "inter milan", "inter", "napoli", "juventus",
    "flamengo", "palmeiras", "corinthians",
}


def classificar_liga(competicao: str) -> str:
    c = remover_acentos(competicao).lower()
    if any(x in c for x in LIGAS_PERIGOSAS):
        return "PERIGOSA"
    if any(x in c for x in LIGAS_UNDER):
        return "UNDER"
    if any(x in c for x in LIGAS_PREMIUM):
        return "PREMIUM"
    if any(x in c for x in LIGAS_MODERADAS):
        return "MODERADA"
    return "NEUTRA"


def liga_ajuste(liga: str) -> int:
    return {"PREMIUM": 5, "MODERADA": 1, "NEUTRA": 0, "UNDER": -8, "PERIGOSA": -12}.get(liga, 0)


# =========================================================
# LADOS / CONTEXTO
# =========================================================

def valor_lado(m: Metricas, campo: str, lado: str) -> float:
    val = getattr(m, campo, (0, 0))
    if not isinstance(val, tuple) or len(val) < 2:
        return 0
    return float(val[0] if lado == "CASA" else val[1] if lado == "FORA" else 0)


def soma_lados(m: Metricas, campo: str) -> float:
    val = getattr(m, campo, (0, 0))
    if not isinstance(val, tuple) or len(val) < 2:
        return 0
    return float(val[0] + val[1])


def lado_favorito(m: Metricas) -> Tuple[str, float]:
    oc, _, of = m.odds
    if not oc or not of:
        return "DESCONHECIDO", 0.0
    if oc < of:
        return "CASA", oc
    if of < oc:
        return "FORA", of
    return "EQUILIBRADO", oc


def lado_zebra_fn(fav: str) -> str:
    if fav == "CASA":
        return "FORA"
    if fav == "FORA":
        return "CASA"
    return "DESCONHECIDO"


def lado_vencendo(m: Metricas) -> str:
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return "DESCONHECIDO"
    if gc > gf:
        return "CASA"
    if gf > gc:
        return "FORA"
    return "EMPATE"


def lado_perdendo(m: Metricas) -> str:
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return "DESCONHECIDO"
    if gc < gf:
        return "CASA"
    if gf < gc:
        return "FORA"
    return "EMPATE"


def dados_lado(m: Metricas, lado: str) -> Dict[str, float]:
    return {
        "ap": valor_lado(m, "ataques_perigosos", lado),
        "ataques": valor_lado(m, "ataques", lado),
        "cantos": valor_lado(m, "cantos", lado),
        "posse": valor_lado(m, "posse", lado),
        "rb": valor_lado(m, "remates_baliza", lado),
        "rl": valor_lado(m, "remates_lado", lado),
        "rda": valor_lado(m, "remates_dentro_area", lado),
        "u5": max(0, valor_lado(m, "ultimos5", lado)),
        "u10": max(0, valor_lado(m, "ultimos10", lado)),
        "chance": valor_lado(m, "chance_golo", lado),
        "xg": valor_lado(m, "xg", lado),
        "xgl": valor_lado(m, "xgl", lado),
    }


def ip_lado(m: Metricas, lado: str) -> Dict[str, float]:
    p = m.pressao_alfa or _pressao_vazia()
    suf = "casa" if lado == "CASA" else "fora"
    return {
        "pico": float(p.get(f"ip_pico_{suf}", 0)),
        "media": float(p.get(f"ip_media_{suf}", 0)),
        "c10": float(p.get(f"ip_consec_10_{suf}", 0)),
        "c15": float(p.get(f"ip_consec_15_{suf}", 0)),
        "c18": float(p.get(f"ip_consec_18_{suf}", 0)),
        "c22": float(p.get(f"ip_consec_22_{suf}", 0)),
    }


def preencher_contexto_calculado(m: Metricas) -> None:
    m.liga = classificar_liga(m.competicao)
    m.lado_favorito, m.odd_favorito = lado_favorito(m)
    m.lado_zebra = lado_zebra_fn(m.lado_favorito)

    if eh_ht(m.estrategia):
        m.lado_pressionante = m.lado_favorito if m.lado_favorito in {"CASA", "FORA"} else "DESCONHECIDO"
    else:
        casa_pontos = (
            dados_lado(m, "CASA")["ap"] * 1.0
            + dados_lado(m, "CASA")["u5"] * 2.2
            + dados_lado(m, "CASA")["rb"] * 4.2
            + dados_lado(m, "CASA")["chance"] * 1.35
        )
        fora_pontos = (
            dados_lado(m, "FORA")["ap"] * 1.0
            + dados_lado(m, "FORA")["u5"] * 2.2
            + dados_lado(m, "FORA")["rb"] * 4.2
            + dados_lado(m, "FORA")["chance"] * 1.35
        )
        if casa_pontos - fora_pontos >= 8:
            m.lado_dominante = "CASA"
            m.lado_pressionante = "CASA"
        elif fora_pontos - casa_pontos >= 8:
            m.lado_dominante = "FORA"
            m.lado_pressionante = "FORA"
        else:
            m.lado_dominante = "EQUILIBRADO"
            m.lado_pressionante = (
                m.lado_favorito
                if m.lado_favorito in {"CASA", "FORA"} and m.odd_favorito <= 1.85
                else "DESCONHECIDO"
            )


# =========================================================
# VALIDAÇÃO DE CONSISTÊNCIA ESTRATÉGIA-MINUTO
# =========================================================

def validar_consistencia_estrategia(m: Metricas) -> Tuple[bool, str]:
    if eh_ht(m.estrategia) and m.tempo > 45:
        return False, f"HT_STRATEGIA_COM_MINUTO_{m.tempo}"
    if eh_ft(m.estrategia) and m.tempo < 46:
        return False, f"FT_STRATEGIA_COM_MINUTO_{m.tempo}"
    return True, "OK"


# =========================================================
# FUNÇÕES FT — NÍVEL GLOBAL
# =========================================================

def favorito_nao_vencendo(m: Metricas) -> bool:
    if m.lado_favorito not in {"CASA", "FORA"}:
        return False
    vencedor = lado_vencendo(m)
    return vencedor == "EMPATE" or vencedor != m.lado_favorito


def faixa_favorito(m: Metricas) -> str:
    odd = m.odd_favorito
    if not odd:
        return "SEM_ODD"
    if odd <= 1.30:
        return "SUPER_FAVORITO"
    if odd <= 1.55:
        return "FAVORITO_FORTE"
    if odd <= 1.85:
        return "FAVORITO_CONTEXTUAL"
    if odd <= 2.20:
        return "FAVORITO_FRACO_SO_EXTREMO"
    return "SEM_BONUS_FAVORITO"


def pressao_viva_lado_ft(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return d["u5"] >= 3 or d["u10"] >= 7 or ip["pico"] >= 20 or ip["c18"] >= 2 or ip["c22"] >= 1


def pressao_morta_lado(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return d["u5"] <= 1 and d["u10"] <= 3 and ip["pico"] < 18 and ip["c18"] == 0


def consequencia_real_lado_ft(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    return d["rb"] >= 1 or d["rb"] + d["rl"] >= 4 or d["cantos"] >= 2 or d["chance"] >= 8 or d["xg"] >= 0.28


def consequencia_minima_emocional(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return (
        d["rb"] >= 1
        or d["rl"] >= 2
        or d["cantos"] >= 2
        or d["chance"] >= 8
        or d["xg"] >= 0.25
        or (ip["pico"] >= 24 and d["ap"] >= 15)
    )


def contexto_emocional_vivo_ft(m: Metricas, lado: str) -> Tuple[bool, str]:
    """
    CORREÇÃO V005: placar aberto (total>=3, dif>=2) só libera o lado PERDENDO.
    O lado vencedor por 3x0, 4x0 etc não tem fome — jogo encerrado emocionalmente.
    """
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return True, "PLACAR_DESCONHECIDO"

    total_gols = gc + gf
    dif = abs(gc - gf)
    fav = m.lado_favorito
    perdendo = lado_perdendo(m)
    vencendo = lado_vencendo(m)

    if total_gols >= 3 and dif >= 2:
        # Lado vencendo por placar aberto não tem fome — jogo resolvido
        if lado == vencendo:
            return False, "PLACAR_RESOLVIDO_VENCEDOR_SEM_NECESSIDADE"
        # Lado perdendo: só libera se ainda tem pressão e consequência real
        if pressao_viva_lado_ft(m, lado) and consequencia_real_lado_ft(m, lado):
            return True, "TIME_ATRAS_REAGE_PLACAR_ABERTO"
        return False, "PLACAR_ABERTO_DEMAIS_SEM_VALOR"

    if vencendo == "EMPATE":
        return True, "EMPATE_CONTEXTO_VIVO"
    if dif <= 1:
        return True, "PLACAR_APERTADO"
    if fav in {"CASA", "FORA"} and perdendo == fav:
        return True, "FAVORITO_ATRAS_DO_PLACAR"

    if dif >= 2 and lado == vencendo:
        if pressao_viva_lado_ft(m, lado) and consequencia_real_lado_ft(m, lado):
            return True, "MASSACRE_CONTINUA_VIVO"
        return False, "PLACAR_RESOLVIDO_SEM_FOME"

    if dif >= 2 and lado == perdendo:
        if pressao_viva_lado_ft(m, lado) and consequencia_real_lado_ft(m, lado):
            return True, "TIME_ATRAS_PRECISA_REAGIR_COM_PRESSAO"
        return False, "TIME_ATRAS_SEM_PRESSAO_REAL"

    return True, "CONTEXTO_NEUTRO_VIVO"


def pressao_extrema_lado_ft(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return (
        d["ap"] >= 25
        and (d["chance"] >= 10 or d["rb"] >= 2 or d["cantos"] >= 4 or d["rl"] >= 7)
        and (ip["pico"] >= 24 or ip["c18"] >= 3 or ip["c22"] >= 2)
    )


def vermelho_contra_pressionante(m: Metricas) -> bool:
    vc, vf = m.vermelhos
    if m.lado_pressionante == "CASA" and vc > vf:
        return True
    if m.lado_pressionante == "FORA" and vf > vc:
        return True
    return False


def avaliar_valor_pos_evento_ft(m: Metricas) -> Tuple[str, str, int, bool]:
    lado = m.lado_pressionante
    fav = m.lado_favorito
    ultimo_lado = m.ultimo_gol_lado
    tempo = m.tempo
    ultimo = m.ultimo_gol
    minutos = tempo - ultimo if ultimo else 999

    if lado not in {"CASA", "FORA"}:
        return "SEM_VALOR_ESPECIAL", "SEM_LADO_PRESSIONANTE", 0, False

    fav_nv = favorito_nao_vencendo(m)
    fav_press = fav == lado and pressao_viva_lado_ft(m, lado)

    if fav in {"CASA", "FORA"} and fav_nv and fav_press:
        faixa = faixa_favorito(m)
        if faixa == "SUPER_FAVORITO":
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "SUPER_FAVORITO_NAO_VENCE_E_PRESSIONA", 10, True
        if faixa == "FAVORITO_FORTE":
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "FAVORITO_FORTE_NAO_VENCE_E_PRESSIONA", 7, True
        if faixa == "FAVORITO_CONTEXTUAL" and consequencia_real_lado_ft(m, lado):
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "FAVORITO_CONTEXTUAL_NAO_VENCE_COM_CONSEQUENCIA", 4, True
        if faixa == "FAVORITO_FRACO_SO_EXTREMO" and pressao_extrema_lado_ft(m, lado):
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "FAVORITO_FRACO_SO_EXTREMO_VALIDADO", 1, True

    if not ultimo or minutos > 12 or ultimo_lado == "DESCONHECIDO":
        return "SEM_VALOR_ESPECIAL", "SEM_GOL_RECENTE_RELEVANTE", 0, fav_press and fav_nv

    if ultimo_lado != lado and pressao_viva_lado_ft(m, lado):
        if fav == lado or ultimo_lado == m.lado_zebra:
            ajuste = 10 if m.odd_favorito and m.odd_favorito <= 1.30 else 7
            return "GOL_CONTRA_FLUXO_VALORIZA", "ZEBRA_MARCOU_E_PRESSIONANTE_SEGUE_VIVO", ajuste, True
        return "GOL_CONTRA_FLUXO_VALORIZA", "ADVERSARIO_MARCOU_E_PRESSAO_CONTINUA", 5, True

    if ultimo_lado == lado:
        if pressao_morta_lado(m, lado):
            return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_E_RITMO_CAIU", -14, False
        if pressao_viva_lado_ft(m, lado):
            return "PRESSAO_PREMIADA_MAS_CONTINUA", "GOL_PREMIOU_MAS_PRESSAO_CONTINUA", -2, True
        return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_SEM_CONTINUIDADE_CLARA", -8, False

    return "SEM_VALOR_ESPECIAL", "GOL_RECENTE_SEM_LEITURA_ESPECIAL", 0, False


def finalizacao_minima_lado_ft(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    return d["rb"] >= 1 or d["rda"] >= 1 or d["xg"] >= 0.15 or d["chance"] >= 5


def funil_ft_contextual(m: Metricas) -> Tuple[bool, int, str, Dict[str, Any]]:
    lado = m.lado_pressionante
    detalhes: Dict[str, Any] = {}

    if m.liga == "PERIGOSA":
        return False, 72, "FUNIL_LIGA_PERIGOSA", detalhes
    if lado not in {"CASA", "FORA"}:
        return False, 72, "FUNIL_SEM_LADO_PRESSIONANTE", detalhes
    if vermelho_contra_pressionante(m):
        return False, 74, "FUNIL_VERMELHO_CONTRA_PRESSIONANTE", detalhes

    pressao = pressao_viva_lado_ft(m, lado)
    consequencia = consequencia_real_lado_ft(m, lado)
    extremo = pressao_extrema_lado_ft(m, lado)
    fav_nao_vence = favorito_nao_vencendo(m) and m.lado_favorito == lado
    super_fav = bool(m.odd_favorito and m.odd_favorito <= 1.30 and fav_nao_vence)

    valor_classe, _, _, proteger_ia = avaliar_valor_pos_evento_ft(m)
    m.valor_pos_evento_classe = valor_classe
    m.protecao_ia_ativa = proteger_ia

    valor_forte = valor_classe in {
        "GOL_CONTRA_FLUXO_VALORIZA",
        "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA",
        "PRESSAO_PREMIADA_MAS_CONTINUA",
    }
    tem_finalizacao = finalizacao_minima_lado_ft(m, lado)
    valor_forte_validado = valor_forte and tem_finalizacao
    consequencia_minima = consequencia_minima_emocional(m, lado)
    emocional_vivo, motivo_emocional = contexto_emocional_vivo_ft(m, lado)

    detalhes.update({
        "pressao_viva": pressao,
        "consequencia": consequencia,
        "pressao_extrema": extremo,
        "favorito_nao_vence": fav_nao_vence,
        "super_favorito_nao_vence": super_fav,
        "valor_forte_validado": valor_forte_validado,
        "tem_finalizacao_minima": tem_finalizacao,
        "consequencia_minima_emocional": consequencia_minima,
        "contexto_emocional_vivo": emocional_vivo,
        "motivo_emocional": motivo_emocional,
    })

    if pressao_morta_lado(m, lado):
        return False, 74, "FUNIL_PRESSAO_MORTA", detalhes

    if not emocional_vivo and not (super_fav and extremo):
        return False, 76, f"FUNIL_CONTEXTO_EMOCIONAL_MORTO_{motivo_emocional}", detalhes

    if not pressao:
        return False, 76, "FUNIL_FT_SEM_PRESSAO_VIVA", detalhes

    excecao_contextual_valida = (
        (valor_forte_validado or (super_fav and extremo))
        and consequencia_minima
    )

    if not consequencia and not excecao_contextual_valida:
        if valor_forte and not tem_finalizacao:
            return False, 78, "FUNIL_FT_VALOR_FORTE_SEM_FINALIZACAO", detalhes
        return False, 78, "FUNIL_FT_SEM_CONSEQUENCIA_MINIMA", detalhes

    if m.odd_favorito and m.odd_favorito > 1.85 and not extremo and not (valor_forte_validado and consequencia_minima):
        return False, 80, "FUNIL_FAVORITO_FRACO_SEM_NUMEROS_EXTREMOS", detalhes

    return True, 100, "FUNIL_FT_APROVADO", detalhes


def score_pressao_viva_ft(m: Metricas) -> int:
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return 0
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    score = 0
    score += min(14, d["u5"] * 2.0)
    score += min(14, d["u10"] * 1.2)
    if ip["pico"] >= 20:
        score += 5
    if ip["pico"] >= 24:
        score += 4
    if ip["pico"] >= 30:
        score += 4
    score += min(8, ip["c18"] * 2)
    score += min(8, ip["c22"] * 3)
    return clamp(score, 0, 32)


def score_consequencia_ft(m: Metricas) -> int:
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return 0
    d = dados_lado(m, lado)
    score = 0
    score += min(12, d["rb"] * 4)
    score += min(8, d["rl"] * 1.2)
    score += min(6, d["cantos"] * 0.9)
    score += min(9, d["chance"] * 0.8)
    score += min(7, d["xg"] * 10)
    if d["ap"] >= 20:
        score += 3
    if d["ap"] >= 35:
        score += 3
    return clamp(score, 0, 32)


def score_favoritismo_ft(m: Metricas) -> int:
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return 0
    faixa = faixa_favorito(m)
    base = {"SUPER_FAVORITO": 10, "FAVORITO_FORTE": 7, "FAVORITO_CONTEXTUAL": 4, "FAVORITO_FRACO_SO_EXTREMO": 1}.get(faixa, 0)
    if favorito_nao_vencendo(m):
        base += 6 if faixa in {"SUPER_FAVORITO", "FAVORITO_FORTE"} else 3
    elif lado_vencendo(m) == fav:
        gc, gf = extrair_gols_placar(m.placar)
        if gc is not None and gf is not None and abs(gc - gf) >= 2:
            base -= 5
    return clamp(base, -8, 22)


def score_relogio_ft(m: Metricas) -> int:
    if 65 <= m.tempo <= 77:
        return 8
    if 78 <= m.tempo <= 81:
        return 3
    if 82 <= m.tempo <= 83:
        return -4
    if 84 <= m.tempo <= 86:
        return -8
    if m.tempo >= 87:
        return -14
    if m.tempo < 63:
        return -6
    return 1


def aplicar_travas_finais_ft(m: Metricas, score: int) -> Tuple[int, str, bool]:
    lado = m.lado_pressionante
    if m.liga == "PERIGOSA":
        return min(score, 74), "LIGA_PERIGOSA", True
    if vermelho_contra_pressionante(m):
        return min(score, 76), "VERMELHO_CONTRA_PRESSIONANTE", True
    if lado not in {"CASA", "FORA"}:
        return min(score, 72), "SEM_LADO_PRESSIONANTE", True
    if pressao_morta_lado(m, lado):
        return min(score, 74), "PRESSAO_MORTA", True
    if not consequencia_real_lado_ft(m, lado):
        if not (m.odd_favorito <= 1.30 and favorito_nao_vencendo(m) and pressao_extrema_lado_ft(m, lado)):
            return min(score, 76), "SEM_CONSEQUENCIA_REAL", True
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    if ip["pico"] >= 24 and d["chance"] <= 3 and d["rb"] == 0 and d["rl"] <= 2 and d["xg"] < 0.25:
        return min(score, 76), "FAKE_PRESSURE_IP_SEM_CONSEQUENCIA", True
    if m.valor_pos_evento_classe == "PRESSAO_PREMIADA_MORREU":
        return min(score, 74), "PRESSAO_PREMIADA_MORREU", True
    return score, "SEM_TRAVA_FINAL", False


def score_python_ft(m: Metricas) -> DecisaoPython:
    """FT: chama funções globais — sem aninhamento."""
    preencher_contexto_calculado(m)

    passou_funil, teto_funil, motivo_funil, detalhes_funil = funil_ft_contextual(m)

    base = 45
    componentes = {
        "pressao_viva": score_pressao_viva_ft(m),
        "consequencia": score_consequencia_ft(m),
        "favoritismo": score_favoritismo_ft(m),
        "liga": liga_ajuste(m.liga),
        "relogio": score_relogio_ft(m),
    }

    valor_classe, valor_motivo, ajuste_evento, proteger_ia = avaliar_valor_pos_evento_ft(m)
    m.valor_pos_evento_classe = valor_classe
    m.valor_pos_evento_motivo = valor_motivo
    m.protecao_ia_ativa = proteger_ia
    componentes["valor_pos_evento"] = ajuste_evento

    score_bruto = base + sum(componentes.values())
    score = clamp(score_bruto)

    if not passou_funil:
        score = min(score, teto_funil)
        detalhes = {
            "componentes": componentes,
            "score_bruto": score_bruto,
            "valor_pos_evento_classe": valor_classe,
            "funil": motivo_funil,
            "funil_detalhes": detalhes_funil,
        }
        return DecisaoPython(score=score, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_funil, detalhes=detalhes)

    score, motivo_trava, bloqueado_trava = aplicar_travas_finais_ft(m, score)
    corte = CORTE_GOL_FT
    aprovado = score >= corte and not bloqueado_trava
    status = "APROVADO" if aprovado else "REPROVADO"
    motivo = motivo_trava if bloqueado_trava else f"score={score} corte={corte}"

    detalhes = {
        "componentes": componentes,
        "score_bruto": score_bruto,
        "corte": corte,
        "valor_pos_evento_classe": valor_classe,
        "funil": motivo_funil,
        "trava": motivo_trava,
    }
    return DecisaoPython(score=score, aprovado_pre_ia=aprovado, status=status, motivo=motivo, detalhes=detalhes)


# =========================================================
# FUNÇÕES HT — NÍVEL GLOBAL (3 PORTAS)
# =========================================================

def funil_ht_massacre(m: Metricas) -> Tuple[bool, int, str]:
    """Porta 1 HT: Massacre Total → 92%"""
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, 72, "SEM_LADO_PRESSIONANTE"

    ap_casa, ap_fora = m.ataques_perigosos
    diff_ap = (ap_casa - ap_fora) if lado == "CASA" else (ap_fora - ap_casa)
    if diff_ap < 18:
        return False, 80, f"AP_DIF_{diff_ap}_<18"

    u10_casa, u10_fora = m.ultimos10
    diff_u10 = (u10_casa - u10_fora) if lado == "CASA" else (u10_fora - u10_casa)
    if diff_u10 < 12:
        return False, 80, f"U10_DIF_{diff_u10}_<12"

    u5_casa, u5_fora = m.ultimos5
    diff_u5 = (u5_casa - u5_fora) if lado == "CASA" else (u5_fora - u5_casa)
    if diff_u5 < 5:
        return False, 78, f"U5_DIF_{diff_u5}_<5"

    d = dados_lado(m, lado)
    consequencia_forte = d["rb"] >= 1 or d["chance"] >= 8 or (d["rb"] + d["rl"]) >= 6
    if not consequencia_forte:
        return False, 78, "CONSEQUENCIA_FORTE_INSUFICIENTE"

    m.ht_score_nivel = "MASSACRE"
    return True, CORTE_HT_MASSACRE, "MASSACRE_TOTAL_92"


def funil_ht_dominio_convertivel(m: Metricas) -> Tuple[bool, int, str]:
    """Porta 2 HT: Domínio Convertível → 87%"""
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, 72, "SEM_LADO_PRESSIONANTE"

    ap_casa, ap_fora = m.ataques_perigosos
    diff_ap = (ap_casa - ap_fora) if lado == "CASA" else (ap_fora - ap_casa)
    if diff_ap < 12:
        return False, 80, f"AP_DIF_{diff_ap}_<12"

    u10_casa, u10_fora = m.ultimos10
    diff_u10 = (u10_casa - u10_fora) if lado == "CASA" else (u10_fora - u10_casa)
    if diff_u10 < 8:
        return False, 80, f"U10_DIF_{diff_u10}_<8"

    u5_casa, u5_fora = m.ultimos5
    diff_u5 = (u5_casa - u5_fora) if lado == "CASA" else (u5_fora - u5_casa)
    if diff_u5 <= 0:
        return False, 78, f"U5_DIF_{diff_u5}_<=0"

    u5_lado = m.ultimos5[0] if lado == "CASA" else m.ultimos5[1]
    if u5_lado < 4:
        return False, 76, f"U5_LADO_{u5_lado}_<4"

    d = dados_lado(m, lado)
    cantos_adv = valor_lado(m, "cantos", m.lado_zebra)
    cantos_diff = d["cantos"] - cantos_adv

    consequencia_convertivel = (
        d["rb"] >= 1
        or d["chance"] >= 7
        or (d["rb"] + d["rl"]) >= 5
        or cantos_diff >= 2
    )
    if not consequencia_convertivel:
        return False, 78, "SEM_CONSEQUENCIA_CONVERTIVEL"

    m.ht_score_nivel = "CONVERTIVEL"
    return True, CORTE_HT_CONVERTIVEL, "DOMINIO_CONVERTIVEL_87"


def funil_ht_super_favorito_crise(m: Metricas) -> Tuple[bool, int, str]:
    """Porta 3 HT: Super Favorito em Crise → 86%"""
    lado = m.lado_favorito
    if lado not in {"CASA", "FORA"}:
        return False, 72, "SEM_FAVORITO"

    if m.odd_favorito > 1.50:
        return False, 76, f"ODD_{m.odd_favorito}_>1.50"

    vencedor = lado_vencendo(m)
    if vencedor == lado:
        return False, 74, "FAVORITO_NAO_ESTA_PERDENDO"
    if vencedor == "EMPATE":
        return False, 74, "JOGO_EMPATADO_NAO_E_CRISE"

    if m.tempo < 15 or m.tempo > 40:
        return False, 72, f"JANELA_TEMPO_{m.tempo}_FORA_15-40"

    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    pressao_viva = d["u5"] >= 2 or d["u10"] >= 4 or ip["pico"] >= 18
    if not pressao_viva:
        return False, 74, "SUPER_FAVORITO_PERDENDO_SEM_PRESSAO"

    consequencia = d["rb"] >= 1 or d["chance"] >= 5 or d["xg"] >= 0.15
    if not consequencia:
        return False, 74, "SUPER_FAVORITO_PERDENDO_SEM_CONSEQUENCIA"

    ap_adv = valor_lado(m, "ataques_perigosos", m.lado_zebra)
    colapso_ap = d["ap"] < ap_adv * 0.60
    colapso_consequencia = d["rb"] == 0 and d["chance"] < 4 and d["xg"] < 0.10
    if colapso_ap and colapso_consequencia:
        return False, 72, "FAVORITO_COLAPSADO"

    total_ap = m.ataques_perigosos[0] + m.ataques_perigosos[1]
    total_fin = sum(m.remates_baliza) + sum(m.remates_lado)
    if total_ap < 30 and total_fin < 8:
        return False, 70, "JOGO_MORTO_SEM_VOLUME"

    m.ht_score_nivel = "SUPER_FAVORITO_CRISE"
    return True, CORTE_HT_CRISE, "SUPER_FAVORITO_EM_CRISE_86"


# =========================================================
# ROTEADOR PRINCIPAL
# =========================================================

def score_python_contextual(m: Metricas, chave: str) -> DecisaoPython:
    """Roteia HT (3 portas) ou FT."""

    consistente, motivo_consistente = validar_consistencia_estrategia(m)
    if not consistente:
        log(f"⛔ INCONSISTENCIA_ESTRATEGIA | {m.estrategia} | {motivo_consistente} | {m.jogo}")
        return DecisaoPython(score=70, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_consistente)

    if m.parser_confianca <= 2:
        log(f"🔴 BLOQUEADO_PARSER_CRITICO | confianca={m.parser_confianca}/8 | {m.jogo}")
        return DecisaoPython(score=65, aprovado_pre_ia=False, status="REPROVADO", motivo=f"PARSER_CRITICO_{m.parser_confianca}/8")

    if eh_ht(m.estrategia):
        motivos: List[str] = []

        aprovado, score, motivo = funil_ht_massacre(m)
        if aprovado:
            log(f"📊 HT_MASSACRE | {m.jogo} | {motivo}")
            return DecisaoPython(score=score, aprovado_pre_ia=True, status="APROVADO", motivo=motivo, detalhes={"ht_nivel": "MASSACRE", "porta": 1})
        motivos.append(f"MASSACRE:{motivo}")

        aprovado, score, motivo = funil_ht_dominio_convertivel(m)
        if aprovado:
            log(f"📊 HT_CONVERTIVEL | {m.jogo} | {motivo}")
            return DecisaoPython(score=score, aprovado_pre_ia=True, status="APROVADO", motivo=motivo, detalhes={"ht_nivel": "CONVERTIVEL", "porta": 2})
        motivos.append(f"CONVERTIVEL:{motivo}")

        aprovado, score, motivo = funil_ht_super_favorito_crise(m)
        if aprovado:
            log(f"📊 HT_CRISE | {m.jogo} | {motivo}")
            return DecisaoPython(score=score, aprovado_pre_ia=True, status="APROVADO", motivo=motivo, detalhes={"ht_nivel": "SUPER_FAVORITO_CRISE", "porta": 3})
        motivos.append(f"CRISE:{motivo}")

        motivo_final = " | ".join(motivos)
        log(f"📊 HT_REPROVADO | {m.jogo} | {motivo_final}")
        return DecisaoPython(score=72, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_final, detalhes={"ht_nivel": "REPROVADO", "motivos": motivos})

    return score_python_ft(m)


# =========================================================
# IA AUDITORA (apenas FT)
# =========================================================

def montar_prompt_ia_ft(m: Metricas) -> str:
    return f"""Você é a IA Auditora do projeto COUTIPS/ALFA para SEGUNDO TEMPO.
Responda obrigatoriamente em UMA linha no formato:
DECISAO=APROVAR|BLOQUEAR; CONFIANCA=0-100; MOTIVO=texto curto

DADOS:
Estratégia: {m.estrategia}
Jogo: {m.jogo}
Minuto: {m.tempo}
Placar: {m.placar}
Liga: {m.liga}
Odds: {m.odds}
Favorito: {m.lado_favorito} odd {m.odd_favorito}
Pressionante: {m.lado_pressionante}
Último gol: {m.ultimo_gol}' {m.ultimo_gol_lado}
AP: {m.ataques_perigosos}
U5: {m.ultimos5}
U10: {m.ultimos10}
Cantos: {m.cantos}
RB: {m.remates_baliza}
Remates lado: {m.remates_lado}
Chance gol: {m.chance_golo}
xG: {m.xg}
Valor pós-evento: {m.valor_pos_evento_classe} | {m.valor_pos_evento_motivo}""".strip()


async def consultar_openai_ft(m: Metricas, score_py: int) -> Tuple[str, int, str]:
    if not OPENAI_HABILITADO or not OPENAI_API_KEY:
        return "APROVAR", score_py, "OPENAI_DESATIVADA"

    prompt = montar_prompt_ia_ft(m)
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "Você é uma IA auditora objetiva de futebol ao vivo. Responda somente no formato pedido."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 120,
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=OPENAI_TIMEOUT) as http:
            r = await http.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"].strip()
        decisao = "APROVAR" if "APROVAR" in content.upper() else "BLOQUEAR" if "BLOQUEAR" in content.upper() else "APROVAR"
        conf = pegar_numero(r"CONFIANCA\s*=\s*(\d+)", content, score_py)
        if conf == score_py:
            conf = pegar_numero(r"CONFIANÇA\s*=\s*(\d+)", content, score_py)
        return decisao, clamp(conf), content[:250]
    except Exception as e:
        log(f"⚠️ OpenAI falhou | {type(e).__name__}: {e}")
        return "APROVAR", score_py, "OPENAI_FALHOU"


def calcular_protecao_ia_ft(m: Metricas, score_py: int, decisao_ia: str, confianca_ia: int) -> DecisaoIA:
    original = confianca_ia
    proteger = False
    motivo = "SEM_PROTECAO"

    lado = m.lado_pressionante
    if lado in {"CASA", "FORA"} and score_py >= 82 and not vermelho_contra_pressionante(m):
        if m.valor_pos_evento_classe in {"GOL_CONTRA_FLUXO_VALORIZA", "PRESSAO_PREMIADA_MAS_CONTINUA", "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA"}:
            proteger = True
            motivo = m.valor_pos_evento_classe
        elif m.lado_favorito == lado and favorito_nao_vencendo(m) and pressao_viva_lado_ft(m, lado):
            proteger = True
            motivo = "FAVORITO_NAO_VENCE_COM_PRESSAO_VIVA"

    impedimentos = []
    if m.liga == "PERIGOSA":
        impedimentos.append("LIGA_PERIGOSA")
    if lado not in {"CASA", "FORA"}:
        impedimentos.append("SEM_LADO_PRESSIONANTE")
    elif pressao_morta_lado(m, lado):
        impedimentos.append("PRESSAO_MORTA")
    if score_py < 82:
        impedimentos.append("SCORE_PYTHON_BAIXO")
    if vermelho_contra_pressionante(m):
        impedimentos.append("VERMELHO_CONTRA_PRESSIONANTE")

    if impedimentos:
        proteger = False
        motivo = "NAO_PROTEGER_" + "+".join(impedimentos)

    if proteger:
        piso = 78
        if m.odd_favorito and m.odd_favorito <= 1.30 and favorito_nao_vencendo(m):
            piso = 82
        corrigida = max(confianca_ia, piso)
        log(f"🛡️ PROTECAO_IA_FT | original={original} | corrigida={corrigida} | motivo={motivo}")
        return DecisaoIA(decisao_ia, original, corrigida, "IA_PROTEGIDA", True, motivo)

    return DecisaoIA(decisao_ia, original, confianca_ia, "SEM_PROTECAO", False, motivo)


# =========================================================
# MENSAGEM TELEGRAM / CSV
# =========================================================

def emoji_liga(liga: str) -> str:
    return {"PREMIUM": "🏆", "MODERADA": "📊", "NEUTRA": "⚪", "UNDER": "⚠️", "PERIGOSA": "🚨"}.get(liga, "⚪")


def emoji_score(score: int) -> str:
    return "💎" if score >= 90 else "🎯"


def titulo_periodo(m: Metricas) -> str:
    if m.estrategia == "ALFA_HT_CONFIRMACAO":
        return "ALFA - CONFIRMADO | PRIMEIRO TEMPO" if m.tempo <= 45 else "ALFA - CONFIRMADO | SEGUNDO TEMPO"
    if m.estrategia == "ALFA_FT_CONFIRMACAO":
        return "ALFA - CONFIRMADO | SEGUNDO TEMPO"
    if m.tempo >= 46:
        return "ALFA - AO VIVO | SEGUNDO TEMPO"
    return "ALFA - AO VIVO | PRIMEIRO TEMPO"


def formatar_alerta_cliente(m: Metricas, score: int) -> str:
    link = m.bet365 or ""
    linhas = [
        f"{emoji_score(score)} {titulo_periodo(m)}",
        "",
        f"🏟 {html.escape(m.jogo)}",
        f"⏱ {m.tempo}' | {m.placar}",
        f"🎯 {m.mercado}",
        f"📊 COUTIPS: {score}%",
        f"{emoji_liga(m.liga)} Liga: {m.liga}",
        f"💰 Odd mínima de entrada: {ODD_MINIMA_CLIENTE}",
        "📝 SIGA SUA GESTÃO DE BANCA",
        "⛔ APOSTE COM RESPONSABILIDADE ⛔",
    ]
    if link:
        linhas.append(f"🔗 {link}")
    return "\n".join(linhas)


def canal_auditoria(m: Metricas, aprovado: bool) -> str:
    if eh_ht(m.estrategia):
        return AUDIT_HT_OK if aprovado else AUDIT_HT_NO
    return AUDIT_FT_OK if aprovado else AUDIT_FT_NO


async def enviar_auditoria(m: Metricas, score_py: int, score_ia: int, score_medio: int, aprovado: bool, motivo: str) -> None:
    canal = canal_auditoria(m, aprovado)
    if not canal:
        return
    status = "APROVADO" if aprovado else "REPROVADO"
    emoji = "✅" if aprovado else "❌"
    texto = (
        f"{emoji} {status} | {m.estrategia}\n"
        f"🏟 {m.jogo}\n"
        f"⏱ {m.tempo}' | {m.placar}\n"
        f"📊 PY={score_py}% | IA={score_ia}% | MÉDIA={score_medio}%\n"
        f"🏆 Liga: {m.liga}\n"
        f"🧠 Valor: {m.valor_pos_evento_classe}\n"
        f"📝 {motivo}"
    )
    await send_resiliente(canal, texto)


CSV_FIELDS = [
    "data_hora", "jogo", "estrategia", "minuto", "placar", "mercado",
    "score_python", "decisao_ia", "ia_original", "ia_corrigida", "score_medio",
    "lado_favorito", "odd_favorito", "lado_pressionante", "ultimo_gol_lado",
    "valor_pos_evento_classe", "valor_pos_evento_motivo", "protecao_ia_ativa",
    "liga", "decisao_final", "motivo_bloqueio", "parser_confianca", "ht_nivel",
    "resultado_manual", "cornerpro", "bet365",
]


def garantir_csv() -> None:
    if CSV_PATH.exists():
        return
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()


def registrar_csv(m: Metricas, decisao_py: DecisaoPython, decisao_ia: Optional[DecisaoIA], score_medio: int, decisao_final: str, motivo: str) -> None:
    garantir_csv()
    row = {
        "data_hora": now_iso(), "jogo": m.jogo, "estrategia": m.estrategia,
        "minuto": m.tempo, "placar": m.placar, "mercado": m.mercado,
        "score_python": decisao_py.score,
        "decisao_ia": decisao_ia.decisao if decisao_ia else "HT_SEM_IA",
        "ia_original": decisao_ia.confianca_original if decisao_ia else 0,
        "ia_corrigida": decisao_ia.confianca_corrigida if decisao_ia else 0,
        "score_medio": score_medio, "lado_favorito": m.lado_favorito,
        "odd_favorito": m.odd_favorito, "lado_pressionante": m.lado_pressionante,
        "ultimo_gol_lado": m.ultimo_gol_lado,
        "valor_pos_evento_classe": m.valor_pos_evento_classe,
        "valor_pos_evento_motivo": m.valor_pos_evento_motivo,
        "protecao_ia_ativa": m.protecao_ia_ativa, "liga": m.liga,
        "decisao_final": decisao_final, "motivo_bloqueio": motivo,
        "parser_confianca": m.parser_confianca,
        "ht_nivel": getattr(m, "ht_score_nivel", ""),
        "resultado_manual": "", "cornerpro": m.cornerpro, "bet365": m.bet365,
    }
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(row)


# =========================================================
# ENVIO RESILIENTE / FILA
# =========================================================

async def send_resiliente(canal: str, mensagem: str, parse_mode: Optional[str] = None, max_tentativas: int = 3) -> bool:
    for tentativa in range(1, max_tentativas + 1):
        try:
            if parse_mode:
                await client.send_message(canal, mensagem, parse_mode=parse_mode)
            else:
                await client.send_message(canal, mensagem)
            return True
        except FloodWaitError as e:
            espera = int(getattr(e, "seconds", 10)) + 1
            log(f"⚠️ FloodWait {espera}s | canal={canal}")
            await asyncio.sleep(espera)
        except TypeNotFoundError:
            log(f"⚠️ TypeNotFoundError tent={tentativa}/{max_tentativas}")
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(2)
            await client.connect()
        except Exception as e:
            log(f"⚠️ Erro envio tent={tentativa}/{max_tentativas} | {type(e).__name__}: {e}")
            await asyncio.sleep(1)
    log(f"❌ ALERTA NÃO ENVIADO | canal={canal}")
    return False


async def trabalhador_fila_envio() -> None:
    log("✅ Fila de envio iniciada.")
    while True:
        item = await fila_envio.get()
        try:
            canal, mensagem, parse_mode = item
            await send_resiliente(canal, mensagem, parse_mode=parse_mode)
            await asyncio.sleep(INTERVALO_ENVIO_SEGUNDOS)
        except Exception as e:
            log(f"❌ Erro fila_envio: {type(e).__name__}: {e}")
            log(traceback.format_exc())
        finally:
            fila_envio.task_done()


async def enfileirar_envio(canal: str, mensagem: str, parse_mode: Optional[str] = None) -> None:
    try:
        await fila_envio.put((canal, mensagem, parse_mode))
    except asyncio.QueueFull:
        log("❌ FILA CHEIA — alerta descartado")


# =========================================================
# CHAVE ÚNICA / COOLDOWN / DESTINO
# =========================================================

def chave_alerta_unica(texto: str) -> str:
    limpo = remover_acentos(texto)
    return f"{normalizar_chave_jogo(extrair_jogo(limpo))}|{detectar_estrategia(limpo)}|{extrair_tempo(limpo)}|{extrair_resultado(limpo)}"


def pode_enviar(chave: str) -> bool:
    ultimo = ultimos_enviados.get(chave)
    if not ultimo:
        return True
    return time.time() - float(ultimo.get("recebido_em", 0)) >= COOLDOWN_SEGUNDOS


def marcar_enviado(chave: str, m: Metricas, score: int) -> None:
    ultimos_enviados[chave] = {"recebido_em": time.time(), "jogo": m.jogo, "score": score}


def destino_principal(m: Metricas, score_medio: int) -> str:
    if MODO_TESTE:
        return CONFIRMATION_CHANNEL
    if eh_confirmacao(m.estrategia):
        return TARGET_CHANNEL if score_medio >= 92 else CONFIRMATION_CHANNEL
    return TARGET_CHANNEL


# =========================================================
# PROCESSAMENTO DE ALERTA
# =========================================================

async def processar_alerta(alerta: Alerta) -> None:
    m = alerta.metricas
    chave = alerta.chave_jogo

    decisao_py = score_python_contextual(m, chave)
    log(f"📊 PY | {m.estrategia} | score={decisao_py.score}% | {m.jogo} | Liga={m.liga} | Press={m.lado_pressionante} | Valor={m.valor_pos_evento_classe}")

    # HT: sem IA
    if eh_ht(m.estrategia):
        score_medio = decisao_py.score
        aprovado = decisao_py.aprovado_pre_ia
        motivo_final = "APROVADO" if aprovado else f"REPROVADO: {decisao_py.motivo}"
        registrar_csv(m, decisao_py, None, score_medio, "APROVADO" if aprovado else "REPROVADO", motivo_final)
        await enviar_auditoria(m, decisao_py.score, 0, score_medio, aprovado, motivo_final)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}

        if not aprovado:
            log(f"⛔ BLOQUEADO | {m.jogo} | {decisao_py.motivo}")
            return
        if not pode_enviar(chave):
            log(f"⏳ COOLDOWN | {m.jogo}")
            return

        mensagem = formatar_alerta_cliente(m, score_medio)
        canal = destino_principal(m, score_medio)
        await enfileirar_envio(canal, mensagem)
        marcar_enviado(chave, m, score_medio)
        log(f"✅ ENVIADO | {m.estrategia} | score={score_medio}% | {m.ht_score_nivel} | {m.jogo}")
        return

    # FT: com IA
    decisao_ia_txt, confianca_ia, motivo_ia = await consultar_openai_ft(m, decisao_py.score)
    decisao_ia = calcular_protecao_ia_ft(m, decisao_py.score, decisao_ia_txt, confianca_ia)

    if decisao_ia.confianca_corrigida <= 45 and not decisao_ia.protecao_ativa:
        score_medio = round((decisao_py.score + decisao_ia.confianca_corrigida) / 2)
        motivo = f"IA_BLOQUEIO_CRITICO | {motivo_ia}"
        log(f"⛔ BLOQUEADO IA CRITICA | IA={decisao_ia.confianca_corrigida}% | {m.jogo}")
        registrar_csv(m, decisao_py, decisao_ia, score_medio, "REPROVADO", motivo)
        await enviar_auditoria(m, decisao_py.score, decisao_ia.confianca_corrigida, score_medio, False, motivo)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}
        return

    score_medio = clamp((decisao_py.score + decisao_ia.confianca_corrigida) / 2)
    corte = CORTE_GOL_FT
    aprovado = score_medio >= corte and decisao_py.status != "REPROVADO"

    if eh_confirmacao(m.estrategia) and score_medio >= 92:
        aprovado = True

    motivo_final = "APROVADO" if aprovado else f"MÉDIA={score_medio}% < {corte}% OU trava_py={decisao_py.motivo}"
    registrar_csv(m, decisao_py, decisao_ia, score_medio, "APROVADO" if aprovado else "REPROVADO", motivo_final)
    await enviar_auditoria(m, decisao_py.score, decisao_ia.confianca_corrigida, score_medio, aprovado, motivo_final)
    ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}

    if not aprovado:
        log(f"⛔ BLOQUEADO | score_medio={score_medio}% < {corte}% | {m.jogo}")
        return
    if not pode_enviar(chave):
        log(f"⏳ COOLDOWN | {m.jogo}")
        return

    mensagem = formatar_alerta_cliente(m, score_medio)
    canal = destino_principal(m, score_medio)
    await enfileirar_envio(canal, mensagem)
    marcar_enviado(chave, m, score_medio)
    log(f"✅ ENVIADO | {m.estrategia} | score={score_medio}% | {m.jogo}")


async def janela_decisao(chave: str) -> None:
    await asyncio.sleep(JANELA_DECISAO_SEGUNDOS)
    async with lock_jogo(chave):
        alertas = pendentes_por_jogo.pop(chave, [])
        tarefas_decisao.pop(chave, None)
    if not alertas:
        return
    alerta = sorted(alertas, key=lambda a: a["recebido_em"])[-1]
    await processar_alerta(alerta["alerta"])


async def receber_mensagem(event: events.NewMessage.Event) -> None:
    try:
        texto = event.raw_text or ""
        if not mensagem_valida(texto):
            return

        msg_id = str(getattr(event.message, "id", ""))
        unique = f"{getattr(event, 'chat_id', '')}:{msg_id}:{hash(texto)}"
        if unique in mensagens_processadas:
            return
        mensagens_processadas[unique] = time.time()

        m = extrair_metricas(texto)
        chave = normalizar_chave_jogo(m.jogo)
        alerta = Alerta(texto=texto, metricas=m, chave_jogo=chave, recebido_em=time.time())
        log(f"📩 EVENTO | {m.estrategia} | {m.jogo} | {m.tempo}' | {m.placar}")

        async with lock_jogo(chave):
            pendentes_por_jogo.setdefault(chave, []).append({"alerta": alerta, "recebido_em": alerta.recebido_em})
            if chave not in tarefas_decisao or tarefas_decisao[chave].done():
                tarefas_decisao[chave] = asyncio.create_task(janela_decisao(chave))
                log(f"⏳ JANELA | {m.estrategia} | {JANELA_DECISAO_SEGUNDOS}s | {m.jogo}")
    except Exception as e:
        log(f"❌ Erro receber_mensagem: {type(e).__name__}: {e}")
        log(traceback.format_exc())


# =========================================================
# WATCHDOG / MAIN
# =========================================================

async def watchdog() -> None:
    while True:
        try:
            limpar_memoria_interna()
            log(
                f"🐕 WATCHDOG | fila={fila_envio.qsize()} | pendentes={len(pendentes_por_jogo)} | "
                f"cache={len(ultimos_enviados)} | leituras={len(ultimas_leituras_por_jogo)}"
            )
        except Exception as e:
            log(f"⚠️ Watchdog erro: {e}")
        await asyncio.sleep(WATCHDOG_SEGUNDOS)


async def main() -> None:
    global tarefa_envio
    logar_versao_inicial()
    garantir_csv()

    tarefa_envio = asyncio.create_task(trabalhador_fila_envio())
    asyncio.create_task(watchdog())

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        await receber_mensagem(event)

    @client.on(events.MessageEdited(incoming=True))
    async def handler_edit(event):
        await receber_mensagem(event)

    log("🚀 INICIANDO BOT")
    await client.start()
    log("✅ TELEGRAM CONECTADO")
    log(f"🤖 OpenAI {'ATIVA' if OPENAI_HABILITADO and OPENAI_API_KEY else 'DESATIVADA'}")
    log(f"📡 Canais: principal={TARGET_CHANNEL} | confirmação={CONFIRMATION_CHANNEL}")
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("🛑 Encerrado manualmente")
    except Exception as exc:
        log(f"❌ FALHA FATAL: {type(exc).__name__}: {exc}")
        log(traceback.format_exc())
        raise
