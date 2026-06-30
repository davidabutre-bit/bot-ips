# -*- coding: utf-8 -*-
"""
COUTIPS / ALFA — SISTEMA AO VIVO (DC01.2)

Isolado do Pré-Live em 30/06/2026 — o motor pré-live (scraper TheoBorges,
market engine, dataclasses) virou um processo próprio em prelive.py, com
sessão Telegram própria (SESSION_STRING_PRELIVE). Esta separação foi feita
depois do crash de 27/06/2026 (AuthKeyDuplicatedError), causado por uma
falha do pré-live que derrubou o ao vivo por compartilharem o mesmo
processo e a mesma sessão. A partir desta versão, isso não pode mais
acontecer — cada um roda isolado, com seu próprio lock de instância única.

Start command: python main.py
"""

from __future__ import annotations

import asyncio
import csv
import html
import hashlib
import json
import logging
import os
import re
import sys
import threading as _threading
import time
import traceback
import unicodedata
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Bot Auditor V2 (item 19/29-06) — módulo separado, opcional. Se o arquivo
# não existir ou der erro de import, o resto do sistema continua
# funcionando normal — o auditor é só uma camada de inteligência por
# cima, nunca pode derrubar o ao vivo/pré-live.
try:
    import coutips_auditor_v2 as auditor_v2
    AUDITOR_V2_DISPONIVEL = True
except Exception as _e_auditor_import:
    auditor_v2 = None
    AUDITOR_V2_DISPONIVEL = False
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

try:
    from telethon.errors.common import TypeNotFoundError
except Exception:  # pragma: no cover
    class TypeNotFoundError(Exception):
        pass


# =========================================================
# VERSÃO / CONFIGURAÇÃO BASE
# =========================================================

VERSAO_COUTIPS = "ALFA_COUTIPS_2026_06_30_PRELIVE_ISOLADO_V006"

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,  # CORRIGIDO (28/06): sem isso, vai pro stderr e o
    # Railway marca toda linha de stderr como "severity":"error", mesmo
    # sendo um WATCHDOG OK ou um INFO normal — log duplicado e marcado
    # como erro sem ser erro nenhum.
)

API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()

# Interruptor de emergência (30/06): liga/desliga o Auditor V2 sem precisar
# subir código novo. Default TRUE (auditor ligado) — só desliga se alguém
# colocar AUDITOR_ENABLED=false/0/no no Railway. Nunca afeta o ao vivo —
# o auditor é só uma camada de inteligência por cima, opcional por desenho.
AUDITOR_ENABLED = os.getenv("AUDITOR_ENABLED", "true").strip().lower() not in ("false", "0", "no")
if not API_ID_RAW or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado no Railway/.env.")
try:
    API_ID = int(API_ID_RAW)
except ValueError as exc:
    raise RuntimeError("API_ID inválido — precisa ser número inteiro.") from exc

SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

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

AUDITORIA_CHAT_IDS_RAW = os.getenv("AUDITORIA_CHAT_IDS", "").strip()
AUDITORIA_CHAT_IDS = {
    int(x.strip())
    for x in AUDITORIA_CHAT_IDS_RAW.split(",")
    if x.strip().lstrip("-").isdigit()
}

# =========================================================
# NOVOS CANAIS PRÉ-LIVE V2 (CRIADOS POR VOCÊ)
# =========================================================

# CANAL_LOGS_PRELIVE: nome legado, mas usado pelo AO VIVO como canal de
# destino da auditoria HTML (v13) — não remover, não é exclusivo do pré-live.
CANAL_LOGS_PRELIVE = os.getenv("CANAL_LOGS_PRELIVE", "")             # https://t.me/Cout_aud

MODO_TESTE = os.getenv("MODO_TESTE", "false").lower() == "true"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
OPENAI_HABILITADO = os.getenv("OPENAI_HABILITADO", "true").lower() == "true"
OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))

CORTE_GOL_HT = int(os.getenv("CORTE_GOL_HT", "86"))
CORTE_GOL_FT = int(os.getenv("CORTE_GOL_FT", "83"))
CORTE_CONFIRMACAO_GOL_HT = int(os.getenv("CORTE_CONFIRMACAO_GOL_HT", "85"))
CORTE_CONFIRMACAO_GOL_FT = int(os.getenv("CORTE_CONFIRMACAO_GOL_FT", "80"))
CORTE_OBSERVACAO = int(os.getenv("CORTE_OBSERVACAO_JANELA", "82"))

COOLDOWN_SEGUNDOS = int(os.getenv("COOLDOWN_SEGUNDOS", "600"))
CACHE_MAX_SEGUNDOS = int(os.getenv("CACHE_MAX_SEGUNDOS", "3600"))
CACHE_MAX_ENTRADAS = int(os.getenv("CACHE_MAX_ENTRADAS", "800"))
JANELA_DECISAO_SEGUNDOS = float(os.getenv("JANELA_DECISAO_SEGUNDOS", "8"))
INTERVALO_ENVIO_SEGUNDOS = float(os.getenv("INTERVALO_ENVIO_SEGUNDOS", "5"))
WATCHDOG_SEGUNDOS = int(os.getenv("WATCHDOG_SEGUNDOS", "60"))
CSV_PATH = Path(os.getenv("CSV_PATH", "auditoria_alfa.csv"))

ODD_MINIMA_CLIENTE = os.getenv("ODD_MINIMA_CLIENTE", "1.65")


# =========================================================
# VALIDAÇÃO DE VARIÁVEIS DE AMBIENTE (MUDANÇA 15)
# =========================================================

REQUIRED_ENV = [
    "API_ID", "API_HASH", "SESSION_STRING",
    "TARGET_CHANNEL", "CONFIRMATION_CHANNEL",
]

OPTIONAL_ENV = [
    "FREE_CHANNEL", "AUDIT_HT_OK", "AUDIT_HT_NO",
    "AUDIT_FT_OK", "AUDIT_FT_NO", "BACKUP_CHANNEL",
    "OPENAI_API_KEY", "OPENAI_MODEL",
    "CANAL_LOGS_PRELIVE",
]


def validar_env() -> None:
    """Valida todas as variáveis de ambiente críticas (Mudança 15)."""
    faltando = []
    for var in REQUIRED_ENV:
        if not os.getenv(var):
            faltando.append(var)

    if faltando:
        log(f"❌ Variáveis obrigatórias faltando: {', '.join(faltando)}")
        raise RuntimeError(f"Variáveis obrigatórias faltando: {', '.join(faltando)}")

    try:
        int(os.getenv("API_ID"))
    except ValueError:
        raise RuntimeError("API_ID deve ser um número inteiro")

    # Verificar canais
    for canal in [TARGET_CHANNEL, CONFIRMATION_CHANNEL]:
        if canal and not canal.startswith("@"):
            log(f"⚠️ Canal sem @: {canal}")

    log("✅ Variáveis de ambiente validadas")


# =========================================================
# V008 — FLAGS DE ROLLBACK / SEGURANÇA OPERACIONAL
# =========================================================

HABILITAR_CONFIRMACAO_V2 = os.getenv("HABILITAR_CONFIRMACAO_V2", "true").lower() == "true"
HABILITAR_BLOQUEIO_BASE_SEM_MERCADO = os.getenv("HABILITAR_BLOQUEIO_BASE_SEM_MERCADO", "true").lower() == "true"
HABILITAR_BLOQUEIO_PARSER_CRITICO = os.getenv("HABILITAR_BLOQUEIO_PARSER_CRITICO", "true").lower() == "true"
HABILITAR_HT_PREMIUM_V2 = os.getenv("HABILITAR_HT_PREMIUM_V2", "true").lower() == "true"

HABILITAR_SCORE_V9 = os.getenv("HABILITAR_SCORE_V9", "true").lower() == "true"
V9_CAP_JOGO_ABERTO = int(os.getenv("V9_CAP_JOGO_ABERTO", "90"))
V9_CAP_APROVADO_COMUM = int(os.getenv("V9_CAP_APROVADO_COMUM", "88"))
V9_CAP_LIGA_UNDER = int(os.getenv("V9_CAP_LIGA_UNDER", "84"))
V9_CAP_LIGA_UNDER_FRACA = int(os.getenv("V9_CAP_LIGA_UNDER_FRACA", "80"))
V9_CAP_FINALIZACAO_BAIXA = int(os.getenv("V9_CAP_FINALIZACAO_BAIXA", "86"))
V9_CAP_PRESSAO_RECENTE_FRACA = int(os.getenv("V9_CAP_PRESSAO_RECENTE_FRACA", "86"))

HABILITAR_UNDER_PROVA_EXTRA = os.getenv("HABILITAR_UNDER_PROVA_EXTRA", "true").lower() == "true"

HABILITAR_V13_AUDITORIA_HTML = os.getenv("HABILITAR_V13_AUDITORIA_HTML", "true").lower() == "true"

HABILITAR_VOLUME_FT = os.getenv("HABILITAR_VOLUME_FT", "true").lower() == "true"
HABILITAR_SNIPER_PERFIL_140 = os.getenv("HABILITAR_SNIPER_PERFIL_140", "true").lower() == "true"

HABILITAR_V26_FAV_NAO_PRESSIONANTE = os.getenv("HABILITAR_V26_FAV_NAO_PRESSIONANTE", "true").lower() == "true"
V26_FAV_NAO_PRESSIONANTE_PENALIDADE = int(os.getenv("V26_FAV_NAO_PRESSIONANTE_PENALIDADE", "8"))

HABILITAR_V26_PERSISTENCIA_TELEGRAM = os.getenv("HABILITAR_V26_PERSISTENCIA_TELEGRAM", "true").lower() == "true"
V26_ESTADO_TAG = "#COUTIPS_ESTADO_V26"

HABILITAR_V27_POS70_OFF = os.getenv("HABILITAR_V27_POS70_OFF", "true").lower() == "true"
HABILITAR_V27_ALAVANCAGEM_SO_FT = os.getenv("HABILITAR_V27_ALAVANCAGEM_SO_FT", "true").lower() == "true"
HABILITAR_V27_HT_MODERADO_ALAV_OBS = os.getenv("HABILITAR_V27_HT_MODERADO_ALAV_OBS", "true").lower() == "true"
HABILITAR_V27_UNDER_TETO_82 = os.getenv("HABILITAR_V27_UNDER_TETO_82", "true").lower() == "true"
HABILITAR_V27_CONTINUIDADE_POS_GOL = os.getenv("HABILITAR_V27_CONTINUIDADE_POS_GOL", "true").lower() == "true"
HABILITAR_V27_REFINO_PLACAR_FT = os.getenv("HABILITAR_V27_REFINO_PLACAR_FT", "true").lower() == "true"
HABILITAR_V27_EMPATE_MAIS_RIGIDO = os.getenv("HABILITAR_V27_EMPATE_MAIS_RIGIDO", "true").lower() == "true"
HABILITAR_V27_PERDEDOR_UM_EXIGE_REACAO = os.getenv("HABILITAR_V27_PERDEDOR_UM_EXIGE_REACAO", "true").lower() == "true"
V27_UNDER_TETO_SCORE = int(os.getenv("V27_UNDER_TETO_SCORE", "82"))

HABILITAR_V28 = os.getenv("HABILITAR_V28", "true").lower() == "true"
HABILITAR_V28_HT_MODERADO_BLOQUEIO = os.getenv("HABILITAR_V28_HT_MODERADO_BLOQUEIO", "true").lower() == "true"
HABILITAR_V28_UNDER_BLOQUEIO_FORTE = os.getenv("HABILITAR_V28_UNDER_BLOQUEIO_FORTE", "true").lower() == "true"
HABILITAR_V28_DNA_PROJETADO = os.getenv("HABILITAR_V28_DNA_PROJETADO", "true").lower() == "true"
HABILITAR_V28_RELOGIO_FT = os.getenv("HABILITAR_V28_RELOGIO_FT", "true").lower() == "true"
V28_UNDER_TETO_SCORE = int(os.getenv("V28_UNDER_TETO_SCORE", str(V27_UNDER_TETO_SCORE)))

HABILITAR_DC01_CHAMA_PLACAR_ELASTICO = os.getenv("HABILITAR_DC01_CHAMA_PLACAR_ELASTICO", "true").lower() == "true"
DC01_CHAMA_DIF_MIN_PLACAR_ELASTICO = int(os.getenv("DC01_CHAMA_DIF_MIN_PLACAR_ELASTICO", "3"))

HABILITAR_DC01_1_SNIPER_NECESSIDADE = os.getenv("HABILITAR_DC01_1_SNIPER_NECESSIDADE", "true").lower() == "true"
HABILITAR_DC01_1_HT_PLACAR_LARGO_NECESSIDADE = os.getenv("HABILITAR_DC01_1_HT_PLACAR_LARGO_NECESSIDADE", "true").lower() == "true"
DC01_1_HT_DIF_MIN_PLACAR_LARGO = int(os.getenv("DC01_1_HT_DIF_MIN_PLACAR_LARGO", "3"))

HABILITAR_DC01_2_CONF01 = os.getenv("HABILITAR_DC01_2_CONF01", "true").lower() == "true"
HABILITAR_DC01_2_V26_CAOS_BIDIRECIONAL = os.getenv("HABILITAR_DC01_2_V26_CAOS_BIDIRECIONAL", "true").lower() == "true"

HABILITAR_DC01_3_EMPATE_FAVORITO = os.getenv("HABILITAR_DC01_3_EMPATE_FAVORITO", "true").lower() == "true"
HABILITAR_DC01_3_1X0_TRES_DE_QUATRO = os.getenv("HABILITAR_DC01_3_1X0_TRES_DE_QUATRO", "true").lower() == "true"
HABILITAR_DC01_3_PERDEDOR_PRESSAO_SIMPLES = os.getenv("HABILITAR_DC01_3_PERDEDOR_PRESSAO_SIMPLES", "true").lower() == "true"

HABILITAR_DC01_4_GOL_RECENTE_PRESSIONANTE_AP = os.getenv("HABILITAR_DC01_4_GOL_RECENTE_PRESSIONANTE_AP", "true").lower() == "true"
HABILITAR_DC01_4_CHAMA_ELASTICO_PERDEDOR_DIRETO = os.getenv("HABILITAR_DC01_4_CHAMA_ELASTICO_PERDEDOR_DIRETO", "true").lower() == "true"

HABILITAR_V29_MELHORIAS = os.getenv("HABILITAR_V29_MELHORIAS", "true").lower() == "true"
HABILITAR_V29_IA_NOVA = os.getenv("HABILITAR_V29_IA_NOVA", "true").lower() == "true"

V29_CONF_FAV_VENCENDO_GOL_RECENTE_JANELA = int(os.getenv("V29_CONF_FAV_VENCENDO_GOL_RECENTE_JANELA", "15"))
V29_CONF_FAV_VENCENDO_U10_MIN = int(os.getenv("V29_CONF_FAV_VENCENDO_U10_MIN", "10"))
V29_CONF_FAV_VENCENDO_RB_MIN = int(os.getenv("V29_CONF_FAV_VENCENDO_RB_MIN", "6"))
V29_CONF_FAV_VENCENDO_XG_MIN = float(os.getenv("V29_CONF_FAV_VENCENDO_XG_MIN", "2.0"))
V29_CONF_PLACAR_LARGO_MIN_DIFF = int(os.getenv("V29_CONF_PLACAR_LARGO_MIN_DIFF", "3"))
V29_DECAIMENTO_XG_MIN = float(os.getenv("V29_DECAIMENTO_XG_MIN", "1.5"))
V29_DECAIMENTO_U10_MIN = int(os.getenv("V29_DECAIMENTO_U10_MIN", "8"))
V29_MASSACRE_IP_MIN = float(os.getenv("V29_MASSACRE_IP_MIN", "25.0"))
V29_MASSACRE_RB_MIN = int(os.getenv("V29_MASSACRE_RB_MIN", "10"))
V29_MASSACRE_CHANCE_MIN = int(os.getenv("V29_MASSACRE_CHANCE_MIN", "12"))
V29_MASSACRE_DIFF_MIN = int(os.getenv("V29_MASSACRE_DIFF_MIN", "3"))
V29_MASSACRE_GOL_ANTES_MIN = int(os.getenv("V29_MASSACRE_GOL_ANTES_MIN", "60"))

HABILITAR_V11_GRUPO_GRATUITO = os.getenv("HABILITAR_V11_GRUPO_GRATUITO", "true").lower() == "true"
HABILITAR_V11_ALAVANCAGEM = os.getenv("HABILITAR_V11_ALAVANCAGEM", "true").lower() == "true"
HABILITAR_V11_AUSTRALIA = os.getenv("HABILITAR_V11_AUSTRALIA", "true").lower() == "true"
HABILITAR_V11_HT_CORRECOES = os.getenv("HABILITAR_V11_HT_CORRECOES", "true").lower() == "true"
HABILITAR_HT_BONUS_SUPER_FAV_VENCENDO = os.getenv("HABILITAR_HT_BONUS_SUPER_FAV_VENCENDO", "true").lower() == "true"

COMPLETE_CHANNEL = os.getenv("COMPLETE_CHANNEL") or CONFIRMATION_CHANNEL
FREE_CHANNEL = os.getenv("FREE_CHANNEL") or TARGET_CHANNEL

V11_FREE_J1_LIMITE = int(os.getenv("V11_FREE_J1_LIMITE", "3"))
V11_FREE_J2_LIMITE = int(os.getenv("V11_FREE_J2_LIMITE", "4"))
V11_FREE_J3_LIMITE = int(os.getenv("V11_FREE_J3_LIMITE", "4"))
V11_FREE_GOL_VANTAGEM_MIN = int(os.getenv("V11_FREE_GOL_VANTAGEM_MIN", "10"))
V11_FREE_COOLDOWN_JOGO_HORAS = int(os.getenv("V11_FREE_COOLDOWN_JOGO_HORAS", "24"))
V11_TZ_OFFSET_HORAS = int(os.getenv("V11_TZ_OFFSET_HORAS", "-4"))

PARSER_CONFIANCA_CRITICA = int(os.getenv("PARSER_CONFIANCA_CRITICA", "2"))

# client é criado dentro de main() a cada reinício — ver comentário lá.
client = None


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

pendentes_confirmacao_ft: Dict[str, Dict[str, Any]] = {}
tarefas_timeout_confirmacao_ft: Dict[str, asyncio.Task] = {}

v29_aprovados_por_jogo: Dict[str, float] = {}

v11_gratis_contadores: Dict[str, int] = {}
v11_gratis_enviados_por_jogo: Dict[str, float] = {}

_V17_ESTADO_FILE = "v17_gratis_estado.json"
_V29_COOLDOWN_FILE = "v29_cooldowns.json"


# =========================================================
# FUNÇÕES DE UTILIDADE / LOG (MODIFICADO PARA CANAL DE LOGS - MUDANÇA 2)
# =========================================================

def _data_dir() -> Path:
    d = Path(os.getenv("DATA_DIR", "/data"))
    if not d.exists():
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            d = Path("auditoria_html")
            d.mkdir(parents=True, exist_ok=True)
    return d


def _v17_estado_path() -> Path:
    return _data_dir() / _V17_ESTADO_FILE


def _v29_cooldown_path() -> Path:
    return _data_dir() / _V29_COOLDOWN_FILE


def log(msg: str) -> None:
    """Log interno (Railway/stdout). NÃO envia mais log bruto para o Telegram.

    Decisão registrada na sessão de 27/06/2026: @Cout_aud (CANAL_LOGS_PRELIVE)
    passa a receber apenas as auditorias em HTML (ver v13_enviar_htmls_telegram),
    nunca log bruto. O envio de log bruto causava spam e erros
    "Cannot send requests while disconnected" quando log() era chamado
    antes do client.start() (ex.: durante validar_env()).

    CORRIGIDO (28/06): só usa print() agora. As chamadas extras de
    `logging.info/warning/error` duplicavam cada linha no Railway (uma
    via stdout, outra via o logger) sem adicionar nenhuma informação —
    o print() já cobre tudo que esse log precisa.
    """
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


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


# Competições de seleção/torneio internacional — jogadas em campo neutro
# (ou onde "casa" não significa o que significa no futebol de clube).
# Usado pelo pré-live pra não aplicar o mercado "Vitória Casa/Fora" nesses
# jogos, já que a vantagem de jogar em casa simplesmente não existe ali.
_TORNEIOS_CAMPO_NEUTRO = (
    "copa do mundo", "world cup", "mundial", "eliminatorias", "eliminatoria",
    "qualifiers", "euro ", "eurocopa", "european championship",
    "copa america", "copa américa", "nations league", "liga das nacoes",
    "liga das nações", "gold cup", "copa africa", "copa áfrica", "afcon",
    "africa cup of nations", "uefa nations", "concacaf", "asian cup",
    "copa asiatica",
)

_TERMOS_AMISTOSO = ("amistoso", "friendly", "friendlies", "preseason", "pre-season")


def eh_campo_neutro(liga: str) -> bool:
    """True se a competição é de seleção/torneio internacional — onde o
    conceito de "jogar em casa" não vale do jeito que vale em clube."""
    if not liga:
        return False
    l = remover_acentos(liga).lower()
    return any(remover_acentos(termo) in l for termo in _TORNEIOS_CAMPO_NEUTRO)


def eh_amistoso(liga: str) -> bool:
    """Item 13 (28/06): amistoso é categoria de atenção PRÓPRIA, separada
    de campo neutro. Time reserva, sem pressão real de resultado — mas
    amistoso de clube ainda tem casa/fora de verdade (não bloqueia
    Vitória Casa/Fora como campo neutro faz), só reduz a confiança geral.
    """
    if not liga:
        return False
    l = remover_acentos(liga).lower()
    return any(remover_acentos(termo) in l for termo in _TERMOS_AMISTOSO)


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def lock_jogo(chave: str) -> asyncio.Lock:
    if chave not in locks_por_jogo:
        locks_por_jogo[chave] = asyncio.Lock()
    return locks_por_jogo[chave]


# =========================================================
# FUNÇÃO AUXILIAR PARA XG TOTAL (MUDANÇA 24)
# =========================================================

def get_xg_total(m: "Metricas") -> float:
    """Retorna o xG total do jogo (soma casa + fora)."""
    if isinstance(m.xg, tuple) and len(m.xg) >= 2:
        return float(m.xg[0] + m.xg[1])
    return 0.0


# =========================================================
# PERSISTÊNCIA V17 COM ATOMICIDADE (MUDANÇA 2)
# =========================================================

def v17_carregar_estado() -> None:
    global v11_gratis_contadores, v11_gratis_enviados_por_jogo
    try:
        p = _v17_estado_path()
        if not p.exists():
            log("📂 V17 estado grátis: arquivo não encontrado, iniciando zerado")
            return
        dados = json.loads(p.read_text(encoding="utf-8"))
        v11_gratis_contadores = {k: int(v) for k, v in dados.get("contadores", {}).items()}
        v11_gratis_enviados_por_jogo = {k: float(v) for k, v in dados.get("cooldowns", {}).items()}
        log(f"📂 V17 estado grátis carregado | contadores={len(v11_gratis_contadores)} | cooldowns={len(v11_gratis_enviados_por_jogo)}")
    except Exception as e:
        log(f"⚠️ V17 carregar_estado erro | {type(e).__name__}: {e}")


def v17_salvar_estado() -> None:
    """Persiste contadores com atomicidade (Mudança 2)."""
    try:
        dados = {
            "contadores": dict(v11_gratis_contadores),
            "cooldowns": dict(v11_gratis_enviados_por_jogo),
            "atualizado_em": datetime.now().isoformat(),
        }
        path = _v17_estado_path()
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
    except Exception as e:
        log(f"⚠️ V17 salvar_estado erro | {type(e).__name__}: {e}")


# =========================================================
# PERSISTÊNCIA V29 COOLDOWN (MUDANÇA 1)
# =========================================================

def v29_carregar_cooldowns() -> None:
    global v29_aprovados_por_jogo
    try:
        p = _v29_cooldown_path()
        if not p.exists():
            log("📂 V29 cooldowns: arquivo não encontrado, iniciando zerado")
            v29_aprovados_por_jogo = {}
            return
        dados = json.loads(p.read_text(encoding="utf-8"))
        v29_aprovados_por_jogo = {k: float(v) for k, v in dados.get("cooldowns", {}).items()}
        log(f"📂 V29 cooldowns carregados | {len(v29_aprovados_por_jogo)} jogos")
    except Exception as e:
        log(f"⚠️ V29 carregar_cooldowns erro | {type(e).__name__}: {e}")
        v29_aprovados_por_jogo = {}


def v29_salvar_cooldowns() -> None:
    """Persiste cooldowns do V29 (Mudança 1)."""
    try:
        dados = {
            "cooldowns": dict(v29_aprovados_por_jogo),
            "atualizado_em": datetime.now().isoformat(),
        }
        path = _v29_cooldown_path()
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
    except Exception as e:
        log(f"⚠️ V29 salvar_cooldowns erro | {type(e).__name__}: {e}")


# =========================================================
# PERSISTÊNCIA V26 COM BACKUP (MUDANÇA 3) - MODIFICADO PARA CANAL DE LOGS (MUDANÇA 3 e 4)
# =========================================================

_v26_msg_estado_id: Optional[int] = None

# Bot Auditor V2 — instância única, inicializada em main() depois que o
# client conecta. Fica None se a inicialização falhar (sistema continua
# funcionando sem o auditor, só sem essa camada extra).
_auditor_v2_manager = None
_auditor_v2_scheduler = None


async def v26_salvar_estado_telegram() -> None:
    """Salva estado no canal de auditoria (Cout_aud).

    CORRIGIDO (28/06): na rodada anterior eu tinha movido esse estado pro
    canal de confirmação, supondo que Cout_aud devia ser só HTML — decisão
    minha, não pedida. Comparando com o canal real, o lugar certo do
    #COUTIPS_ESTADO_V26 é de volta no Cout_aud, junto com WATCHDOG e o
    resto da auditoria técnica.
    """
    if not HABILITAR_V26_PERSISTENCIA_TELEGRAM:
        return
    global _v26_msg_estado_id
    try:
        dados = {
            "contadores": dict(v11_gratis_contadores),
            "cooldowns": dict(v11_gratis_enviados_por_jogo),
            "atualizado_em": datetime.now().isoformat(),
        }
        texto = f"{V26_ESTADO_TAG}\n{json.dumps(dados, ensure_ascii=False)}"

        canal_destino = CANAL_LOGS_PRELIVE if CANAL_LOGS_PRELIVE else CONFIRMATION_CHANNEL

        if _v26_msg_estado_id:
            try:
                await client.edit_message(canal_destino, _v26_msg_estado_id, texto)
            except Exception:
                pass

        msg = await client.send_message(canal_destino, texto)
        _v26_msg_estado_id = msg.id

        backup_channel = os.getenv("BACKUP_CHANNEL")
        if backup_channel:
            await client.send_message(backup_channel, f"{V26_ESTADO_TAG}_BACKUP\n{json.dumps(dados, ensure_ascii=False)}")

        log(f"💾 V26 estado salvo no Telegram | msg_id={_v26_msg_estado_id} | canal={canal_destino}")
    except Exception as e:
        log(f"⚠️ V26 salvar_estado_telegram erro | {type(e).__name__}: {e}")


async def v26_carregar_estado_telegram() -> None:
    """Lê estado do canal de auditoria (Cout_aud), com fallback no canal de confirmação."""
    if not HABILITAR_V26_PERSISTENCIA_TELEGRAM:
        return
    global v11_gratis_contadores, v11_gratis_enviados_por_jogo, _v26_msg_estado_id
    try:
        canais = []
        if CANAL_LOGS_PRELIVE:
            canais.append(CANAL_LOGS_PRELIVE)
        if CONFIRMATION_CHANNEL and CONFIRMATION_CHANNEL != CANAL_LOGS_PRELIVE:
            canais.append(CONFIRMATION_CHANNEL)

        for canal in canais:
            try:
                async for msg in client.iter_messages(canal, limit=50):
                    if msg.text and V26_ESTADO_TAG in msg.text:
                        linhas = msg.text.split("\n", 1)
                        if len(linhas) < 2:
                            continue
                        dados = json.loads(linhas[1])
                        v11_gratis_contadores = {k: int(v) for k, v in dados.get("contadores", {}).items()}
                        v11_gratis_enviados_por_jogo = {k: float(v) for k, v in dados.get("cooldowns", {}).items()}
                        _v26_msg_estado_id = msg.id
                        log(f"📂 V26 estado carregado do Telegram | msg_id={msg.id} | canal={canal}")
                        return
            except Exception as e:
                log(f"⚠️ V26 carregar_estado_telegram erro no canal {canal}: {type(e).__name__}: {e}")
                continue

        log("📂 V26 estado Telegram: nenhuma mensagem encontrada, iniciando zerado")
    except Exception as e:
        log(f"⚠️ V26 carregar_estado_telegram erro | {type(e).__name__}: {e}")


# =========================================================
# CACHE COM CLASSE ROBUSTA (MUDANÇA 21)
# =========================================================

class CacheComLimpeza:
    def __init__(self, max_size: int = 1000, max_age: int = 3600):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self.max_size = max_size
        self.max_age = max_age
        self._lock = asyncio.Lock()

    async def set(self, key: str, value: Any) -> None:
        async with self._lock:
            self._cache[key] = (value, time.time())
            if len(self._cache) > self.max_size * 1.2:
                await self._limpar()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._cache:
                return None
            value, ts = self._cache[key]
            if time.time() - ts > self.max_age:
                del self._cache[key]
                return None
            return value

    async def _limpar(self) -> None:
        agora = time.time()
        for key in list(self._cache.keys()):
            _, ts = self._cache[key]
            if agora - ts > self.max_age:
                del self._cache[key]

        if len(self._cache) > self.max_size:
            sorted_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][1])
            for key in sorted_keys[:len(self._cache) - self.max_size]:
                del self._cache[key]


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
            ordenadas = sorted(cache.keys(), key=lambda x: cache[x].get("recebido_em", 0) if isinstance(cache[x], dict) else cache[x])
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

    removidos = 0
    for k in list(v11_gratis_enviados_por_jogo.keys()):
        ts = float(v11_gratis_enviados_por_jogo.get(k, 0) or 0)
        if agora - ts > V11_FREE_COOLDOWN_JOGO_HORAS * 3600:
            v11_gratis_enviados_por_jogo.pop(k, None)
            removidos += 1
    if removidos > 0:
        v17_salvar_estado()

    for k in list(pendentes_confirmacao_ft.keys()):
        item = pendentes_confirmacao_ft.get(k, {})
        ts = float(item.get("recebido_em", 0) or 0)
        if ts and agora - ts > 900:
            pendentes_confirmacao_ft.pop(k, None)
            tarefa = tarefas_timeout_confirmacao_ft.pop(k, None)
            if tarefa and not tarefa.done():
                tarefa.cancel()


# =========================================================
# MODELOS DE DADOS (LIVE - INALTERADOS)
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
    parser_observacoes: List[str] = field(default_factory=list)
    fluxo_decisao: str = "NORMAL"
    fluxo_motivo: str = ""

    previsao_over05_ht: float = 0.0
    grupo_gratuito: str = "NAO"
    motivo_grupo_gratuito: str = ""
    alavancagem: str = "NAO"
    motivo_alavancagem: str = ""
    australia_leitura: str = ""
    motivo_australia: str = ""
    destino_final: str = ""

    penalidade_v26_fav_nao_press: int = 0

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
# MODELOS DE DADOS PRÉ-LIVE V2 (ADICIONADOS - MUDANÇA 8)
# =========================================================

def chave_alerta_unica(texto: str) -> str:
    limpo = remover_acentos(texto)
    jogo = extrair_jogo(limpo)
    tempo = extrair_tempo(limpo)
    resultado = extrair_resultado(limpo)
    estrategia = detectar_estrategia(limpo)
    if eh_ht(estrategia):
        return f"{normalizar_chave_jogo(jogo)}|{estrategia}|{resultado}"
    return f"{normalizar_chave_jogo(jogo)}|{estrategia}|{tempo}|{resultado}"


def pode_enviar(chave: str) -> bool:
    agora = time.time()
    ultimo = ultimos_enviados.get(chave)
    if not ultimo:
        return True
    return agora - float(ultimo.get("recebido_em", 0)) >= COOLDOWN_SEGUNDOS


def marcar_enviado(chave: str, m: Metricas, score: int) -> None:
    ultimos_enviados[chave] = {"recebido_em": time.time(), "jogo": m.jogo, "score": score}


def destino_principal(m: Metricas, score_medio: int) -> str:
    return COMPLETE_CHANNEL


async def registrar_bloqueio_fluxo(
    m: Metricas,
    motivo: str,
    decisao: str = "REPROVADO",
    score: int = 0,
    ia_consultada: bool = True,
) -> None:
    decisao_py = DecisaoPython(score=score, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo, detalhes={})
    decisao_ia = DecisaoIA(decisao="BLOQUEAR", confianca_original=score, confianca_corrigida=score, motivo="FLUXO_PRE_SCORE", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
    registrar_csv(m, decisao_py, decisao_ia, score, decisao, motivo)
    await enviar_auditoria(m, score, score, score, False, motivo, ia_consultada=ia_consultada)


# =========================================================
# FUNÇÕES AUXILIARES DO PROCESSAR_ALERTA (REFATORAÇÃO MUDANÇA 22)
# =========================================================

def _forcar_estrategia(m: Metricas) -> Metricas:
    if contem_volume_ft_bruto(m.texto_bruto) and m.estrategia != "VOLUME_FT":
        antigo = m.estrategia
        m.estrategia = "VOLUME_FT"
        m.parser_observacoes.append(f"V20_FORCE_VOLUME_FT:{antigo}->VOLUME_FT")
        preencher_contexto_calculado(m)
        log(f"🧭 V20 FORCE_VOLUME_FT | {antigo}->VOLUME_FT | {m.jogo} | {m.tempo}'")
    if contem_sniper_bruto(m.texto_bruto) and m.estrategia != "SNIPER_FT":
        antigo = m.estrategia
        m.estrategia = "SNIPER_FT"
        m.parser_observacoes.append(f"SNIPER_FORCE:{antigo}->SNIPER_FT")
        preencher_contexto_calculado(m)
        log(f"🎯 SNIPER FORCE | {antigo}->SNIPER_FT | {m.jogo} | {m.tempo}'")
    return m


async def _aplicar_bloqueios_imediatos(m: Metricas, chave: str = "") -> bool:
    if HABILITAR_BLOQUEIO_BASE_SEM_MERCADO and competicao_base_bloqueada(m):
        m.fluxo_decisao = "BLOQUEADO_BASE_SEM_MERCADO"
        m.fluxo_motivo = "U18_U19_U20_SUB18_SUB19_SUB20"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"⛔ {motivo} | {m.jogo} | {m.competicao}")
        # Mostra o PY real na auditoria em vez de 0% — sem isso não dá pra
        # avaliar se esse filtro de categoria está cortando jogo bom.
        # VOLUME_FT reprovado já nem chega no canal (ver canal_auditoria),
        # então pular o cálculo pra ele economiza processamento de graça.
        if eh_volume_ft(m.estrategia):
            await registrar_bloqueio_fluxo(m, motivo, score=0)
        else:
            try:
                decisao_py_real = score_python_contextual(m, chave)
                await registrar_bloqueio_fluxo(m, motivo, score=decisao_py_real.score, ia_consultada=False)
            except Exception as e:
                log(f"⚠️ Erro ao calcular PY real pra auditoria de bloqueio | {type(e).__name__}: {e}")
                await registrar_bloqueio_fluxo(m, motivo, score=0, ia_consultada=False)
        return True

    if HABILITAR_BLOQUEIO_PARSER_CRITICO and m.parser_confianca <= PARSER_CONFIANCA_CRITICA:
        m.fluxo_decisao = "BLOQUEADO_PARSER_CRITICO"
        m.fluxo_motivo = f"confianca={m.parser_confianca}/8"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"⛔ {motivo} | {m.jogo}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        return True

    if HABILITAR_V27_POS70_OFF and contem_pos70_bruto(m.texto_bruto):
        m.fluxo_decisao = "V27_POS70_DESATIVADO"
        m.fluxo_motivo = "POS70_OFF_REDUCAO_VOLUME_REDUNDANCIA"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"🔇 {motivo} | {m.jogo}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        return True

    return False


async def _aplicar_filtros_v29(m: Metricas, chave: str, chave_jogo_base: str) -> bool:
    if v29_cooldown_universal_aprovado(chave_jogo_base) and not eh_confirmacao(m.estrategia):
        m.fluxo_decisao = "V29_COOLDOWN_UNIVERSAL"
        m.fluxo_motivo = f"JOGO_JA_APROVADO_HOJE | {m.jogo}"
        log(f"⏳ V29 COOLDOWN_UNIVERSAL | {m.jogo} | já foi aprovado hoje")
        await registrar_bloqueio_fluxo(m, f"V29_COOLDOWN_UNIVERSAL | {m.fluxo_motivo}", score=0)
        return True

    if eh_ft(m.estrategia) and not eh_confirmacao(m.estrategia):
        bloqueio_rb, motivo_rb = v29_trava_rb_zero_liga_fraca(m)
        if bloqueio_rb:
            m.fluxo_decisao = "V29_TRAVA_RB_ZERO_LIGA_FRACA"
            m.fluxo_motivo = motivo_rb
            log(f"⛔ V29 RB_ZERO_LIGA_FRACA | {m.jogo} | {motivo_rb}")
            await registrar_bloqueio_fluxo(m, f"V29_TRAVA_RB_ZERO_LIGA_FRACA | {motivo_rb}", score=0)
            return True

    if m.estrategia == "CHAMA_FT":
        bloqueio_dec, motivo_dec = v29_trava_decaimento_pressao_2p(m)
        if bloqueio_dec:
            m.fluxo_decisao = "V29_TRAVA_DECAIMENTO_PRESSAO_2P"
            m.fluxo_motivo = motivo_dec
            log(f"⛔ V29 DECAIMENTO_PRESSAO_2P | {m.jogo} | {motivo_dec}")
            await registrar_bloqueio_fluxo(m, f"V29_TRAVA_DECAIMENTO_PRESSAO_2P | {motivo_dec}", score=0)
            return True

    return False


def _aplicar_massacre_excecao_perigosa(m: Metricas) -> bool:
    if HABILITAR_V29_MELHORIAS and m.liga == "PERIGOSA":
        massacre_ok_pre, massacre_motivo_pre = v29_massacre_absoluto_vencedor(m)
        if massacre_ok_pre:
            m.fluxo_motivo = (m.fluxo_motivo or "") + f" | V29_MASSACRE_EXCECAO_PERIGOSA"
            log(f"✅ V29 MASSACRE_ABSOLUTO_EXCECAO_PERIGOSA | {m.jogo} | {massacre_motivo_pre}")
            return True
    return False


async def _processar_ht_moderado(m: Metricas) -> bool:
    if not contem_ht_moderado_bruto(m.texto_bruto):
        return False

    if HABILITAR_V28 and HABILITAR_V28_HT_MODERADO_BLOQUEIO:
        m.fluxo_decisao = "V28_HT_MODERADO_BLOQUEADO"
        m.fluxo_motivo = "HT_MODERADO_DESCONTINUADO_V28_TAXA_53PCT"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"⛔ {motivo} | {m.jogo}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        return True

    if HABILITAR_V27_HT_MODERADO_ALAV_OBS:
        m.fluxo_decisao = "V27_HT_MODERADO_ALAVANCAGEM_OBSERVACAO"
        lado_ht = m.lado_pressionante
        pos_gol_ht = gol_recente_do_pressionante(m, janela=3)
        ht_ok, ht_motivo = massacre_contextual_ht(m, lado_ht, pos_gol_recente=pos_gol_ht)
        m.fluxo_motivo = ht_motivo
        if not ht_ok:
            motivo = f"V27_HT_MODERADO_BLOQUEADO_SEM_MASSACRE | {ht_motivo}"
            log(f"⛔ {motivo} | {m.jogo}")
            await registrar_bloqueio_fluxo(m, motivo, score=0)
            return True
        log(f"🧪 V27 HT_MODERADO PASSOU COMO HT_ALAVANCAGEM_OBS | {m.jogo} | {ht_motivo}")

    return False


async def _processar_sniper_v2(m: Metricas) -> bool:
    if not eh_sniper_ft(m.estrategia):
        return False

    ok_sniper, motivo_sniper = filtro_sniper_ft_v2(m)
    if not ok_sniper:
        m.fluxo_decisao = "SNIPER_V2_BLOQUEADO"
        m.fluxo_motivo = motivo_sniper
        decisao_py_sniper = DecisaoPython(score=0, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_sniper, detalhes={})
        decisao_ia_sniper = DecisaoIA(decisao="BLOQUEAR", confianca_original=0, confianca_corrigida=0, motivo="SNIPER_V2_FILTRO", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
        registrar_csv(m, decisao_py_sniper, decisao_ia_sniper, 0, "REPROVADO", motivo_sniper)
        # Mudança 4: Sniper bloqueios vão para auditoria
        await enviar_auditoria(m, 0, 0, 0, False, motivo_sniper)
        v13_registrar(m, 0, False, motivo_sniper)
        log(f"🔇 SNIPER V2 BLOQUEADO | {motivo_sniper} | {m.jogo}")
        return True

    m.fluxo_decisao = "SNIPER_V2_APROVADO_PARA_SCORE"
    m.fluxo_motivo = motivo_sniper
    log(f"🎯 SNIPER V2 FILTRO PASSOU | {motivo_sniper} | {m.jogo}")
    return False


async def _processar_volume_ft(m: Metricas) -> bool:
    if not (HABILITAR_VOLUME_FT and eh_volume_ft(m.estrategia)):
        return False

    ok_vol, motivo_vol = filtro_volume_ft(m)
    if not ok_vol:
        m.fluxo_decisao = "VOLUME_FT_BLOQUEADO"
        m.fluxo_motivo = motivo_vol
        log(f"🔇 VOLUME_FT BLOQUEADO SILENCIOSO | {motivo_vol} | {m.jogo}")
        return True

    log(f"🟢 VOLUME_FT FILTRO PASSOU | {motivo_vol} | {m.jogo}")
    m.fluxo_decisao = "VOLUME_FT_APROVADO"
    m.estrategia = "ALFA_FT"
    return False


async def _processar_dc01_chama_ft(m: Metricas, chave: str) -> bool:
    acao_dc01, motivo_dc01 = dc01_chama_placar_elastico(m)
    if acao_dc01 == "BLOQUEAR":
        m.fluxo_decisao = "DC01_CHAMA_PLACAR_ELASTICO_BLOQUEADO"
        m.fluxo_motivo = motivo_dc01
        log(f"⛔ DC01 CHAMA PLACAR ELÁSTICO BLOQUEADO | {m.jogo} | {motivo_dc01}")
        await registrar_bloqueio_fluxo(m, f"{m.fluxo_decisao} | {motivo_dc01}", score=0)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
        return True

    if acao_dc01 == "CONFIRMAR":
        m.fluxo_decisao = "DC01_CHAMA_PLACAR_ELASTICO_AGUARDANDO_CONFIRMACAO"
        m.fluxo_motivo = motivo_dc01
        pendentes_confirmacao_ft[chave] = {"metricas": m, "recebido_em": time.time(), "motivo": motivo_dc01}
        timeout_s = timeout_confirmacao_segundos(m)
        tarefa_antiga = tarefas_timeout_confirmacao_ft.pop(chave, None)
        if tarefa_antiga and not tarefa_antiga.done():
            tarefa_antiga.cancel()
        tarefas_timeout_confirmacao_ft[chave] = asyncio.create_task(cancelar_pendente_confirmacao_ft_depois(chave, timeout_s))
        log(f"⏳ DC01 CHAMA PLACAR ELÁSTICO → CONFIRMAÇÃO | timeout={timeout_s}s | {m.jogo} | {motivo_dc01}")
        await registrar_bloqueio_fluxo(m, f"AGUARDANDO_CONFIRMACAO_DC01 | {motivo_dc01}", decisao="AGUARDANDO", score=0)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
        return True

    if acao_dc01 == "APROVAR":
        m.fluxo_decisao = "DC01_CHAMA_PLACAR_ELASTICO_APROVADO_PARA_SCORE"
        m.fluxo_motivo = (m.fluxo_motivo or "") + f" | {motivo_dc01}"
        log(f"✅ DC01 CHAMA PLACAR ELÁSTICO PASSOU | {m.jogo} | {motivo_dc01}")

    return False


def _aplicar_v26_penalidade(m: Metricas) -> None:
    if not (HABILITAR_V26_FAV_NAO_PRESSIONANTE and eh_ft(m.estrategia) and not eh_confirmacao(m.estrategia)):
        return

    fav_v26 = m.lado_favorito
    press_v26 = m.lado_pressionante
    if fav_v26 not in {"CASA", "FORA"} or press_v26 not in {"CASA", "FORA"} or fav_v26 == press_v26:
        return

    caos_nivel, caos_motivo = v26_detectar_caos_bidirecional(m)
    if caos_nivel == "FORTE":
        penalidade_efetiva = 0
        log(f"🔥 V26_CAOS_BIDIRECIONAL_FORTE | penalidade=0 (zerada) | {m.jogo} | {caos_motivo}")
        m.fluxo_motivo = (m.fluxo_motivo or "") + f" | {caos_motivo}"
    elif caos_nivel == "MEDIO":
        penalidade_efetiva = 2
        log(f"⚡ V26_CAOS_BIDIRECIONAL_MEDIO | penalidade=-2 (reduzida de -{V26_FAV_NAO_PRESSIONANTE_PENALIDADE}) | {m.jogo} | {caos_motivo}")
        m.fluxo_motivo = (m.fluxo_motivo or "") + f" | {caos_motivo}"
    else:
        penalidade_efetiva = V26_FAV_NAO_PRESSIONANTE_PENALIDADE
        motivo_nao_press = f"V26_FAV_NAO_PRESSIONANTE_PENALIDADE={penalidade_efetiva} | fav={fav_v26} press={press_v26}"
        log(f"⚠️ V26 FAV_NAO_PRESSIONANTE | penalidade=-{penalidade_efetiva} | {m.jogo} | {motivo_nao_press}")
        m.fluxo_motivo = (m.fluxo_motivo or "") + f" | {motivo_nao_press}"

    m.penalidade_v26_fav_nao_press = penalidade_efetiva


async def _processar_confirmacao_ft(m: Metricas, chave: str) -> bool:
    if not (HABILITAR_CONFIRMACAO_V2 and eh_confirmacao(m.estrategia) and eh_ft(m.estrategia)):
        return False

    pendente = pendentes_confirmacao_ft.pop(chave, None)
    tarefa = tarefas_timeout_confirmacao_ft.pop(chave, None)
    if tarefa and not tarefa.done():
        tarefa.cancel()

    if pendente:
        old: Metricas = pendente.get("metricas")
        ok_conf, motivo_conf, detalhes_conf = comparar_alertas_confirmacao(old, m)
        m.fluxo_decisao = "CONFIRMACAO_CRUZADA_APROVADA" if ok_conf else "CONFIRMACAO_CRUZADA_BLOQUEADA"
        m.fluxo_motivo = motivo_conf
        log(f"🧪 FT_CONF_CRUZADA | ok={ok_conf} | {m.jogo} | {motivo_conf}")
        if not ok_conf:
            await registrar_bloqueio_fluxo(m, f"{m.fluxo_decisao} | {motivo_conf}", score=0)
            ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
            return True
        return False

    if gol_recente_pressionante_resolveu_confirmacao(m, janela=5):
        motivo = (
            f"CONF_ISOLADA_BLOQUEADA_GOL_RECENTE_PRESSIONANTE_RESOLVEU | "
            f"ultimo={m.ultimo_gol}' {m.ultimo_gol_lado} | placar={m.placar}"
        )
        m.fluxo_decisao = "CONFIRMACAO_ISOLADA_BLOQUEADA"
        m.fluxo_motivo = motivo
        log(f"⛔ {motivo} | {m.jogo}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
        return True

    ok_iso, motivo_iso = confirmacao_isolada_valida(m)
    m.fluxo_decisao = "CONFIRMACAO_ISOLADA_APROVADA_PARA_SCORE" if ok_iso else "CONFIRMACAO_ISOLADA_BLOQUEADA"
    m.fluxo_motivo = motivo_iso
    log(f"🧪 FT_CONF_ISOLADA | ok={ok_iso} | {m.jogo} | {motivo_iso}")
    if not ok_iso:
        await registrar_bloqueio_fluxo(m, f"{m.fluxo_decisao} | {motivo_iso}", score=0)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
        return True

    return False


async def _processar_primeiro_alerta_ft(m: Metricas, chave: str) -> bool:
    aguardar, motivo_aguardar = (False, "CONFIRMACAO_V2_DESATIVADA")
    if HABILITAR_CONFIRMACAO_V2:
        aguardar, motivo_aguardar = deve_aguardar_confirmacao_ft(m)

    if aguardar:
        m.fluxo_decisao = "AGUARDANDO_FT_CONFIRMACAO"
        m.fluxo_motivo = motivo_aguardar
        pendentes_confirmacao_ft[chave] = {"metricas": m, "recebido_em": time.time(), "motivo": motivo_aguardar}
        timeout_s = timeout_confirmacao_segundos(m)
        tarefa_antiga = tarefas_timeout_confirmacao_ft.pop(chave, None)
        if tarefa_antiga and not tarefa_antiga.done():
            tarefa_antiga.cancel()
        tarefas_timeout_confirmacao_ft[chave] = asyncio.create_task(cancelar_pendente_confirmacao_ft_depois(chave, timeout_s))
        log(f"⏳ AGUARDANDO_FT_CONFIRMACAO | timeout={timeout_s}s | {m.jogo} | {motivo_aguardar}")
        await registrar_bloqueio_fluxo(m, f"AGUARDANDO_CONFIRMACAO | {motivo_aguardar}", decisao="AGUARDANDO", score=0)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
        return True

    return False


async def _processar_score_e_ia(m: Metricas, chave: str) -> None:
    decisao_py = score_python_contextual(m, chave)
    log(
        f"📊 PY_PROCESSADO | {m.estrategia} | Gol={decisao_py.score}% | {m.jogo} | "
        f"Liga={m.liga} | Fav={m.lado_favorito}/{m.odd_favorito} | Press={m.lado_pressionante} | Valor={m.valor_pos_evento_classe} | Fluxo={m.fluxo_decisao}"
    )

    decisao_ia_txt, confianca_ia, motivo_ia = await consultar_openai(m, decisao_py)
    decisao_ia = calcular_protecao_ia(m, decisao_py, decisao_ia_txt, confianca_ia)

    if decisao_ia.confianca_corrigida <= 45 and not decisao_ia.protecao_ativa:
        score_medio = round((decisao_py.score + decisao_ia.confianca_corrigida) / 2)
        motivo = f"IA_BLOQUEIO_CRITICO | {motivo_ia}"
        log(f"⛔ BLOQUEADO IA CRITICA | IA={decisao_ia.confianca_corrigida}% | {m.jogo}")
        registrar_csv(m, decisao_py, decisao_ia, score_medio, "REPROVADO", motivo)
        await enviar_auditoria(m, decisao_py.score, decisao_ia.confianca_corrigida, score_medio, False, motivo)
        v13_registrar(m, score_medio, False, motivo)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}
        return

    score_medio = clamp((decisao_py.score + decisao_ia.confianca_corrigida) / 2)

    if HABILITAR_V27_UNDER_TETO_82 and m.liga == "UNDER" and score_medio > V27_UNDER_TETO_SCORE:
        score_under_original = score_medio
        score_medio = V27_UNDER_TETO_SCORE
        m.fluxo_motivo = (m.fluxo_motivo or "") + f" | V27_UNDER_TETO_{V27_UNDER_TETO_SCORE}:{score_under_original}->{score_medio}"
        log(f"🟡 V27 UNDER teto aplicado | score {score_under_original}% → {score_medio}% | {m.jogo}")

    corte = corte_por_estrategia(m.estrategia)
    aprovado = score_medio >= corte and decisao_py.status != "REPROVADO"

    if eh_confirmacao(m.estrategia) and score_medio >= 92:
        aprovado = True

    if aprovado:
        alavanca_pre, alavanca_motivo_pre = selo_alavancagem_v11(m, score_medio)
        m.alavancagem = "SIM" if alavanca_pre else "NAO"
        m.motivo_alavancagem = alavanca_motivo_pre
        if MODO_TESTE:
            m.grupo_gratuito = "NAO"
            m.motivo_grupo_gratuito = "MODO_TESTE_ATIVO"
            m.destino_final = COMPLETE_CHANNEL
        else:
            gratis_pre, gratis_motivo_pre = elegivel_grupo_gratuito_v11(m, chave, score_medio)
            m.grupo_gratuito = "SIM" if gratis_pre else "NAO"
            m.motivo_grupo_gratuito = gratis_motivo_pre
            destinos_pre = [COMPLETE_CHANNEL]
            if gratis_pre:
                destinos_pre.append(FREE_CHANNEL)
            m.destino_final = ",".join(destinos_pre)
    else:
        m.destino_final = "NAO_ENVIADO"

    motivo_final = "APROVADO" if aprovado else f"MÉDIA={score_medio}% < {corte}% OU trava_py={decisao_py.motivo}"
    registrar_csv(m, decisao_py, decisao_ia, score_medio, "APROVADO" if aprovado else "REPROVADO", motivo_final)
    await enviar_auditoria(m, decisao_py.score, decisao_ia.confianca_corrigida, score_medio, aprovado, motivo_final)
    v13_registrar(m, score_medio, aprovado, motivo_final)

    if aprovado and HABILITAR_V27_HT_MODERADO_ALAV_OBS and not (HABILITAR_V28 and HABILITAR_V28_HT_MODERADO_BLOQUEIO) and contem_ht_moderado_bruto(m.texto_bruto):
        m.destino_final = "SOMENTE_AUDITORIA_V27_HT_ALAVANCAGEM_OBS"
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}
        log(f"🧪 V27 HT_ALAVANCAGEM OBSERVACAO — aprovado mas não enviado ao principal | score={score_medio}% | {m.jogo}")
        return

    ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}

    if not aprovado:
        log(f"⛔ BLOQUEADO FINAL | score_medio={score_medio}% | corte={corte}% | {m.jogo}")
        return

    if not pode_enviar(chave):
        log(f"⏳ COOLDOWN | {m.jogo}")
        return

    alavanca_ok, alavanca_motivo = selo_alavancagem_v11(m, score_medio)
    m.alavancagem = "SIM" if alavanca_ok else "NAO"
    m.motivo_alavancagem = alavanca_motivo

    mensagem_completa = formatar_alerta_canal_completo(m, score_medio, alavancagem=alavanca_ok)
    canal_completo = destino_principal(m, score_medio)
    await enfileirar_envio(canal_completo, mensagem_completa)
    destinos = [canal_completo]

    if MODO_TESTE:
        m.grupo_gratuito = "NAO"
        m.motivo_grupo_gratuito = "MODO_TESTE_ATIVO"
    else:
        gratis_ok, gratis_motivo = elegivel_grupo_gratuito_v11(m, chave, score_medio)
        m.motivo_grupo_gratuito = gratis_motivo
        if gratis_ok:
            mensagem_gratis = formatar_alerta_cliente(m, score_medio, alavancagem=False)
            await enfileirar_envio(FREE_CHANNEL, mensagem_gratis)
            marcar_envio_gratis_v11(chave)
            m.grupo_gratuito = "SIM"
            destinos.append(FREE_CHANNEL)
        else:
            m.grupo_gratuito = "NAO"

    m.destino_final = ",".join(destinos)
    marcar_enviado(chave, m, score_medio)
    if HABILITAR_V29_MELHORIAS:
        periodo_marca = "HT" if eh_ht(m.estrategia) else "FT"
        v29_marcar_aprovado_universal(f"{normalizar_chave_jogo(m.jogo)}__{periodo_marca}")
    log(
        f"✅ ENVIADO/ENFILEIRADO V11 | {m.estrategia} | score={score_medio}% | "
        f"destinos={m.destino_final} | gratis={m.grupo_gratuito}:{m.motivo_grupo_gratuito} | "
        f"alfa={m.alavancagem}:{m.motivo_alavancagem} | {m.jogo}"
    )


# =========================================================
# PROCESSAR ALERTA REFATORADO (MUDANÇA 22)
# =========================================================

async def processar_alerta(alerta: Alerta) -> None:
    m = alerta.metricas
    chave = alerta.chave_jogo

    # 1. Forçar estratégia
    m = _forcar_estrategia(m)

    # 2. Bloqueios imediatos
    if await _aplicar_bloqueios_imediatos(m, chave):
        return

    # 3. V29 — cooldown universal (separado por período HT/FT — ver item 7
    # da revisão de 28/06: antes a chave era só o nome do jogo, então um
    # bot de HT aprovado bloqueava 24h qualquer bot de FT no mesmo jogo,
    # mesmo sendo mercados diferentes que não deviam competir pelo mesmo
    # "slot" de cooldown).
    periodo_cooldown = "HT" if eh_ht(m.estrategia) else "FT"
    chave_jogo_base = f"{normalizar_chave_jogo(m.jogo)}__{periodo_cooldown}"
    if await _aplicar_filtros_v29(m, chave, chave_jogo_base):
        return

    # 4. V29 — exceção massacre absoluto (liga PERIGOSA)
    _aplicar_massacre_excecao_perigosa(m)

    # 5. HT_MODERADO
    if await _processar_ht_moderado(m):
        return

    # 6. SNIPER V2 (Mudança 10: antes do V29)
    if await _processar_sniper_v2(m):
        return

    # 7. VOLUME FT
    if await _processar_volume_ft(m):
        return

    # 8. DC01 CHAMA FT
    if await _processar_dc01_chama_ft(m, chave):
        return

    # 9. V26 — FAV_NAO_PRESSIONANTE
    _aplicar_v26_penalidade(m)

    # 10. Confirmação FT
    if await _processar_confirmacao_ft(m, chave):
        return

    # 11. Primeiro alerta FT aguardando confirmação
    if await _processar_primeiro_alerta_ft(m, chave):
        return

    # 12. Score + IA
    await _processar_score_e_ia(m, chave)


async def janela_decisao(chave: str) -> None:
    await asyncio.sleep(JANELA_DECISAO_SEGUNDOS)
    async with lock_jogo(chave):
        alertas = pendentes_por_jogo.pop(chave, [])
        tarefas_decisao.pop(chave, None)
    if not alertas:
        return
    alerta = sorted(alertas, key=lambda a: a["recebido_em"])[-1]
    await processar_alerta(alerta["alerta"])


def auditoria_autorizada(event: events.NewMessage.Event) -> bool:
    try:
        if bool(getattr(event, "out", False)) or bool(getattr(event, "outgoing", False)):
            return True
        chat_id = int(getattr(event, "chat_id", 0) or 0)
        sender_id = getattr(event, "sender_id", None)
        if sender_id is None and AUDITORIA_CHAT_IDS:
            return True
        sender_id_int = int(sender_id or 0)
        if AUDITORIA_CHAT_IDS and (chat_id in AUDITORIA_CHAT_IDS or sender_id_int in AUDITORIA_CHAT_IDS):
            return True
        return False
    except Exception:
        return False


async def receber_mensagem(event: events.NewMessage.Event) -> None:
    try:
        texto = event.raw_text or ""

        texto_lower = texto.strip().lower()
        if texto_lower in ("/auditoria", "auditoria"):
            chat_id = str(getattr(event, "chat_id", ""))
            sender_id = str(getattr(event, "sender_id", ""))
            if not auditoria_autorizada(event):
                log(f"⛔ V19 auditoria bloqueada | chat_id={chat_id} | sender_id={sender_id}")
                return
            log(f"📋 V19 comando auditoria autorizado | chat_id={chat_id} | sender_id={sender_id}")
            hoje = datetime.now()
            enviou_algo = False
            for tipo in ("aprovados", "reprovados"):
                arq_json = _v13_arquivo_json(tipo, hoje)
                arq_html = _v13_arquivo_html(tipo, hoje)
                if arq_json.exists():
                    v13_gerar_html(tipo, hoje)
                if arq_html.exists():
                    try:
                        await client.send_file(
                            event.chat_id,
                            str(arq_html),
                            caption=f"📋 COUTIPS {tipo.upper()} — {hoje.strftime('%d/%m/%Y')} (sob demanda)",
                        )
                        enviou_algo = True
                        await asyncio.sleep(1)
                    except Exception as e:
                        log(f"⚠️ V15 envio auditoria erro | {tipo} | {e}")
            if not enviou_algo:
                await client.send_message(event.chat_id, "📋 Nenhum registro de auditoria para hoje ainda.")
            return

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

        log(f"📩 EVENTO RECEBIDO | {m.estrategia} | {m.jogo} | {m.tempo}' | {m.placar}")

        async with lock_jogo(chave):
            pendentes_por_jogo.setdefault(chave, []).append({"alerta": alerta, "recebido_em": alerta.recebido_em})
            if chave not in tarefas_decisao or tarefas_decisao[chave].done():
                tarefas_decisao[chave] = asyncio.create_task(janela_decisao(chave))
                log(f"⏳ ALERTA EM JANELA DE DECISÃO | {m.estrategia} | aguardando {JANELA_DECISAO_SEGUNDOS}s | {m.jogo}")
    except Exception as e:
        log(f"❌ Erro receber_mensagem: {type(e).__name__}: {e}")
        log(traceback.format_exc())


# =========================================================
# WATCHDOG
# =========================================================

async def watchdog() -> None:
    while True:
        try:
            limpar_memoria_interna()
            j1 = chave_contador_gratis_v11("J1")
            j2 = chave_contador_gratis_v11("J2")
            j3 = chave_contador_gratis_v11("J3")
            log(
                f"🐕 WATCHDOG OK | fila={fila_envio.qsize()} | pendentes={len(pendentes_por_jogo)} | "
                f"cache={len(ultimos_enviados)} | leituras={len(ultimas_leituras_por_jogo)} | "
                f"janelas=J1:{v11_gratis_contadores.get(j1, 0)}/{V11_FREE_J1_LIMITE} "
                f"J2:{v11_gratis_contadores.get(j2, 0)}/{V11_FREE_J2_LIMITE} "
                f"J3:{v11_gratis_contadores.get(j3, 0)}/{V11_FREE_J3_LIMITE}"
            )
        except Exception as e:
            log(f"⚠️ Watchdog erro: {e}")
        await asyncio.sleep(WATCHDOG_SEGUNDOS)



def contem_volume_ft_bruto(texto: str) -> bool:
    raw = remover_acentos(texto or "").upper()
    primeiros = raw[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", primeiros)
    return bool(
        "VOLUMEFT" in compacto
        or "BOTVOLUMEFT" in compacto
        or re.search(r"\b(?:BOT\s*)?VOLUME\s*[-_ ]*\s*FT\b", primeiros)
    )


def contem_pos70_bruto(texto: str) -> bool:
    raw = remover_acentos(texto or "").upper()
    primeiros = raw[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", primeiros)
    return bool(
        "POS70" in compacto
        or "POSSETENTA" in compacto
        or re.search(r"\bPOS\s*[-_ ]*70\b", primeiros)
        or re.search(r"\bP[OÓ]S\s*70\b", primeiros)
    )


def contem_ht_moderado_bruto(texto: str) -> bool:
    raw = remover_acentos(texto or "").upper()[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", raw)
    return "HTMODERADO" in compacto or bool(re.search(r"\bHT\s*[-_ ]*MODERADO\b", raw))


def contem_sniper_bruto(texto: str) -> bool:
    raw = remover_acentos(texto or "").upper()[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", raw)
    return "SNIPER" in compacto or bool(re.search(r"\b(?:BOT\s*)?SNIPER\s*[-_ ]*(?:FT|2T)?\b", raw))


def detectar_estrategia(texto: str) -> str:
    raw = remover_acentos(texto or "").upper()
    raw = raw.replace("_", " ")
    raw = re.sub(r"\s+", " ", raw)

    estrategia_txt = raw
    m = re.search(
        r"ALERTA\s+ESTRATEGIA\s*:\s*(.*?)(?:\s+JOGO\s*:|\s+COMPETICAO\s*:|\s+TEMPO\s*:|$)",
        raw,
        re.IGNORECASE,
    )
    if m and m.group(1).strip():
        estrategia_txt = m.group(1).strip()

    compacto = re.sub(r"[^A-Z0-9]+", "", estrategia_txt)
    compacto_full = re.sub(r"[^A-Z0-9]+", "", raw)

    if contem_volume_ft_bruto(estrategia_txt) or contem_volume_ft_bruto(raw[:350]):
        return "VOLUME_FT"

    if contem_sniper_bruto(estrategia_txt) or contem_sniper_bruto(raw[:350]):
        return "SNIPER_FT"

    if (
        "BOTHTCONFIRMACAO" in compacto
        or "HTCONFIRMACAO" in compacto
        or "ALFAHTCONFIRMACAO" in compacto
    ):
        return "ALFA_HT_CONFIRMACAO"
    if (
        "BOTFTCONFIRMACAO" in compacto
        or "FTCONFIRMACAO" in compacto
        or "ALFAFTCONFIRMACAO" in compacto
    ):
        return "ALFA_FT_CONFIRMACAO"

    if "ARCEHT" in compacto or compacto == "ARCE":
        return "ARCE_HT"
    if "CHAMAFT" in compacto or compacto == "CHAMA":
        return "CHAMA_FT"

    if (
        "BOTHT" in compacto
        or "HTPREMIUM" in compacto
        or "HTPREMIUN" in compacto
        or "HTMODERADO" in compacto
        or "IPSHT" in compacto
        or "ALFAHT" in compacto
        or "PRIMEIROTEMPO" in compacto
        or compacto in {"HT", "1T"}
    ):
        return "ALFA_HT"

    if (
        "BOTFT" in compacto
        or "FTPREMIUM" in compacto
        or "FTPREMIUN" in compacto
        or "FTMODERADO" in compacto
        or "IPSFT" in compacto
        or "POS70" in compacto
        or "ALFAFT" in compacto
        or "SEGUNDOTEMPO" in compacto
        or compacto in {"FT", "2T"}
    ):
        return "ALFA_FT"

    if "SEGUNDOTEMPO" in compacto_full or "2T" in compacto_full:
        return "ALFA_FT"
    if "PRIMEIROTEMPO" in compacto_full or "1T" in compacto_full:
        return "ALFA_HT"
    return "ALFA_FT"


def eh_ht(estrategia: str) -> bool:
    return estrategia in {"ALFA_HT", "ALFA_HT_CONFIRMACAO", "ARCE_HT"}


def eh_ft(estrategia: str) -> bool:
    return estrategia in {"ALFA_FT", "ALFA_FT_CONFIRMACAO", "CHAMA_FT", "VOLUME_FT", "SNIPER_FT"}


def eh_sniper_ft(estrategia: str) -> bool:
    return estrategia == "SNIPER_FT"


def eh_confirmacao(estrategia: str) -> bool:
    return estrategia in {"ALFA_HT_CONFIRMACAO", "ALFA_FT_CONFIRMACAO"}


def eh_volume_ft(estrategia: str) -> bool:
    return estrategia == "VOLUME_FT"


def corrigir_estrategia_por_minuto(estrategia: str, tempo: int, observacoes: List[str]) -> str:
    try:
        t = int(tempo or 0)
    except Exception:
        return estrategia

    if t >= 46 and eh_ht(estrategia):
        novo = "ALFA_FT_CONFIRMACAO" if eh_confirmacao(estrategia) else "ALFA_FT"
        observacoes.append(f"PERIODO_CORRIGIDO_PELO_MINUTO:{estrategia}->{novo}@{t}")
        return novo
    if 1 <= t < 46 and eh_ft(estrategia):
        novo = "ALFA_HT_CONFIRMACAO" if eh_confirmacao(estrategia) else "ALFA_HT"
        observacoes.append(f"PERIODO_CORRIGIDO_PELO_MINUTO:{estrategia}->{novo}@{t}")
        return novo
    return estrategia


def minuto_gol_delta(m: "Metricas") -> int:
    if not m.ultimo_gol:
        return 999
    return int(m.tempo or 0) - int(m.ultimo_gol or 0)


def gol_recente(m: "Metricas", janela: int = 5) -> bool:
    d = minuto_gol_delta(m)
    return -1 <= d <= janela


def gols_lado(m: "Metricas", lado: str) -> Tuple[int, int]:
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return 0, 0
    if lado == "CASA":
        return gc, gf
    if lado == "FORA":
        return gf, gc
    return 0, 0


def ultimo_gol_aumentou_vantagem(m: "Metricas") -> bool:
    lado = m.ultimo_gol_lado
    if lado not in {"CASA", "FORA"}:
        return False
    gols_pro, gols_contra = gols_lado(m, lado)
    if gols_pro <= 0:
        return False
    return (gols_pro - 1) > gols_contra


def ultimo_gol_deixou_lado_vencendo(m: "Metricas") -> bool:
    lado = m.ultimo_gol_lado
    if lado not in {"CASA", "FORA"}:
        return False
    gols_pro, gols_contra = gols_lado(m, lado)
    return gols_pro > gols_contra


def ultimo_gol_empatou_ou_reduziu(m: "Metricas") -> bool:
    lado = m.ultimo_gol_lado
    if lado not in {"CASA", "FORA"}:
        return False
    gols_pro, gols_contra = gols_lado(m, lado)
    if gols_pro <= 0:
        return False
    antes_pro = gols_pro - 1
    if antes_pro < gols_contra and gols_pro == gols_contra:
        return True
    if antes_pro < gols_contra and gols_pro < gols_contra:
        return True
    return False


def gol_recente_do_pressionante(m: "Metricas", janela: int = 5) -> bool:
    return gol_recente(m, janela) and m.ultimo_gol_lado == m.lado_pressionante and m.lado_pressionante in {"CASA", "FORA"}


def gol_recente_pressionante_aumentou_vantagem(m: "Metricas", janela: int = 5) -> bool:
    return gol_recente_do_pressionante(m, janela) and ultimo_gol_aumentou_vantagem(m)


def gol_recente_pressionante_resolveu_confirmacao(m: "Metricas", janela: int = 5) -> bool:
    if not gol_recente_do_pressionante(m, janela):
        return False
    if ultimo_gol_empatou_ou_reduziu(m):
        return False
    return ultimo_gol_deixou_lado_vencendo(m) or ultimo_gol_aumentou_vantagem(m)


def competicao_base_bloqueada(m: "Metricas") -> bool:
    texto = remover_acentos(f"{m.competicao} {m.jogo}").upper()
    padroes = [
        r"\bU\s*-?\s*(18|19|20)\b",
        r"\bSUB\s*-?\s*(18|19|20)\b",
        r"\bSUB\s*(18|19|20)\b",
        r"\bUNDER\s*-?\s*(18|19|20)\b",
    ]
    return any(re.search(p, texto) for p in padroes)


def massacre_contextual_ht(m: "Metricas", lado: str, pos_gol_recente: bool = False) -> Tuple[bool, str]:
    if lado not in {"CASA", "FORA"}:
        return False, "HT_SEM_LADO"
    d = dados_lado(m, lado)
    op = "FORA" if lado == "CASA" else "CASA"
    od = dados_lado(m, op)
    ip = ip_lado(m, lado)

    ap_diff = ap_diff_lado(m, lado)
    finalizacao = d["rb"] >= 1 or (d["rb"] + d["rl"]) >= 4 or d["chance"] >= 8 or d["xg"] >= 0.25
    territorio = d["u5"] >= 5 and d["u10"] >= 10 and ap_diff >= 15
    pressao_ip = ip["pico"] >= 22 or ip["c18"] >= 2 or ip["c22"] >= 1
    favorito_ok = (m.lado_favorito == lado) or (m.odd_favorito and m.odd_favorito <= 1.60)

    if pos_gol_recente:
        finalizacao = d["rb"] >= 2 or (d["rb"] + d["rl"]) >= 5 or d["chance"] >= 10 or d["xg"] >= 0.35
        territorio = d["u5"] >= 6 and d["u10"] >= 12 and ap_diff >= 20
        pressao_ip = ip["pico"] >= 24 or ip["c18"] >= 3 or ip["c22"] >= 1

    gol_cedo_zebra = bool(m.ultimo_gol and m.ultimo_gol < 10 and m.ultimo_gol_lado == m.lado_zebra and m.lado_favorito == lado)
    if gol_cedo_zebra:
        favorito_ok = True

    ok = bool(favorito_ok and territorio and pressao_ip and finalizacao)
    motivo = (
        f"fav_ok={favorito_ok}|ap_diff={ap_diff}|u5={d['u5']}|u10={d['u10']}|"
        f"rb={d['rb']}|rl={d['rl']}|chance={d['chance']}|xg={d['xg']:.2f}|"
        f"ip_pico={ip['pico']}|pos_gol={pos_gol_recente}|gol_cedo_zebra={gol_cedo_zebra}"
    )
    return ok, motivo


def mensagem_valida(texto: str) -> bool:
    t = remover_acentos(texto).upper()
    return "ALERTA ESTRATEGIA" in t and "JOGO:" in t and "TEMPO:" in t


# =========================================================
# EXTRAÇÃO DE DADOS (PARSER) — MUDANÇAS 7, 8, 9 (INALTERADO)
# =========================================================

def extrair_jogo(texto: str) -> str:
    t = normalizar(texto)
    m = re.search(r"Jogo:\s*(.+)", t, re.IGNORECASE)
    if m:
        jogo = limpar_linha(m.group(1).split("\n")[0])
        if not re.match(r"^\d+(?:\.\d+)?\s*x\s*\d+(?:\.\d+)?$", jogo):
            return jogo

    for linha in t.splitlines():
        if " x " in linha.lower() or " vs " in linha.lower():
            if re.search(r"[A-Za-z]", linha):
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
    """Extrai odds com múltiplos formatos (Mudança 8)."""
    padroes = [
        r"Odds.*?:\s*([0-9.,]+)\s*[/\-]\s*([0-9.,]+)\s*[/\-]\s*([0-9.,]+)",
        r"Odds.*?:\s*([0-9.,]+)\s*/\s*([0-9.,]+)\s*/\s*([0-9.,]+)",
        r"Odds.*?:\s*([0-9.,]+)\s*-\s*([0-9.,]+)\s*-\s*([0-9.,]+)",
        r"Odd.*?Casa\s*[:=]\s*([0-9.,]+).*?Fora\s*[:=]\s*([0-9.,]+)",
    ]

    for padrao in padroes:
        m = re.search(padrao, texto, re.IGNORECASE)
        if m:
            try:
                return tuple(float(x.replace(",", ".")) for x in m.groups())
            except Exception:
                continue

    return (0.0, 0.0, 0.0)


def extrair_ultimo_gol_lado(texto: str) -> str:
    t = remover_acentos(texto).upper()
    for padrao in (
        r"ULTIMO\s+GOLO:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)",
        r"ULTIMO\s+GOL:\s*\d+\s*['’]?\s*(CASA|FORA|HOME|AWAY)",
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
    for minuto, lado in re.findall(r"(\d+)\s*['’]?\s*(CASA|FORA|HOME|AWAY)", linha):
        eventos.append((int(minuto), "CASA" if lado in {"CASA", "HOME"} else "FORA"))
    return eventos


def extrair_links(texto: str) -> Tuple[str, str]:
    bet365 = ""
    corner = ""
    texto_sep = re.sub(r"(https?://)", r" \1", texto)
    for link in re.findall(r"https?://[^\s]+", texto_sep, re.IGNORECASE):
        link = link.strip().rstrip(".")
        if "bet365" in link.lower() and not bet365:
            bet365 = link
        if "cornerprobet" in link.lower() and "/analysis/" in link.lower() and not corner:
            corner = link
    return bet365, corner


def extrair_pressao_alfa(texto: str) -> Dict[str, float]:
    """Extrai IP com múltiplos nomes (Mudança 9)."""
    base = normalizar(texto)
    sem = remover_acentos(base)

    padroes = [
        r"Índice de Pressão:(.+)",
        r"Indice de Pressao:(.+)",
        r"Pressão ALFA:(.+)",
        r"Pressao ALFA:(.+)",
        r"IP:(.+)",
        r"Índice IP:(.+)",
    ]

    bloco = None
    for padrao in padroes:
        m = re.search(padrao, base, re.IGNORECASE | re.DOTALL)
        if m:
            bloco = m.group(1)
            break

    if not bloco:
        for padrao in padroes:
            m = re.search(padrao, sem, re.IGNORECASE | re.DOTALL)
            if m:
                bloco = m.group(1)
                break

    if not bloco:
        return _pressao_vazia()

    bloco = bloco.split("https://")[0]
    casa_vals: List[float] = []
    fora_vals: List[float] = []

    for seg in re.split(r"[;|,]", bloco):
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


def calcular_confianca_parser(m: "Metricas") -> int:
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
    observacoes_parser: List[str] = []
    estrategia_original = detectar_estrategia(tl)
    tempo_extraido = extrair_tempo(tl)
    estrategia = corrigir_estrategia_por_minuto(estrategia_original, tempo_extraido, observacoes_parser)
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
        tempo=tempo_extraido,
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
        ultimos5=pegar_par(r"(?:Ultimos|Últimos)\s*5['’]?:\s*(-?\d+)\s*\([^)]*\)\s*-\s*(-?\d+)", tl),
        ultimos10=pegar_par(r"(?:Ultimos|Últimos)\s*10['’]?:\s*(-?\d+)\s*\([^)]*\)\s*-\s*(-?\d+)", tl),
        odds=extrair_odds(tl),
        ultimo_gol=extrair_ultimo_gol_minuto(tl),
        ultimo_gol_lado=extrair_ultimo_gol_lado(tl),
        ultimos_cantos_lados=extrair_ultimos_cantos_lados(tl),
        chance_golo=pegar_par(r"Chance de Golo:Casa=(\d+)\s*/\s*Fora=(\d+)", tl),
        xg=xg,
        xgl=xgl,
        xgi=xgi,
        avgxg=pegar_float_par(r"avgXGaFavor:Casa=([0-9.,]+)\s*/\s*Fora=([0-9.,]+)", tl),
        previsao_over05_ht=pegar_float(r"Previsao\s+Over\s+0\.5HT\s+golos\s*%:\s*([0-9.,]+)", remover_acentos(tl), 0.0),
        pressao_alfa=extrair_pressao_alfa(tl),
        bet365=bet365,
        cornerpro=corner,
        texto_bruto=texto,
    )
    m.parser_observacoes.extend(observacoes_parser)
    preencher_contexto_calculado(m)
    m.parser_confianca = calcular_confianca_parser(m)
    if m.parser_confianca <= 2:
        log(f"🔴 PARSER_CRITICO | confianca={m.parser_confianca}/8 | {m.jogo} | {m.tempo}' — possível mudança de formato CornerPro")
    elif m.parser_confianca <= 4:
        log(f"⚠️ PARSER_ALERTA | confianca={m.parser_confianca}/8 | {m.jogo} | {m.tempo}' — verificar campos zerados")
    if m.parser_observacoes:
        log(f"🟡 PARSER_OBS | {m.jogo} | {m.tempo}' | " + " | ".join(m.parser_observacoes))
    return m


# =========================================================
# LIGAS (LIVE - INALTERADO)
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


def time_elite_transicao_lado(m: Metricas) -> str:
    jogo = remover_acentos(m.jogo).lower()
    partes = re.split(r"\s+x\s+|\s+vs\s+", jogo, maxsplit=1)
    if len(partes) < 2:
        return "DESCONHECIDO"
    casa, fora = partes[0], partes[1]
    for nome in _TIMES_ELITE_TRANSICAO:
        if nome in casa:
            return "CASA"
        if nome in fora:
            return "FORA"
    return "DESCONHECIDO"


def bonus_time_elite_transicao(m: Metricas) -> Tuple[int, str]:
    lado_elite = time_elite_transicao_lado(m)
    if lado_elite not in {"CASA", "FORA"}:
        return 0, "SEM_TIME_ELITE"
    vencedor = lado_vencendo(m)
    if vencedor == "EMPATE" or vencedor != lado_elite:
        return 6, f"TIME_ELITE_TRANSICAO_{lado_elite}"
    return 0, "TIME_ELITE_JA_VENCENDO"


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
# LADOS / CONTEXTO (LIVE - INALTERADO)
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


def lado_zebra(fav: str) -> str:
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


def lado_oposto(lado: str) -> str:
    if lado == "CASA":
        return "FORA"
    if lado == "FORA":
        return "CASA"
    return "DESCONHECIDO"


def diferenca_placar(m: Metricas) -> int:
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return 0
    return abs(gc - gf)


def total_gols_placar(m: Metricas) -> int:
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return 0
    return gc + gf


def minutos_desde_ultimo_gol(m: Metricas) -> int:
    if not m.ultimo_gol:
        return 999
    return int(m.tempo or 0) - int(m.ultimo_gol or 0)


def ap_diff_lado(m: Metricas, lado: str) -> float:
    if lado not in {"CASA", "FORA"}:
        return 0.0
    return valor_lado(m, "ataques_perigosos", lado) - valor_lado(m, "ataques_perigosos", lado_oposto(lado))


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


def pontuar_lado(m: Metricas, lado: str) -> float:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return (
        d["ap"] * 1.0
        + d["u5"] * 2.2
        + d["u10"] * 1.4
        + d["rb"] * 4.2
        + d["rl"] * 1.2
        + d["rda"] * 1.5
        + d["cantos"] * 0.9
        + d["chance"] * 1.35
        + d["xg"] * 9.0
        + ip["pico"] * 0.5
        + ip["c18"] * 2.0
        + ip["c22"] * 3.0
    )


def lado_dominante(m: Metricas) -> Tuple[str, float]:
    casa = pontuar_lado(m, "CASA")
    fora = pontuar_lado(m, "FORA")
    dif = casa - fora
    if dif >= 8:
        return "CASA", dif
    if dif <= -8:
        return "FORA", abs(dif)
    return "EQUILIBRADO", abs(dif)


def lado_pressionante(m: Metricas) -> str:
    dom, dif = lado_dominante(m)
    fav, odd = lado_favorito(m)
    if dom in {"CASA", "FORA"}:
        return dom
    if fav in {"CASA", "FORA"} and odd <= (1.50 if eh_ht(m.estrategia) else 1.85):
        return fav
    return "DESCONHECIDO"


def preencher_contexto_calculado(m: Metricas) -> None:
    m.liga = classificar_liga(m.competicao)
    m.lado_favorito, m.odd_favorito = lado_favorito(m)
    m.lado_zebra = lado_zebra(m.lado_favorito)
    m.lado_dominante, _ = lado_dominante(m)
    m.lado_pressionante = lado_pressionante(m)


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
    if eh_ht(m.estrategia):
        if odd <= 1.50:
            return "FAVORITO_FORTE"
        return "SEM_BONUS_FAVORITO"
    if odd <= 1.55:
        return "FAVORITO_FORTE"
    if odd <= 1.85:
        return "FAVORITO_CONTEXTUAL"
    if odd <= 2.20:
        return "FAVORITO_FRACO_SO_EXTREMO"
    return "SEM_BONUS_FAVORITO"


def pressao_viva_lado(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    if eh_ht(m.estrategia):
        return d["u5"] >= 4 or d["u10"] >= 8 or ip["pico"] >= 22 or ip["c18"] >= 2 or ip["c22"] >= 1
    return d["u5"] >= 3 or d["u10"] >= 7 or ip["pico"] >= 20 or ip["c18"] >= 2 or ip["c22"] >= 1


def pressao_morta_lado(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return d["u5"] <= 1 and d["u10"] <= 3 and ip["pico"] < 18 and ip["c18"] == 0


def xg_baixo_compensado_por_rb(m: Metricas, lado: str) -> bool:
    return False


def ht_bonus_super_fav_vencendo_v12_2(m: Metricas) -> Tuple[bool, str]:
    if not HABILITAR_HT_BONUS_SUPER_FAV_VENCENDO:
        return False, "FLAG_OFF"
    if not eh_ht(m.estrategia):
        return False, "NAO_HT"
    if int(m.tempo or 0) > 37:
        return False, f"MINUTO_ACIMA_37_{m.tempo}"
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "SEM_FAVORITO"
    if fav != m.lado_pressionante:
        return False, "FAVORITO_NAO_PRESSIONANTE"
    odd_ok = (fav == "FORA" and m.odd_favorito <= 1.55) or (fav == "CASA" and m.odd_favorito <= 1.30)
    if not odd_ok:
        return False, f"ODD_FORA_DA_REGRA_{fav}_{m.odd_favorito}"
    if lado_vencendo(m) != fav or diferenca_placar(m) != 1:
        return False, f"PLACAR_NAO_VENCE_1_{m.placar}"
    if not pressao_extrema_lado(m, fav):
        return False, "SEM_PRESSAO_EXTREMA"
    return True, f"SUPER_FAV_{fav}_ODD={m.odd_favorito}_VENCE_1_HT_PRESSAO_EXTREMA"


def consequencia_real_lado(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    if eh_ht(m.estrategia):
        return (
            d["rb"] >= 1
            or d["rb"] + d["rl"] >= 3
            or d["cantos"] >= 1
            or d["chance"] >= 6
            or d["xg"] >= 0.20
        )
    return d["rb"] >= 1 or d["rb"] + d["rl"] >= 4 or d["cantos"] >= 2 or d["chance"] >= 8 or d["xg"] >= 0.28


def vermelho_contra_pressionante(m: Metricas) -> bool:
    vc, vf = m.vermelhos
    if m.lado_pressionante == "CASA" and vc > vf:
        return True
    if m.lado_pressionante == "FORA" and vf > vc:
        return True
    return False


def contexto_emocional_vivo(m: Metricas, lado: str) -> Tuple[bool, str]:
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return True, "PLACAR_DESCONHECIDO"

    total_gols = gc + gf
    dif = abs(gc - gf)
    fav = m.lado_favorito
    perdendo = lado_perdendo(m)
    vencendo = lado_vencendo(m)

    if eh_ht(m.estrategia):
        if total_gols >= 3 and dif >= 2:
            if lado == vencendo and pressao_extrema_lado(m, lado) and consequencia_real_lado(m, lado):
                return True, "HT_MASSACRE_CONTINUA_MESMO_PLACAR_ABERTO"
            return False, "HT_PLACAR_ABERTO_DEMAIS_SEM_VALOR"
        return True, "HT_CONTEXTO_VIVO"

    if vencendo == "EMPATE":
        return True, "FT_EMPATE_CONTEXTO_VIVO"
    if dif <= 1:
        return True, "FT_PLACAR_APERTADO"
    if fav in {"CASA", "FORA"} and perdendo == fav:
        return True, "FT_FAVORITO_ATRAS_DO_PLACAR"

    if dif >= 2 and lado == vencendo:
        if pressao_extrema_lado(m, lado) and consequencia_real_lado(m, lado):
            return True, "MASSACRE_CONTINUA_VIVO"
        return False, "PLACAR_RESOLVIDO_SEM_FOME"

    if dif >= 2 and lado == perdendo:
        if pressao_viva_lado(m, lado) and consequencia_real_lado(m, lado):
            return True, "TIME_ATRAS_PRECISA_REAGIR_COM_PRESSAO"
        return False, "TIME_ATRAS_SEM_PRESSAO_REAL"

    return True, "CONTEXTO_NEUTRO_VIVO"


def consequencia_minima_emocional(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return (
        d["rb"] >= 1
        or d["rl"] >= 2
        or d["cantos"] >= 2
        or d["chance"] >= (6 if eh_ht(m.estrategia) else 8)
        or d["xg"] >= (0.18 if eh_ht(m.estrategia) else 0.25)
        or (ip["pico"] >= 24 and d["ap"] >= 15)
    )


# =========================================================
# VALOR PÓS-EVENTO (LIVE - INALTERADO)
# =========================================================

def avaliar_valor_pos_evento(m: Metricas) -> Tuple[str, str, int, bool]:
    lado = m.lado_pressionante
    fav = m.lado_favorito
    zebra = m.lado_zebra
    ultimo_lado = m.ultimo_gol_lado
    tempo = m.tempo
    ultimo = m.ultimo_gol
    minutos = tempo - ultimo if ultimo else 999

    if lado not in {"CASA", "FORA"}:
        return "SEM_VALOR_ESPECIAL", "SEM_LADO_PRESSIONANTE", 0, False

    fav_nv = favorito_nao_vencendo(m)
    fav_press = fav == lado and pressao_viva_lado(m, lado)

    if fav in {"CASA", "FORA"} and fav_nv and fav_press:
        faixa = faixa_favorito(m)
        if faixa == "SUPER_FAVORITO":
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "SUPER_FAVORITO_NAO_VENCE_E_PRESSIONA", 10, True
        if faixa == "FAVORITO_FORTE":
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "FAVORITO_FORTE_NAO_VENCE_E_PRESSIONA", 7, True
        if faixa == "FAVORITO_CONTEXTUAL" and consequencia_real_lado(m, lado):
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "FAVORITO_CONTEXTUAL_NAO_VENCE_COM_CONSEQUENCIA", 4, True
        if faixa == "FAVORITO_FRACO_SO_EXTREMO" and pressao_extrema_lado(m, lado):
            return "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA", "FAVORITO_FRACO_SO_EXTREMO_VALIDADO", 1, True

    if not ultimo or minutos > 12 or ultimo_lado == "DESCONHECIDO":
        return "SEM_VALOR_ESPECIAL", "SEM_GOL_RECENTE_RELEVANTE", 0, fav_press and fav_nv

    if ultimo_lado != lado and pressao_viva_lado(m, lado):
        if fav == lado or ultimo_lado == zebra:
            ajuste = 10 if m.odd_favorito and m.odd_favorito <= 1.30 else 7
            return "GOL_CONTRA_FLUXO_VALORIZA", "ZEBRA_MARCOU_E_PRESSIONANTE_SEGUE_VIVO", ajuste, True
        return "GOL_CONTRA_FLUXO_VALORIZA", "ADVERSARIO_MARCOU_E_PRESSAO_CONTINUA", 5, True

    if ultimo_lado == lado:
        # Mudança 13: penalidade reduzida de -14 para -10
        if gol_recente_do_pressionante(m, janela=5) and ultimo_gol_deixou_lado_vencendo(m) and pressao_pos_gol_esfriou(m, lado):
            return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_U10_CONTAMINADO_U5_CAIU", -10, False
        if pressao_morta_lado(m, lado):
            return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_E_RITMO_CAIU", -10, False
        if pressao_viva_lado(m, lado):
            return "PRESSAO_PREMIADA_MAS_CONTINUA", "GOL_PREMIOU_MAS_PRESSAO_CONTINUA", -2, True
        return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_SEM_CONTINUIDADE_CLARA", -8, False

    return "SEM_VALOR_ESPECIAL", "GOL_RECENTE_SEM_LEITURA_ESPECIAL", 0, False


def pressao_pos_gol_esfriou(m: "Metricas", lado: str) -> bool:
    if lado not in {"CASA", "FORA"}:
        return False
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return (
        d.get("u5", 0) <= 1
        and d.get("rb", 0) <= 0
        and d.get("cantos", 0) == 0
        and ip.get("pico", 0) < 10
    )


def pressao_extrema_lado(m: Metricas, lado: str) -> bool:
    if lado not in {"CASA", "FORA"}:
        return False
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    return (
        d["ap"] >= 25
        and (d["chance"] >= 10 or d["rb"] >= 2 or d["cantos"] >= 4 or d["rl"] >= 7)
        and (ip["pico"] >= 24 or ip["c18"] >= 3 or ip["c22"] >= 2)
    )


# =========================================================
# SCORE PYTHON CONTEXTUAL (LIVE - INALTERADO)
# =========================================================

def score_pressao_viva(m: Metricas) -> int:
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


def score_consequencia(m: Metricas) -> int:
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


def score_favoritismo(m: Metricas) -> int:
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        elite_bonus, _ = bonus_time_elite_transicao(m)
        return clamp(elite_bonus, -8, 18)
    faixa = faixa_favorito(m)
    base = {"SUPER_FAVORITO": 10, "FAVORITO_FORTE": 7, "FAVORITO_CONTEXTUAL": 4, "FAVORITO_FRACO_SO_EXTREMO": 1}.get(faixa, 0)
    if favorito_nao_vencendo(m):
        base += 6 if faixa in {"SUPER_FAVORITO", "FAVORITO_FORTE"} else 3
    elif lado_vencendo(m) == fav:
        gc, gf = extrair_gols_placar(m.placar)
        if gc is not None and gf is not None and abs(gc - gf) >= 2:
            base -= 5

    elite_bonus, _ = bonus_time_elite_transicao(m)
    base += elite_bonus
    return clamp(base, -8, 22)


def score_relogio(m: Metricas) -> int:
    if eh_ht(m.estrategia):
        if 18 <= m.tempo <= 35:
            return 6
        if m.tempo < 15:
            return -4
        return 1
    if HABILITAR_V28 and HABILITAR_V28_RELOGIO_FT:
        if 62 <= m.tempo <= 81:
            return 7
        if 82 <= m.tempo <= 85:
            return 3
        if m.tempo > 85:
            return -5
        return 1
    if 62 <= m.tempo <= 80:
        return 7
    if 81 <= m.tempo <= 85:
        return 3
    if m.tempo > 85:
        return -5
    return 1


# =========================================================
# SCORE CONFIRMAÇÃO CORRIGIDO (MUDANÇA 12) - INALTERADO
# =========================================================

def score_confirmacao(m: Metricas, chave: str) -> Tuple[int, str]:
    if not eh_confirmacao(m.estrategia):
        return 0, "NAO_E_CONFIRMACAO"
    anterior = ultimas_leituras_por_jogo.get(chave)
    if not anterior:
        return 2, "CONFIRMACAO_SEM_HISTORICO"
    old: Metricas = anterior.get("metricas")
    if not old:
        return 2, "CONFIRMACAO_SEM_METRICAS_ANTERIORES"

    lado = m.lado_pressionante if m.lado_pressionante in {"CASA", "FORA"} else m.lado_favorito
    if lado not in {"CASA", "FORA"}:
        return 1, "CONFIRMACAO_SEM_LADO"

    pontos = 0
    motivos = []
    for campo, peso in (("ataques_perigosos", 2), ("cantos", 2), ("chance_golo", 3), ("remates_lado", 1), ("remates_baliza", 2)):
        if valor_lado(m, campo, lado) > valor_lado(old, campo, lado):
            pontos += peso
            motivos.append(f"{campo}_subiu")

    # Mudança 12: usar > em vez de >=
    if ip_lado(m, lado)["pico"] > ip_lado(old, lado)["pico"]:
        pontos += 2
        motivos.append("ip_subiu")
    ip_diff = ip_lado(m, lado)["pico"] - ip_lado(old, lado)["pico"]
    if ip_diff >= 5:
        pontos += 2
        motivos.append(f"ip_subiu_{ip_diff:.1f}")

    if favorito_nao_vencendo(m):
        pontos += 2
        motivos.append("favorito_ainda_nao_resolveu")
    return clamp(pontos, 0, 12), "+".join(motivos) if motivos else "CONFIRMACAO_FRACA"


def aplicar_travas_finais(m: Metricas, score: int) -> Tuple[int, str, bool]:
    lado = m.lado_pressionante
    if m.liga == "PERIGOSA":
        return min(score, 74), "LIGA_PERIGOSA", True
    if vermelho_contra_pressionante(m):
        return min(score, 76), "VERMELHO_CONTRA_PRESSIONANTE", True
    if lado not in {"CASA", "FORA"}:
        return min(score, 72), "SEM_LADO_PRESSIONANTE", True
    if pressao_morta_lado(m, lado):
        return min(score, 74), "PRESSAO_MORTA", True
    if not consequencia_real_lado(m, lado):
        if not (m.odd_favorito <= 1.30 and favorito_nao_vencendo(m) and pressao_extrema_lado(m, lado)):
            return min(score, 76), "SEM_CONSEQUENCIA_REAL", True
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    if ip["pico"] >= 24 and d["chance"] <= 3 and d["rb"] == 0 and d["rl"] <= 2 and d["xg"] < 0.25:
        return min(score, 76), "FAKE_PRESSURE_IP_SEM_CONSEQUENCIA", True
    if m.valor_pos_evento_classe == "PRESSAO_PREMIADA_MORREU":
        return min(score, 74), "PRESSAO_PREMIADA_MORREU", True
    return score, "SEM_TRAVA_FINAL", False


def aplicar_teto_score_v9(m: Metricas, score: int, detalhes_funil: Optional[Dict[str, Any]] = None) -> Tuple[int, str]:
    if not HABILITAR_SCORE_V9:
        return score, "SCORE_V9_DESATIVADO"
    if eh_ht(m.estrategia):
        return score, "SCORE_V9_HT_PRESERVADO"

    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return min(score, 82), "SCORE_V9_SEM_LADO_TETO_82"

    detalhes_funil = detalhes_funil or {}
    cenario = str(detalhes_funil.get("cenario_ft", ""))
    d = dados_lado(m, lado)
    oposto = "FORA" if lado == "CASA" else "CASA"
    od = dados_lado(m, oposto)
    ip = ip_lado(m, lado)
    ap_diff = ap_diff_lado(m, lado)

    massacre_extremo = (
        d["ap"] >= 60
        and ap_diff >= 30
        and d["cantos"] >= 5
        and (d["rb"] >= 2 or d["rl"] >= 7 or d["chance"] >= 12)
        and (d["u10"] >= 7 or ip["pico"] >= 24 or ip["c18"] >= 2)
    )
    elite_contextual = (
        m.lado_favorito == lado
        and favorito_nao_vencendo(m)
        and d["ap"] >= 45
        and ap_diff >= 18
        and (d["u10"] >= 6 or ip["pico"] >= 22)
        and (d["rb"] >= 1 or d["rl"] >= 5 or d["chance"] >= 9 or d["xg"] >= 0.45)
    )
    if massacre_extremo:
        return score, "SCORE_V9_LIBERADO_MASSACRE_EXTREMO"
    if elite_contextual and score <= 94:
        return score, "SCORE_V9_LIBERADO_ELITE_CONTEXTUAL"
    if elite_contextual and score > 94:
        return 94, "SCORE_V9_TETO_94_ELITE_CONTEXTUAL_NAO_EXTREMO"

    cap = 96
    motivos: List[str] = []

    if cenario == "CAOS_PRODUTIVO":
        cap = min(cap, V9_CAP_JOGO_ABERTO)
        motivos.append("JOGO_ABERTO_NAO_MASSACRE")

    if m.liga == "UNDER":
        cap = min(cap, V9_CAP_LIGA_UNDER)
        motivos.append("LIGA_UNDER")
        if d["xg"] < 0.45 and d["chance"] < 8 and d["rb"] <= 1:
            cap = min(cap, V9_CAP_LIGA_UNDER_FRACA)
            motivos.append("UNDER_COM_PRODUCAO_BAIXA")

    if d["rb"] <= 1 and d["chance"] < 8 and d["xg"] < 0.45:
        cap = min(cap, V9_CAP_FINALIZACAO_BAIXA)
        motivos.append("FINALIZACAO_BAIXA")

    if d["u5"] <= 2 and d["u10"] <= 5 and not elite_contextual:
        cap = min(cap, V9_CAP_PRESSAO_RECENTE_FRACA)
        motivos.append("PRESSAO_RECENTE_FRACA")

    if not motivos and not massacre_extremo and not elite_contextual:
        cap = min(cap, V9_CAP_APROVADO_COMUM)
        motivos.append("APROVADO_COMUM_SEM_ELITE")

    if score > cap:
        return cap, "SCORE_V9_TETO_" + "+".join(motivos)
    return score, "SCORE_V9_SEM_REDUCAO_" + ("+".join(motivos) if motivos else "FORTE")


def finalização_minima_lado(m: Metricas, lado: str) -> bool:
    d = dados_lado(m, lado)
    return d["rb"] >= 1 or d["rda"] >= 1 or d["xg"] >= 0.15 or d["chance"] >= 5


# =========================================================
# CAMADA DE CENÁRIO FT (LIVE - INALTERADO)
# =========================================================

_XGL_MIN_PERDEDOR_REAGINDO = 0.50


def v27_continuidade_pos_gol(m: Metricas) -> Tuple[bool, str]:
    if not HABILITAR_V27_CONTINUIDADE_POS_GOL:
        return True, "V27_CONT_POS_GOL_OFF"
    if not eh_ft(m.estrategia):
        return True, "V27_CONT_POS_GOL_NAO_FT"
    if not m.ultimo_gol or m.ultimo_gol_lado not in {"CASA", "FORA"}:
        return True, "V27_SEM_GOL_RECENTE"
    delta = minutos_desde_ultimo_gol(m)
    if delta < -1 or delta > 8:
        return True, f"V27_GOL_FORA_JANELA_{delta}MIN"

    lado = m.lado_pressionante if m.lado_pressionante in {"CASA", "FORA"} else m.ultimo_gol_lado
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    cantos_pos_gol = [ev for ev in m.ultimos_cantos_lados if ev[0] >= m.ultimo_gol and ev[1] == lado]
    gol_lado_vencedor = ultimo_gol_deixou_lado_vencendo(m) or ultimo_gol_aumentou_vantagem(m)
    gol_do_press = m.ultimo_gol_lado == m.lado_pressionante
    gol_do_fav = m.ultimo_gol_lado == m.lado_favorito

    if not (gol_lado_vencedor or gol_do_press or gol_do_fav):
        return True, "V27_GOL_NAO_PREMIOU_PRESSIONANTE"

    if HABILITAR_DC01_3_EMPATE_FAVORITO:
        gol_do_fav_empatou = (
            gol_do_fav
            and lado_vencendo(m) == "EMPATE"
            and m.ultimo_gol_lado == m.lado_favorito
        )
        if gol_do_fav_empatou:
            provas_minimas_empate = 1
        else:
            provas_minimas_empate = None
    else:
        provas_minimas_empate = None

    provas = 0
    if d["u5"] >= 3:
        provas += 1
    if d["u10"] >= 7:
        provas += 1
    if d["rb"] >= 1 or d["rl"] >= 3 or d["chance"] >= 8 or d["xg"] >= 0.28:
        provas += 1
    if cantos_pos_gol:
        provas += 1
    if ip["pico"] >= 20 or ip["c18"] >= 1:
        provas += 1

    provas_minimas_final = None
    if HABILITAR_DC01_4_GOL_RECENTE_PRESSIONANTE_AP and gol_do_press and provas_minimas_empate is None:
        lado_op = "FORA" if lado == "CASA" else "CASA"
        d_op = dados_lado(m, lado_op)
        ap_press = d["u10"] + d["u5"]
        ap_op = d_op["u10"] + d_op["u5"]
        press_domina_ap = ap_press > ap_op
        jogo_nao_esfriou = d["u5"] > 0 or d["rb"] > 0 or ip["pico"] >= 10
        if press_domina_ap and jogo_nao_esfriou:
            provas_minimas_final = 1

    if delta <= 3:
        exige = provas_minimas_final if provas_minimas_final is not None else (provas_minimas_empate if provas_minimas_empate is not None else 3)
        ok = provas >= exige
    else:
        exige = provas_minimas_final if provas_minimas_final is not None else (provas_minimas_empate if provas_minimas_empate is not None else 2)
        ok = provas >= exige

    motivo = (
        f"V27_CONT_POS_GOL provas={provas} delta={delta} lado={lado} "
        f"u5={d['u5']} u10={d['u10']} rb={d['rb']} rl={d['rl']} "
        f"chance={d['chance']} xg={d['xg']:.2f} cantos_pos={len(cantos_pos_gol)} "
        f"ip={ip['pico']} gol_vencedor={gol_lado_vencedor} gol_press={gol_do_press} gol_fav={gol_do_fav}"
    )
    return ok, motivo if ok else "BLOQUEIO_" + motivo


def v27_empate_tem_prova_suficiente(m: Metricas, lado: str) -> Tuple[bool, str]:
    if not HABILITAR_V27_EMPATE_MAIS_RIGIDO:
        return True, "V27_EMPATE_OFF"
    if lado_vencendo(m) != "EMPATE" or not eh_ft(m.estrategia):
        return True, "V27_NAO_EMPATE_FT"
    d = dados_lado(m, lado)
    provas = [
        d["rb"] >= 2,
        d["chance"] >= 10,
        d["xg"] >= 0.40,
        pressao_extrema_lado(m, lado),
    ]
    total = sum(1 for x in provas if x)
    ok = total >= 2
    motivo = f"V27_EMPATE_PROVAS={total}/4 rb={d['rb']} chance={d['chance']} xg={d['xg']:.2f} extrema={pressao_extrema_lado(m,lado)}"
    return ok, motivo


def v27_perdedor_um_tem_reacao(m: Metricas, lado: str) -> Tuple[bool, str]:
    if not HABILITAR_V27_PERDEDOR_UM_EXIGE_REACAO:
        return True, "V27_PERDEDOR_UM_OFF"
    if not eh_ft(m.estrategia):
        return True, "V27_NAO_FT"
    if diferenca_placar(m) != 1 or lado_vencendo(m) in {"EMPATE", "DESCONHECIDO"}:
        return True, "V27_NAO_PERDE_1"
    if lado != lado_perdendo(m):
        return True, "V27_LADO_NAO_E_PERDEDOR"
    d = dados_lado(m, lado)
    if HABILITAR_DC01_3_PERDEDOR_PRESSAO_SIMPLES:
        pressao_dupla = d["u5"] >= 3 and d["u10"] >= 6
        pressao_simples = d["u5"] >= 4 or d["u10"] >= 7
        reacao_viva = pressao_dupla or pressao_simples or (pressao_viva_lado(m, lado) and consequencia_real_lado(m, lado))
    else:
        pressao_dupla = d["u5"] >= 3 and d["u10"] >= 6
        reacao_viva = pressao_dupla or (pressao_viva_lado(m, lado) and consequencia_real_lado(m, lado))
    motivo = f"V27_PERDEDOR_1 reacao={reacao_viva} u5={d['u5']} u10={d['u10']} consequencia={consequencia_real_lado(m,lado)}"
    return bool(reacao_viva), motivo


def v27_favorito_vencendo_1x0_exige_extra(m: Metricas, lado: str) -> Tuple[bool, str]:
    if not HABILITAR_V27_REFINO_PLACAR_FT:
        return True, "V27_PLACAR_OFF"
    if not eh_ft(m.estrategia):
        return True, "V27_NAO_FT"
    total = total_gols_placar(m)
    if total != 1 or diferenca_placar(m) != 1 or lado != lado_vencendo(m):
        return True, "V27_NAO_1X0_VENCEDOR"
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    if HABILITAR_DC01_3_1X0_TRES_DE_QUATRO:
        criterios = [
            d["u5"] >= 5,
            d["u10"] >= 9,
            (d["rb"] >= 2 or d["chance"] >= 10 or d["xg"] >= 0.45),
            (ip["pico"] >= 22 or ip["c18"] >= 2),
        ]
        ok = sum(criterios) >= 3
    else:
        ok = (
            d["u5"] >= 5
            and d["u10"] >= 9
            and (d["rb"] >= 2 or d["chance"] >= 10 or d["xg"] >= 0.45)
            and (ip["pico"] >= 22 or ip["c18"] >= 2)
        )
    motivo = f"V27_1X0_EXTRA ok={ok} u5={d['u5']} u10={d['u10']} rb={d['rb']} chance={d['chance']} xg={d['xg']:.2f} ip={ip['pico']}"
    return ok, motivo


def continuidade_pos_gol(m: "Metricas", lado: str) -> Tuple[bool, str]:
    if not m.ultimo_gol or lado not in {"CASA", "FORA"}:
        return True, "SEM_GOL_ANTERIOR"
    delta = int(m.tempo or 0) - int(m.ultimo_gol or 0)
    if delta > 10:
        return True, f"GOL_ANTIGO_{delta}MIN"
    cantos_pos_gol = sum(
        1 for min_c, lado_c in m.ultimos_cantos_lados
        if min_c > m.ultimo_gol and lado_c == lado
    )
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    tem_u5 = d["u5"] > 0
    tem_ip = ip["pico"] > 10
    tem_canto = cantos_pos_gol > 0
    if tem_u5 or tem_ip or tem_canto:
        return True, f"CONT_VIVA u5={d['u5']} ip={ip['pico']} cantos_pos={cantos_pos_gol} delta={delta}min"
    return False, f"PRESSAO_MORTA_POS_GOL u5={d['u5']} ip={ip['pico']} cantos_pos={cantos_pos_gol} delta={delta}min"


def v28_dna_projetado(m: "Metricas", lado: str) -> Tuple[int, str]:
    if not (HABILITAR_V28 and HABILITAR_V28_DNA_PROJETADO):
        return 0, "V28_DNA_OFF"
    if lado not in {"CASA", "FORA"}:
        return 0, "V28_DNA_SEM_LADO"
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    odd = m.odd_favorito or 0.0
    favorito_ok = m.lado_favorito == lado and odd > 0
    l15_proxy = d["u10"] + d["u5"]
    consequencia = d["xg"] >= 0.45 or d["rb"] >= 2 or d["chance"] >= 10
    pressao = l15_proxy >= 10 or ip["pico"] >= 22 or ip["c18"] >= 2

    if favorito_ok and odd <= 1.55 and consequencia and pressao:
        return 4, f"DNA_ELITE odd={odd} xg={d['xg']:.2f} l15={l15_proxy}"
    if favorito_ok and odd <= 1.85 and (consequencia or pressao) and l15_proxy >= 7:
        return 2, f"DNA_FORTE odd={odd} xg={d['xg']:.2f} l15={l15_proxy}"
    if favorito_ok and odd <= 2.20 and (d["xg"] >= 0.28 or d["chance"] >= 8 or d["rb"] >= 1):
        return -1, f"DNA_MODERADO odd={odd} xg={d['xg']:.2f} l15={l15_proxy}"
    return -3, f"DNA_FRACO odd={odd} xg={d['xg']:.2f} l15={l15_proxy}"


def v28_under_extraordinario(m: "Metricas", lado: str) -> Tuple[bool, str]:
    if lado not in {"CASA", "FORA"}:
        return False, "V28_UNDER_SEM_LADO"
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    extremo = pressao_extrema_lado(m, lado)
    prova = (
        (d["rb"] >= 2 and d["xg"] >= 0.45)
        or (d["chance"] >= 12 and d["u5"] >= 4 and d["u10"] >= 8)
        or (extremo and consequencia_real_lado(m, lado) and (ip["pico"] >= 24 or ip["c18"] >= 2))
    )
    motivo = (
        f"V28_UNDER_EXTRAORDINARIO={prova} rb={d['rb']} xg={d['xg']:.2f} "
        f"chance={d['chance']} u5={d['u5']} u10={d['u10']} ip={ip['pico']} extremo={extremo}"
    )
    return bool(prova), motivo


@dataclass
class CenarioFT:
    codigo: str
    bloqueia: bool
    teto_score: int
    bonus_fav_cap: int
    motivo: str


def classificar_cenario_ft(m: Metricas) -> CenarioFT:
    if eh_ht(m.estrategia):
        return CenarioFT("NAO_FT", False, 100, 99, "CHAMADA_INDEVIDA_HT")

    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return CenarioFT("SEM_LADO", False, 100, 99, "SEM_LADO_PRESSIONANTE")

    if HABILITAR_V28 and HABILITAR_V28_UNDER_BLOQUEIO_FORTE and m.liga == "UNDER":
        under_ok, under_motivo = v28_under_extraordinario(m, lado)
        if not under_ok:
            return CenarioFT(
                "V28_LIGA_UNDER_BLOQUEADA", True, V28_UNDER_TETO_SCORE, 99,
                under_motivo
            )

    d = dados_lado(m, lado)
    gc, gf = extrair_gols_placar(m.placar)
    gc = gc or 0
    gf = gf or 0
    total_gols = gc + gf
    dif = abs(gc - gf)
    vencendo = lado_vencendo(m)
    perdendo = lado_perdendo(m)
    fav = m.lado_favorito

    xgl_lado = d.get("xgl", 0.0)

    cont_ok, cont_motivo = v27_continuidade_pos_gol(m)
    if not cont_ok:
        return CenarioFT("V27_SEM_CONTINUIDADE_POS_GOL", True, 78, 99, cont_motivo)

    empate_ok, empate_motivo = v27_empate_tem_prova_suficiente(m, lado)
    if not empate_ok:
        return CenarioFT("V27_EMPATE_SEM_PROVA_EXTRA", True, 80, 99, empate_motivo)

    perdedor_ok, perdedor_motivo = v27_perdedor_um_tem_reacao(m, lado)
    if not perdedor_ok:
        return CenarioFT("V27_PERDEDOR_1_SEM_REACAO", True, 80, 99, perdedor_motivo)

    placar_ok, placar_motivo = v27_favorito_vencendo_1x0_exige_extra(m, lado)
    if not placar_ok:
        return CenarioFT("V27_1X0_SEM_PROVA_EXTRA", True, 80, 4, placar_motivo)

    if d["u5"] == 0 and d["u10"] == 0 and d["rb"] == 0 and d["xg"] < 0.10:
        return CenarioFT(
            "JOGO_MORTO_FT", True, 78, 99,
            f"U5=0_U10=0_RB=0_XG={d['xg']:.2f}_AUSENCIA_TOTAL"
        )

    if d["rb"] == 0 and d["chance"] < 8 and d["xg"] < 0.25:
        return CenarioFT(
            "FAKE_PRESSURE", True, 76, 99,
            f"RB=0_CHANCE={d['chance']}_XG={d['xg']:.2f}_SEM_FINALIZACAO_REAL"
        )

    pressao_dupla = d["u5"] >= 3 and d["u10"] >= 6

    if total_gols >= 4 and dif <= 1:
        return CenarioFT(
            "CAOS_PRODUTIVO", False, 100, 99,
            f"JOGO_ABERTO_{gc}x{gf}_DIF={dif}_TOTAL_GOLS={total_gols}"
        )

    if dif >= 2 and lado == vencendo:
        if not pressao_extrema_lado(m, lado):
            return CenarioFT(
                "PLACAR_RESOLVIDO", True, 78, 3,
                f"VENCENDO_{gc}x{gf}_SEM_PRESSAO_EXTREMA_PARA_AMPLIAR"
            )
        return CenarioFT(
            "VENCEDOR_EXTREMO", False, 100, 5,
            f"VENCENDO_{gc}x{gf}_PRESSAO_EXTREMA_CONTINUA"
        )

    if dif >= 2 and lado == perdendo:
        tem_pressao = pressao_dupla or pressao_viva_lado(m, lado)
        tem_consequencia = consequencia_real_lado(m, lado)
        xgl_ok = xgl_lado >= _XGL_MIN_PERDEDOR_REAGINDO
        if tem_pressao and tem_consequencia and xgl_ok:
            return CenarioFT(
                "PERDEDOR_REAGINDO", False, 100, 99,
                f"PERDENDO_{gc}x{gf}_PRESSAO+CONSEQUENCIA+XGL={xgl_lado:.2f}"
            )
        return CenarioFT(
            "PERDEDOR_SEM_REACAO_REAL", True, 78, 99,
            f"PERDENDO_{gc}x{gf}_XGL={xgl_lado:.2f}_FALTA_COMBINACAO"
        )

    if dif == 1 and lado == vencendo and xgl_lado < 0.55:
        return CenarioFT(
            "FAVORITO_VENCENDO_XGL_FRACO", False, 100, 4,
            f"VENCENDO_1_GOL_XGL={xgl_lado:.2f}_HISTORICO_FRACO"
        )

    return CenarioFT("ALFA_REAL", False, 100, 99, "CENARIO_IDEAL_CONFIRMADO")


def liga_under_exige_prova_extra(m: Metricas) -> bool:
    if not HABILITAR_UNDER_PROVA_EXTRA:
        return False
    texto = remover_acentos(f"{m.competicao} {m.jogo}").lower()
    if "argentina" in texto:
        return False
    termos_rigidos = (
        "venezuela", "chile", "peru", "colombia", "bolivia",
        "paraguay", "paraguai", "mexico", "equador", "ecuador",
        "uruguay", "uruguai",
    )
    return m.liga == "UNDER" or any(t in texto for t in termos_rigidos)


def prova_extra_liga_under(m: Metricas, lado: str) -> Tuple[bool, str]:
    if lado not in {"CASA", "FORA"}:
        return False, "UNDER_SEM_LADO"
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)

    if d["rb"] >= 2 and (d["u5"] >= 3 or d["u10"] >= 7):
        return True, "UNDER_OK_RB_FORTE"
    if d["xg"] >= 0.60 and d["chance"] >= 9:
        return True, "UNDER_OK_XG_CHANCE"
    if d["u5"] >= 4 and d["u10"] >= 8 and ip["pico"] >= 24 and consequencia_real_lado(m, lado):
        return True, "UNDER_OK_PRESSAO_ELITE"
    if d["rda"] >= 3 and d["chance"] >= 10:
        return True, "UNDER_OK_AREA_CHANCE"
    if pressao_extrema_lado(m, lado) and d["xg"] >= 0.45 and d["rb"] >= 1:
        return True, "UNDER_OK_EXTREMO"

    return False, (
        f"UNDER_SEM_PROVA_EXTRA rb={d['rb']} rda={d['rda']} "
        f"xg={d['xg']:.2f} chance={d['chance']} "
        f"u5={d['u5']} u10={d['u10']} ip={ip['pico']}"
    )


# =========================================================
# FUNIL OBRIGATÓRIO HÍBRIDO (LIVE - INALTERADO)
# =========================================================

def funil_obrigatorio_hibrido(m: Metricas) -> Tuple[bool, int, str, Dict[str, Any]]:
    lado = m.lado_pressionante
    detalhes: Dict[str, Any] = {}

    if m.liga == "PERIGOSA":
        return False, 72, "FUNIL_LIGA_PERIGOSA", detalhes
    if lado not in {"CASA", "FORA"}:
        return False, 72, "FUNIL_SEM_LADO_PRESSIONANTE", detalhes
    if vermelho_contra_pressionante(m):
        return False, 74, "FUNIL_VERMELHO_CONTRA_PRESSIONANTE", detalhes

    if liga_under_exige_prova_extra(m):
        under_ok, under_motivo = prova_extra_liga_under(m, lado)
        detalhes["under_prova_extra"] = under_ok
        detalhes["under_prova_extra_motivo"] = under_motivo
        if not under_ok:
            return False, 78, f"FUNIL_UNDER_SEM_PROVA_EXTRA | {under_motivo}", detalhes

    pressao = pressao_viva_lado(m, lado)
    consequencia = consequencia_real_lado(m, lado)
    extremo = pressao_extrema_lado(m, lado)
    fav_nao_vence = favorito_nao_vencendo(m) and m.lado_favorito == lado
    super_fav = bool(m.odd_favorito and m.odd_favorito <= 1.30 and fav_nao_vence)

    valor_forte = m.valor_pos_evento_classe in {
        "GOL_CONTRA_FLUXO_VALORIZA",
        "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA",
        "PRESSAO_PREMIADA_MAS_CONTINUA",
    }

    tem_finalizacao = finalização_minima_lado(m, lado)
    valor_forte_validado = valor_forte and tem_finalizacao

    consequencia_minima = consequencia_minima_emocional(m, lado)
    emocional_vivo, motivo_emocional = contexto_emocional_vivo(m, lado)

    detalhes.update({
        "pressao_viva": pressao,
        "consequencia": consequencia,
        "pressao_extrema": extremo,
        "favorito_nao_vence": fav_nao_vence,
        "super_favorito_nao_vence": super_fav,
        "valor_pos_evento_forte": valor_forte,
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

    if eh_ht(m.estrategia):
        ht_morto, ht_motivo_dc01_1 = dc01_1_ht_placar_largo_sem_necessidade(m, lado)
        detalhes["dc01_1_ht_placar_largo"] = ht_morto
        detalhes["dc01_1_ht_placar_largo_motivo"] = ht_motivo_dc01_1
        if ht_morto:
            return False, 78, "FUNIL_DC01_1_HT_PLACAR_LARGO_SEM_NECESSIDADE | " + ht_motivo_dc01_1, detalhes

        if m.lado_favorito != lado and m.odd_favorito and m.odd_favorito > 1.60:
            return False, 80, f"FUNIL_HT_ZEBRA_DOMINANDO_ODD={m.odd_favorito:.2f}", detalhes

        if not pressao:
            return False, 76, "FUNIL_HT_SEM_PRESSAO_VIVA", detalhes

        if HABILITAR_HT_PREMIUM_V2:
            pos_gol_recente = gol_recente_do_pressionante(m, janela=3)
            massacre_ok, massacre_motivo = massacre_contextual_ht(m, lado, pos_gol_recente=pos_gol_recente)
            detalhes["ht_massacre_contextual"] = massacre_ok
            detalhes["ht_massacre_motivo"] = massacre_motivo
            detalhes["ht_pos_gol_recente"] = pos_gol_recente

            if not massacre_ok:
                return False, 82 if pos_gol_recente else 80, f"FUNIL_HT_SEM_MASSACRE_CONTEXTUAL_{massacre_motivo}", detalhes

        if not consequencia and not (super_fav and extremo and consequencia_minima):
            return False, 78, "FUNIL_HT_SEM_CONSEQUENCIA_MINIMA", detalhes
        return True, 100, "FUNIL_HT_PREMIUM_MASSACRE_APROVADO", detalhes

    cenario = classificar_cenario_ft(m)
    detalhes["cenario_ft"] = cenario.codigo
    detalhes["cenario_ft_motivo"] = cenario.motivo

    if cenario.bloqueia:
        return False, cenario.teto_score, f"FUNIL_FT_CENARIO_{cenario.codigo}", detalhes

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


def score_python_contextual(m: Metricas, chave: str) -> DecisaoPython:
    preencher_contexto_calculado(m)
    valor_classe, valor_motivo, ajuste_evento, proteger_ia = avaliar_valor_pos_evento(m)
    m.valor_pos_evento_classe = valor_classe
    m.valor_pos_evento_motivo = valor_motivo
    m.protecao_ia_ativa = proteger_ia

    passou_funil, teto_funil, motivo_funil, detalhes_funil = funil_obrigatorio_hibrido(m)

    base = 45
    _, elite_motivo = bonus_time_elite_transicao(m)

    # Mudança 11: V26 aplicado ANTES do score
    penalidade_v26 = getattr(m, 'penalidade_v26_fav_nao_press', 0)

    componentes = {
        "pressao_viva": score_pressao_viva(m),
        "consequencia": score_consequencia(m),
        "favoritismo": score_favoritismo(m),
        "valor_pos_evento": ajuste_evento,
        "liga": liga_ajuste(m.liga),
        "relogio": score_relogio(m),
    }
    conf_score, conf_motivo = score_confirmacao(m, chave)
    componentes["confirmacao"] = conf_score

    dna_ajuste, dna_motivo = v28_dna_projetado(m, m.lado_pressionante)
    if HABILITAR_V28 and HABILITAR_V28_DNA_PROJETADO:
        componentes["dna_v28"] = dna_ajuste

    score_bruto = base + sum(componentes.values())

    # Mudança 11: Aplicar penalidade no score bruto
    if penalidade_v26:
        score_bruto_original = score_bruto
        score_bruto = max(0, score_bruto - penalidade_v26)
        log(f"📉 V26 penalidade aplicada no score bruto | {score_bruto_original} → {score_bruto} | -{penalidade_v26}")

    if eh_ht(m.estrategia):
        if m.lado_pressionante in {"CASA", "FORA"} and pressao_extrema_lado(m, m.lado_pressionante):
            score_bruto += 4

        bonus_super_fav_ok, bonus_super_fav_motivo = ht_bonus_super_fav_vencendo_v12_2(m)
        if bonus_super_fav_ok:
            score_bruto += 5
    else:
        cenario_ft = classificar_cenario_ft(m)
        if cenario_ft.bonus_fav_cap < 99:
            componentes["favoritismo"] = min(componentes["favoritismo"], cenario_ft.bonus_fav_cap)
            score_bruto = base + sum(componentes.values())

        if favorito_nao_vencendo(m) and m.lado_favorito == m.lado_pressionante:
            score_bruto += 4

    score = clamp(score_bruto, 0, 96)

    if not passou_funil:
        score = min(score, teto_funil)
        detalhes = {
            "componentes": componentes,
            "score_bruto": score_bruto,
            "confirmacao_motivo": conf_motivo,
            "corte": corte_por_estrategia(m.estrategia),
            "valor_pos_evento_classe": valor_classe,
            "valor_pos_evento_motivo": valor_motivo,
            "funil": motivo_funil,
            "funil_detalhes": detalhes_funil,
            "time_elite": elite_motivo,
            "ht_bonus_super_fav_vencendo": locals().get("bonus_super_fav_ok", False),
            "ht_bonus_super_fav_motivo": locals().get("bonus_super_fav_motivo", "NAO_AVALIADO"),
            "penalidade_v26": penalidade_v26,
        }
        return DecisaoPython(score=score, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_funil, detalhes=detalhes)

    score, motivo_trava, bloqueado_trava = aplicar_travas_finais(m, score)

    motivo_teto_v9 = "SCORE_V9_NAO_APLICADO"
    if not bloqueado_trava:
        score, motivo_teto_v9 = aplicar_teto_score_v9(m, score, detalhes_funil)

    corte = corte_por_estrategia(m.estrategia)
    aprovado = score >= corte and not bloqueado_trava
    status = "APROVADO" if aprovado else "REPROVADO"
    motivo = motivo_trava if bloqueado_trava else f"score={score} corte={corte} valor={valor_classe}"

    detalhes = {
        "componentes": componentes,
        "score_bruto": score_bruto,
        "confirmacao_motivo": conf_motivo,
        "corte": corte,
        "valor_pos_evento_classe": valor_classe,
        "valor_pos_evento_motivo": valor_motivo,
        "funil": motivo_funil,
        "funil_detalhes": detalhes_funil,
        "trava": motivo_trava,
        "teto_v9": motivo_teto_v9,
        "time_elite": elite_motivo,
        "ht_bonus_super_fav_vencendo": locals().get("bonus_super_fav_ok", False),
        "ht_bonus_super_fav_motivo": locals().get("bonus_super_fav_motivo", "NAO_AVALIADO"),
        "cenario_ft": detalhes_funil.get("cenario_ft", "N/A"),
        "cenario_ft_motivo": detalhes_funil.get("cenario_ft_motivo", "N/A"),
        "penalidade_v26": penalidade_v26,
    }
    return DecisaoPython(score=score, aprovado_pre_ia=aprovado, status=status, motivo=motivo, detalhes=detalhes)


def corte_por_estrategia(estrategia: str) -> int:
    if estrategia == "ALFA_HT_CONFIRMACAO":
        return CORTE_CONFIRMACAO_GOL_HT
    if estrategia == "ALFA_FT_CONFIRMACAO":
        return CORTE_CONFIRMACAO_GOL_FT
    if eh_ht(estrategia):
        return CORTE_GOL_HT
    return CORTE_GOL_FT


# =========================================================
# IA AUDITORA — COM SANITIZAÇÃO (MUDANÇA 18) - INALTERADO
# =========================================================

def sanitizar_prompt(texto: str) -> str:
    """Sanitiza texto para uso em prompts da IA (Mudança 18)."""
    texto = str(texto or "")
    texto = texto.replace("'", "").replace('"', "")
    texto = texto.replace("\n", " ").replace("\r", " ")
    texto = texto.replace(";", ",").replace("|", ",")
    return texto[:500]


def montar_prompt_ia(m: Metricas, decisao_py: DecisaoPython) -> str:
    if HABILITAR_V29_IA_NOVA:
        return f"""
Você é a IA Auditora do projeto COUTIPS/ALFA. Sua função principal é ELIMINAR FALSOS POSITIVOS.

FILOSOFIA OBRIGATÓRIA:
Você não procura oportunidades. Você procura armadilhas.
Prefira bloquear um jogo bom a aprovar um jogo ruim.
Se houver dúvida relevante, penalize.

HIERARQUIA DE DECISÃO:
1. Procure motivos para REPROVAR.
2. Procure sinais de pressão fake.
3. Procure sinais de perda de contexto ou motivação.
4. Só então avalie motivos para aprovar.

PERGUNTA CENTRAL:
"Existe evidência real de que o próximo gol continua vivo neste momento, ou os números estão criando uma falsa impressão de valor?"

CASOS QUE GERAM SUSPEITA AUTOMÁTICA (penalize fortemente):
- Favorito vencendo por 3 ou mais gols
- Gol recente seguido de queda brusca de pressão
- Ataques perigosos altos sem remates compatíveis (fake pressure)
- Pressão concentrada apenas nos últimos 5 minutos
- Domínio estatístico sem invasão efetiva (AP alto, RB baixo, xG baixo)
- Mercado já muito fechado sem valor restante

REGRAS DE CONTEXTO (não bloquear automaticamente):
- Gol recente do favorito que CONTINUOU pressionando é neutro, não negativo
- Gol contra o fluxo (adversário marcou mas favorito segue dominando) é positivo
- Super favorito empatando ou perdendo com pressão sustentada é contexto positivo

Responda em UMA linha no formato:
DECISAO=APROVAR|BLOQUEAR; CONFIANCA=0-100; PRESSAO_SUSTENTAVEL=0-100; RISCO_FAKE_PRESSURE=0-100; CONTINUIDADE_OFENSIVA=0-100; MOTIVACAO_DE_GOL=0-100; MOTIVO=texto curto

DADOS:
Estratégia: {sanitizar_prompt(m.estrategia)}
Jogo: {sanitizar_prompt(m.jogo)}
Competição: {sanitizar_prompt(m.competicao)}
Minuto: {sanitizar_prompt(str(m.tempo))}
Placar: {sanitizar_prompt(m.placar)}
Mercado: {sanitizar_prompt(m.mercado)}
Liga: {sanitizar_prompt(m.liga)}
Odds: {sanitizar_prompt(str(m.odds))}
Favorito: {sanitizar_prompt(m.lado_favorito)} odd {sanitizar_prompt(str(m.odd_favorito))}
Dominante: {sanitizar_prompt(m.lado_dominante)}
Pressionante: {sanitizar_prompt(m.lado_pressionante)}
Último gol: {sanitizar_prompt(str(m.ultimo_gol))}' {sanitizar_prompt(m.ultimo_gol_lado)}
AP: {sanitizar_prompt(str(m.ataques_perigosos))}
U5: {sanitizar_prompt(str(m.ultimos5))}
U10: {sanitizar_prompt(str(m.ultimos10))}
Cantos: {sanitizar_prompt(str(m.cantos))}
RB: {sanitizar_prompt(str(m.remates_baliza))}
Remates lado: {sanitizar_prompt(str(m.remates_lado))}
RDA: {sanitizar_prompt(str(m.remates_dentro_area))}
Chance gol: {sanitizar_prompt(str(m.chance_golo))}
xG: {sanitizar_prompt(str(m.xg))}
IP: {sanitizar_prompt(str(m.pressao_alfa))}
Valor pós-evento: {sanitizar_prompt(m.valor_pos_evento_classe)} | {sanitizar_prompt(m.valor_pos_evento_motivo)}
Score Python: {sanitizar_prompt(str(decisao_py.score))}
Motivo Python: {sanitizar_prompt(decisao_py.motivo)}
""".strip()

    return f"""
Você é a IA Auditora do projeto COUTIPS/ALFA.
Python é o motor principal. Sua função é auditar fake pressure e incoerências, não destruir contexto institucional forte.

REGRAS IMPORTANTES:
- Gol recente não é automaticamente negativo.
- Gol recente só é negativo quando premiou a pressão do lado que gerava o alerta e essa pressão morreu.
- Se a zebra/adversário marcou contra o fluxo e o favorito/pressionante continua pressionando, isso é positivo para over.
- Se o favorito marcou mas continuou pressionando, não bloquear automaticamente; avaliar continuidade pós-gol.
- Favorito forte/super favorito empatando ou perdendo com pressão sustentada é contexto positivo.
- RDA=0 ou xG baixo não podem matar sozinhos quando há super favorito não vencendo, pressão viva, cantos/chance/IP fortes.

Responda obrigatoriamente em UMA linha no formato:
DECISAO=APROVAR|BLOQUEAR; CONFIANCA=0-100; MOTIVO=texto curto

DADOS:
Estratégia: {sanitizar_prompt(m.estrategia)}
Jogo: {sanitizar_prompt(m.jogo)}
Competição: {sanitizar_prompt(m.competicao)}
Minuto: {sanitizar_prompt(str(m.tempo))}
Placar: {sanitizar_prompt(m.placar)}
Mercado: {sanitizar_prompt(m.mercado)}
Liga: {sanitizar_prompt(m.liga)}
Odds: {sanitizar_prompt(str(m.odds))}
Favorito: {sanitizar_prompt(m.lado_favorito)} odd {sanitizar_prompt(str(m.odd_favorito))}
Dominante: {sanitizar_prompt(m.lado_dominante)}
Pressionante: {sanitizar_prompt(m.lado_pressionante)}
Último gol: {sanitizar_prompt(str(m.ultimo_gol))}' {sanitizar_prompt(m.ultimo_gol_lado)}
AP: {sanitizar_prompt(str(m.ataques_perigosos))}
U5: {sanitizar_prompt(str(m.ultimos5))}
U10: {sanitizar_prompt(str(m.ultimos10))}
Cantos: {sanitizar_prompt(str(m.cantos))}
RB: {sanitizar_prompt(str(m.remates_baliza))}
Remates lado: {sanitizar_prompt(str(m.remates_lado))}
RDA: {sanitizar_prompt(str(m.remates_dentro_area))}
Chance gol: {sanitizar_prompt(str(m.chance_golo))}
xG: {sanitizar_prompt(str(m.xg))}
IP: {sanitizar_prompt(str(m.pressao_alfa))}
Valor pós-evento: {sanitizar_prompt(m.valor_pos_evento_classe)} | {sanitizar_prompt(m.valor_pos_evento_motivo)}
Score Python: {sanitizar_prompt(str(decisao_py.score))}
Motivo Python: {sanitizar_prompt(decisao_py.motivo)}
""".strip()


async def consultar_openai(m: Metricas, decisao_py: DecisaoPython) -> Tuple[str, int, str]:
    if not OPENAI_HABILITADO or not OPENAI_API_KEY:
        return "APROVAR", decisao_py.score, "OPENAI_DESATIVADA_USANDO_SCORE_PYTHON"

    prompt = montar_prompt_ia(m, decisao_py)

    if HABILITAR_V29_IA_NOVA:
        system_prompt = (
            "Você é uma IA auditora conservadora especializada em eliminar falsos positivos em apostas de futebol ao vivo. "
            "Sua missão principal é encontrar armadilhas, não oportunidades. "
            "Responda SOMENTE no formato exato pedido, sem texto adicional."
        )
        max_tokens = 180
    else:
        system_prompt = "Você é uma IA auditora objetiva de futebol ao vivo. Responda somente no formato pedido."
        max_tokens = 120

    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=OPENAI_TIMEOUT) as http:
            r = await http.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"].strip()

        decisao = "APROVAR" if "APROVAR" in content.upper() else "BLOQUEAR" if "BLOQUEAR" in content.upper() else "APROVAR"
        conf = pegar_numero(r"CONFIANCA\s*=\s*(\d+)", content, decisao_py.score)
        if conf == decisao_py.score:
            conf = pegar_numero(r"CONFIANÇA\s*=\s*(\d+)", content, decisao_py.score)

        if HABILITAR_V29_IA_NOVA:
            pressao_sust = pegar_numero(r"PRESSAO_SUSTENTAVEL\s*=\s*(\d+)", content, 0)
            risco_fake = pegar_numero(r"RISCO_FAKE_PRESSURE\s*=\s*(\d+)", content, 0)
            continuidade = pegar_numero(r"CONTINUIDADE_OFENSIVA\s*=\s*(\d+)", content, 0)
            motivacao = pegar_numero(r"MOTIVACAO_DE_GOL\s*=\s*(\d+)", content, 0)
            log(
                f"🤖 OpenAI V29 | {m.jogo} | {decisao} | score={conf} | "
                f"pressao={pressao_sust} fake={risco_fake} cont={continuidade} motiv={motivacao}"
            )
        else:
            log(f"🤖 OpenAI | {m.jogo} | {decisao} | confiança={conf}")

        motivo = content[:300]
        return decisao, clamp(conf, 0, 95), motivo
    except Exception as e:
        log(f"⚠️ OpenAI falhou | usando score Python | {type(e).__name__}: {e}")
        return "APROVAR", decisao_py.score, "OPENAI_FALHOU_USANDO_SCORE_PYTHON"


def calcular_protecao_ia(m: Metricas, decisao_py: DecisaoPython, decisao_ia: str, confianca_ia: int) -> DecisaoIA:
    original = confianca_ia
    proteger = False
    motivo = "SEM_PROTECAO"
    piso = 0

    lado = m.lado_pressionante
    if lado in {"CASA", "FORA"} and decisao_py.score >= 82 and not vermelho_contra_pressionante(m):
        if m.valor_pos_evento_classe in {"GOL_CONTRA_FLUXO_VALORIZA", "PRESSAO_PREMIADA_MAS_CONTINUA", "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA"}:
            proteger = True
            motivo = m.valor_pos_evento_classe
        elif eh_confirmacao(m.estrategia) and decisao_py.detalhes.get("componentes", {}).get("confirmacao", 0) >= 6:
            proteger = True
            motivo = "CONFIRMACAO_MELHOROU_CLARAMENTE"
        elif m.lado_favorito == lado and favorito_nao_vencendo(m) and pressao_viva_lado(m, lado):
            proteger = True
            motivo = "FAVORITO_NAO_VENCE_COM_PRESSAO_VIVA"

    impedimentos = []
    if m.liga == "PERIGOSA":
        impedimentos.append("LIGA_PERIGOSA")
    if lado not in {"CASA", "FORA"}:
        impedimentos.append("SEM_LADO_PRESSIONANTE")
    elif pressao_morta_lado(m, lado):
        impedimentos.append("PRESSAO_MORTA")
    if decisao_py.score < 82:
        impedimentos.append("SCORE_PYTHON_BAIXO")
    if vermelho_contra_pressionante(m):
        impedimentos.append("VERMELHO_CONTRA_PRESSIONANTE")

    if impedimentos:
        proteger = False
        motivo = "NAO_PROTEGER_" + "+".join(impedimentos)

    if proteger:
        if eh_ft(m.estrategia):
            piso = 78
            if m.odd_favorito and m.odd_favorito <= 1.30 and favorito_nao_vencendo(m):
                piso = 82
            if eh_confirmacao(m.estrategia):
                piso = max(piso, 84)
        else:
            piso = 80
            if (m.odd_favorito and m.odd_favorito <= 1.30) or pressao_extrema_lado(m, lado):
                piso = 84
        corrigida = max(confianca_ia, piso)
        log(f"🛡️ PROTECAO_IA_CONTEXTUAL_ATIVA | ia_original={original} | ia_corrigida={corrigida} | motivo={motivo} | score_python={decisao_py.score}")
        m.protecao_ia_ativa = True
        return DecisaoIA(decisao_ia, original, corrigida, "IA_PROTEGIDA", True, motivo)

    log(f"🧪 PROTECAO_IA_NAO_ATIVADA | motivo={motivo} | ia={original} | score_python={decisao_py.score}")
    m.protecao_ia_ativa = False
    # BUG CORRIGIDO (28/06): esse caminho (proteção NÃO ativada) nunca
    # tinha um `return` — a função caía no fim e devolvia None. Qualquer
    # alerta que passasse por aqui (score_python baixo, liga perigosa,
    # sem lado pressionante, etc) quebrava com
    # "AttributeError: 'NoneType' object has no attribute 'confianca_corrigida'"
    # em _processar_score_e_ia, e o jogo era perdido silenciosamente
    # (a exceção só aparecia como "Task exception was never retrieved").
    return DecisaoIA(decisao_ia, original, original, "SEM_PROTECAO", False, motivo)

def emoji_liga(liga: str) -> str:
    return {"PREMIUM": "🏆", "MODERADA": "📊", "NEUTRA": "⚪", "UNDER": "🟡", "PERIGOSA": "🔴"}.get(liga, "⚪")


def emoji_score(score: int) -> str:
    return "💎" if score >= 90 else "🎯"


def titulo_periodo(estrategia: str, tempo: int = 0) -> str:
    periodo = "SEGUNDO TEMPO" if int(tempo or 0) >= 46 else "PRIMEIRO TEMPO"
    if eh_confirmacao(estrategia):
        return f"ALFA - CONFIRMADO | {periodo}"
    return f"ALFA - AO VIVO | {periodo}"


def formatar_alerta_cliente(m: Metricas, score: int, alavancagem: bool = False) -> str:
    link = m.bet365 or ""
    linhas = [
        f"{emoji_score(score)} {titulo_periodo(m.estrategia, m.tempo)}",
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
    msg = "\n".join(linhas)
    if alavancagem:
        return "🔺 ALAVANCAGEM 🔺\n\n" + msg
    return msg


def formatar_alerta_canal_completo(m: Metricas, score: int, alavancagem: bool = False) -> str:
    link = m.bet365 or ""
    linhas = [
        nome_visual_auditoria(m),
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
    msg = "\n".join(linhas)
    if alavancagem:
        return "🔺 ALAVANCAGEM 🔺\n\n" + msg
    return msg


def agora_operacional_v11() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=V11_TZ_OFFSET_HORAS)


def janela_grupo_gratis_v11(dt: Optional[datetime] = None) -> Tuple[Optional[str], int]:
    dt = dt or agora_operacional_v11()
    minutos = dt.hour * 60 + dt.minute
    if 6 * 60 + 30 <= minutos < 12 * 60:
        return "J1", V11_FREE_J1_LIMITE
    if 12 * 60 <= minutos < 18 * 60:
        return "J2", V11_FREE_J2_LIMITE
    if 18 * 60 <= minutos < 23 * 60:
        return "J3", V11_FREE_J3_LIMITE
    return None, 0


def chave_contador_gratis_v11(janela: str, dt: Optional[datetime] = None) -> str:
    dt = dt or agora_operacional_v11()
    return f"{dt.strftime('%Y-%m-%d')}:{janela}"


def grupo_gratis_tem_vaga_v11() -> Tuple[bool, str]:
    janela, limite = janela_grupo_gratis_v11()
    if not janela:
        return False, "FORA_DAS_JANELAS"
    chave_janela = chave_contador_gratis_v11(janela)
    usados = int(v11_gratis_contadores.get(chave_janela, 0))
    if usados >= limite:
        return False, f"SEM_VAGA_{janela}_{usados}/{limite}"
    return True, f"COM_VAGA_{janela}_{usados}/{limite}"


def marcar_envio_gratis_v11(chave_jogo: str) -> None:
    janela, limite = janela_grupo_gratis_v11()
    if not janela:
        return
    chave_janela = chave_contador_gratis_v11(janela)
    v11_gratis_contadores[chave_janela] = int(v11_gratis_contadores.get(chave_janela, 0)) + 1
    v11_gratis_enviados_por_jogo[chave_jogo] = time.time()
    v17_salvar_estado()
    asyncio.create_task(v26_salvar_estado_telegram())


def jogo_em_cooldown_gratis_v11(chave_jogo: str) -> bool:
    ts = float(v11_gratis_enviados_por_jogo.get(chave_jogo, 0) or 0)
    return ts > 0 and time.time() - ts < V11_FREE_COOLDOWN_JOGO_HORAS * 3600


def favorito_vencendo_por_um_extremo_v18(m: Metricas, score: int = 0) -> Tuple[bool, str]:
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "SEM_FAVORITO"
    if lado_vencendo(m) != fav:
        return False, "FAVORITO_NAO_VENCE"
    if diferenca_placar(m) != 1:
        return False, "FAVORITO_VENCE_MAIS_DE_1"
    if fav != m.lado_pressionante:
        return False, "FAVORITO_VENCE_MAS_NAO_E_PRESSIONANTE"
    if m.ultimo_gol and m.ultimo_gol_lado == fav and minutos_desde_ultimo_gol(m) < V11_FREE_GOL_VANTAGEM_MIN:
        return False, f"GOL_VANTAGEM_RECENTE_{minutos_desde_ultimo_gol(m)}MIN"

    d = dados_lado(m, fav)
    ip = ip_lado(m, fav)
    ap_diff = ap_diff_lado(m, fav)

    pressao_absurda = (
        d["u5"] >= 5
        and d["u10"] >= 10
        and ap_diff >= 25
        and (ip["pico"] >= 24 or ip["c18"] >= 3 or ip["c22"] >= 2)
    )
    dominio_convertivel_absurdo = (
        d["rb"] >= 3
        or d["chance"] >= 12
        or d["xg"] >= 0.60
        or (d["rl"] >= 8 and d["cantos"] >= 4)
    )
    score_ok = score <= 0 or score >= 90

    if pressao_absurda and dominio_convertivel_absurdo and score_ok:
        return True, (
            f"FAVORITO_VENCE_1_MAS_EXTREMO_V18|score={score}|"
            f"ap_diff={ap_diff}|u5={d['u5']}|u10={d['u10']}|"
            f"rb={d['rb']}|chance={d['chance']}|xg={d['xg']:.2f}|ip={ip['pico']}"
        )

    return False, (
        f"FAVORITO_VENCE_1_SEM_EXTREMO_V18|score={score}|"
        f"pressao_absurda={pressao_absurda}|dominio_absurdo={dominio_convertivel_absurdo}|"
        f"ap_diff={ap_diff}|u5={d['u5']}|u10={d['u10']}|"
        f"rb={d['rb']}|chance={d['chance']}|xg={d['xg']:.2f}|ip={ip['pico']}"
    )


def apto_contexto_grupo_gratuito_v11(m: Metricas, score: int = 0) -> Tuple[bool, str]:
    if not HABILITAR_V11_GRUPO_GRATUITO:
        return False, "V11_GRUPO_GRATUITO_DESATIVADO"
    if eh_confirmacao(m.estrategia):
        return False, "CONF_FORA_DO_GRATIS"
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "SEM_FAVORITO"
    vencedor = lado_vencendo(m)

    if vencedor != fav and vencedor != "EMPATE":
        return True, "CENARIO_A_FAVORITO_PERDENDO"
    if vencedor == "EMPATE":
        return True, "CENARIO_B_FAVORITO_EMPATANDO"

    ok_c, motivo_c = favorito_vencendo_por_um_extremo_v18(m, score=score)
    if ok_c:
        return True, "CENARIO_C_EXTREMO_" + motivo_c
    return False, motivo_c


def liga_australiana_v11(m: Metricas) -> bool:
    texto = remover_acentos(f"{m.competicao} {m.jogo}").lower()
    return "australia" in texto


def leitura_australia_v11(m: Metricas) -> Tuple[str, str]:
    if not HABILITAR_V11_AUSTRALIA or not liga_australiana_v11(m):
        return "NAO_APLICA", "NAO_AUSTRALIA_OU_FLAG_OFF"

    dif = diferenca_placar(m)
    total = total_gols_placar(m)
    delta_gol = minutos_desde_ultimo_gol(m)
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return "CAUTELA", "AUSTRALIA_SEM_LADO_PRESSIONANTE"

    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    continuidade_forte = (
        d["u5"] >= 4
        or d["u10"] >= 8
        or ip["pico"] >= 20
        or ip["c18"] >= 2
        or d["rb"] >= 2
        or d["chance"] >= 9
    )
    continuidade_absurda = (
        d["u5"] >= 5
        and (ip["pico"] >= 24 or d["u10"] >= 10)
        and (d["rb"] >= 2 or d["chance"] >= 10 or d["cantos"] >= 2)
    )

    if dif >= 2 and delta_gol <= 5:
        if continuidade_absurda:
            return "CAUTELA", f"DIF={dif}_GOL_RECENTE_{delta_gol}MIN_MAS_CONTINUIDADE_ABSURDA"
        return "CAUTELA", f"DIF={dif}_GOL_RECENTE_{delta_gol}MIN_PRESSAO_PREMIADA"
    if dif >= 2 and 6 <= delta_gol <= 20 and continuidade_forte:
        return "OK", f"DIF={dif}_GOL_{delta_gol}MIN_JANELA_FORTE_CONTINUIDADE"
    if dif >= 6 and not continuidade_absurda:
        return "CAUTELA", f"DIF={dif}_CAUTELA_MAXIMA_SEM_CONTINUIDADE_ABSURDA"
    if dif >= 4 and not continuidade_forte:
        return "CAUTELA", f"DIF={dif}_SEM_PROVA_EXTRA_CONTINUIDADE"
    if total >= 4 and continuidade_forte:
        return "OK", f"PLACAR_ABERTO_TOTAL={total}_COM_CONTINUIDADE"
    return "OK", f"AUSTRALIA_SEM_RISCO_ESPECIAL_DIF={dif}_GOLDELTA={delta_gol}"


def liga_cautela_forte_v11(m: Metricas) -> bool:
    texto = remover_acentos(f"{m.competicao} {m.jogo}").lower()
    termos = (
        "brazil serie d", "brasil serie d", "brasileiro serie d",
        "bolivia liga de futbol prof", "ecuador liga pro",
        "south america copa sudamericana", "peru segunda division",
        "slovakia 3. liga",
    )
    return any(t in texto for t in termos)


def tres_pilares_estilo_v11(m: Metricas, lado: str) -> Tuple[bool, str]:
    if lado not in {"CASA", "FORA"}:
        return False, "SEM_LADO"
    d = dados_lado(m, lado)
    p1 = bool(m.odd_favorito and m.odd_favorito <= 1.55)
    p2 = bool(d["rb"] >= 2 or d["chance"] >= 9)
    p3 = bool(d["u5"] >= 4 and d["u10"] >= 8)
    ok = p1 and p2 and p3
    return ok, f"p1_odd={p1}|p2_rb_chance={p2}|p3_u5u10={p3}"


def selo_alavancagem_v11(m: Metricas, score: int) -> Tuple[bool, str]:
    if not HABILITAR_V11_ALAVANCAGEM:
        return False, "V11_ALAVANCAGEM_DESATIVADA"
    if (HABILITAR_V27_ALAVANCAGEM_SO_FT or HABILITAR_V28) and (not eh_ft(m.estrategia) or eh_confirmacao(m.estrategia)):
        return False, "ALAVANCAGEM_SOMENTE_FT"
    if eh_confirmacao(m.estrategia):
        return False, "CONF_NAO_RECEBE_ALAVANCAGEM"
    if HABILITAR_V27_ALAVANCAGEM_SO_FT and eh_ht(m.estrategia):
        return False, "V27_ALAVANCAGEM_HT_DESATIVADA_SOMENTE_FT"
    lado = m.lado_pressionante
    fav = m.lado_favorito
    if lado not in {"CASA", "FORA"} or fav not in {"CASA", "FORA"}:
        return False, "SEM_LADO_OU_FAVORITO"
    if fav != lado:
        return False, "FAVORITO_NAO_E_PRESSIONANTE"
    if not favorito_nao_vencendo(m):
        return False, "FAVORITO_JA_VENCE"
    if liga_cautela_forte_v11(m):
        return False, "LIGA_CAUTELA_FORTE"

    leitura_aus, motivo_aus = leitura_australia_v11(m)
    if leitura_aus == "CAUTELA":
        return False, "AUSTRALIA_CAUTELA_" + motivo_aus

    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    pilares_ok, pilares_motivo = tres_pilares_estilo_v11(m, lado)

    if eh_ht(m.estrategia):
        gc, gf = extrair_gols_placar(m.placar)
        if gc is None or gf is None:
            return False, "HT_PLACAR_INVALIDO"
        gols_fav = gc if fav == "CASA" else gf
        gols_adv = gf if fav == "CASA" else gc
        if gols_fav >= gols_adv:
            return False, f"HT_FAVORITO_NAO_PERDE | {m.placar}"
        massacre_ok, massacre_motivo = massacre_contextual_ht(m, lado, pos_gol_recente=False)
        if massacre_ok and pilares_ok:
            return True, "HT_MASSACRE_ALFA_FAV_PERDENDO|" + massacre_motivo + "|" + pilares_motivo
        return False, "HT_SEM_CONDICAO_ALAVANCAGEM|" + massacre_motivo + "|" + pilares_motivo

    pressao_extrema = pressao_extrema_lado(m, lado) or (d["u5"] >= 5 and d["u10"] >= 10 and ip["pico"] >= 22)
    dominio_convertivel = d["rb"] >= 2 or d["chance"] >= 10 or d["xg"] >= 0.50
    score_ok = score >= 86

    if score_ok and pressao_extrema and dominio_convertivel and pilares_ok:
        return True, f"FT_ALFA_FAVORITO_NAO_VENCE_PRESSAO_EXTREMA|{pilares_motivo}|score={score}"
    return False, (
        f"SEM_ALFA score_ok={score_ok}|pressao_extrema={pressao_extrema}|"
        f"dominio_convertivel={dominio_convertivel}|{pilares_motivo}|score={score}"
    )


def elegivel_grupo_gratuito_v11(m: Metricas, chave_jogo: str, score: int = 0) -> Tuple[bool, str]:
    ok_ctx, motivo_ctx = apto_contexto_grupo_gratuito_v11(m, score=score)
    if not ok_ctx:
        return False, motivo_ctx
    if jogo_em_cooldown_gratis_v11(chave_jogo):
        return False, "COOLDOWN_JOGO_GRATIS"
    leitura_aus, motivo_aus = leitura_australia_v11(m)
    m.australia_leitura = leitura_aus
    m.motivo_australia = motivo_aus
    if leitura_aus == "CAUTELA":
        return False, "AUSTRALIA_CAUTELA_" + motivo_aus
    if liga_cautela_forte_v11(m):
        return False, "LIGA_CAUTELA_FORTE_GRATIS"
    vaga, motivo_vaga = grupo_gratis_tem_vaga_v11()
    if not vaga:
        return False, motivo_vaga
    return True, motivo_ctx + "|" + motivo_vaga


def canal_auditoria(m: Metricas, aprovado: bool) -> str:
    # VOLUME_FT reprovado morre silencioso (CSV/log only) — regra já
    # documentada no projeto, mas nunca implementada aqui. Aprovado do
    # VOLUME_FT continua indo normal pro canal de aprovados.
    if not aprovado and eh_volume_ft(m.estrategia):
        return ""
    if eh_ht(m.estrategia):
        return AUDIT_HT_OK if aprovado else AUDIT_HT_NO
    return AUDIT_FT_OK if aprovado else AUDIT_FT_NO


def nome_visual_auditoria(m: Metricas) -> str:
    est = (m.estrategia or "").upper()
    bruto = remover_acentos(m.texto_bruto or "").upper()[:350]
    if "SNIPER" in est or "SNIPER" in bruto:
        return "🎯 SNIPER | 2T"
    if "ARCE" in est:
        return "⚡ ARCE HT | 1T"
    if "CHAMA" in est:
        return "🔥 CHAMA FT | 2T"
    if "VOLUME" in est:
        return "📊 VOLUME FT | 2T"
    if eh_ht(est):
        return "💎 ALFA HT | 1T"
    if eh_ft(est):
        return "💎 ALFA FT | 2T"
    return m.estrategia


async def enviar_auditoria(
    m: Metricas,
    score_py: int,
    score_ia: int,
    score_medio: int,
    aprovado: bool,
    motivo: str,
    ia_consultada: bool = True,
) -> None:
    canal = canal_auditoria(m, aprovado)
    if not canal:
        return
    status = "APROVADO" if aprovado else "REPROVADO"
    emoji = "✅" if aprovado else "❌"
    ia_txt = f"{score_ia}%" if ia_consultada else "N/A (bloqueado antes da IA)"
    medio_txt = f"{score_medio}%" if ia_consultada else f"{score_py}% (só PY, sem IA)"
    texto = (
        f"{emoji} {status} | {nome_visual_auditoria(m)}\n"
        f"🏟 {m.jogo}\n"
        f"⏱ {m.tempo}' | {m.placar}\n"
        f"📊 PY={score_py}% | IA={ia_txt} | MÉDIA={medio_txt}\n"
        f"🏆 Liga: {m.liga}\n"
        f"🧠 Valor: {m.valor_pos_evento_classe}"
        + (f" ({m.valor_pos_evento_motivo})" if m.valor_pos_evento_motivo else "")
        + f"\n📝 {motivo}"
    )
    await send_resiliente(canal, texto)


def garantir_csv() -> None:
    if CSV_PATH.exists():
        return
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()


CSV_FIELDS = [
    "data_hora", "jogo", "estrategia", "minuto", "placar", "mercado",
    "score_python", "decisao_ia", "ia_original", "ia_corrigida", "score_medio",
    "lado_favorito", "odd_favorito", "lado_pressionante", "ultimo_gol_lado",
    "valor_pos_evento_classe", "valor_pos_evento_motivo", "protecao_ia_ativa",
    "liga", "decisao_final", "motivo_bloqueio", "parser_confianca",
    "parser_observacoes", "fluxo_decisao", "fluxo_motivo",
    "teto_v9",
    "grupo_gratuito", "motivo_grupo_gratuito",
    "alavancagem", "motivo_alavancagem",
    "australia_leitura", "motivo_australia",
    "destino_final", "modo_teste",
    "previsao_over05_ht",
    "resultado_manual", "cornerpro", "bet365",
    "penalidade_v26",
    "detalhes_completos",
    "texto_bruto_raw",
]


# =========================================================
# SANITIZAÇÃO CSV (MUDANÇA 17) - INALTERADO
# =========================================================

def sanitizar_csv(texto: str) -> str:
    """Sanitiza valores para prevenir CSV Injection (Mudança 17)."""
    if not texto:
        return ""
    texto = str(texto)
    if texto.startswith(("=", "+", "-", "@")):
        return "'" + texto
    return texto


def _extrair_times_do_jogo(jogo: str) -> Tuple[str, str]:
    partes = re.split(r"\s+x\s+|\s+vs\s+", jogo or "", maxsplit=1)
    if len(partes) < 2:
        return (jogo or "", "")
    return (partes[0].strip(), partes[1].strip())


def montar_snapshot_auditor_v2(m: Metricas, decisao_py: DecisaoPython, decisao_ia: DecisaoIA,
                                score_medio: int, decisao_final: str, motivo: str) -> Dict[str, Any]:
    """Item 19 (29/06) — traduz o que o sistema já calcula pro formato que
    o Bot Auditor V2 espera. HONESTO: nem todo subcomponente de score que
    o Auditor V2 tenta guardar (pressure/ip/chance_gol/shots/rb/contexto/
    favoritismo) existe separado no nosso pipeline hoje — a maior parte
    do detalhe vive dentro de decisao_py.detalhes como dicionário solto,
    não como esses campos específicos. Preenche o que existe de verdade
    (total, base) e deixa o resto vazio em vez de inventar número.
    """
    home_team, away_team = _extrair_times_do_jogo(m.jogo)
    detalhes = decisao_py.detalhes or {}
    return {
        "fixture_id": None,
        "match_id": None,
        # Adicionado em 30/06 — link da CornerPro pro próprio jogo, que já
        # chega em todo alerta. Sem isso, o CornerProProvider do Auditor V2
        # não tem como saber qual URL buscar depois que o jogo termina.
        "cornerpro_url": m.cornerpro or None,
        "league": m.liga,
        "country": None,
        "season": None,
        "home_team": home_team,
        "away_team": away_team,
        "match_date": datetime.now().date().isoformat(),
        "match_time": None,
        "market": m.mercado,
        "period": "HT" if eh_ht(m.estrategia) else "FT",
        "decision_type": m.estrategia,
        "approved": decisao_final == "APROVADO",
        "decision_minute": m.tempo,
        "decision_reasons": [motivo] if motivo else [],
        "scores": {
            "total": score_medio,
            "base": decisao_py.score,
            # demais subcomponentes não existem separados no pipeline atual —
            # deixados de fora de propósito, ver docstring.
            "components": detalhes,
        },
        "context": {
            "favorite_team": "home" if m.lado_favorito == "CASA" else ("away" if m.lado_favorito == "FORA" else None),
            "favorite_odd": m.odd_favorito,
            "pressure_team": "home" if m.lado_pressionante == "CASA" else ("away" if m.lado_pressionante == "FORA" else None),
            "game_context": m.valor_pos_evento_classe or None,
        },
    }


def _registrar_no_auditor_v2(m: Metricas, decisao_py: DecisaoPython, decisao_ia: DecisaoIA,
                              score_medio: int, decisao_final: str, motivo: str) -> None:
    """Nunca deixa o Auditor V2 quebrar o resto do sistema — qualquer erro
    aqui só vira log, o ao vivo/pré-live segue intocado."""
    if _auditor_v2_manager is None:
        return
    try:
        snapshot = montar_snapshot_auditor_v2(m, decisao_py, decisao_ia, score_medio, decisao_final, motivo)
        _auditor_v2_manager.registrar_decisao_v2(snapshot)
    except Exception as e:
        log(f"⚠️ Auditor V2 falhou ao registrar decisão (não afeta o resto) | {type(e).__name__}: {e}")


def registrar_csv(m: Metricas, decisao_py: DecisaoPython, decisao_ia: DecisaoIA, score_medio: int, decisao_final: str, motivo: str) -> None:
    garantir_csv()
    row = {
        "data_hora": now_iso(),
        "jogo": sanitizar_csv(m.jogo),
        "estrategia": sanitizar_csv(m.estrategia),
        "minuto": m.tempo,
        "placar": sanitizar_csv(m.placar),
        "mercado": sanitizar_csv(m.mercado),
        "score_python": decisao_py.score,
        "decisao_ia": decisao_ia.decisao,
        "ia_original": decisao_ia.confianca_original,
        "ia_corrigida": decisao_ia.confianca_corrigida,
        "score_medio": score_medio,
        "lado_favorito": sanitizar_csv(m.lado_favorito),
        "odd_favorito": m.odd_favorito,
        "lado_pressionante": sanitizar_csv(m.lado_pressionante),
        "ultimo_gol_lado": sanitizar_csv(m.ultimo_gol_lado),
        "valor_pos_evento_classe": sanitizar_csv(m.valor_pos_evento_classe),
        "valor_pos_evento_motivo": sanitizar_csv(m.valor_pos_evento_motivo),
        "protecao_ia_ativa": m.protecao_ia_ativa,
        "liga": sanitizar_csv(m.liga),
        "decisao_final": sanitizar_csv(decisao_final),
        "motivo_bloqueio": sanitizar_csv(motivo),
        "parser_confianca": m.parser_confianca,
        "parser_observacoes": " | ".join(sanitizar_csv(x) for x in m.parser_observacoes),
        "fluxo_decisao": sanitizar_csv(m.fluxo_decisao),
        "fluxo_motivo": sanitizar_csv(m.fluxo_motivo),
        "teto_v9": decisao_py.detalhes.get("teto_v9", ""),
        "grupo_gratuito": sanitizar_csv(m.grupo_gratuito),
        "motivo_grupo_gratuito": sanitizar_csv(m.motivo_grupo_gratuito),
        "alavancagem": sanitizar_csv(m.alavancagem),
        "motivo_alavancagem": sanitizar_csv(m.motivo_alavancagem),
        "australia_leitura": sanitizar_csv(m.australia_leitura),
        "motivo_australia": sanitizar_csv(m.motivo_australia),
        "destino_final": sanitizar_csv(m.destino_final),
        "modo_teste": str(MODO_TESTE),
        "previsao_over05_ht": m.previsao_over05_ht,
        "resultado_manual": "",
        "cornerpro": m.cornerpro,
        "bet365": m.bet365,
        "penalidade_v26": getattr(m, 'penalidade_v26_fav_nao_press', 0),
        "detalhes_completos": sanitizar_csv(
            json.dumps(decisao_py.detalhes, ensure_ascii=False, default=str)
            if decisao_py.detalhes else ""
        ),
        "texto_bruto_raw": sanitizar_csv((m.texto_bruto or "")[:1500]),
    }
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)

    _registrar_no_auditor_v2(m, decisao_py, decisao_ia, score_medio, decisao_final, motivo)


def gerar_resumo_resultados() -> str:
    """Item 10 (28/06): resumo de acerto por estratégia, usando o que já
    foi auditado manualmente (coluna resultado_manual) no CSV existente.

    IMPORTANTE — limite honesto: isso é um resumo do que já aconteceu,
    não um backtest de fórmula nova. Backtest de verdade (rodar a fórmula
    nova contra jogos antigos e comparar) precisa do texto bruto de cada
    alerta — por isso a coluna texto_bruto_raw foi adicionada agora no
    CSV. A partir de hoje, dá pra fazer esse backtest real; pra alertas
    anteriores a essa mudança, esse dado não existe.
    """
    if not CSV_PATH.exists():
        return "📊 Ainda não há CSV de auditoria pra resumir."

    contagem: Dict[str, Dict[str, int]] = {}
    total_linhas = 0
    total_com_resultado = 0

    try:
        with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_linhas += 1
                estrategia = row.get("estrategia") or "DESCONHECIDA"
                resultado = (row.get("resultado_manual") or "").strip().lower()
                if not resultado:
                    continue
                total_com_resultado += 1
                contagem.setdefault(estrategia, {"green": 0, "red": 0})
                if "green" in resultado or "verde" in resultado:
                    contagem[estrategia]["green"] += 1
                elif "red" in resultado or "vermelho" in resultado:
                    contagem[estrategia]["red"] += 1
    except Exception as e:
        return f"⚠️ Erro lendo CSV pra resumo: {type(e).__name__}: {e}"

    if not contagem:
        return (
            f"📊 RESUMO DE RESULTADOS\n"
            f"Total de linhas no CSV: {total_linhas}\n"
            f"Nenhuma linha tem resultado_manual preenchido ainda — "
            f"audite no HTML turbo antes de pedir resumo."
        )

    linhas = [f"📊 RESUMO DE RESULTADOS (auditado manualmente)", f"Total no CSV: {total_linhas} | Com resultado: {total_com_resultado}", ""]
    for estrategia, c in sorted(contagem.items(), key=lambda kv: -(kv[1]["green"] + kv[1]["red"])):
        total_est = c["green"] + c["red"]
        if total_est == 0:
            continue
        pct = round(100 * c["green"] / total_est, 1)
        linhas.append(f"• {estrategia}: {c['green']}G / {c['red']}R ({total_est} jogos) — {pct}% acerto")

    return "\n".join(linhas)


# =========================================================
# ENVIO RESILIENTE COM BACKOFF (MUDANÇA 19) - INALTERADO
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
            espera = int(getattr(e, "seconds", 10)) * (tentativa + 1)
            log(f"⚠️ FloodWait {espera}s | tentativa {tentativa}/{max_tentativas}")
            await asyncio.sleep(espera)
        except TypeNotFoundError:
            log(f"⚠️ TypeNotFoundError tent={tentativa}/{max_tentativas} | reconectando")
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(2)
            await client.connect()
        except Exception as e:
            log(f"⚠️ Erro tent={tentativa}/{max_tentativas} | {type(e).__name__}: {e}")
            await asyncio.sleep(2 ** tentativa)
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
            log(f"❌ Erro trabalhador_fila_envio: {type(e).__name__}: {e}")
            log(traceback.format_exc())
        finally:
            fila_envio.task_done()


async def enfileirar_envio(canal: str, mensagem: str, parse_mode: Optional[str] = None) -> None:
    try:
        await fila_envio.put((canal, mensagem, parse_mode))
    except asyncio.QueueFull:
        log("❌ FILA CHEIA — alerta descartado por segurança")


# =========================================================
# V007 — FLUXO FT CHAMA → ESPERA → CONFIRMAÇÃO (LIVE - INALTERADO)
# =========================================================

def deve_aguardar_confirmacao_ft(m: Metricas) -> Tuple[bool, str]:
    if not eh_ft(m.estrategia) or eh_confirmacao(m.estrategia):
        return False, "NAO_E_FT_GATILHO"
    if not (gol_recente_do_pressionante(m, janela=5) and ultimo_gol_deixou_lado_vencendo(m)):
        return False, "SEM_GOL_RECENTE_PRESSIONANTE_DEIXANDO_VENCENDO"
    if pressao_pos_gol_esfriou(m, m.lado_pressionante):
        return True, (
            f"GOL_RECENTE_PRESSIONANTE_DEIXOU_VENCENDO_E_PRESSAO_ESFRIOU | "
            f"ultimo={m.ultimo_gol}' {m.ultimo_gol_lado} | placar={m.placar} | press={m.lado_pressionante}"
        )
    return True, (
        f"GOL_RECENTE_PRESSIONANTE_DEIXOU_VENCENDO | "
        f"ultimo={m.ultimo_gol}' {m.ultimo_gol_lado} | placar={m.placar} | press={m.lado_pressionante}"
    )


# =========================================================
# dc01_1_zebra_ameaca_real COM 3 PROVAS (MUDANÇA 6) - INALTERADO
# =========================================================

def dc01_1_zebra_ameaca_real(m: "Metricas", zebra: str) -> Tuple[bool, str]:
    """DC01.1 — mede se a zebra está viva (Mudança 6: exige 3 provas)."""
    if zebra not in {"CASA", "FORA"}:
        return False, "ZEBRA_INVALIDA"
    d = dados_lado(m, zebra)
    ip = ip_lado(m, zebra)

    provas = []
    motivos = []

    if d["rb"] >= 1:
        provas.append(1.5)
        motivos.append(f"RB={d['rb']}")
    if d["rl"] >= 2:
        provas.append(1.0)
        motivos.append(f"RL={d['rl']}")
    if d["cantos"] >= 1:
        provas.append(0.5)
        motivos.append(f"CANTOS={d['cantos']}")
    if d["u5"] >= 2 or d["u10"] >= 4:
        provas.append(0.8)
        motivos.append(f"U5={d['u5']}_U10={d['u10']}")
    if d["chance"] >= 5 or d["xg"] >= 0.20:
        provas.append(1.0)
        motivos.append(f"CHANCE={d['chance']}_XG={d['xg']:.2f}")
    if ip["pico"] >= 16 or ip["c15"] >= 1:
        provas.append(0.7)
        motivos.append(f"IP={ip['pico']}_C15={ip['c15']}")

    total = sum(provas)
    ok = total >= 3.0

    return ok, f"ZEBRA_AMEACA_PROVAS={total:.1f}/3|" + "|".join(motivos)


def confirmacao_isolada_valida(m: Metricas) -> Tuple[bool, str]:
    if not eh_confirmacao(m.estrategia) or not eh_ft(m.estrategia):
        return False, "NAO_E_FT_CONFIRMACAO"
    if int(m.tempo or 0) > 82:
        return False, f"CONFIRMACAO_FORA_JANELA_{m.tempo}"
    fav = m.lado_favorito
    lado = m.lado_pressionante
    if fav not in {"CASA", "FORA"} or lado not in {"CASA", "FORA"}:
        return False, "SEM_FAVORITO_OU_PRESSIONANTE"

    if HABILITAR_DC01_2_CONF01:
        diff_iso = diferenca_placar(m)
        vence_iso = lado_vencendo(m)
        if vence_iso == fav and diff_iso == 1:
            zebra_iso = lado_oposto(fav)
            zebra_viva_iso, zebra_motivo_iso = dc01_1_zebra_ameaca_real(m, zebra_iso)
            if not zebra_viva_iso:
                return False, f"CONF01_FAVORITO_VENCE_1_ZEBRA_MORTA | {zebra_motivo_iso}"

    bloqueio_v29_1, motivo_v29_1 = v29_trava_conf_favorito_vencendo_gol_recente(m)
    if bloqueio_v29_1:
        return False, motivo_v29_1

    bloqueio_v29_2, motivo_v29_2 = v29_trava_conf_placar_largo(m)
    if bloqueio_v29_2:
        return False, motivo_v29_2

    if fav == lado and favorito_nao_vencendo(m) and pressao_viva_lado(m, lado) and consequencia_minima_emocional(m, lado):
        return True, "CONF_ISOLADA_FAVORITO_NAO_VENCE_COM_PRESSAO"

    if gol_recente(m, janela=6) and m.ultimo_gol_lado != lado and fav == lado and pressao_viva_lado(m, lado):
        return True, "CONF_ISOLADA_GOL_CONTRA_FLUXO_GEROU_URGENCIA"

    return False, "CONF_ISOLADA_SEM_URGENCIA_NOVA"


def comparar_alertas_confirmacao(old: Metricas, novo: Metricas) -> Tuple[bool, str, Dict[str, Any]]:
    detalhes: Dict[str, Any] = {}
    lado = old.lado_pressionante if old.lado_pressionante in {"CASA", "FORA"} else novo.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "CONF_SEM_LADO_COMPARAVEL", detalhes

    if gol_recente_pressionante_resolveu_confirmacao(novo, janela=5):
        return False, (
            f"CONF_GOL_RECENTE_PRESSIONANTE_RESOLVEU | ultimo={novo.ultimo_gol}' {novo.ultimo_gol_lado} | "
            f"placar={novo.placar} | press={novo.lado_pressionante}"
        ), detalhes

    bloqueio_v29_1, motivo_v29_1 = v29_trava_conf_favorito_vencendo_gol_recente(novo)
    if bloqueio_v29_1:
        return False, motivo_v29_1, detalhes

    bloqueio_v29_2, motivo_v29_2 = v29_trava_conf_placar_largo(novo)
    if bloqueio_v29_2:
        return False, motivo_v29_2, detalhes

    if HABILITAR_DC01_2_CONF01:
        fav_conf = novo.lado_favorito
        if fav_conf in {"CASA", "FORA"}:
            diff_conf = diferenca_placar(novo)
            vence_conf = lado_vencendo(novo)
            if vence_conf == fav_conf and diff_conf == 1:
                zebra_conf = lado_oposto(fav_conf)
                zebra_viva_conf, zebra_motivo_conf = dc01_1_zebra_ameaca_real(novo, zebra_conf)
                if not zebra_viva_conf:
                    return False, (
                        f"CONF01_FAVORITO_VENCE_1_ZEBRA_MORTA | fav={fav_conf} | placar={novo.placar} | {zebra_motivo_conf}"
                    ), detalhes

    campos = [
        ("ataques_perigosos", "AP"),
        ("ultimos5", "U5"),
        ("ultimos10", "U10"),
        ("chance_golo", "CHANCE"),
        ("remates_baliza", "RB"),
        ("remates_lado", "RL"),
        ("cantos", "CANTOS"),
    ]
    melhoras = 0
    quedas_fortes = 0
    partes = []
    for campo, nome in campos:
        atual = valor_lado(novo, campo, lado)
        anterior = valor_lado(old, campo, lado)
        delta = atual - anterior
        detalhes[f"delta_{nome.lower()}"] = delta
        partes.append(f"{nome}:{anterior}->{atual}({delta:+.1f})")
        if delta > 0:
            melhoras += 1
        if nome in {"AP", "U5", "U10", "CHANCE"} and delta < -1:
            quedas_fortes += 1

    ip_atual = ip_lado(novo, lado)
    ip_old = ip_lado(old, lado)
    ip_manteve = ip_atual["pico"] >= ip_old["pico"] or ip_atual["c18"] >= ip_old["c18"] or ip_atual["c22"] >= ip_old["c22"]
    if ip_manteve:
        melhoras += 1
    detalhes["ip_pico_old"] = ip_old["pico"]
    detalhes["ip_pico_novo"] = ip_atual["pico"]
    detalhes["melhoras"] = melhoras
    detalhes["quedas_fortes"] = quedas_fortes

    if quedas_fortes >= 2:
        return False, "CONFIRMACAO_MOSTROU_QUEDA_DE_PRESSAO | " + " | ".join(partes), detalhes

    pressao_ok = pressao_viva_lado(novo, lado)
    consequencia_ok = consequencia_minima_emocional(novo, lado) or consequencia_real_lado(novo, lado)
    if melhoras >= 3 and pressao_ok and consequencia_ok:
        return True, "CONFIRMACAO_MANTEVE_OU_MELHOROU_PRESSAO | " + " | ".join(partes), detalhes

    return False, "CONFIRMACAO_FRACA_SEM_MELHORA_SUFFICIENTE | " + " | ".join(partes), detalhes


async def cancelar_pendente_confirmacao_ft_depois(chave: str, timeout_segundos: int) -> None:
    try:
        await asyncio.sleep(timeout_segundos)
        item = pendentes_confirmacao_ft.pop(chave, None)
        tarefas_timeout_confirmacao_ft.pop(chave, None)
        if not item:
            return
        old: Metricas = item.get("metricas")
        if old:
            old.fluxo_decisao = "CANCELADO_SEM_CONFIRMACAO"
            old.fluxo_motivo = item.get("motivo", "SEM_MOTIVO")
            log(f"❌ CANCELADO_SEM_FT_CONFIRMACAO | {old.jogo} | {old.tempo}' | {old.fluxo_motivo}")
    except asyncio.CancelledError:
        return
    except Exception as e:
        log(f"⚠️ Erro timeout confirmação FT | {chave} | {type(e).__name__}: {e}")


def timeout_confirmacao_segundos(m: Metricas) -> int:
    tempo = int(m.tempo or 0)
    if tempo >= 82:
        return 90
    return max(120, min(600, (82 - tempo + 1) * 60))


# =========================================================
# DC01 — CHAMA_FT PLACAR ELÁSTICO (LIVE - INALTERADO)
# =========================================================

def dc01_chama_placar_elastico(m: "Metricas") -> Tuple[str, str]:
    if not HABILITAR_DC01_CHAMA_PLACAR_ELASTICO:
        return "NAO_APLICA", "DC01_CHAMA_PLACAR_FLAG_OFF"
    if m.estrategia != "CHAMA_FT":
        return "NAO_APLICA", f"DC01_NAO_CHAMA_FT_{m.estrategia}"
    if not eh_ft(m.estrategia) or eh_confirmacao(m.estrategia):
        return "NAO_APLICA", "DC01_NAO_FT_OU_CONFIRMACAO"

    dif = diferenca_placar(m)
    if dif < DC01_CHAMA_DIF_MIN_PLACAR_ELASTICO:
        return "NAO_APLICA", f"DC01_DIF_{dif}_ABAIXO_{DC01_CHAMA_DIF_MIN_PLACAR_ELASTICO}"

    vencedor = lado_vencendo(m)
    perdedor = lado_perdendo(m)
    lado = m.lado_pressionante
    if vencedor not in {"CASA", "FORA"} or perdedor not in {"CASA", "FORA"}:
        return "NAO_APLICA", "DC01_PLACAR_SEM_VENCEDOR_PERDEDOR"
    if lado not in {"CASA", "FORA"}:
        return "BLOQUEAR", "DC01_CHAMA_PLACAR_ELASTICO_SEM_LADO_PRESSIONANTE"

    d_perd = dados_lado(m, perdedor)
    d_venc = dados_lado(m, vencedor)
    ip_perd = ip_lado(m, perdedor)
    ip_venc = ip_lado(m, vencedor)

    perdedor_ja_marcou = gols_lado(m, perdedor)[0] > 0
    delta_gol = minutos_desde_ultimo_gol(m)
    gol_recente_vencedor = bool(m.ultimo_gol_lado == vencedor and -1 <= delta_gol <= 8)

    perdedor_pressao_forte = (
        (ip_perd["c18"] >= 3 or ip_perd["c22"] >= 2 or ip_perd["pico"] >= 24)
        and (d_perd["u5"] >= 3 or d_perd["u10"] >= 7)
        and (d_perd["chance"] >= 6 or d_perd["rb"] >= 1 or d_perd["rl"] >= 2 or d_perd["xg"] >= 0.20)
    )
    perdedor_pressao_absurda = (
        (ip_perd["c18"] >= 5 or ip_perd["c22"] >= 3 or ip_perd["pico"] >= 28)
        and (d_perd["u5"] >= 5 or d_perd["u10"] >= 10)
    )

    vencedor_vivo = (
        d_venc["u5"] >= 1
        or d_venc["u10"] >= 3
        or d_venc["rb"] >= 1
        or d_venc["rl"] >= 2
        or d_venc["chance"] >= 5
        or d_venc["xg"] >= 0.20
        or ip_venc["pico"] >= 15
        or ip_venc["c15"] >= 1
    )
    vencedor_extremo = pressao_extrema_lado(m, vencedor) or (
        (ip_venc["pico"] >= 24 or ip_venc["c18"] >= 2)
        and (d_venc["u5"] >= 4 or d_venc["u10"] >= 8)
        and (d_venc["rb"] >= 2 or d_venc["chance"] >= 10 or d_venc["rl"] >= 5)
    )

    ambos_mortos = (
        d_perd["u5"] <= 1 and d_perd["u10"] <= 3 and d_perd["rb"] == 0 and d_perd["rl"] <= 1 and ip_perd["pico"] < 15
        and d_venc["u5"] <= 1 and d_venc["u10"] <= 3 and d_venc["rb"] == 0 and d_venc["rl"] <= 1 and ip_venc["pico"] < 15
    )

    resumo = (
        f"dif={dif}|press={lado}|vencedor={vencedor}|perdedor={perdedor}|"
        f"perd_u5={d_perd['u5']} u10={d_perd['u10']} rb={d_perd['rb']} rl={d_perd['rl']} chance={d_perd['chance']} xg={d_perd['xg']:.2f} "
        f"ip={ip_perd['pico']}/c18={ip_perd['c18']}/c22={ip_perd['c22']}|"
        f"venc_u5={d_venc['u5']} u10={d_venc['u10']} rb={d_venc['rb']} rl={d_venc['rl']} chance={d_venc['chance']} xg={d_venc['xg']:.2f} "
        f"ip={ip_venc['pico']}/c18={ip_venc['c18']}|"
        f"perd_marcou={perdedor_ja_marcou}|gol_rec_venc={gol_recente_vencedor}|delta_gol={delta_gol}"
    )

    if ambos_mortos:
        return "BLOQUEAR", "DC01_CHAMA_PLACAR_ELASTICO_MORTO | " + resumo

    massacre_ok, massacre_motivo = v29_massacre_absoluto_vencedor(m)
    if massacre_ok:
        return "APROVAR", f"DC01_MASSACRE_VENCEDOR_ABSOLUTO | {massacre_motivo} | {resumo}"

    if lado == perdedor:
        if perdedor_pressao_absurda and vencedor_vivo:
            return "APROVAR", "DC01_CHAMA_PLACAR_ELASTICO_CAOS_PERDEDOR_ABSURDO_VENCEDOR_VIVO | " + resumo
        if perdedor_pressao_forte and vencedor_vivo:
            return "APROVAR", "DC01_CHAMA_PLACAR_ELASTICO_CAOS_PERDEDOR_FORTE_VENCEDOR_VIVO | " + resumo
        if HABILITAR_DC01_4_CHAMA_ELASTICO_PERDEDOR_DIRETO:
            if perdedor_pressao_forte or (vencedor_vivo and perdedor_ja_marcou):
                return "APROVAR", "DC01_CHAMA_PLACAR_ELASTICO_DC014_PERDEDOR_DIRETO | " + resumo
            if vencedor_vivo or perdedor_ja_marcou:
                return "CONFIRMAR", "DC01_CHAMA_PLACAR_ELASTICO_MEIO_TERMO_PERDEDOR | " + resumo
        else:
            if perdedor_pressao_forte or vencedor_vivo or perdedor_ja_marcou:
                return "CONFIRMAR", "DC01_CHAMA_PLACAR_ELASTICO_MEIO_TERMO_PERDEDOR | " + resumo
        return "BLOQUEAR", "DC01_CHAMA_PLACAR_ELASTICO_PERDEDOR_PRESSAO_MORNA_VENCEDOR_MORTO | " + resumo

    if lado == vencedor:
        if vencedor_extremo and not gol_recente_vencedor and d_venc["u5"] >= 4 and d_venc["u10"] >= 8:
            return "APROVAR", "DC01_CHAMA_PLACAR_ELASTICO_VENCEDOR_EXTREMO_SEM_GOL_RECENTE | " + resumo
        if vencedor_vivo or vencedor_extremo:
            return "CONFIRMAR", "DC01_CHAMA_PLACAR_ELASTICO_VENCEDOR_VIVO_EXIGE_CONFIRMACAO | " + resumo
        return "BLOQUEAR", "DC01_CHAMA_PLACAR_ELASTICO_VENCEDOR_SATISFEITO | " + resumo

    return "CONFIRMAR", "DC01_CHAMA_PLACAR_ELASTICO_LADO_DIVERGENTE_MEIO_TERMO | " + resumo


# =========================================================
# V29 — FUNÇÕES DE APOIO ÀS NOVAS TRAVAS (LIVE - INALTERADO)
# =========================================================

def v29_massacre_absoluto_vencedor(m: "Metricas") -> Tuple[bool, str]:
    if not HABILITAR_V29_MELHORIAS:
        return False, "V29_MELHORIAS_OFF"

    vencedor = lado_vencendo(m)
    if vencedor not in {"CASA", "FORA"}:
        return False, "V29_SEM_VENCEDOR"

    dif = diferenca_placar(m)
    if dif < V29_MASSACRE_DIFF_MIN:
        return False, f"V29_DIF_{dif}_ABAIXO_{V29_MASSACRE_DIFF_MIN}"

    if m.ultimo_gol and int(m.ultimo_gol) >= V29_MASSACRE_GOL_ANTES_MIN:
        return False, f"V29_GOL_RECENTE_{m.ultimo_gol}_NAO_ANTES_MIN{V29_MASSACRE_GOL_ANTES_MIN}"

    d_venc = dados_lado(m, vencedor)
    ip_venc = ip_lado(m, vencedor)

    if ip_venc["pico"] < V29_MASSACRE_IP_MIN:
        return False, f"V29_IP_PICO_{ip_venc['pico']:.1f}_ABAIXO_{V29_MASSACRE_IP_MIN}"
    if d_venc["rb"] < V29_MASSACRE_RB_MIN:
        return False, f"V29_RB_{d_venc['rb']}_ABAIXO_{V29_MASSACRE_RB_MIN}"
    if d_venc["chance"] < V29_MASSACRE_CHANCE_MIN:
        return False, f"V29_CHANCE_{d_venc['chance']}_ABAIXO_{V29_MASSACRE_CHANCE_MIN}"

    return True, (
        f"V29_DC01_MASSACRE_VENCEDOR_ABSOLUTO | venc={vencedor} dif={dif} "
        f"ip_pico={ip_venc['pico']:.1f} rb={d_venc['rb']} chance={d_venc['chance']} "
        f"ultimo_gol={m.ultimo_gol}'"
    )


def v29_trava_conf_favorito_vencendo_gol_recente(novo: "Metricas") -> Tuple[bool, str]:
    if not HABILITAR_V29_MELHORIAS:
        return False, "V29_MELHORIAS_OFF"

    fav = novo.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "V29_SEM_FAVORITO"

    vencendo = lado_vencendo(novo)
    if vencendo != fav:
        return False, "V29_FAV_NAO_VENCENDO"

    if novo.ultimo_gol_lado != fav:
        return False, "V29_GOL_NAO_DO_FAVORITO"

    delta = minutos_desde_ultimo_gol(novo)
    if delta > V29_CONF_FAV_VENCENDO_GOL_RECENTE_JANELA:
        return False, f"V29_GOL_HA_{delta}MIN_FORA_JANELA_{V29_CONF_FAV_VENCENDO_GOL_RECENTE_JANELA}"

    d_fav = dados_lado(novo, fav)
    xg_total = get_xg_total(novo)

    if d_fav["u10"] >= V29_CONF_FAV_VENCENDO_U10_MIN:
        return False, f"V29_U10_{d_fav['u10']}_JUSTIFICA_CONF"
    if d_fav["rb"] >= V29_CONF_FAV_VENCENDO_RB_MIN:
        return False, f"V29_RB_{d_fav['rb']}_JUSTIFICA_CONF"
    if xg_total >= V29_CONF_FAV_VENCENDO_XG_MIN:
        return False, f"V29_XG_{xg_total:.2f}_JUSTIFICA_CONF"

    return True, (
        f"V29_TRAVA_CONF_FAV_VENCENDO_GOL_RECENTE | fav={fav} gol_ha={delta}min "
        f"u10={d_fav['u10']} rb={d_fav['rb']} xg={xg_total:.2f}"
    )


def v29_trava_conf_placar_largo(novo: "Metricas") -> Tuple[bool, str]:
    """Item 2 — bloqueia confirmação quando favorito está perdendo por 3+ gols.
    Mudança 14: exceção para pressão extrema.
    """
    if not HABILITAR_V29_MELHORIAS:
        return False, "V29_MELHORIAS_OFF"

    fav = novo.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "V29_SEM_FAVORITO"

    vencendo = lado_vencendo(novo)
    if vencendo == fav or vencendo == "EMPATE":
        return False, "V29_FAV_NAO_PERDENDO"

    dif = diferenca_placar(novo)
    if dif < V29_CONF_PLACAR_LARGO_MIN_DIFF:
        return False, f"V29_DIF_{dif}_ABAIXO_{V29_CONF_PLACAR_LARGO_MIN_DIFF}"

    # Mudança 14: Exceção para pressão extrema do favorito
    d_fav = dados_lado(novo, fav)
    ip_fav = ip_lado(novo, fav)
    if d_fav["u5"] >= 4 and d_fav["u10"] >= 8 and ip_fav["pico"] >= 24:
        return False, f"V29_EXCECAO_PRESSAO_EXTREMA | fav={fav} u5={d_fav['u5']} u10={d_fav['u10']} ip={ip_fav['pico']}"

    return True, f"V29_TRAVA_CONF_PLACAR_LARGO | fav={fav} perdendo_por={dif} placar={novo.placar}"


def v29_trava_decaimento_pressao_2p(m: "Metricas") -> Tuple[bool, str]:
    """Item 10 — bloqueia CHAMA_FT quando favorito vencendo mas relaxou no 2ºP.
    Mudança 5: usa U5/U10 como proxy de AP do 2ºP.
    """
    if not HABILITAR_V29_MELHORIAS:
        return False, "V29_MELHORIAS_OFF"

    if m.estrategia != "CHAMA_FT":
        return False, "V29_NAO_CHAMA_FT"

    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "V29_SEM_FAVORITO"

    vencendo = lado_vencendo(m)
    if vencendo != fav:
        return False, "V29_FAV_NAO_VENCENDO"

    adv = lado_oposto(fav)
    d_fav = dados_lado(m, fav)
    d_adv = dados_lado(m, adv)

    u5_fav = d_fav["u5"]
    u10_fav = d_fav["u10"]
    u5_adv = d_adv["u5"]
    u10_adv = d_adv["u10"]

    # Mudança 5: Usar U5/U10 como proxy do 2ºP
    if not (u5_fav < u5_adv and u10_fav < u10_adv):
        return False, f"V29_FAV_RECENTES_U5={u5_fav}>{u5_adv}_U10={u10_fav}>{u10_adv}_SEM_DECAIMENTO"

    xg_total = get_xg_total(m)

    if xg_total >= V29_DECAIMENTO_XG_MIN and u10_fav >= V29_DECAIMENTO_U10_MIN:
        return False, f"V29_XG_{xg_total:.2f}_U10={u10_fav}_JUSTIFICA"

    return True, (
        f"V29_TRAVA_DECAIMENTO_2P | fav={fav} u5={u5_fav}x{u5_adv} u10={u10_fav}x{u10_adv} xg={xg_total:.2f}"
    )


def v29_trava_rb_zero_liga_fraca(m: "Metricas") -> Tuple[bool, str]:
    if not HABILITAR_V29_MELHORIAS:
        return False, "V29_MELHORIAS_OFF"

    if m.liga not in {"NEUTRA", "PERIGOSA"}:
        return False, f"V29_LIGA_{m.liga}_NAO_APLICA_RB_ZERO"

    massacre_ok, _ = v29_massacre_absoluto_vencedor(m)
    if massacre_ok:
        return False, "V29_MASSACRE_ABSOLUTO_EXCECAO_RB_ZERO"

    vencendo = lado_vencendo(m)
    if vencendo not in {"CASA", "FORA"}:
        return False, "V29_SEM_VENCEDOR_PARA_VERIFICAR_PERDEDOR"

    perdedor = lado_oposto(vencendo)
    rb_perdedor = valor_lado(m, "remates_baliza", perdedor)

    if rb_perdedor > 0:
        return False, f"V29_PERDEDOR_RB={rb_perdedor}_OK"

    return True, f"V29_TRAVA_RB_ZERO_PERDEDOR | liga={m.liga} perdedor={perdedor} rb=0"


def v29_cooldown_universal_aprovado(chave_jogo: str) -> bool:
    if not HABILITAR_V29_MELHORIAS:
        return False
    ts = v29_aprovados_por_jogo.get(chave_jogo, 0)
    if not ts:
        return False
    return time.time() - ts < 86400


def v29_marcar_aprovado_universal(chave_jogo: str) -> None:
    v29_aprovados_por_jogo[chave_jogo] = time.time()
    v29_salvar_cooldowns()


# =========================================================
# SNIPER V2 — LEITURA SEPARADA FT (LIVE - INALTERADO)
# =========================================================

def sniper_contexto_placar(m: "Metricas") -> Tuple[str, str]:
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return "SEM_FAVORITO", "SNIPER_SEM_FAVORITO"
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return "PLACAR_INVALIDO", "SNIPER_PLACAR_INVALIDO"
    gols_fav = gc if fav == "CASA" else gf
    gols_adv = gf if fav == "CASA" else gc
    diff = gols_fav - gols_adv
    if diff < 0:
        return "VALUE", f"FAVORITO_PERDENDO_{gols_fav}x{gols_adv}"
    if diff == 0:
        return "VALUE", f"FAVORITO_EMPATANDO_{gols_fav}x{gols_adv}"
    if diff == 1:
        return "SAFE", f"FAVORITO_VENCENDO_1_{gols_fav}x{gols_adv}"
    if diff == 2:
        return "RISCO", f"FAVORITO_VENCENDO_2_{gols_fav}x{gols_adv}"
    return "MORTO", f"FAVORITO_SATISFEITO_DIFF_{diff}_{gols_fav}x{gols_adv}"


def sniper_pressao_premiada(m: "Metricas") -> Tuple[bool, str]:
    if not m.ultimo_gol or m.ultimo_gol_lado not in {"CASA", "FORA"}:
        return False, "SNIPER_SEM_GOL_RECENTE"
    delta = minutos_desde_ultimo_gol(m)
    if delta < -1 or delta > 8:
        return False, f"SNIPER_GOL_FORA_JANELA_{delta}MIN"
    fav = m.lado_favorito
    press = m.lado_pressionante
    if fav not in {"CASA", "FORA"}:
        return False, "SNIPER_SEM_FAVORITO"
    if m.ultimo_gol_lado == fav and (ultimo_gol_deixou_lado_vencendo(m) or ultimo_gol_aumentou_vantagem(m)):
        return True, f"SNIPER_PRESSAO_PREMIADA_FAV_GOL_{delta}MIN | {m.placar}"
    if press in {"CASA", "FORA"} and m.ultimo_gol_lado == press and (ultimo_gol_deixou_lado_vencendo(m) or ultimo_gol_aumentou_vantagem(m)):
        return True, f"SNIPER_PRESSAO_PREMIADA_PRESS_GOL_{delta}MIN | {m.placar}"
    return False, f"SNIPER_GOL_RECENTE_NAO_PREMIOU_PRESSAO_{delta}MIN"


def sniper_gol_contra_fluxo(m: "Metricas") -> Tuple[bool, str]:
    if not m.ultimo_gol or m.ultimo_gol_lado not in {"CASA", "FORA"}:
        return False, "SNIPER_SEM_GOL"
    delta = minutos_desde_ultimo_gol(m)
    if delta < -1 or delta > 8:
        return False, f"SNIPER_GOL_FORA_JANELA_{delta}MIN"
    fav = m.lado_favorito
    press = m.lado_pressionante
    if fav in {"CASA", "FORA"} and press == fav and m.ultimo_gol_lado != fav:
        if pressao_viva_lado(m, fav):
            return True, f"SNIPER_GOL_CONTRA_FLUXO_VALORIZA_{delta}MIN | fav={fav} | gol={m.ultimo_gol_lado}"
    return False, "SNIPER_SEM_GOL_CONTRA_FLUXO"


def dc01_1_sniper_necessidade_real(m: "Metricas") -> Tuple[bool, str]:
    if not HABILITAR_DC01_1_SNIPER_NECESSIDADE:
        return True, "DC01_1_SNIPER_NECESSIDADE_OFF"
    if not eh_sniper_ft(m.estrategia):
        return True, "NAO_SNIPER"

    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "SNIPER_NECESSIDADE_SEM_FAVORITO"

    vencedor = lado_vencendo(m)
    diff = diferenca_placar(m)

    if vencedor == "EMPATE":
        return True, "SNIPER_NECESSIDADE_FAVORITO_EMPATANDO"
    if vencedor != fav:
        return True, "SNIPER_NECESSIDADE_FAVORITO_PERDENDO"

    if diff >= 2:
        return False, f"SNIPER_SEM_NECESSIDADE_FAVORITO_VENCE_{diff}"

    zebra = lado_oposto(fav)
    zebra_ok, zebra_motivo = dc01_1_zebra_ameaca_real(m, zebra)
    if not zebra_ok:
        return False, "SNIPER_FAVORITO_VENCE_1_SEM_AMEACA_DA_ZEBRA | " + zebra_motivo

    return True, "SNIPER_FAVORITO_VENCE_1_MAS_ZEBRA_VIVA | " + zebra_motivo


def dc01_1_ht_adversario_tem_vida(m: "Metricas", adversario: str) -> Tuple[bool, str]:
    if adversario not in {"CASA", "FORA"}:
        return False, "ADVERSARIO_INVALIDO"
    d = dados_lado(m, adversario)
    ip = ip_lado(m, adversario)
    provas = 0
    motivos = []
    if d["rb"] >= 1:
        provas += 1
        motivos.append(f"RB={d['rb']}")
    if d["rl"] >= 2:
        provas += 1
        motivos.append(f"RL={d['rl']}")
    if d["cantos"] >= 1:
        provas += 1
        motivos.append(f"CANTOS={d['cantos']}")
    if d["u5"] >= 2 or d["u10"] >= 4:
        provas += 1
        motivos.append(f"U5={d['u5']}_U10={d['u10']}")
    if d["chance"] >= 4 or d["xg"] >= 0.15:
        provas += 1
        motivos.append(f"CHANCE={d['chance']}_XG={d['xg']:.2f}")
    if ip["pico"] >= 14 or ip["c10"] >= 2:
        provas += 1
        motivos.append(f"IP={ip['pico']}_C10={ip['c10']}")
    return provas >= 2, f"ADVERSARIO_VIDA_PROVAS={provas}/6|" + "|".join(motivos)


def dc01_1_ht_placar_largo_sem_necessidade(m: "Metricas", lado: str) -> Tuple[bool, str]:
    if not HABILITAR_DC01_1_HT_PLACAR_LARGO_NECESSIDADE:
        return False, "DC01_1_HT_PLACAR_LARGO_OFF"
    if not eh_ht(m.estrategia):
        return False, "NAO_HT"
    diff = diferenca_placar(m)
    if diff < DC01_1_HT_DIF_MIN_PLACAR_LARGO:
        return False, f"HT_DIF_{diff}_MENOR_QUE_LIMITE"
    vencedor = lado_vencendo(m)
    if vencedor not in {"CASA", "FORA"}:
        return False, "HT_SEM_VENCEDOR"
    if lado != vencedor:
        return False, "HT_LADO_ANALISADO_NAO_E_VENCEDOR"

    adversario = lado_oposto(vencedor)
    adv_vivo, adv_motivo = dc01_1_ht_adversario_tem_vida(m, adversario)
    odds_muito_desequilibradas = bool(m.odd_favorito and m.odd_favorito <= 1.25)

    d_v = dados_lado(m, vencedor)
    ip_v = ip_lado(m, vencedor)
    massacre_fora_da_curva = (
        d_v["u5"] >= 7
        and d_v["u10"] >= 14
        and (d_v["rb"] >= 4 or d_v["chance"] >= 15 or d_v["xg"] >= 0.80)
        and (ip_v["pico"] >= 28 or ip_v["c18"] >= 3 or ip_v["c22"] >= 2)
    )

    if adv_vivo:
        return False, "HT_PLACAR_LARGO_MAS_ADVERSARIO_VIVO | " + adv_motivo

    if massacre_fora_da_curva and not odds_muito_desequilibradas:
        return False, "HT_PLACAR_LARGO_MAS_MASSACRE_FORA_DA_CURVA | " + adv_motivo

    return True, (
        f"HT_PLACAR_LARGO_SEM_NECESSIDADE | diff={diff}|odd_fav={m.odd_favorito}|"
        f"vencedor={vencedor}|adversario={adversario}|{adv_motivo}|"
        f"massacre_fora_curva={massacre_fora_da_curva}|odds_muito_desequilibradas={odds_muito_desequilibradas}"
    )


def v26_detectar_caos_bidirecional(m: "Metricas") -> Tuple[str, str]:
    if not HABILITAR_DC01_2_V26_CAOS_BIDIRECIONAL:
        return "NORMAL", "DC01_2_V26_CAOS_OFF"

    fav = m.lado_favorito
    zebra = lado_oposto(fav) if fav in {"CASA", "FORA"} else None
    if not zebra:
        return "NORMAL", "V26_CAOS_SEM_FAVORITO"

    d_z = dados_lado(m, zebra)
    d_f = dados_lado(m, fav)
    ip_z = ip_lado(m, zebra)
    ip_f = ip_lado(m, fav)

    sinais_zebra_viva = 0
    motivos_z: list = []

    if d_z["u5"] >= 2:
        sinais_zebra_viva += 2
        motivos_z.append(f"ZEBRA_U5={d_z['u5']}")
    if d_z["u10"] >= 4:
        sinais_zebra_viva += 1
        motivos_z.append(f"ZEBRA_U10={d_z['u10']}")
    if d_z["rb"] >= 1:
        sinais_zebra_viva += 2
        motivos_z.append(f"ZEBRA_RB={d_z['rb']}")
    if d_z["cantos"] >= 1:
        sinais_zebra_viva += 1
        motivos_z.append(f"ZEBRA_CANTOS={d_z['cantos']}")
    if d_z["chance"] >= 4 or d_z["xg"] >= 0.15:
        sinais_zebra_viva += 1
        motivos_z.append(f"ZEBRA_CHANCE={d_z['chance']}_XG={d_z['xg']:.2f}")
    if ip_z["pico"] >= 14 or ip_z["c10"] >= 1:
        sinais_zebra_viva += 1
        motivos_z.append(f"ZEBRA_IP={ip_z['pico']}_C10={ip_z['c10']}")

    sinais_fav_ativo = 0
    if d_f["u5"] >= 2:
        sinais_fav_ativo += 1
    if d_f["rb"] >= 1:
        sinais_fav_ativo += 1
    if ip_f["pico"] >= 18:
        sinais_fav_ativo += 1

    delta_gol = minutos_desde_ultimo_gol(m)
    gol_recente_sinal = -1 <= delta_gol <= 10

    motivo_base = (
        f"zebra_sinais={sinais_zebra_viva}|fav_ativo={sinais_fav_ativo}"
        f"|gol_recente={gol_recente_sinal}|{','.join(motivos_z)}"
    )

    if sinais_zebra_viva >= 5 and sinais_fav_ativo >= 2 and (gol_recente_sinal or d_z["u5"] >= 3):
        return "FORTE", f"V26_CAOS_BIDIRECIONAL_FORTE | {motivo_base}"

    if sinais_zebra_viva >= 3 and sinais_fav_ativo >= 1:
        return "MEDIO", f"V26_CAOS_BIDIRECIONAL_MEDIO | {motivo_base}"

    return "NORMAL", f"V26_CAOS_NORMAL_ZEBRA_FRACA | {motivo_base}"


def filtro_sniper_ft_v2(m: "Metricas") -> Tuple[bool, str]:
    if not eh_sniper_ft(m.estrategia):
        return True, "NAO_SNIPER"

    t = int(m.tempo or 0)
    if t < 60 or t > 78:
        return False, f"SNIPER_FORA_JANELA_{t}"

    fav = m.lado_favorito
    press = m.lado_pressionante
    if fav not in {"CASA", "FORA"}:
        return False, "SNIPER_SEM_FAVORITO"
    # Item 9 (28/06): perfil documentado nos cadernos físicos é odd máx
    # 1.40 + super favorito (91.7% acerto / 46.7% ROI histórico) — o código
    # ainda usava 1.60, mais solto que o que foi validado manualmente.
    # Flag pra rollback instantâneo se o aperto cortar volume demais.
    odd_maxima_sniper = 1.40 if HABILITAR_SNIPER_PERFIL_140 else 1.60
    if m.odd_favorito and m.odd_favorito > odd_maxima_sniper:
        return False, f"SNIPER_ODD_FAVORITO_ACIMA_{odd_maxima_sniper} | odd={m.odd_favorito}"
    if press != fav:
        return False, f"SNIPER_FAVORITO_NAO_PRESSIONANTE | fav={fav} press={press}"

    necessidade_ok, necessidade_motivo = dc01_1_sniper_necessidade_real(m)
    if not necessidade_ok:
        return False, necessidade_motivo

    classe_placar, motivo_placar = sniper_contexto_placar(m)
    if classe_placar == "MORTO":
        return False, "SNIPER_FAVORITO_SATISFEITO | " + motivo_placar
    if classe_placar == "RISCO" and not pressao_extrema_lado(m, fav):
        return False, "SNIPER_FAVORITO_VENCE_2_SEM_EXTREMO | " + motivo_placar

    d = dados_lado(m, fav)
    ip = ip_lado(m, fav)
    pressao_minima = d["u5"] >= 2 or d["u10"] >= 6 or ip["pico"] >= 20 or ip["c18"] >= 2 or ip["c22"] >= 1
    consequencia_minima = d["rb"] >= 1 or d["rl"] >= 3 or d["chance"] >= 8 or d["xg"] >= 0.25 or d["cantos"] >= 2
    if not pressao_minima:
        return False, f"SNIPER_SEM_PRESSAO_VIVA | u5={d['u5']} u10={d['u10']} ip={ip['pico']}"
    if not consequencia_minima:
        return False, f"SNIPER_SEM_CONSEQUENCIA | rb={d['rb']} rl={d['rl']} chance={d['chance']} xg={d['xg']:.2f} cantos={d['cantos']}"

    premiada, motivo_premiada = sniper_pressao_premiada(m)
    if premiada:
        cont_ok, cont_motivo = continuidade_pos_gol(m, fav)
        if not cont_ok:
            return False, motivo_premiada + " | " + cont_motivo
        provas = 0
        if d["u5"] >= 4:
            provas += 1
        if d["u10"] >= 8:
            provas += 1
        if d["rb"] >= 2 or d["chance"] >= 10 or d["xg"] >= 0.40:
            provas += 1
        if ip["pico"] >= 22 or ip["c18"] >= 2:
            provas += 1
        if provas < 3:
            return False, f"SNIPER_PRESSAO_PREMIADA_SEM_NOVO_CICLO | provas={provas}/4 | {motivo_premiada} | {cont_motivo}"

    contra_fluxo, motivo_contra = sniper_gol_contra_fluxo(m)
    if contra_fluxo:
        m.fluxo_motivo = (m.fluxo_motivo or "") + " | " + motivo_contra

    return True, (
        f"SNIPER_V2_PASSOU | classe={classe_placar} | {motivo_placar} | "
        f"u5={d['u5']} u10={d['u10']} rb={d['rb']} rl={d['rl']} "
        f"chance={d['chance']} xg={d['xg']:.2f} ip={ip['pico']} | "
        f"premiada={premiada}:{motivo_premiada} | contra_fluxo={contra_fluxo}:{motivo_contra}"
    )


# =========================================================
# V014 — FILTRO EXCLUSIVO VOLUME_FT (LIVE - INALTERADO)
# =========================================================

def volume_ft_favorito_vencendo_extremo_v21(m: "Metricas") -> Tuple[bool, str]:
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "V21_SEM_FAVORITO"
    if lado_vencendo(m) != fav:
        return False, "V21_FAVORITO_NAO_VENCE"
    if fav != m.lado_pressionante:
        return False, "V21_FAVORITO_VENCE_MAS_NAO_E_PRESSIONANTE"

    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return False, "V21_PLACAR_INVALIDO"
    gols_fav = gc if fav == "CASA" else gf
    gols_adv = gf if fav == "CASA" else gc
    diff = gols_fav - gols_adv
    if diff <= 0:
        return False, "V21_FAVORITO_NAO_ESTA_VENCENDO"

    if m.ultimo_gol and m.ultimo_gol_lado == fav and minutos_desde_ultimo_gol(m) < V11_FREE_GOL_VANTAGEM_MIN:
        return False, f"V21_GOL_VANTAGEM_RECENTE_{minutos_desde_ultimo_gol(m)}MIN"

    d = dados_lado(m, fav)
    op = lado_oposto(fav)
    od = dados_lado(m, op)
    ip = ip_lado(m, fav)
    ap_diff = ap_diff_lado(m, fav)

    recente_dominante = d["u5"] > od["u5"] and d["u10"] > od["u10"]

    if diff == 1:
        pressao_absurda = (
            d["u5"] >= 6
            and d["u10"] >= 12
            and ap_diff >= 30
            and recente_dominante
            and (ip["pico"] >= 26 or ip["c18"] >= 3 or ip["c22"] >= 2)
        )
        dominio_convertivel_absurdo = (
            d["rb"] >= 3
            or d["chance"] >= 14
            or d["xg"] >= 0.70
            or (d["rl"] >= 9 and d["cantos"] >= 4)
        )
    else:
        pressao_absurda = (
            d["u5"] >= 7
            and d["u10"] >= 14
            and ap_diff >= 38
            and recente_dominante
            and (ip["pico"] >= 28 or ip["c18"] >= 4 or ip["c22"] >= 3)
        )
        dominio_convertivel_absurdo = (
            d["rb"] >= 4
            or d["chance"] >= 17
            or d["xg"] >= 0.90
            or (d["rl"] >= 11 and d["cantos"] >= 5)
        )

    if pressao_absurda and dominio_convertivel_absurdo:
        return True, (
            f"V21_FAVORITO_VENCENDO_{diff}_MAS_EXTREMO|"
            f"ap_diff={ap_diff}|u5={d['u5']}x{od['u5']}|u10={d['u10']}x{od['u10']}|"
            f"rb={d['rb']}|rl={d['rl']}|cantos={d['cantos']}|chance={d['chance']}|"
            f"xg={d['xg']:.2f}|ip={ip['pico']}|diff={diff}"
        )

    return False, (
        f"V21_FAVORITO_VENCENDO_{diff}_SEM_EXTREMO|"
        f"pressao_absurda={pressao_absurda}|dominio_absurdo={dominio_convertivel_absurdo}|"
        f"recente_dominante={recente_dominante}|ap_diff={ap_diff}|"
        f"u5={d['u5']}x{od['u5']}|u10={d['u10']}x{od['u10']}|"
        f"rb={d['rb']}|rl={d['rl']}|cantos={d['cantos']}|chance={d['chance']}|"
        f"xg={d['xg']:.2f}|ip={ip['pico']}|diff={diff}"
    )


def _volume_ft_contexto_placar_prioritario(m: "Metricas") -> Tuple[bool, str]:
    fav = m.lado_favorito
    if fav not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_FAVORITO_IDENTIFICADO"
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return False, "VOLUME_FT_PLACAR_INVALIDO"

    gols_fav = gc if fav == "CASA" else gf
    gols_adv = gf if fav == "CASA" else gc

    if gols_fav <= gols_adv:
        return True, "VOLUME_FT_FAVORITO_EMPATANDO_OU_PERDENDO"

    ok_extremo, motivo_extremo = volume_ft_favorito_vencendo_extremo_v21(m)
    if ok_extremo:
        return True, "VOLUME_FT_FAVORITO_VENCENDO_EXTREMO|" + motivo_extremo
    return False, "VOLUME_FT_FAVORITO_VENCENDO_BLOQUEADO|" + motivo_extremo


def _volume_ft_gol_recente_ok(m: "Metricas") -> Tuple[bool, str]:
    if not gol_recente(m, janela=5):
        return True, "VOLUME_FT_SEM_GOL_RECENTE"
    fav = m.lado_favorito
    lado_gol = m.ultimo_gol_lado
    if lado_gol == fav and ultimo_gol_deixou_lado_vencendo(m):
        return False, f"VOLUME_FT_GOL_RECENTE_FAV_VENCENDO | {m.ultimo_gol}' {lado_gol} | {m.placar}"
    return True, f"VOLUME_FT_GOL_RECENTE_OK | {m.ultimo_gol}' {lado_gol} | {m.placar}"


def _volume_ft_pressao_viva(m: "Metricas") -> Tuple[bool, str]:
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE"
    idx = 0 if lado == "CASA" else 1
    u10_fav = m.ultimos10[idx]
    u10_adv = m.ultimos10[1 - idx]
    u5_fav = m.ultimos5[idx]
    u5_adv = m.ultimos5[1 - idx]
    if (u10_fav - u10_adv) < 4:
        return False, f"VOLUME_FT_U10_INSUFICIENTE | {u10_fav}x{u10_adv}"
    if (u5_fav - u5_adv) < 2:
        return False, f"VOLUME_FT_U5_INSUFICIENTE | {u5_fav}x{u5_adv}"
    return True, f"VOLUME_FT_PRESSAO_VIVA | U10={u10_fav}x{u10_adv} U5={u5_fav}x{u5_adv}"


def _volume_ft_ataques_perigosos(m: "Metricas") -> Tuple[bool, str]:
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE_AP"
    idx = 0 if lado == "CASA" else 1
    ap_fav = m.ataques_perigosos[idx]
    ap_adv = m.ataques_perigosos[1 - idx]
    if (ap_fav - ap_adv) < 8:
        return False, f"VOLUME_FT_AP_DIFERENCA_INSUFICIENTE | {ap_fav}x{ap_adv}"
    if ap_adv > 0 and (ap_fav / ap_adv) < 1.4:
        return False, f"VOLUME_FT_AP_RAZAO_INSUFICIENTE | {ap_fav}x{ap_adv}"
    return True, f"VOLUME_FT_AP_OK | {ap_fav}x{ap_adv}"


def _volume_ft_remates(m: "Metricas") -> Tuple[bool, str]:
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE_RB"
    idx = 0 if lado == "CASA" else 1
    rb_fav = m.remates_baliza[idx]
    rb_adv = m.remates_baliza[1 - idx]
    rl_fav = m.remates_lado[idx]
    if rb_fav < 2 and rl_fav < 4:
        return False, f"VOLUME_FT_REMATES_INSUFICIENTES | RB={rb_fav} RL={rl_fav}"
    if rb_fav <= rb_adv and rb_fav < 3:
        return False, f"VOLUME_FT_RB_SEM_VANTAGEM | {rb_fav}x{rb_adv}"
    return True, f"VOLUME_FT_REMATES_OK | RB={rb_fav}x{rb_adv} RL={rl_fav}"


def _volume_ft_chance_golo(m: "Metricas") -> Tuple[bool, str]:
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE_CHANCE"
    idx = 0 if lado == "CASA" else 1
    chance = m.chance_golo[idx]
    if chance < 8:
        return False, f"VOLUME_FT_CHANCE_INSUFICIENTE | chance={chance}"
    return True, f"VOLUME_FT_CHANCE_OK | chance={chance}"


def _volume_ft_favorito_pressionando(m: "Metricas") -> Tuple[bool, str]:
    fav = m.lado_favorito
    press = m.lado_pressionante
    if fav not in {"CASA", "FORA"} or press not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_FAV_OU_PRESS"
    if fav != press:
        return False, f"VOLUME_FT_FAV_NAO_PRESSIONANTE | fav={fav} press={press}"
    return True, f"VOLUME_FT_FAV_PRESSIONANTE | {fav}"


def _volume_ft_minuto(m: "Metricas") -> Tuple[bool, str]:
    t = int(m.tempo or 0)
    if t > 85:
        return False, f"VOLUME_FT_FORA_JANELA_TARDIO | {t}'"
    if t < 65:
        return False, f"VOLUME_FT_FORA_JANELA_CEDO | {t}'"
    return True, f"VOLUME_FT_MINUTO_OK | {t}'"


def filtro_volume_ft(m: "Metricas") -> Tuple[bool, str]:
    checks = [
        _volume_ft_minuto,
        _volume_ft_contexto_placar_prioritario,
        _volume_ft_gol_recente_ok,
        _volume_ft_favorito_pressionando,
        _volume_ft_pressao_viva,
        _volume_ft_ataques_perigosos,
        _volume_ft_remates,
        _volume_ft_chance_golo,
    ]
    for check in checks:
        ok, motivo = check(m)
        if not ok:
            return False, motivo
    return True, "VOLUME_FT_FILTRO_PASSOU"


# =========================================================
# V013 — AUDITORIA HTML AUTOMÁTICA (LIVE - INALTERADO)
# =========================================================

import json
import threading as _threading

_v13_lock = _threading.Lock()
_V13_ESTRATEGIAS_EXCLUIDAS_REPROV = {"BOT_VOLUME_FT", "BOT_VOLUME_HT", "VOLUME_FT"}


def _v13_arquivo_json(tipo: str, dt: Optional[datetime] = None) -> Path:
    dt = dt or datetime.now()
    nome = f"{tipo}_{dt.strftime('%Y_%m_%d')}.json"
    return _data_dir() / nome


def _v13_arquivo_html(tipo: str, dt: Optional[datetime] = None) -> Path:
    dt = dt or datetime.now()
    nome = f"{tipo}_{dt.strftime('%Y_%m_%d')}.html"
    return _data_dir() / nome


def v13_registrar(m: "Metricas", score_medio: int, aprovado: bool, motivo: str) -> None:
    if not HABILITAR_V13_AUDITORIA_HTML:
        return
    if not aprovado and m.estrategia in _V13_ESTRATEGIAS_EXCLUIDAS_REPROV:
        return
    tipo = "aprovados" if aprovado else "reprovados"
    entrada = {
        "ts": now_iso(),
        "bot": m.estrategia,
        "jogo": m.jogo,
        "minuto": m.tempo,
        "placar": m.placar,
        "score": score_medio,
        "liga": m.liga,
        "motivo": motivo if not aprovado else "",
        "cornerpro": m.cornerpro,
    }
    try:
        with _v13_lock:
            arq = _v13_arquivo_json(tipo)
            registros: list = []
            if arq.exists():
                try:
                    registros = json.loads(arq.read_text(encoding="utf-8"))
                except Exception:
                    registros = []
            registros.append(entrada)
            # Mudança 20: Limitar a 500 registros
            if len(registros) > 500:
                registros = registros[-500:]
            arq.write_text(json.dumps(registros, ensure_ascii=False, indent=2), encoding="utf-8")
            v13_gerar_html(tipo)
    except Exception as e:
        log(f"⚠️ V13 registrar erro | {type(e).__name__}: {e}")


# =========================================================
# ESCAPAMENTO HTML (MUDANÇA 16) - INALTERADO
# =========================================================

def escape_html(texto: str) -> str:
    """Escapa caracteres HTML para prevenir XSS (Mudança 16)."""
    return html.escape(str(texto or ""))


def v13_gerar_html(tipo: str, dt: Optional[datetime] = None) -> Path:
    dt = dt or datetime.now()
    arq_json = _v13_arquivo_json(tipo, dt)
    arq_html = _v13_arquivo_html(tipo, dt)
    registros: list = []
    if arq_json.exists():
        try:
            registros = json.loads(arq_json.read_text(encoding="utf-8"))
        except Exception:
            registros = []

    titulo = "APROVADOS" if tipo == "aprovados" else "REPROVADOS"
    cor_topo = "#00c896" if tipo == "aprovados" else "#e94560"
    data_fmt = dt.strftime("%d/%m/%Y")
    data_key = dt.strftime("%Y_%m_%d")
    total = len(registros)
    storage_key = f"coutips_turbo_{tipo}_{data_key}"

    itens_js = []
    for i, r in enumerate(registros):
        link = escape_html(r.get("cornerpro", "") or "")
        link_safe = link.replace("'", "\\'")
        bot = escape_html(r.get("bot", "") or "").replace("'", "\\'")
        jogo = escape_html(r.get("jogo", "") or "").replace("'", "\\'")
        minuto = escape_html(str(r.get("minuto", "") or "")).replace("'", "\\'")
        placar = escape_html(r.get("placar", "") or "").replace("'", "\\'")
        score = escape_html(str(r.get("score", "") or "")).replace("'", "\\'")
        liga = escape_html(r.get("liga", "") or "").replace("'", "\\'")
        motivo = escape_html(r.get("motivo", "") or "").replace("'", "\\'")
        uid_base = f"{r.get('bot','')}|{r.get('jogo','')}|{r.get('minuto','')}|{r.get('placar','')}|{r.get('score','')}|{i}"
        uid = hashlib.sha1(uid_base.encode("utf-8", errors="ignore")).hexdigest()[:16]
        itens_js.append(
            f"{{id:'{uid}',bot:'{bot}',match:'{jogo}',min:'{minuto}',"
            f"placar:'{placar}',score:'{score}',liga:'{liga}',"
            f"motivo:'{motivo}',link:'{link_safe}'}}"
        )

    games_js = "[" + ",\n".join(itens_js) + "]" if itens_js else "[]"

    conteudo = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>COUTIPS {titulo} — {data_fmt}</title>
<style>
:root{{--bg:#0d0f14;--s1:#161b24;--s2:#1e2533;--border:#2a3244;--red:#e94560;--gold:#f0a500;--green:#00c896;--blue:#4a9eff;--text:#e8ecf0;--muted:#6a7a8e}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{height:100%;overflow:hidden}}
body{{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;display:flex;flex-direction:column}}
#topbar{{background:var(--s1);border-bottom:2px solid {cor_topo};padding:6px 14px;display:flex;align-items:center;gap:10px;flex-shrink:0;flex-wrap:wrap}}
.logo{{font-size:10px;font-weight:800;letter-spacing:3px;color:{cor_topo}}}
.tsub{{font-size:10px;color:var(--muted)}}
.sep{{width:1px;height:28px;background:var(--border);flex-shrink:0}}
.spacer{{flex:1}}
.pill{{background:var(--s2);border:1px solid var(--border);border-radius:20px;padding:3px 10px;font-size:11px;color:var(--muted);display:flex;align-items:center;gap:5px}}
.pill b{{font-size:13px;font-weight:700}}
.pill.g b{{color:var(--green)}}.pill.r b{{color:var(--red)}}.pill.p b{{color:var(--gold)}}
.csv-btn{{padding:4px 14px;background:#0d1a2e;border:1px solid #2a3e5a;border-radius:4px;color:var(--blue);font-size:11px;font-weight:700;cursor:pointer}}
.csv-btn:hover{{background:#1a2e4a}}
#main{{display:flex;flex:1;overflow:hidden;min-height:0}}
#sidebar{{width:290px;flex-shrink:0;border-right:1px solid var(--border);overflow-y:auto;background:var(--bg)}}
.item{{padding:8px 10px;border-bottom:1px solid var(--border);cursor:pointer;display:flex;align-items:center;gap:7px;border-left:3px solid transparent;user-select:none;transition:background .1s}}
.item:hover{{background:var(--s2)}}
.item.sel{{background:#0d1a2e;border-left-color:var(--blue)}}
.item.dg{{border-left-color:var(--green)!important}}
.item.dr{{border-left-color:var(--red)!important}}
.item.ds{{opacity:0.4}}
.dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0;background:var(--border)}}
.dot.g{{background:var(--green)}}.dot.r{{background:var(--red)}}.dot.s{{background:#333}}
.ib{{flex:1;min-width:0}}
.im{{font-weight:600;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.imeta{{font-size:10px;color:var(--muted);margin-top:2px;display:flex;gap:5px;flex-wrap:wrap}}
.sc{{font-family:monospace;font-weight:700;font-size:11px;color:var(--text)}}
.bd{{padding:1px 5px;border-radius:3px;font-size:9px;font-weight:700;background:#1a1e2a;color:#4a9eff;border:1px solid #1e2e4a}}
#right{{flex:1;display:flex;flex-direction:column;min-width:0;overflow:hidden}}
#frame-wrap{{flex:1;position:relative;overflow:hidden;background:#111}}
#frame-wrap iframe{{width:100%;height:100%;border:none;display:block}}
#no-game{{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;background:var(--bg);pointer-events:none}}
#no-game.hidden{{display:none}}
#bottombar{{background:var(--s1);border-top:1px solid var(--border);padding:7px 14px;display:flex;align-items:center;gap:8px;flex-shrink:0}}
#mlabel{{font-size:11px;font-weight:600;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--muted)}}
.abtn{{padding:7px 18px;border-radius:5px;border:1px solid var(--border);background:var(--s2);color:var(--muted);font-size:13px;font-weight:700;cursor:pointer;transition:all .15s}}
.abtn:hover{{opacity:.8}}
.abtn.ag{{background:#004a30;border-color:var(--green);color:var(--green)}}
.abtn.ar{{background:#4a0020;border-color:var(--red);color:var(--red)}}
.abtn.as{{background:#1e2533;border-color:#3a4555;color:#4a5566}}
.kh{{font-size:10px;color:var(--muted);display:flex;gap:6px;align-items:center}}
.k{{background:var(--s2);border:1px solid var(--border);border-radius:3px;padding:1px 5px;font-family:monospace;font-size:10px}}
.k.kg{{border-color:var(--green);color:var(--green)}}.k.kr{{border-color:var(--red);color:var(--red)}}
#prog{{font-size:10px;color:var(--muted);white-space:nowrap}}
::-webkit-scrollbar{{width:4px}}
::-webkit-scrollbar-track{{background:var(--bg)}}
::-webkit-scrollbar-thumb{{background:var(--border);border-radius:2px}}
</style>
</head>
<body>
<div id="topbar">
  <div><div class="logo">COUTIPS / {titulo}</div><div class="tsub">{data_fmt}</div></div>
  <div class="sep"></div>
  <div class="spacer"></div>
  <div class="pill p"><b id="ct">{total}</b> jogos</div>
  <div class="pill g">🟢 <b id="cg">0</b></div>
  <div class="pill r">🔴 <b id="cr">0</b></div>
  <div class="pill"><b id="cp">{total}</b> pend.</div>
  <button class="csv-btn" onclick="exportCSV()">⬇ Exportar CSV</button>
</div>
<div id="main">
  <div id="sidebar"></div>
  <div id="right">
    <div id="frame-wrap">
      <iframe id="frm" src="about:blank"></iframe>
      <div id="no-game">
        <div style="font-size:32px;color:var(--border)">←</div>
        <div style="font-size:13px;color:var(--muted)">Selecione um jogo na lista</div>
        <div style="font-size:11px;color:var(--border)">G = Green · R = Red · N = próximo pendente</div>
      </div>
    </div>
    <div id="bottombar">
      <div id="mlabel">Nenhum jogo selecionado</div>
      <button class="abtn" id="bg" onclick="setResult(sel,'green')">🟢 G</button>
      <button class="abtn" id="br" onclick="setResult(sel,'red')">🔴 R</button>
      <button class="abtn" id="bs" onclick="setResult(sel,'skip')">— S</button>
      <div class="sep"></div>
      <div class="kh">
        <span><span class="k kg">G</span></span>
        <span><span class="k kr">R</span></span>
        <span><span class="k">S</span></span>
        <span><span class="k">↑↓</span></span>
        <span><span class="k">N</span> pend.</span>
      </div>
      <div class="spacer"></div>
      <div id="prog"></div>
    </div>
  </div>
</div>
<script>
const GAMES={games_js};
const KEY={json.dumps(storage_key)};
let state={{}};
let sel=null;
try{{state=JSON.parse(localStorage.getItem(KEY))||{{}};}}catch(e){{}}
function save(){{try{{localStorage.setItem(KEY,JSON.stringify(state));}}catch(e){{}}}}
function renderSidebar(){{
  const sb=document.getElementById('sidebar');
  let h='';
  GAMES.forEach((g)=>{{
    const r=state[g.id];
    const dc=r==='green'?'dg':r==='red'?'dr':r==='skip'?'ds':'';
    const sc=g.id===sel?'sel':'';
    const dot=r==='green'?'g':r==='red'?'r':r==='skip'?'s':'';
    h+=`<div class="item ${{sc}} ${{dc}}" id="i-${{g.id}}" onclick="pick('${{g.id}}')">
      <div class="dot ${{dot}}"></div>
      <div class="ib">
        <div class="im">${{g.match}}</div>
        <div class="imeta">
          <span class="bd">${{g.bot}}</span>
          <span class="sc">${{g.placar}}</span>
          <span>${{g.min}}</span>
          <span style="color:var(--muted)">${{g.score}}%</span>
        </div>
      </div>
    </div>`;
  }});
  sb.innerHTML=h;
  updStats();
}}
function pick(id){{
  sel=id;
  const g=GAMES.find(x=>x.id===id);
  if(!g)return;
  document.querySelectorAll('.item').forEach(el=>{{
    el.classList.remove('sel');
    if(el.id==='i-'+id){{el.classList.add('sel');el.scrollIntoView({{block:'nearest'}});}}
  }});
  document.getElementById('frm').src=g.link||'about:blank';
  document.getElementById('no-game').classList.add('hidden');
  document.getElementById('mlabel').innerHTML=`${{g.match}} &nbsp;·&nbsp; ${{g.placar}} &nbsp;·&nbsp; ${{g.min}} &nbsp;·&nbsp; ${{g.score}}% &nbsp;·&nbsp; ${{g.liga}}`;
  updBtns();
}}
function updBtns(){{
  const r=state[sel];
  ['bg','br','bs'].forEach(id=>{{document.getElementById(id).className='abtn';}});
  if(r==='green')document.getElementById('bg').className='abtn ag';
  if(r==='red')  document.getElementById('br').className='abtn ar';
  if(r==='skip') document.getElementById('bs').className='abtn as';
}}
function setResult(id,result){{
  if(!id)return;
  state[id]=result;
  save();
  renderSidebar();
  document.querySelectorAll('.item').forEach(el=>el.classList.remove('sel'));
  const el=document.getElementById('i-'+id);
  if(el)el.classList.add('sel');
  updBtns();
  setTimeout(()=>{{
    const idx=GAMES.findIndex(g=>g.id===id);
    const nxt=GAMES.slice(idx+1).find(g=>!state[g.id]);
    if(nxt)pick(nxt.id);
  }},250);
}}
function nextPending(){{
  const idx=sel?GAMES.findIndex(g=>g.id===sel):-1;
  const nxt=GAMES.slice(idx+1).find(g=>!state[g.id])||GAMES.find(g=>!state[g.id]);
  if(nxt)pick(nxt.id);
}}
function updStats(){{
  const g=Object.values(state).filter(v=>v==='green').length;
  const r=Object.values(state).filter(v=>v==='red').length;
  const s=Object.values(state).filter(v=>v==='skip').length;
  const p=GAMES.length-g-r-s;
  document.getElementById('cg').textContent=g;
  document.getElementById('cr').textContent=r;
  document.getElementById('cp').textContent=p;
  const done=g+r;
  document.getElementById('prog').textContent=done>0?`${{done}}/${{GAMES.length}} · ${{Math.round(g/done*100)}}% green`:`${{GAMES.length}} jogos`;
}}
function exportCSV(){{
  const rows=[['ID','BOT','JOGO','LIGA','MIN','PLACAR','SCORE','RESULTADO','MOTIVO','LINK']];
  GAMES.forEach(g=>{{
    rows.push([g.id,g.bot,g.match,g.liga,g.min,g.placar,g.score,state[g.id]||'pendente',g.motivo||'',g.link||'']);
  }});
  const csv=rows.map(r=>r.map(c=>'"'+String(c).replace(/"/g,'""')+'"').join(';')).join('\\n');
  const blob=new Blob([csv],{{type:'text/csv;charset=utf-8;'}});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;a.download='coutips_{tipo}_{data_key}.csv';a.click();
  URL.revokeObjectURL(url);
}}
document.addEventListener('keydown',e=>{{
  const t=document.activeElement.tagName;
  if(t==='INPUT'||t==='TEXTAREA')return;
  if(e.key==='g'||e.key==='G'){{if(sel)setResult(sel,'green');}}
  else if(e.key==='r'||e.key==='R'){{if(sel)setResult(sel,'red');}}
  else if(e.key==='s'||e.key==='S'){{if(sel)setResult(sel,'skip');}}
  else if(e.key==='n'||e.key==='N')nextPending();
  else if(e.key==='ArrowDown'){{const idx=GAMES.findIndex(g=>g.id===sel);if(idx<GAMES.length-1)pick(GAMES[idx+1].id);}}
  else if(e.key==='ArrowUp'){{const idx=GAMES.findIndex(g=>g.id===sel);if(idx>0)pick(GAMES[idx-1].id);}}
}});
renderSidebar();
const first=GAMES.find(g=>!state[g.id]);
if(first)pick(first.id);
</script>
</body>
</html>"""

    try:
        arq_html.write_text(conteudo, encoding="utf-8")
        log(f"📄 DC01.4 HTML TURBO gerado | {arq_html.name} | {total} registros")
    except Exception as e:
        log(f"⚠️ V13 gerar_html erro | {type(e).__name__}: {e}")
    return arq_html


async def v13_enviar_htmls_telegram() -> None:
    if not HABILITAR_V13_AUDITORIA_HTML:
        return
    ontem = datetime.now() - timedelta(days=1)
    # Mudança 11: Enviar HTML para o canal de logs se configurado
    canal = CANAL_LOGS_PRELIVE if CANAL_LOGS_PRELIVE else CONFIRMATION_CHANNEL
    for tipo in ("aprovados", "reprovados"):
        arq_html = _v13_arquivo_html(tipo, ontem)
        arq_json = _v13_arquivo_json(tipo, ontem)
        if arq_json.exists():
            v13_gerar_html(tipo, ontem)
        if not arq_html.exists():
            log(f"⚠️ V13 envio HTML | arquivo não encontrado: {arq_html.name}")
            continue
        try:
            await client.send_file(
                canal,
                str(arq_html),
                caption=f"📋 COUTIPS {tipo.upper()} — {ontem.strftime('%d/%m/%Y')}",
            )
            log(f"📤 V13 HTML enviado | {arq_html.name} → {canal}")
            await asyncio.sleep(2)
        except Exception as e:
            log(f"⚠️ V13 envio HTML erro | {arq_html.name} | {type(e).__name__}: {e}")


async def v13_agendador() -> None:
    log("🕐 V13 agendador iniciado — envio diário às 00:05")
    while True:
        try:
            agora = datetime.now()
            proximo = agora.replace(hour=0, minute=5, second=0, microsecond=0)
            if agora >= proximo:
                proximo += timedelta(days=1)
            espera = (proximo - agora).total_seconds()
            log(f"🕐 V13 próximo envio em {int(espera//3600)}h{int((espera%3600)//60)}m")
            await asyncio.sleep(espera)
            await v13_enviar_htmls_telegram()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log(f"⚠️ V13 agendador erro | {type(e).__name__}: {e}")


# =========================================================
# PROTEÇÃO CONTRA INSTÂNCIA DUPLICADA
# =========================================================
# Portado de main.py (sistema ao vivo) — o PRELIVE_V2 nunca teve essa
# proteção, e foi exatamente a ausência dela (+ retry) que permitiu o
# AuthKeyDuplicatedError de 27/06/2026 virar crash loop infinito.

import fcntl


def verificar_instancia_unica():
    """Garante que apenas uma instância do bot está rodando neste host.

    Usa um arquivo de lock (fcntl.flock) — se uma segunda instância tentar
    iniciar enquanto a primeira está rodando, ela detecta e encerra sozinha
    em vez de competir pela mesma sessão Telegram.
    """
    lock_path = "/tmp/alfa_bot.lock"
    try:
        lock_file = open(lock_path, "w")
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        log(f"✅ Instância única confirmada (PID {os.getpid()})")
        return lock_file
    except IOError:
        log("❌ OUTRA INSTÂNCIA JÁ ESTÁ RODANDO NESTE HOST — encerrando este processo.")
        log("   Verifique no Railway se há mais de 1 réplica ativa em Settings.")
        raise SystemExit(1)


async def _notificar_crash_telegram(motivo: str, detalhe: str = "") -> None:
    """Avisa o canal técnico que o bot caiu e está reiniciando.

    Usa uma conexão Telegram temporária e independente — tenta uma vez só,
    sem bloquear o restart se falhar.
    """
    try:
        canal = CONFIRMATION_CHANNEL or TARGET_CHANNEL
        if not canal:
            return
        agora = time.strftime("%H:%M:%S")
        det = (detalhe[:200] + "...") if len(detalhe) > 200 else detalhe
        msg = (
            f"🚨 <b>ALERTA OPERACIONAL</b>\n"
            f"⚠️ Bot caiu e está reiniciando\n"
            f"🕐 {agora}\n"
            f"📋 Motivo: {motivo}"
            + (f"\n💬 {det}" if det else "")
        )
        if SESSION_STRING:
            cli_tmp = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        else:
            cli_tmp = TelegramClient("coutips_v2_session", API_ID, API_HASH)
        await cli_tmp.connect()
        await cli_tmp.send_message(canal, msg, parse_mode="html")
        await cli_tmp.disconnect()
    except Exception as e:
        log(f"⚠️ Não consegui notificar crash no Telegram: {e}")


async def _notificar_sessao_morta_telegram(tentativas: int) -> None:
    """Aviso final quando a sessão Telegram está permanentemente revogada.

    Diferente de _notificar_crash_telegram: aqui o processo vai PARAR de
    vez (não faz sentido o Railway ficar reiniciando — a authkey já está
    morta e só uma sessão nova resolve). Ver RESUMO_SESSAO_PRELIVE_27-06-2026.
    """
    motivo = (
        f"AuthKeyDuplicatedError persistente após {tentativas} tentativas. "
        f"A sessão Telegram foi revogada permanentemente — retry não resolve. "
        f"É necessário gerar uma SESSION_STRING nova e garantir replicas=1 no Railway "
        f"antes de redeployar."
    )
    log(f"🛑 {motivo}")
    await _notificar_crash_telegram("Sessão Telegram morta — processo parado", motivo)


# =========================================================
# MAIN
# =========================================================

def logar_versao_inicial() -> None:
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log(f"🚀 VERSAO_COUTIPS_ATIVA = {VERSAO_COUTIPS}")
    log("✅ Sistemas: AO VIVO (parser/score/IA/auditoria) + PRÉ-LIVE V2 (scraper/market engine)")
    log("✅ Correções desta versão (sessão 27-28/06/2026):")
    log("  1. Proteção contra instância duplicada + retry com backoff p/ AuthKeyDuplicatedError")
    log("  2. Parada definitiva (sem crash loop infinito) se a sessão estiver morta de fato")
    log("  3. @Cout_aud (CANAL_LOGS_PRELIVE) não recebe mais log bruto — só auditoria HTML (v13)")
    log("  4. Estado V26 não usa mais @Cout_aud como storage — usa CONFIRMATION_CHANNEL")
    log("  5. Scraper pré-live: 1 requisição por jogo (não mais 3 redundantes por #fragmento)")
    log("  6. Scraper pré-live: extração escopada por card/sub-aba (corrige colisão de labels)")
    log("  7. Scraper pré-live: tradução label PT->slug (pipeline de feature parava de zerar)")
    log("  8. Merge dos 5 fragmentos DeepSeek em um único arquivo, sem duplicar MarketEngine/MAIN")
    log("  9. Pré-live: score de Gols/Escanteios/BTTS/HT agora cruza ataque de um time x defesa do outro")
    log(" 10. Pré-live: confiabilidade real (não finge mais ALTA/10 jogos sempre) + fallback 10→5→histórico completo")
    log(" 11. Pré-live: 'Vitória Casa/Fora' não entra mais em jogo de seleção/torneio em campo neutro")
    log(" 12. #COUTIPS_ESTADO_V26 de volta pro canal Cout_aud (não fica mais no canal de confirmação)")
    log(" 13. Pré-live: bugs over_25_ft/over_15_ft/over_05_ht e cartões corrigidos (comparado com HTML real)")
    log(" 14. VOLUME_FT reprovado não vai mais pro canal FT-REPROVADO")
    log(" 15. Score real (PY) mostrado em vez de 0% quando bloqueado por categoria sub-20/19/18")
    log(" 16. Log duplicado/marcado como erro no Railway corrigido (stdout em vez de stderr)")
    log(" 17. Cooldown universal separado por período HT/FT (não bloqueia mais um pelo outro)")
    log(" 18. CSV de auditoria com detalhes_completos + texto_bruto_raw (base pra backtest real)")
    log(" 19. SNIPER apertado pro perfil documentado (odd máx 1.40, era 1.60) — flag HABILITAR_SNIPER_PERFIL_140")
    log(" 20. Comando /resumo — resumo de acerto por estratégia a partir do CSV auditado")
    log(" 21. Auditoria mostra o motivo do valor pós-evento, não só a classe")
    log(" 22. Pré-live usa classificação de liga (PREMIUM/MODERADA/NEUTRA/UNDER/PERIGOSA) do ao vivo")
    log(" 23. Pré-live: amistoso é atenção própria (penalidade), separado de campo neutro (bloqueio)")
    log(" 24. Pré-live: abaixo do mínimo ideal mas acima do piso absoluto, manda SAFE com confiança reduzida")
    log(" 25. Auditor V2 integrado — CornerPro removido da cascata, bug de INSERT corrigido, matching por nome+data")
    log(f"📊 Corte HT={CORTE_GOL_HT}% | Corte FT={CORTE_GOL_FT}%")
    log(f"📡 Canal gols={TARGET_CHANNEL} | confirmação={CONFIRMATION_CHANNEL}")
    log(f"📡 Auditoria HTML (Cout_aud)={CANAL_LOGS_PRELIVE or 'NÃO CONFIGURADO (usa CONFIRMATION_CHANNEL)'}")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


async def main() -> None:
    global tarefa_envio, fila_envio, client

    validar_env()
    logar_versao_inicial()
    garantir_csv()
    v17_carregar_estado()
    v29_carregar_cooldowns()

    # CORRIGIDO (30/06) — causa raiz do crash loop:
    # Tanto fila_envio quanto o TelegramClient são criados uma única vez
    # fora de main(), presos ao primeiro event loop. Quando asyncio.run()
    # reinicia após um crash, cria um event loop NOVO — e os dois objetos
    # explodem imediatamente com "event loop must not change after
    # connection" / "Queue bound to different event loop". Solução: recriar
    # os dois aqui, dentro de main(), a cada chamada.
    fila_envio = asyncio.Queue(maxsize=200)
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

    tarefa_envio = asyncio.create_task(trabalhador_fila_envio())
    asyncio.create_task(watchdog())
    if HABILITAR_V13_AUDITORIA_HTML:
        asyncio.create_task(v13_agendador())

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        await receber_mensagem(event)

    @client.on(events.MessageEdited(incoming=True))
    async def handler_edit(event):
        return

    @client.on(events.NewMessage(outgoing=True))
    async def handler_outgoing(event):
        try:
            texto = (event.raw_text or "").strip().lower()
            if texto in ("auditoria", "/auditoria"):
                await receber_mensagem(event)
        except Exception as e:
            log(f"⚠️ handler_outgoing erro | {type(e).__name__}: {e}")

    @client.on(events.NewMessage(incoming=True))
    async def handler_resumo(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/resumo":
            log("📩 /resumo RECEBIDO (INCOMING)")
            resumo = gerar_resumo_resultados()
            await client.send_message(event.chat_id, resumo)

    @client.on(events.NewMessage(from_users='me'))
    async def handler_resumo_me(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/resumo":
            log("📩 /resumo RECEBIDO (CHAT PRIVADO)")
            resumo = gerar_resumo_resultados()
            await client.send_message("me", resumo)

    @client.on(events.NewMessage(incoming=True))
    async def handler_status_auditor(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/status_auditor":
            log("📩 /status_auditor RECEBIDO (INCOMING)")
            if _auditor_v2_manager is None:
                await client.send_message(event.chat_id, "⚠️ Auditor V2 não está ativo nesta instância.")
                return
            try:
                status = auditor_v2.formatar_resumo_diario_auditor(_auditor_v2_manager)
                await client.send_message(event.chat_id, status, parse_mode="html")
            except Exception as e:
                log(f"⚠️ /status_auditor erro | {type(e).__name__}: {e}")
                await client.send_message(event.chat_id, f"⚠️ Erro ao montar status do auditor: {e}")

    @client.on(events.NewMessage(from_users='me'))
    async def handler_status_auditor_me(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/status_auditor":
            log("📩 /status_auditor RECEBIDO (CHAT PRIVADO)")
            if _auditor_v2_manager is None:
                await client.send_message("me", "⚠️ Auditor V2 não está ativo nesta instância.")
                return
            try:
                status = auditor_v2.formatar_resumo_diario_auditor(_auditor_v2_manager)
                await client.send_message("me", status, parse_mode="html")
            except Exception as e:
                log(f"⚠️ /status_auditor erro | {type(e).__name__}: {e}")
                await client.send_message("me", f"⚠️ Erro ao montar status do auditor: {e}")

    global _auditor_v2_manager, _auditor_v2_scheduler
    if not AUDITOR_ENABLED:
        log("🔌 Auditor V2 DESLIGADO pela variável AUDITOR_ENABLED — bot ao vivo segue 100% normal sem ele.")
        _auditor_v2_manager = None
        _auditor_v2_scheduler = None
    elif AUDITOR_V2_DISPONIVEL:
        try:
            _auditor_v2_manager = auditor_v2.AuditManagerV2()
            _auditor_v2_scheduler = auditor_v2.AuditSchedulerV2()
            _auditor_v2_scheduler.start()  # thread própria — não bloqueia o loop assíncrono do bot
            log(f"🧠 Auditor V2 ativo | providers: {[p.provider_name for p in _auditor_v2_manager.providers]}")
        except Exception as e:
            _auditor_v2_manager = None
            _auditor_v2_scheduler = None
            log(f"⚠️ Auditor V2 não iniciou (bot segue normal sem ele) | {type(e).__name__}: {e}")
    else:
        log("ℹ️ Auditor V2 não encontrado (coutips_auditor_v2.py ausente) — bot segue normal sem ele")

    log("🚀 INICIANDO BOT")

    # Retry com backoff para AuthKeyDuplicatedError — cobre o caso legítimo
    # de overlap de deploy (deploy antigo do Railway ainda não morreu).
    # Se persistir além disso, a sessão está morta de verdade (ver
    # _notificar_sessao_morta_telegram) e quem chama main() decide parar
    # o processo em vez de ficar em crash loop infinito.
    TENTATIVAS_AUTHKEY = 5
    for tentativa in range(1, TENTATIVAS_AUTHKEY + 1):
        try:
            await client.start()
            await client.catch_up()
            break
        except Exception as e:
            if "AuthKeyDuplicated" in str(e) or "two different IP" in str(e):
                log(f"⚠️ Possível deploy antigo ainda ativo (tentativa {tentativa}/{TENTATIVAS_AUTHKEY}). Aguardando 30s...")
                await asyncio.sleep(30)
                if tentativa == TENTATIVAS_AUTHKEY:
                    raise
            else:
                raise

    log("✅ TELEGRAM CONECTADO COM SUCESSO")
    await v26_carregar_estado_telegram()
    log(f"🤖 OpenAI {'ATIVA' if OPENAI_HABILITADO and OPENAI_API_KEY else 'DESATIVADA'} ({OPENAI_MODEL})")
    log(f"📡 Canais ativos: principal={TARGET_CHANNEL} | confirmação={CONFIRMATION_CHANNEL}")
    log(f"📡 Auditoria HTML Pré-Live={CANAL_LOGS_PRELIVE or 'NÃO CONFIGURADO'}")
    log("📡 Comandos disponíveis: /auditoria | /resumo | /status_auditor")
    log("ℹ️ Pré-live agora roda em processo separado (prelive.py) — use /prelive nele.")

    await client.run_until_disconnected()


if __name__ == "__main__":
    _lock = verificar_instancia_unica()

    AUTHKEY_FALHAS_MAX = 8  # acima disso, a sessão está morta — parar de vez, não crash-loop.
    falhas_authkey_consecutivas = 0
    tentativa = 0

    while True:
        tentativa += 1
        try:
            log(f"🚀 INICIANDO BOT | tentativa #{tentativa}")
            asyncio.run(main())
            log("ℹ️ main() retornou normalmente. Reiniciando em 5s...")
            falhas_authkey_consecutivas = 0
            time.sleep(5)
        except KeyboardInterrupt:
            log("🛑 Bot encerrado manualmente.")
            break
        except TypeNotFoundError as e:
            log(f"⚠️ TypeNotFoundError (protocolo Telegram incompatível): {e}")
            log("🔁 Reiniciando em 10s — esse erro é da Telethon, não do nosso código.")
            try:
                asyncio.run(_notificar_crash_telegram("TypeNotFoundError (protocolo Telegram)", str(e)))
            except Exception:
                pass
            time.sleep(10)
        except Exception as e:
            erro_str = str(e)
            if "AuthKeyDuplicated" in erro_str or "two different IP" in erro_str:
                falhas_authkey_consecutivas += 1
                if falhas_authkey_consecutivas >= AUTHKEY_FALHAS_MAX:
                    # Acima do limite: já não é overlap de deploy, é sessão
                    # revogada de fato. Parar — ficar reiniciando pra sempre
                    # contra uma authkey morta só spamma o Railway.
                    try:
                        asyncio.run(_notificar_sessao_morta_telegram(falhas_authkey_consecutivas))
                    except Exception:
                        pass
                    log("🛑 PARANDO O PROCESSO — gere uma SESSION_STRING nova e confirme replicas=1 no Railway.")
                    raise SystemExit(1)
                log(
                    f"⚠️ Sessão duplicada detectada (falha {falhas_authkey_consecutivas}/{AUTHKEY_FALHAS_MAX}). "
                    f"Aguardando 30s para deploy antigo encerrar..."
                )
                time.sleep(30)
            elif "event loop" in erro_str:
                log(f"⚠️ Erro de event loop: {e}. Reiniciando em 5s...")
                time.sleep(5)
            else:
                falhas_authkey_consecutivas = 0
                log(f"❌ ERRO FATAL NO BOT: {e}")
                log(traceback.format_exc())
                try:
                    asyncio.run(_notificar_crash_telegram(f"{type(e).__name__}", erro_str))
                except Exception:
                    pass
                log("🔁 Reiniciando em 10s...")
                time.sleep(10)
