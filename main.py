# -*- coding: utf-8 -*-
"""
COUTIPS / ALFA — DC01.2
Base DC01.2 gerada sobre DC01.1 — ajustes pós-auditoria

Mudanças DC01.2 (sem tocar score, V27, V28, DC01, CHAMA_FT):
1. CONF01: BOT_FT CONFIRMAÇÃO bloqueia favorito vencendo por 1 com zebra morta.
   Motivo: CONF01_FAVORITO_VENCE_1_ZEBRA_MORTA
   Flag: HABILITAR_DC01_2_CONF01

2. V26_CAOS_BIDIRECIONAL: reduz penalidade FAV_NAO_PRESSIONANTE em jogos de trocação.
   - FORTE: penalidade zerada (0) quando zebra muito viva e favorito ainda ativo.
   - MÉDIO: penalidade reduzida para -2 (em vez de -8).
   - NORMAL: mantém penalidade original (-8).
   Flag: HABILITAR_DC01_2_V26_CAOS_BIDIRECIONAL

Nota: SNIPER V2 e HT_PLACAR_LARGO já estavam implementados em DC01.1.
Não foram recriados — flags existentes controlam o comportamento.

Start command: python main.py

================================================================================
MUDANÇAS IMPLEMENTADAS (VERSÃO ATUALIZADA):
================================================================================
1. V29 cooldown universal com persistência em arquivo
2. V17 salvamento com atomicidade (arquivo temporário)
3. V26 backup no Telegram com canal secundário
4. Sniper bloqueios vão para auditoria
5. v29_trava_decaimento_pressao_2p usa U5/U10 como proxy (corrigido)
6. dc01_1_zebra_ameaca_real exige 3 provas (aumentado de 2)
7. extrair_jogo mais robusto (não confunde com odds)
8. extrair_odds aceita múltiplos formatos
9. extrair_pressao_alfa aceita múltiplos nomes
10. Sniper processado antes dos filtros V29
11. V26 penalidade aplicada ANTES do score Python
12. score_confirmacao IP usa > em vez de >=
13. Penalidade PRESSAO_PREMIADA_MORREU reduzida de -14 para -10
14. v29_trava_conf_placar_largo com exceção para pressão extrema
15. Validação de variáveis de ambiente na inicialização
16. HTML escapado com html.escape()
17. CSV sanitizado contra injection
18. Prompt da IA sanitizado
19. FloodWait com backoff exponencial
20. Limite de 500 registros nos JSON do V13
21. Cache com classe robusta
22-25. Refatorações organizacionais (processar_alerta dividida, etc.)
================================================================================
"""

from __future__ import annotations

import asyncio
import csv
import html
import hashlib
import json
import logging
import os
import random
import re
import threading as _threading
import time
import traceback
import unicodedata
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from bs4 import BeautifulSoup

try:
    from telethon.errors.common import TypeNotFoundError
except Exception:  # pragma: no cover
    class TypeNotFoundError(Exception):
        pass


# =========================================================
# VERSÃO / CONFIGURAÇÃO BASE
# =========================================================

VERSAO_COUTIPS = "ALFA_COUTIPS_2026_06_18_V29_MELHORIAS_ATUALIZADO"

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

MODO_TESTE = os.getenv("MODO_TESTE", "false").lower() == "true"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.5")
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
    "TEOLOGIN_EMAIL", "TEOLOGIN_SENHA",
    "FREE_CHANNEL", "AUDIT_HT_OK", "AUDIT_HT_NO",
    "AUDIT_FT_OK", "AUDIT_FT_NO", "BACKUP_CHANNEL",
    "OPENAI_API_KEY", "OPENAI_MODEL",
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

client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH
)


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
# FUNÇÕES DE UTILIDADE / LOG
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
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)
    if msg.startswith("❌"):
        logging.error(msg)
    elif msg.startswith(("⚠️", "⛔", "🟡")):
        logging.warning(msg)
    else:
        logging.info(msg)


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
# PERSISTÊNCIA V26 COM BACKUP (MUDANÇA 3)
# =========================================================

_v26_msg_estado_id: Optional[int] = None


async def v26_salvar_estado_telegram() -> None:
    """Salva estado com backup em canal secundário (Mudança 3)."""
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

        if _v26_msg_estado_id:
            try:
                await client.edit_message(CONFIRMATION_CHANNEL, _v26_msg_estado_id, texto)
            except Exception:
                pass

        msg = await client.send_message(CONFIRMATION_CHANNEL, texto)
        _v26_msg_estado_id = msg.id

        backup_channel = os.getenv("BACKUP_CHANNEL")
        if backup_channel:
            await client.send_message(backup_channel, f"{V26_ESTADO_TAG}_BACKUP\n{json.dumps(dados, ensure_ascii=False)}")

        log(f"💾 V26 estado salvo no Telegram | msg_id={_v26_msg_estado_id}")
    except Exception as e:
        log(f"⚠️ V26 salvar_estado_telegram erro | {type(e).__name__}: {e}")


async def v26_carregar_estado_telegram() -> None:
    if not HABILITAR_V26_PERSISTENCIA_TELEGRAM:
        return
    global v11_gratis_contadores, v11_gratis_enviados_por_jogo, _v26_msg_estado_id
    try:
        async for msg in client.iter_messages(CONFIRMATION_CHANNEL, limit=50):
            if msg.text and V26_ESTADO_TAG in msg.text:
                linhas = msg.text.split("\n", 1)
                if len(linhas) < 2:
                    continue
                dados = json.loads(linhas[1])
                v11_gratis_contadores = {k: int(v) for k, v in dados.get("contadores", {}).items()}
                v11_gratis_enviados_por_jogo = {k: float(v) for k, v in dados.get("cooldowns", {}).items()}
                _v26_msg_estado_id = msg.id
                log(f"📂 V26 estado carregado do Telegram | msg_id={msg.id}")
                return
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
# DETECÇÃO / PARSER
# =========================================================

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
# EXTRAÇÃO DE DADOS (PARSER) — MUDANÇAS 7, 8, 9
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
# VALOR PÓS-EVENTO
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
# SCORE PYTHON CONTEXTUAL
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
# SCORE CONFIRMAÇÃO CORRIGIDO (MUDANÇA 12)
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
# CAMADA DE CENÁRIO FT
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
# FUNIL OBRIGATÓRIO HÍBRIDO
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
# IA AUDITORA — COM SANITIZAÇÃO (MUDANÇA 18)
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
    return DecisaoIA(decisao_ia, original, confianca_ia, "SEM_PROTECAO", False, motivo)


# =========================================================
# MENSAGEM TELEGRAM / CSV
# =========================================================

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


async def enviar_auditoria(m: Metricas, score_py: int, score_ia: int, score_medio: int, aprovado: bool, motivo: str) -> None:
    canal = canal_auditoria(m, aprovado)
    if not canal:
        return
    status = "APROVADO" if aprovado else "REPROVADO"
    emoji = "✅" if aprovado else "❌"
    texto = (
        f"{emoji} {status} | {nome_visual_auditoria(m)}\n"
        f"🏟 {m.jogo}\n"
        f"⏱ {m.tempo}' | {m.placar}\n"
        f"📊 PY={score_py}% | IA={score_ia}% | MÉDIA={score_medio}%\n"
        f"🏆 Liga: {m.liga}\n"
        f"🧠 Valor: {m.valor_pos_evento_classe}\n"
        f"📝 {motivo}"
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
]


# =========================================================
# SANITIZAÇÃO CSV (MUDANÇA 17)
# =========================================================

def sanitizar_csv(texto: str) -> str:
    """Sanitiza valores para prevenir CSV Injection (Mudança 17)."""
    if not texto:
        return ""
    texto = str(texto)
    if texto.startswith(("=", "+", "-", "@")):
        return "'" + texto
    return texto


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
    }
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)


# =========================================================
# ENVIO RESILIENTE COM BACKOFF (MUDANÇA 19)
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
# V007 — FLUXO FT CHAMA → ESPERA → CONFIRMAÇÃO
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
# dc01_1_zebra_ameaca_real COM 3 PROVAS (MUDANÇA 6)
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
# DC01 — CHAMA_FT PLACAR ELÁSTICO
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
# V29 — FUNÇÕES DE APOIO ÀS NOVAS TRAVAS
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
# SNIPER V2 — LEITURA SEPARADA FT
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
    if m.odd_favorito and m.odd_favorito > 1.60:
        return False, f"SNIPER_ODD_FAVORITO_ACIMA_1_60 | odd={m.odd_favorito}"
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
# V014 — FILTRO EXCLUSIVO VOLUME_FT
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
# V013 — AUDITORIA HTML AUTOMÁTICA
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
# ESCAPAMENTO HTML (MUDANÇA 16)
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
    canal = CONFIRMATION_CHANNEL
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
            await asyncio.sleep(60)


# =========================================================
# DECISÃO / PROCESSAMENTO — COM REFATORAÇÃO (MUDANÇAS 22, 23)
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


async def registrar_bloqueio_fluxo(m: Metricas, motivo: str, decisao: str = "REPROVADO", score: int = 0) -> None:
    decisao_py = DecisaoPython(score=score, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo, detalhes={})
    decisao_ia = DecisaoIA(decisao="BLOQUEAR", confianca_original=score, confianca_corrigida=score, motivo="FLUXO_PRE_SCORE", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
    registrar_csv(m, decisao_py, decisao_ia, score, decisao, motivo)
    await enviar_auditoria(m, score, score, score, False, motivo)


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


async def _aplicar_bloqueios_imediatos(m: Metricas) -> bool:
    if HABILITAR_BLOQUEIO_BASE_SEM_MERCADO and competicao_base_bloqueada(m):
        m.fluxo_decisao = "BLOQUEADO_BASE_SEM_MERCADO"
        m.fluxo_motivo = "U18_U19_U20_SUB18_SUB19_SUB20"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"⛔ {motivo} | {m.jogo} | {m.competicao}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
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
        decisao_py_vol = DecisaoPython(score=0, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_vol, detalhes={})
        decisao_ia_vol = DecisaoIA(decisao="BLOQUEAR", confianca_original=0, confianca_corrigida=0, motivo="VOLUME_FT_FILTRO", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
        registrar_csv(m, decisao_py_vol, decisao_ia_vol, 0, "REPROVADO", motivo_vol)
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
        v29_marcar_aprovado_universal(normalizar_chave_jogo(m.jogo))
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
    if await _aplicar_bloqueios_imediatos(m):
        return

    # 3. V29 — cooldown universal
    chave_jogo_base = normalizar_chave_jogo(m.jogo)
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


# =========================================================
# SISTEMA PRÉ-LIVE
# =========================================================

TEOLOGIN_EMAIL = os.getenv("TEOLOGIN_EMAIL", "")
TEOLOGIN_SENHA = os.getenv("TEOLOGIN_SENHA", "")
SCORE_MINIMO_APROVACAO = 85
MEDIA_MINIMA_DIA = 85
QTD_MINIMA_JOGOS = 5
QTD_MAXIMA_COMPLEMENTARES = 3


@dataclass
class EstatisticasTimePreLive:
    nome: str = ""
    gols_marcados: float = 0.0
    gols_sofridos: float = 0.0
    xg: float = 0.0
    xga: float = 0.0
    finalizacoes: float = 0.0
    finalizacoes_sofridas: float = 0.0
    escanteios_favor: float = 0.0
    escanteios_contra: float = 0.0
    over_05_ht: float = 0.0
    over_15_ft: float = 0.0
    over_25_ft: float = 0.0
    btts: float = 0.0
    vitorias: float = 0.0
    derrotas: float = 0.0
    gols_ht: float = 0.0
    gols_ft: float = 0.0
    sofre_ht: float = 0.0
    sofre_ft: float = 0.0
    forma: float = 0.0
    posicao: int = 0
    league_reliability: float = 80.0
    team_reliability: float = 80.0


@dataclass
class JogoPreLive:
    time_casa: str = ""
    time_fora: str = ""
    liga: str = ""
    data: str = ""
    horario: str = ""
    estatisticas_casa: EstatisticasTimePreLive = field(default_factory=EstatisticasTimePreLive)
    estatisticas_fora: EstatisticasTimePreLive = field(default_factory=EstatisticasTimePreLive)
    odds: Dict[str, Any] = field(default_factory=dict)
    url: str = ""


@dataclass
class MercadoPreLive:
    nome: str = ""
    jogo: JogoPreLive = field(default_factory=JogoPreLive)
    score: float = 0.0
    matchup_score: float = 0.0
    arce_score: float = 0.0
    chama_score: float = 0.0
    consistencia_score: float = 0.0
    league_score: float = 0.0
    team_score: float = 0.0
    detalhes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MultiplePreLive:
    nome: str = ""
    mercados: List[MercadoPreLive] = field(default_factory=list)
    score_medio: float = 0.0
    descricao: str = ""


# =========================================================
# SCRAPER PRÉ-LIVE
# =========================================================

class TeoBorgesScraperPreLive:
    def __init__(self):
        self.session = None
        self.logado = False
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        self.client = None

    async def login(self) -> bool:
        if not TEOLOGIN_EMAIL or not TEOLOGIN_SENHA:
            log("❌ TEOLOGIN_EMAIL ou TEOLOGIN_SENHA não configurados!")
            return False

        log(f"🔑 Tentando login no clube.theoborges.com")

        try:
            self.client = httpx.AsyncClient(
                headers=self.headers,
                follow_redirects=True,
                timeout=30.0
            )

            resp = await self.client.get("https://clube.theoborges.com/login")
            if resp.status_code != 200:
                log(f"❌ Erro ao acessar login: {resp.status_code}")
                return False

            soup = BeautifulSoup(resp.text, "html.parser")
            csrf_token = None
            csrf_input = soup.select_one("input[name='_token']")
            if csrf_input:
                csrf_token = csrf_input.get("value")
                log("🔑 Token CSRF encontrado")

            login_data = {
                "email": TEOLOGIN_EMAIL,
                "password": TEOLOGIN_SENHA,
            }
            if csrf_token:
                login_data["_token"] = csrf_token

            resp = await self.client.post(
                "https://clube.theoborges.com/login",
                data=login_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": "https://clube.theoborges.com/login",
                }
            )

            if "login" in str(resp.url).lower() and "dashboard" not in str(resp.url).lower() and "matches" not in str(resp.url).lower():
                log("❌ Login falhou! Verifique email e senha.")
                with open("erro_login.html", "w", encoding="utf-8") as f:
                    f.write(resp.text)
                log("📄 HTML do login salvo em 'erro_login.html'")
                return False

            log(f"✅ Login realizado com sucesso! URL atual: {resp.url}")
            self.logado = True
            return True

        except Exception as e:
            log(f"❌ Erro no login: {type(e).__name__}: {e}")
            return False

    async def buscar_jogos(self, dia: str = "hoje") -> List[str]:
        if not self.logado:
            log("❌ Não está logado!")
            return []

        try:
            url = f"https://clube.theoborges.com/matches?dia={dia}"
            log(f"🔄 Buscando jogos para: {dia}")

            resp = await self.client.get(url)
            if resp.status_code != 200:
                log(f"⚠️ Erro ao buscar jogos: {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            links = []

            for link in soup.select(".match-row"):
                href = link.get("href")
                if href and "/game/" in href:
                    links.append(f"https://clube.theoborges.com{href}")

            if not links:
                for link in soup.select("a[href*='/game/']"):
                    href = link.get("href")
                    if href:
                        links.append(f"https://clube.theoborges.com{href}")

            if not links:
                for link in soup.find_all("a", href=True):
                    href = link.get("href")
                    if href and "/game/" in href:
                        links.append(f"https://clube.theoborges.com{href}")

            log(f"📊 Encontrados {len(links)} links de jogos para {dia}")
            return list(set(links))

        except Exception as e:
            log(f"❌ Erro ao buscar jogos: {type(e).__name__}: {e}")
            return []

    async def extrair_dados_jogo(self, url: str) -> Optional[Dict]:
        try:
            await asyncio.sleep(random.uniform(0.5, 1.5))

            resp = await self.client.get(url)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            times = soup.select_one(".match-name")
            if not times:
                return None

            nome_jogo = times.text.strip()
            partes = re.split(r"\s+x\s+|\s+vs\s+", nome_jogo, maxsplit=1)
            if len(partes) < 2:
                return None

            time_casa = partes[0].strip()
            time_fora = partes[1].strip()

            liga_elem = soup.select_one(".match-competition")
            liga = liga_elem.text.strip() if liga_elem else "Desconhecida"

            odds = {}
            for odd_row in soup.select(".odds-row"):
                cols = odd_row.select("td")
                if len(cols) >= 3:
                    mercado = cols[0].text.strip()
                    odd_casa = cols[1].text.strip()
                    odd_fora = cols[2].text.strip()
                    odds[mercado] = {
                        "casa": float(odd_casa.replace(",", ".")) if odd_casa else 0,
                        "fora": float(odd_fora.replace(",", ".")) if odd_fora else 0,
                    }

            estatisticas = self._extrair_estatisticas(soup, time_casa, time_fora)

            return {
                "time_casa": time_casa,
                "time_fora": time_fora,
                "liga": liga,
                "url": url,
                "odds": odds,
                "estatisticas": estatisticas,
                "nome_jogo": nome_jogo,
            }

        except Exception as e:
            log(f"⚠️ Erro ao extrair dados: {type(e).__name__}: {e}")
            return None

    def _extrair_estatisticas(self, soup: BeautifulSoup, time_casa: str, time_fora: str) -> Dict:
        estat = {"casa": {}, "fora": {}}

        for row in soup.select(".stats-row, .pg-tstable-row"):
            cols = row.select("td, .pg-tstable-cell")
            if len(cols) >= 3:
                label = cols[0].text.strip().lower()
                val_casa = cols[1].text.strip()
                val_fora = cols[2].text.strip()

                if "media" in label and "gols" in label:
                    try:
                        estat["casa"]["media_gols"] = float(val_casa.replace(",", "."))
                        estat["fora"]["media_gols"] = float(val_fora.replace(",", "."))
                    except:
                        pass

                if "xg" in label and "casa" in label.lower():
                    try:
                        estat["casa"]["xg"] = float(val_casa.replace(",", "."))
                        estat["fora"]["xg"] = float(val_fora.replace(",", "."))
                    except:
                        pass

                if "over 2.5" in label:
                    try:
                        estat["casa"]["over_25"] = float(val_casa.replace("%", "").strip())
                        estat["fora"]["over_25"] = float(val_fora.replace("%", "").strip())
                    except:
                        pass

                if "over 1.5" in label:
                    try:
                        estat["casa"]["over_15"] = float(val_casa.replace("%", "").strip())
                        estat["fora"]["over_15"] = float(val_fora.replace("%", "").strip())
                    except:
                        pass

                if "btts" in label or "ambos" in label:
                    try:
                        estat["casa"]["btts"] = float(val_casa.replace("%", "").strip())
                        estat["fora"]["btts"] = float(val_fora.replace("%", "").strip())
                    except:
                        pass

                if "escanteios" in label:
                    try:
                        estat["casa"]["escanteios"] = float(val_casa.replace(",", "."))
                        estat["fora"]["escanteios"] = float(val_fora.replace(",", "."))
                    except:
                        pass

                if "sofre" in label or "gols contra" in label:
                    try:
                        estat["casa"]["gols_sofridos"] = float(val_casa.replace(",", "."))
                        estat["fora"]["gols_sofridos"] = float(val_fora.replace(",", "."))
                    except:
                        pass

        return estat

    async def close(self):
        if self.client:
            await self.client.aclose()


class MatchupEnginePreLive:
    def calcular_matchup(self, jogo: JogoPreLive, mercado: str) -> float:
        casa = jogo.estatisticas_casa
        fora = jogo.estatisticas_fora

        if mercado == "Vitoria_Casa":
            return self._matchup_vitoria_casa(casa, fora)
        elif mercado == "Vitoria_Fora":
            return self._matchup_vitoria_fora(casa, fora)
        elif mercado == "Over_05_HT":
            return self._matchup_over_ht(casa, fora)
        elif mercado == "Over_15_FT":
            return self._matchup_over_15(casa, fora)
        elif mercado == "Over_25_FT":
            return self._matchup_over_25(casa, fora)
        elif mercado == "BTTS":
            return self._matchup_btts(casa, fora)
        elif mercado == "Escanteios_Casa":
            return self._matchup_escanteios(casa, fora, "casa")
        elif mercado == "Escanteios_Fora":
            return self._matchup_escanteios(casa, fora, "fora")
        else:
            return 50.0

    def _matchup_vitoria_casa(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive) -> float:
        ataque_casa = casa.gols_marcados * 0.4 + casa.xg * 0.3 + casa.vitorias * 0.3
        defesa_fora = (100 - fora.derrotas) * 0.5 + (100 - fora.gols_sofridos * 15) * 0.5
        return min(100, (ataque_casa * defesa_fora / 100) * 1.2)

    def _matchup_vitoria_fora(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive) -> float:
        ataque_fora = fora.gols_marcados * 0.4 + fora.xg * 0.3 + fora.vitorias * 0.3
        defesa_casa = (100 - casa.derrotas) * 0.5 + (100 - casa.gols_sofridos * 15) * 0.5
        return min(100, (ataque_fora * defesa_casa / 100) * 1.0)

    def _matchup_over_ht(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive) -> float:
        ataque_ht_casa = casa.over_05_ht * 0.5 + casa.gols_ht * 25
        defesa_ht_fora = fora.over_05_ht * 0.3 + fora.sofre_ht * 15
        return min(100, (ataque_ht_casa + defesa_ht_fora) * 0.6)

    def _matchup_over_15(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive) -> float:
        over_casa = casa.over_15_ft * 0.5 + casa.gols_marcados * 15
        over_fora = fora.over_15_ft * 0.5 + fora.gols_marcados * 15
        return min(100, (over_casa + over_fora) * 0.5)

    def _matchup_over_25(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive) -> float:
        over_casa = casa.over_25_ft * 0.5 + casa.gols_marcados * 15 + casa.xg * 10
        over_fora = fora.over_25_ft * 0.5 + fora.gols_marcados * 15 + fora.xg * 10
        return min(100, (over_casa + over_fora) * 0.4)

    def _matchup_btts(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive) -> float:
        btts_casa = casa.btts * 0.3 + casa.gols_marcados * 10 + casa.gols_sofridos * 10
        btts_fora = fora.btts * 0.3 + fora.gols_marcados * 10 + fora.gols_sofridos * 10
        return min(100, (btts_casa + btts_fora) * 0.5)

    def _matchup_escanteios(self, casa: EstatisticasTimePreLive, fora: EstatisticasTimePreLive, lado: str) -> float:
        if lado == "casa":
            return min(100, casa.escanteios_favor * 12 + fora.escanteios_contra * 8)
        else:
            return min(100, fora.escanteios_favor * 12 + casa.escanteios_contra * 8)


class MarketEnginePreLive:
    def __init__(self):
        self.matchup_engine = MatchupEnginePreLive()

    def calcular_mercados(self, jogo: JogoPreLive) -> List[MercadoPreLive]:
        mercados = []

        tipos = [
            "Vitoria_Casa",
            "Vitoria_Fora",
            "Over_05_HT",
            "Over_15_FT",
            "Over_25_FT",
            "BTTS",
            "Escanteios_Casa",
            "Escanteios_Fora",
        ]

        for tipo in tipos:
            mercado = self._calcular_mercado(jogo, tipo)
            if mercado:
                mercados.append(mercado)

        return mercados

    def _calcular_mercado(self, jogo: JogoPreLive, tipo: str) -> Optional[MercadoPreLive]:
        casa = jogo.estatisticas_casa
        fora = jogo.estatisticas_fora

        matchup_score = self.matchup_engine.calcular_matchup(jogo, tipo)
        arce_score = self._calcular_arce(jogo, tipo)
        chama_score = self._calcular_chama(jogo, tipo)
        consistencia_score = self._calcular_consistencia(jogo, tipo)
        league_score = self._calcular_league(jogo, tipo)
        team_score = self._calcular_team(jogo, tipo)

        score_final = (
            matchup_score * 0.25 +
            arce_score * 0.20 +
            chama_score * 0.20 +
            consistencia_score * 0.15 +
            league_score * 0.10 +
            team_score * 0.10
        )

        score_final = max(0, min(100, score_final))

        return MercadoPreLive(
            nome=tipo,
            jogo=jogo,
            score=score_final,
            matchup_score=matchup_score,
            arce_score=arce_score,
            chama_score=chama_score,
            consistencia_score=consistencia_score,
            league_score=league_score,
            team_score=team_score,
            detalhes={
                "time_casa": casa.nome,
                "time_fora": fora.nome,
                "liga": jogo.liga,
            }
        )

    def _calcular_arce(self, jogo: JogoPreLive, tipo: str) -> float:
        casa = jogo.estatisticas_casa
        fora = jogo.estatisticas_fora

        if tipo == "Over_05_HT":
            return casa.over_05_ht * 0.5 + fora.over_05_ht * 0.3 + casa.gols_ht * 10

        if tipo in ["Vitoria_Casa", "Vitoria_Fora"]:
            return casa.gols_ht * 10 + casa.over_05_ht * 0.3

        return casa.gols_ht * 8 + fora.sofre_ht * 8

    def _calcular_chama(self, jogo: JogoPreLive, tipo: str) -> float:
        casa = jogo.estatisticas_casa
        fora = jogo.estatisticas_fora

        if tipo == "Over_25_FT":
            return casa.over_25_ft * 0.5 + fora.over_25_ft * 0.5

        if tipo == "BTTS":
            return casa.btts * 0.5 + fora.btts * 0.5

        return casa.gols_ft * 10 + fora.gols_ft * 10

    def _calcular_consistencia(self, jogo: JogoPreLive, tipo: str) -> float:
        casa = jogo.estatisticas_casa
        fora = jogo.estatisticas_fora

        forma = casa.forma * 5 + fora.forma * 5

        variabilidade = 0
        if casa.gols_marcados and casa.gols_sofridos:
            variabilidade = abs(casa.gols_marcados - casa.gols_sofridos)

        return min(100, forma + (100 - min(100, variabilidade * 10)))

    def _calcular_league(self, jogo: JogoPreLive, tipo: str) -> float:
        liga = jogo.liga.lower()
        if any(x in liga for x in ["premier", "championship", "bundesliga", "serie a", "la liga"]):
            return 95.0
        if any(x in liga for x in ["brasileirão", "brazil", "argentina", "netherlands", "portugal"]):
            return 85.0
        if any(x in liga for x in ["turkey", "greece", "denmark", "sweden", "norway"]):
            return 75.0
        return 70.0

    def _calcular_team(self, jogo: JogoPreLive, tipo: str) -> float:
        casa = jogo.estatisticas_casa.team_reliability or 80.0
        fora = jogo.estatisticas_fora.team_reliability or 80.0
        return (casa + fora) / 2


class CandidateSelectorPreLive:
    def __init__(self):
        self.market_engine = MarketEnginePreLive()

    def selecionar_candidatos(self, jogos: List[JogoPreLive]) -> Dict[str, Any]:
        todos_mercados = []

        for jogo in jogos:
            mercados = self.market_engine.calcular_mercados(jogo)
            for m in mercados:
                todos_mercados.append(m)

        melhores_por_jogo = {}
        for m in todos_mercados:
            chave = f"{m.jogo.time_casa} x {m.jogo.time_fora}"
            if chave not in melhores_por_jogo or m.score > melhores_por_jogo[chave].score:
                melhores_por_jogo[chave] = m

        candidatos = sorted(melhores_por_jogo.values(), key=lambda x: x.score, reverse=True)

        aprovados = [m for m in candidatos if m.score >= SCORE_MINIMO_APROVACAO]

        if aprovados:
            media = sum(m.score for m in aprovados) / len(aprovados)
            if media < MEDIA_MINIMA_DIA:
                log(f"⚠️ Média do dia {media:.1f} < {MEDIA_MINIMA_DIA}")
                return {"aprovados": [], "media": media, "total": len(candidatos)}

        return {
            "aprovados": aprovados,
            "media": sum(m.score for m in aprovados) / len(aprovados) if aprovados else 0,
            "total": len(candidatos),
            "todos_mercados": todos_mercados,
        }


class AnchorSelectorPreLive:
    def selecionar(self, aprovados: List[MercadoPreLive]) -> Tuple[List[MercadoPreLive], List[List[MercadoPreLive]]]:
        if len(aprovados) < QTD_MINIMA_JOGOS:
            return [], []

        ordenados = sorted(aprovados, key=lambda x: x.score, reverse=True)

        qtd_ancoras = min(4, len(ordenados))
        ancoras = ordenados[:qtd_ancoras]

        complementares = ordenados[qtd_ancoras:]

        complementares_com_mercados = []
        for comp in complementares:
            todos_mercados = [m for m in aprovados if m.jogo == comp.jogo]
            if todos_mercados:
                melhores = sorted(todos_mercados, key=lambda x: x.score, reverse=True)
                complementares_com_mercados.append(melhores[:QTD_MAXIMA_COMPLEMENTARES])

        return ancoras, complementares_com_mercados


class MultipleBuilderPreLive:
    def construir(self, ancoras: List[MercadoPreLive], complementares: List[List[MercadoPreLive]]) -> List[MultiplePreLive]:
        multiplas = []

        if not ancoras or not complementares:
            return multiplas

        complementares_validos = [c for c in complementares if c]

        if len(complementares_validos) < 1:
            return multiplas

        safe_mercados = list(ancoras) + [c[0] for c in complementares_validos if c]
        if safe_mercados:
            multiplas.append(MultiplePreLive(
                nome="SAFE",
                mercados=safe_mercados,
                score_medio=sum(m.score for m in safe_mercados) / len(safe_mercados),
                descricao="Mercados mais confiáveis de cada jogo"
            ))

        pro1_mercados = list(ancoras)
        for comp in complementares_validos:
            if len(comp) >= 2:
                pro1_mercados.append(comp[1])
            else:
                pro1_mercados.append(comp[0])

        if len(pro1_mercados) >= QTD_MINIMA_JOGOS:
            multiplas.append(MultiplePreLive(
                nome="PRO1",
                mercados=pro1_mercados,
                score_medio=sum(m.score for m in pro1_mercados) / len(pro1_mercados),
                descricao="Segunda melhor combinação de mercados"
            ))

        pro2_mercados = list(ancoras)
        for comp in complementares_validos:
            if len(comp) >= 3:
                pro2_mercados.append(comp[2])
            elif len(comp) >= 2:
                pro2_mercados.append(comp[1])
            else:
                pro2_mercados.append(comp[0])

        if len(pro2_mercados) >= QTD_MINIMA_JOGOS:
            multiplas.append(MultiplePreLive(
                nome="PRO2",
                mercados=pro2_mercados,
                score_medio=sum(m.score for m in pro2_mercados) / len(pro2_mercados),
                descricao="Terceira melhor combinação de mercados"
            ))

        diamond_mercados = list(ancoras)
        for i, comp in enumerate(complementares_validos):
            if len(comp) >= 3 and i % 3 == 0:
                diamond_mercados.append(comp[2])
            elif len(comp) >= 2 and i % 2 == 0:
                diamond_mercados.append(comp[1])
            else:
                diamond_mercados.append(comp[0])

        if len(diamond_mercados) >= QTD_MINIMA_JOGOS:
            multiplas.append(MultiplePreLive(
                nome="DIAMOND",
                mercados=diamond_mercados,
                score_medio=sum(m.score for m in diamond_mercados) / len(diamond_mercados),
                descricao="Combinação diversificada para maior retorno"
            ))

        return multiplas


def formatar_multipla_prelive(multiple: MultiplePreLive) -> str:
    linhas = [
        f"📊 **{multiple.nome}**",
        f"📈 Média: **{multiple.score_medio:.1f}%**",
        f"📝 {multiple.descricao}",
        "",
        "---",
        ""
    ]

    for i, m in enumerate(multiple.mercados, 1):
        nome_mercado = m.nome.replace("_", " ")

        linhas.append(
            f"{i}. **{m.jogo.time_casa} x {m.jogo.time_fora}**\n"
            f"   🎯 {nome_mercado} | Score: {m.score:.1f}%"
        )

    linhas.extend([
        "",
        "---",
        f"✅ Total: {len(multiple.mercados)} jogos",
        f"🏆 Liga: {multiple.mercados[0].jogo.liga if multiple.mercados else 'N/A'}",
        "",
        "📝 SIGA SUA GESTÃO DE BANCA",
        "⛔ APOSTE COM RESPONSABILIDADE",
    ])

    return "\n".join(linhas)


async def varrer_site_theoborges_prelive() -> None:
    log("🚀 INICIANDO VARREdura PRÉ-LIVE DO SITE THEOBORGES (COMANDO /prelive)")

    scraper = TeoBorgesScraperPreLive()

    if not await scraper.login():
        log("❌ Falha no login. Encerrando.")
        await client.send_message("me", "❌ Falha no login no clube.theoborges.com. Verifique TEOLOGIN_EMAIL e TEOLOGIN_SENHA.")
        return

    todos_links = []
    for dia in ["hoje", "amanha"]:
        links = await scraper.buscar_jogos(dia)
        todos_links.extend(links)

    if not todos_links:
        log("❌ Nenhum jogo encontrado.")
        await client.send_message("me", "❌ Nenhum jogo encontrado para hoje ou amanhã.")
        await scraper.close()
        return

    log(f"📊 Total de {len(todos_links)} jogos encontrados")

    jogos = []
    for url in todos_links[:30]:
        dados = await scraper.extrair_dados_jogo(url)
        if dados:
            estat_casa = EstatisticasTimePreLive(
                nome=dados["time_casa"],
                gols_marcados=dados["estatisticas"].get("casa", {}).get("media_gols", 0),
                gols_sofridos=dados["estatisticas"].get("casa", {}).get("gols_sofridos", 0),
                xg=dados["estatisticas"].get("casa", {}).get("xg", 0),
                over_05_ht=dados["estatisticas"].get("casa", {}).get("over_05_ht", 0),
                over_15_ft=dados["estatisticas"].get("casa", {}).get("over_15", 0),
                over_25_ft=dados["estatisticas"].get("casa", {}).get("over_25", 0),
                btts=dados["estatisticas"].get("casa", {}).get("btts", 0),
                escanteios_favor=dados["estatisticas"].get("casa", {}).get("escanteios", 0),
                gols_ht=dados["estatisticas"].get("casa", {}).get("gols_ht", 0),
                gols_ft=dados["estatisticas"].get("casa", {}).get("gols_ft", 0),
                sofre_ht=dados["estatisticas"].get("casa", {}).get("sofre_ht", 0),
                sofre_ft=dados["estatisticas"].get("casa", {}).get("sofre_ft", 0),
                team_reliability=80.0,
                forma=0,
            )

            estat_fora = EstatisticasTimePreLive(
                nome=dados["time_fora"],
                gols_marcados=dados["estatisticas"].get("fora", {}).get("media_gols", 0),
                gols_sofridos=dados["estatisticas"].get("fora", {}).get("gols_sofridos", 0),
                xg=dados["estatisticas"].get("fora", {}).get("xg", 0),
                over_05_ht=dados["estatisticas"].get("fora", {}).get("over_05_ht", 0),
                over_15_ft=dados["estatisticas"].get("fora", {}).get("over_15", 0),
                over_25_ft=dados["estatisticas"].get("fora", {}).get("over_25", 0),
                btts=dados["estatisticas"].get("fora", {}).get("btts", 0),
                escanteios_favor=dados["estatisticas"].get("fora", {}).get("escanteios", 0),
                gols_ht=dados["estatisticas"].get("fora", {}).get("gols_ht", 0),
                gols_ft=dados["estatisticas"].get("fora", {}).get("gols_ft", 0),
                sofre_ht=dados["estatisticas"].get("fora", {}).get("sofre_ht", 0),
                sofre_ft=dados["estatisticas"].get("fora", {}).get("sofre_ft", 0),
                team_reliability=80.0,
                forma=0,
            )

            jogo = JogoPreLive(
                time_casa=dados["time_casa"],
                time_fora=dados["time_fora"],
                liga=dados["liga"],
                data="",
                horario="",
                estatisticas_casa=estat_casa,
                estatisticas_fora=estat_fora,
                odds=dados.get("odds", {}),
                url=dados["url"],
            )
            jogos.append(jogo)

    log(f"📊 Dados extraídos de {len(jogos)} jogos")

    await scraper.close()

    if len(jogos) < 2:
        log("❌ Poucos jogos com dados disponíveis")
        await client.send_message("me", "❌ Dados insuficientes para montar múltiplas.")
        return

    selector = CandidateSelectorPreLive()
    resultado = selector.selecionar_candidatos(jogos)
    aprovados = resultado["aprovados"]

    log(f"📊 {len(aprovados)} jogos aprovados (Score ≥ {SCORE_MINIMO_APROVACAO})")

    if len(aprovados) < QTD_MINIMA_JOGOS:
        log(f"❌ Menos de {QTD_MINIMA_JOGOS} jogos aprovados")
        await client.send_message(
            "me",
            f"❌ Apenas {len(aprovados)} jogos aprovados. Mínimo: {QTD_MINIMA_JOGOS}."
        )
        return

    anchor_selector = AnchorSelectorPreLive()
    ancoras, complementares = anchor_selector.selecionar(aprovados)

    if not ancoras or not complementares:
        log("❌ Não foi possível selecionar âncoras ou complementares")
        await client.send_message("me", "❌ Não foi possível montar múltiplas.")
        return

    log(f"📊 {len(ancoras)} âncoras, {len(complementares)} complementares")

    builder = MultipleBuilderPreLive()
    multiplas = builder.construir(ancoras, complementares)

    if not multiplas:
        log("❌ Nenhuma múltipla construída")
        await client.send_message("me", "❌ Não foi possível construir múltiplas.")
        return

    log(f"✅ {len(multiplas)} múltiplas construídas")

    for multiple in multiplas:
        msg = formatar_multipla_prelive(multiple)
        await client.send_message("me", msg)
        log(f"📤 {multiple.nome} enviada")
        await asyncio.sleep(1)

    log("✅ Processo Pré-Live concluído!")


# =========================================================
# MAIN
# =========================================================

def logar_versao_inicial() -> None:
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log(f"🚀 VERSAO_COUTIPS_ATIVA = {VERSAO_COUTIPS}")
    log("✅ MUDANÇAS IMPLEMENTADAS:")
    log("  1. V29 cooldown com persistência")
    log("  2. V17 salvamento atômico")
    log("  3. V26 backup no Telegram")
    log("  4. Sniper bloqueios na auditoria")
    log("  5. Decaimento pressão 2ºP corrigido (U5/U10)")
    log("  6. Zebra exige 3 provas")
    log("  7. extrair_jogo mais robusto")
    log("  8. extrair_odds múltiplos formatos")
    log("  9. extrair_pressao_alfa múltiplos nomes")
    log(" 10. Sniper antes do V29")
    log(" 11. V26 aplicado antes do score")
    log(" 12. Confirmação IP usa >")
    log(" 13. PRESSAO_PREMIADA_MORREU -10")
    log(" 14. Placar largo com exceção")
    log(" 15. Validação de variáveis")
    log(" 16. HTML escapado")
    log(" 17. CSV sanitizado")
    log(" 18. Prompt sanitizado")
    log(" 19. FloodWait backoff")
    log(" 20. JSON limitado 500")
    log(" 21. Cache robusto")
    log(" 22-25. Refatorações")
    log(f"📊 Corte HT={CORTE_GOL_HT}% | Corte FT={CORTE_GOL_FT}%")
    log(f"📡 Canais: grátis={FREE_CHANNEL} | completo={COMPLETE_CHANNEL}")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


async def main() -> None:
    global tarefa_envio

    # Mudança 15: Validar variáveis de ambiente
    validar_env()

    logar_versao_inicial()
    garantir_csv()
    v17_carregar_estado()
    v29_carregar_cooldowns()

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
            log(f"⚠️ V24 handler_outgoing erro | {type(e).__name__}: {e}")

    @client.on(events.NewMessage(incoming=True))
    async def handler_teste(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/teste":
            log("📩 /teste RECEBIDO (INCOMING)")
            await varrer_site_theoborges()

    @client.on(events.NewMessage(from_users='me'))
    async def handler_teste_me(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/teste":
            log("📩 /teste RECEBIDO (CHAT PRIVADO)")
            await varrer_site_theoborges()

    @client.on(events.NewMessage(incoming=True))
    async def handler_prelive(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/prelive":
            log("📩 /prelive RECEBIDO (INCOMING)")
            await varrer_site_theoborges_prelive()

    @client.on(events.NewMessage(from_users='me'))
    async def handler_prelive_me(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/prelive":
            log("📩 /prelive RECEBIDO (CHAT PRIVADO)")
            await varrer_site_theoborges_prelive()

    # Versão original do varrer_site_theoborges (mantida inalterada)
    async def varrer_site_theoborges():
        log("🚀 INICIANDO VARREdura DO SITE THEOBORGES (COMANDO MANUAL)")

        import httpx
        from bs4 import BeautifulSoup

        dias_para_tentar = ["hoje", "amanha"]
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        encontrou_jogos = False

        for dia in dias_para_tentar:
            url_lista = f"https://clube.theoborges.com/matches?dia={dia}"
            log(f"🔄 Tentando buscar jogos para: {dia}")

            async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client_http:
                try:
                    response = await client_http.get(url_lista, headers=headers)
                    if response.status_code != 200:
                        log(f"⚠️ Erro ao acessar lista de jogos ({dia}): {response.status_code}")
                        continue
                except Exception as e:
                    log(f"❌ Erro na requisição para {dia}: {e}")
                    continue

            soup = BeautifulSoup(response.text, "html.parser")

            links_jogos = []

            for link in soup.select(".match-row"):
                href = link.get("href")
                if href and "/game/" in href:
                    url_completa = f"https://clube.theoborges.com{href}"
                    if url_completa not in links_jogos:
                        links_jogos.append(url_completa)

            if not links_jogos:
                for link in soup.select("a[href*='/game/']"):
                    href = link.get("href")
                    if href:
                        url_completa = f"https://clube.theoborges.com{href}"
                        if url_completa not in links_jogos:
                            links_jogos.append(url_completa)

            if not links_jogos:
                for link in soup.find_all("a", href=True):
                    href = link.get("href")
                    if href and "/game/" in href:
                        url_completa = f"https://clube.theoborges.com{href}"
                        if url_completa not in links_jogos:
                            links_jogos.append(url_completa)

            if not links_jogos:
                log(f"ℹ️ Nenhum link de jogo encontrado para {dia}.")
                if dia == dias_para_tentar[-1]:
                    with open("erro_diagnostico.html", "w", encoding="utf-8") as f:
                        f.write(response.text)
                    log("📄 HTML da última tentativa salvo em 'erro_diagnostico.html'.")
                continue

            log(f"📊 Encontrados {len(links_jogos)} jogos para analisar em {dia}.")
            jogos_filtrados = []

            for url in links_jogos[:20]:
                try:
                    response_jogo = await client_http.get(url, headers=headers, timeout=15)
                    if response_jogo.status_code != 200:
                        continue
                    soup_jogo = BeautifulSoup(response_jogo.text, "html.parser")

                    titulo = soup_jogo.select_one(".match-name")
                    nome_jogo = titulo.text.strip() if titulo else "Jogo não identificado"

                    media_gols = 0.0
                    media_gols_element = soup_jogo.select_one(".pg-tstable-row .pgcv:contains('Média total de Gols')")
                    if media_gols_element:
                        try:
                            media_gols = float(media_gols_element.text.strip().replace(',', '.'))
                        except:
                            pass

                    over25 = 0
                    over25_element = soup_jogo.select_one(".pg-tstable-row:contains('Over 2.5 Gols') .pg-tstable-value-home")
                    if over25_element:
                        try:
                            over25 = int(over25_element.text.strip().replace('%', ''))
                        except:
                            pass

                    if media_gols >= 2.5 or over25 >= 60:
                        jogos_filtrados.append({
                            "nome": nome_jogo,
                            "media_gols": media_gols,
                            "over25": over25
                        })

                    await asyncio.sleep(1)
                except Exception as e:
                    log(f"⚠️ Erro processando: {e}")
                    continue

            if jogos_filtrados:
                encontrou_jogos = True
                jogos_filtrados.sort(key=lambda x: x["media_gols"], reverse=True)
                total = len(jogos_filtrados)

                if total >= 8:
                    top, anc, comp = jogos_filtrados[:8], 4, 4
                elif total == 7:
                    top, anc, comp = jogos_filtrados[:7], 3, 4
                elif total == 6:
                    top, anc, comp = jogos_filtrados[:6], 3, 3
                elif total == 5:
                    top, anc, comp = jogos_filtrados[:5], 2, 3
                else:
                    top, anc, comp = jogos_filtrados[:total], 2, total - 2

                mensagem = f"📢 **MÚLTIPLA DE TESTE ({total} JOGOS) — {dia.upper()}** 📢\n\n"
                mensagem += f"**ÂNCORAS ({anc}):**\n"
                for i in range(anc):
                    j = top[i]
                    mercado = "Over 2.5" if j["over25"] >= 65 else "Over 1.5" if j["media_gols"] >= 2.5 else "Escanteios"
                    mensagem += f"{i+1}. {j['nome']} -> **{mercado}**\n"

                mensagem += f"\n**COMPLEMENTOS ({comp}):**\n"
                for i in range(anc, anc + comp):
                    j = top[i]
                    mercado = "Over 2.5" if j["over25"] >= 65 else "Over 1.5" if j["media_gols"] >= 2.5 else "Escanteios"
                    mensagem += f"{i+1}. {j['nome']} -> **{mercado}**\n"

                await client.send_message("me", mensagem)
                log(f"✅ Múltipla enviada com base em {dia}.")
                break

        if not encontrou_jogos:
            await client.send_message("me", "❌ Nenhum jogo encontrado para hoje ou amanhã. Verifique o arquivo 'erro_diagnostico.html' no Railway.")
            log("❌ Nenhum jogo encontrado para hoje ou amanhã. HTML salvo para diagnóstico.")

    log("🚀 INICIANDO BOT")
    await client.start()
    log("✅ TELEGRAM CONECTADO COM SUCESSO")
    await v26_carregar_estado_telegram()
    log(f"🤖 OpenAI {'ATIVA' if OPENAI_HABILITADO and OPENAI_API_KEY else 'DESATIVADA'} ({OPENAI_MODEL})")
    log(f"📡 Canais ativos: principal={TARGET_CHANNEL} | confirmação={CONFIRMATION_CHANNEL}")
    log("📡 Comandos disponíveis: /teste (ao vivo) | /prelive (pré-live) | /auditoria")
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
