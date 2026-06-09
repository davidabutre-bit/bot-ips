# -*- coding: utf-8 -*-
"""
COUTIPS / ALFA — GOAT V008 CONSOLIDADO
Base consolidada a partir dos três V7: ChatGPT + Claude + DeepSeek

Objetivo:
- Manter o score central preservado.
- Aplicar mudanças binárias como binárias.
- Corrigir parser/período sem matar jogo bom por erro bobo.
- Bloquear apenas U18/U19/U20 sem mercado operacional.
- Criar fluxo limpo CHAMA_FT → espera → BOT_FT CONFIRMAÇÃO → comparação → decisão.
- Manter HT como camada premium de massacre contextual real.
- Registrar motivos internos para auditoria.

Start command: python main.py

Mudanças V006:
- Teto Python 96 / IA 95
- HT: exige que favorito seja o lado pressionante (bloqueia zebra dominando)
- Threshold odd para bloqueio HT: > 1.60 (preserva jogos equilibrados)
- Mantém FT inalterado (75.9% de acerto na auditoria)

Start command: python main.py
"""

from __future__ import annotations

import asyncio
import csv
import html
import hashlib
import logging
import os
import re
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

try:
    from telethon.errors.common import TypeNotFoundError
except Exception:  # pragma: no cover
    class TypeNotFoundError(Exception):
        pass


# =========================================================
# VERSÃO / CONFIGURAÇÃO BASE
# =========================================================

VERSAO_COUTIPS = "ALFA_COUTIPS_2026_06_08_V28_SNIPER_V2"

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

# V019 — segurança do comando de auditoria.
# Por padrão, somente comandos OUTGOING do próprio usuário são aceitos.
# Se quiser liberar chat/user específico, configure AUDITORIA_CHAT_IDS com IDs separados por vírgula.
AUDITORIA_CHAT_IDS_RAW = os.getenv("AUDITORIA_CHAT_IDS", "").strip()
AUDITORIA_CHAT_IDS = {
    int(x.strip())
    for x in AUDITORIA_CHAT_IDS_RAW.split(",")
    if x.strip().lstrip("-").isdigit()
}

MODO_TESTE = os.getenv("MODO_TESTE", "false").lower() == "true"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
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
# V008 — FLAGS DE ROLLBACK / SEGURANÇA OPERACIONAL
# =========================================================
# Mantém o V8 seguro para teste em canal técnico. Se algo se comportar mal,
# é possível desligar apenas a camada nova sem reverter o arquivo inteiro.

HABILITAR_CONFIRMACAO_V2 = os.getenv("HABILITAR_CONFIRMACAO_V2", "true").lower() == "true"
HABILITAR_BLOQUEIO_BASE_SEM_MERCADO = os.getenv("HABILITAR_BLOQUEIO_BASE_SEM_MERCADO", "true").lower() == "true"
HABILITAR_BLOQUEIO_PARSER_CRITICO = os.getenv("HABILITAR_BLOQUEIO_PARSER_CRITICO", "true").lower() == "true"
HABILITAR_HT_PREMIUM_V2 = os.getenv("HABILITAR_HT_PREMIUM_V2", "true").lower() == "true"

# V009 — freio de inflação do score.
# Mantém o cérebro/fluxo do V8 e apenas impede que jogos medianos virem 90/96.
HABILITAR_SCORE_V9 = os.getenv("HABILITAR_SCORE_V9", "true").lower() == "true"
V9_CAP_JOGO_ABERTO = int(os.getenv("V9_CAP_JOGO_ABERTO", "90"))
V9_CAP_APROVADO_COMUM = int(os.getenv("V9_CAP_APROVADO_COMUM", "88"))
V9_CAP_LIGA_UNDER = int(os.getenv("V9_CAP_LIGA_UNDER", "84"))
V9_CAP_LIGA_UNDER_FRACA = int(os.getenv("V9_CAP_LIGA_UNDER_FRACA", "80"))
V9_CAP_FINALIZACAO_BAIXA = int(os.getenv("V9_CAP_FINALIZACAO_BAIXA", "86"))
V9_CAP_PRESSAO_RECENTE_FRACA = int(os.getenv("V9_CAP_PRESSAO_RECENTE_FRACA", "86"))

# V010 — trava de prova extra para ligas UNDER/Sul-Americanas problemáticas.
# Argentina excluída da régua rígida por comportamento melhor na auditoria.
HABILITAR_UNDER_PROVA_EXTRA = os.getenv("HABILITAR_UNDER_PROVA_EXTRA", "true").lower() == "true"

# V013 — auditoria HTML automática.
# Gera JSON diário + HTML diário em /data (ou pasta local como fallback).
# Envia HTMLs às 00:05 para CONFIRMATION_CHANNEL. Zero impacto no score/funil.
HABILITAR_V13_AUDITORIA_HTML = os.getenv("HABILITAR_V13_AUDITORIA_HTML", "true").lower() == "true"

# V014 — VOLUME_FT: radar interno da CornerPro com filtro exclusivo antes do score.
# Reprovados morrem silenciosamente. Aprovados seguem fluxo ALFA normal.
HABILITAR_VOLUME_FT = os.getenv("HABILITAR_VOLUME_FT", "true").lower() == "true"

# V026 — FAV_NAO_PRESSIONANTE: penaliza score FT onde favorito não é o lado pressionante.
# Modo auditoria: não bloqueia — penaliza -8 no score e registra motivo no CSV/HTML.
# Baseado em auditoria: 7 reds eliminados com custo de 11 greens (72% → 74.4%).
# Aplica-se ao FT geral (ALFA_FT, CHAMA_FT). Não afeta HT nem CONF.
HABILITAR_V26_FAV_NAO_PRESSIONANTE = os.getenv("HABILITAR_V26_FAV_NAO_PRESSIONANTE", "true").lower() == "true"
V26_FAV_NAO_PRESSIONANTE_PENALIDADE = int(os.getenv("V26_FAV_NAO_PRESSIONANTE_PENALIDADE", "8"))

# V026 — PERSISTENCIA_TELEGRAM: salva estado dos contadores do grupo grátis e auditoria
# via mensagem no canal interno, sobrevivendo a restarts do Railway sem precisar de Volume.
HABILITAR_V26_PERSISTENCIA_TELEGRAM = os.getenv("HABILITAR_V26_PERSISTENCIA_TELEGRAM", "true").lower() == "true"
# Tag usada para identificar mensagens de estado no canal interno.
V26_ESTADO_TAG = "#COUTIPS_ESTADO_V26"

# =========================================================
# V027 — REDUÇÃO DE VOLUME / DISCIPLINA CONTEXTUAL
# Camada cirúrgica pós-auditoria de 195 jogos.
# Não altera o núcleo do score; aplica portões/limites com rollback por flag.
# =========================================================
HABILITAR_V27_POS70_OFF = os.getenv("HABILITAR_V27_POS70_OFF", "true").lower() == "true"
HABILITAR_V27_ALAVANCAGEM_SO_FT = os.getenv("HABILITAR_V27_ALAVANCAGEM_SO_FT", "true").lower() == "true"
HABILITAR_V27_HT_MODERADO_ALAV_OBS = os.getenv("HABILITAR_V27_HT_MODERADO_ALAV_OBS", "true").lower() == "true"
HABILITAR_V27_UNDER_TETO_82 = os.getenv("HABILITAR_V27_UNDER_TETO_82", "true").lower() == "true"
HABILITAR_V27_CONTINUIDADE_POS_GOL = os.getenv("HABILITAR_V27_CONTINUIDADE_POS_GOL", "true").lower() == "true"
HABILITAR_V27_REFINO_PLACAR_FT = os.getenv("HABILITAR_V27_REFINO_PLACAR_FT", "true").lower() == "true"
HABILITAR_V27_EMPATE_MAIS_RIGIDO = os.getenv("HABILITAR_V27_EMPATE_MAIS_RIGIDO", "true").lower() == "true"
HABILITAR_V27_PERDEDOR_UM_EXIGE_REACAO = os.getenv("HABILITAR_V27_PERDEDOR_UM_EXIGE_REACAO", "true").lower() == "true"
V27_UNDER_TETO_SCORE = int(os.getenv("V27_UNDER_TETO_SCORE", "82"))

# =========================================================
# V028 — FILTRO DE QUALIDADE CONTEXTUAL
# Base: V27 com refinamentos adicionais.
# Baseado na auditoria 01-07/06 (195 jogos) e simulacao de 217 alertas.
# Taxa simulada: 94,3% (82G/5R em 87 aprovados).
# Nao reconstroi o score. Reforça funil e adiciona DNA projetado como ajuste leve.
# =========================================================
HABILITAR_V28 = os.getenv("HABILITAR_V28", "true").lower() == "true"
HABILITAR_V28_HT_MODERADO_BLOQUEIO = os.getenv("HABILITAR_V28_HT_MODERADO_BLOQUEIO", "true").lower() == "true"
HABILITAR_V28_UNDER_BLOQUEIO_FORTE = os.getenv("HABILITAR_V28_UNDER_BLOQUEIO_FORTE", "true").lower() == "true"
HABILITAR_V28_DNA_PROJETADO = os.getenv("HABILITAR_V28_DNA_PROJETADO", "true").lower() == "true"
HABILITAR_V28_RELOGIO_FT = os.getenv("HABILITAR_V28_RELOGIO_FT", "true").lower() == "true"
V28_UNDER_TETO_SCORE = int(os.getenv("V28_UNDER_TETO_SCORE", str(V27_UNDER_TETO_SCORE)))

# V012 — camadas cirúrgicas: grupo grátis, ALAVANCAGEM, Austrália especial e HT-2.
# Mantém score, funil, IA e parser centrais preservados.
HABILITAR_V11_GRUPO_GRATUITO = os.getenv("HABILITAR_V11_GRUPO_GRATUITO", "true").lower() == "true"
HABILITAR_V11_ALAVANCAGEM = os.getenv("HABILITAR_V11_ALAVANCAGEM", "true").lower() == "true"
HABILITAR_V11_AUSTRALIA = os.getenv("HABILITAR_V11_AUSTRALIA", "true").lower() == "true"
HABILITAR_V11_HT_CORRECOES = os.getenv("HABILITAR_V11_HT_CORRECOES", "true").lower() == "true"
HABILITAR_HT_BONUS_SUPER_FAV_VENCENDO = os.getenv("HABILITAR_HT_BONUS_SUPER_FAV_VENCENDO", "true").lower() == "true"

# Canal completo = recebe todos os aprovados. Por padrão reaproveita o antigo canal de confirmação.
COMPLETE_CHANNEL = os.getenv("COMPLETE_CHANNEL") or CONFIRMATION_CHANNEL
FREE_CHANNEL = os.getenv("FREE_CHANNEL") or TARGET_CHANNEL

# Janelas do grupo grátis.
V11_FREE_J1_LIMITE = int(os.getenv("V11_FREE_J1_LIMITE", "3"))   # 06:30–12:00
V11_FREE_J2_LIMITE = int(os.getenv("V11_FREE_J2_LIMITE", "4"))   # 12:00–18:00
V11_FREE_J3_LIMITE = int(os.getenv("V11_FREE_J3_LIMITE", "4"))   # 18:00–23:00
V11_FREE_GOL_VANTAGEM_MIN = int(os.getenv("V11_FREE_GOL_VANTAGEM_MIN", "10"))
V11_FREE_COOLDOWN_JOGO_HORAS = int(os.getenv("V11_FREE_COOLDOWN_JOGO_HORAS", "24"))
V11_TZ_OFFSET_HORAS = int(os.getenv("V11_TZ_OFFSET_HORAS", "-4"))

# V12: HT-2 corrigido. HT-3 fica fora desta versão operacional.
# V12_2: bônus cirúrgico para super favorito vencendo por 1 no HT com pressão extrema.

# Segurança do parser: abaixo disso o dado está provavelmente quebrado,
# não apenas com rótulo errado. Rótulo errado corrigível segue normalmente.
PARSER_CONFIANCA_CRITICA = int(os.getenv("PARSER_CONFIANCA_CRITICA", "2"))

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

# V007 — estado específico para jogos FT aguardando confirmação pós-gol.
# Não substitui a janela curta de decisão; é memória operacional entre CHAMA_FT/ALFA_FT e BOT_FT CONFIRMAÇÃO.
pendentes_confirmacao_ft: Dict[str, Dict[str, Any]] = {}
tarefas_timeout_confirmacao_ft: Dict[str, asyncio.Task] = {}

# V011 — estado do grupo grátis.
# V017 — contadores e cooldowns persistidos em JSON para sobreviver a restarts.
v11_gratis_contadores: Dict[str, int] = {}
v11_gratis_enviados_por_jogo: Dict[str, float] = {}

_V17_ESTADO_FILE = "v17_gratis_estado.json"


def _v17_estado_path() -> Path:
    """Caminho do arquivo de estado persistente — usa /data ou fallback local."""
    return _data_dir() / _V17_ESTADO_FILE


def v17_carregar_estado() -> None:
    """Carrega contadores e cooldowns do JSON ao iniciar/reiniciar."""
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
    """Persiste contadores e cooldowns imediatamente após cada envio."""
    try:
        dados = {
            "contadores": dict(v11_gratis_contadores),
            "cooldowns": dict(v11_gratis_enviados_por_jogo),
            "atualizado_em": datetime.now().isoformat(),
        }
        _v17_estado_path().write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log(f"⚠️ V17 salvar_estado erro | {type(e).__name__}: {e}")


# =========================================================
# V026 — PERSISTÊNCIA VIA TELEGRAM
# Salva estado dos contadores no canal interno como backup.
# Sobrevive a restarts mesmo sem Volume /data no Railway.
# Lê a última mensagem com tag V26_ESTADO_TAG ao iniciar.
# =========================================================

_v26_msg_estado_id: Optional[int] = None  # ID da mensagem de estado no Telegram


async def v26_salvar_estado_telegram() -> None:
    """Salva estado dos contadores como mensagem no canal interno."""
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
                return
            except Exception:
                pass  # Se não conseguir editar, envia nova
        msg = await client.send_message(CONFIRMATION_CHANNEL, texto)
        _v26_msg_estado_id = msg.id
        log(f"💾 V26 estado salvo no Telegram | msg_id={_v26_msg_estado_id}")
    except Exception as e:
        log(f"⚠️ V26 salvar_estado_telegram erro | {type(e).__name__}: {e}")


async def v26_carregar_estado_telegram() -> None:
    """Lê o último estado salvo no canal interno ao iniciar."""
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
                log(f"📂 V26 estado carregado do Telegram | msg_id={msg.id} | contadores={len(v11_gratis_contadores)} | cooldowns={len(v11_gratis_enviados_por_jogo)}")
                return
        log("📂 V26 estado Telegram: nenhuma mensagem encontrada, iniciando zerado")
    except Exception as e:
        log(f"⚠️ V26 carregar_estado_telegram erro | {type(e).__name__}: {e}")


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
    log("✅ GOAT V12 FINAL — correção HT-2 + grupo grátis + ALAVANCAGEM + Austrália")
    log("🛡️ Parser: minuto manda no período + observações internas")
    log(f"🛡️ Confirmação V2: {'ATIVA' if HABILITAR_CONFIRMACAO_V2 else 'DESATIVADA'}")
    log(f"🛡️ Bloqueio base U18/U19/U20: {'ATIVO' if HABILITAR_BLOQUEIO_BASE_SEM_MERCADO else 'DESATIVADO'}")
    log(f"🛡️ Parser crítico: {'BLOQUEIA' if HABILITAR_BLOQUEIO_PARSER_CRITICO else 'SÓ OBSERVA'}")
    log(f"🛡️ HT Premium V2: {'ATIVO' if HABILITAR_HT_PREMIUM_V2 else 'DESATIVADO'}")
    log(f"🧮 Score V9 anti-inflação: {'ATIVO' if HABILITAR_SCORE_V9 else 'DESATIVADO'}")
    log(f"🛡️ UNDER prova extra: {'ATIVA' if HABILITAR_UNDER_PROVA_EXTRA else 'DESATIVADA'}")
    log("🛡️ Teto Python 96 / IA 95")
    log(f"📊 Corte HT={CORTE_GOL_HT}% | Corte FT={CORTE_GOL_FT}%")
    log(f"📤 Canal grátis: {FREE_CHANNEL}")
    log(f"🧪 Canal completo: {COMPLETE_CHANNEL}")
    log(f"🆓 V12 grupo grátis: {'ATIVO' if HABILITAR_V11_GRUPO_GRATUITO else 'DESATIVADO'} | limites={V11_FREE_J1_LIMITE}/{V11_FREE_J2_LIMITE}/{V11_FREE_J3_LIMITE}")
    log(f"🔺 V12 ALAVANCAGEM: {'ATIVA' if HABILITAR_V11_ALAVANCAGEM else 'DESATIVADA'}")
    log(f"🇦🇺 V12 Austrália especial: {'ATIVA' if HABILITAR_V11_AUSTRALIA else 'DESATIVADA'}")
    log(f"🧰 V12 HT-2 ap_diff por lado avaliado: {'ATIVO' if HABILITAR_V11_HT_CORRECOES else 'DESATIVADO'}")
    log(f"➕ V12_2 bônus HT super favorito vencendo por 1: {'ATIVO' if HABILITAR_HT_BONUS_SUPER_FAV_VENCENDO else 'DESATIVADO'}")
    log(f"📄 V13 auditoria HTML automática: {'ATIVA' if HABILITAR_V13_AUDITORIA_HTML else 'DESATIVADA'} | dir={_data_dir()}")
    log(f"🔊 V14 VOLUME_FT: {'ATIVO' if HABILITAR_VOLUME_FT else 'DESATIVADO'}")
    log(f"📋 V15 comando auditoria: ATIVO | /auditoria ou 'auditoria' no canal interno")
    log(f"📤 V16 handler outgoing: ATIVO | exclusivo para comando auditoria")
    log(f"💾 V17 persistência contadores grátis: ATIVA | arquivo={_V17_ESTADO_FILE}")
    log("🧱 V18 regra volume/grátis: favorito vencendo só passa em cenário EXTREMO")
    log("🧭 V20 detector estratégia robusto: VOLUME_FT não cai mais como ALFA_FT/HT")
    log("🧱 V21 VOLUME_FT: favorito vencendo por 1+ só passa em cenário EXTREMO")
    log(f"📡 V24 handler canal auditoria: ATIVO | IDs autorizados={AUDITORIA_CHAT_IDS}")
    log("🔧 V25 fixes: link CornerPro, botões HTML, HT duplicado, alavancagem HT restrita")
    log(f"🚫 V26 FAV_NAO_PRESSIONANTE FT: {'ATIVO' if HABILITAR_V26_FAV_NAO_PRESSIONANTE else 'DESATIVADO'}")
    log(f"💾 V26 persistência Telegram: {'ATIVA' if HABILITAR_V26_PERSISTENCIA_TELEGRAM else 'DESATIVADA'} | tag={V26_ESTADO_TAG}")
    log(f"🧹 V27 POS70 OFF: {'ATIVO' if HABILITAR_V27_POS70_OFF else 'DESATIVADO'}")
    log(f"🔺 V27 ALAVANCAGEM só FT: {'ATIVA' if HABILITAR_V27_ALAVANCAGEM_SO_FT else 'DESATIVADA'}")
    log(f"💎 V27 HT_MODERADO→HT_ALAVANCAGEM observação: {'ATIVO' if HABILITAR_V27_HT_MODERADO_ALAV_OBS else 'DESATIVADO'}")
    log(f"🟡 V27 UNDER teto {V27_UNDER_TETO_SCORE}: {'ATIVO' if HABILITAR_V27_UNDER_TETO_82 else 'DESATIVADO'}")
    log(f"🧬 V27 continuidade pós-gol: {'ATIVA' if HABILITAR_V27_CONTINUIDADE_POS_GOL else 'DESATIVADA'}")
    log(f"🎯 V27 refino placar/empate/perdedor+1 FT: {'ATIVO' if HABILITAR_V27_REFINO_PLACAR_FT else 'DESATIVADO'}")
    log(f"⛔ V28 HT_MODERADO bloqueio total: {'ATIVO' if HABILITAR_V28 and HABILITAR_V28_HT_MODERADO_BLOQUEIO else 'DESATIVADO'}")
    log(f"🚫 V28 UNDER bloqueio forte: {'ATIVO' if HABILITAR_V28 and HABILITAR_V28_UNDER_BLOQUEIO_FORTE else 'DESATIVADO'}")
    log(f"🧬 V28 DNA projetado: {'ATIVO' if HABILITAR_V28 and HABILITAR_V28_DNA_PROJETADO else 'DESATIVADO'}")
    log(f"⏱ V28 relógio FT janela 81: {'ATIVO' if HABILITAR_V28 and HABILITAR_V28_RELOGIO_FT else 'DESATIVADO'}")
    log("🎯 SNIPER V2: ATIVO | leitura separada, pressão premiada/gol contra fluxo/necessidade")
    log("🏷️ Auditoria visual: ⚡ ARCE HT | 1T / 🔥 CHAMA FT | 2T / 📊 VOLUME FT | 2T / 🎯 SNIPER | 2T")
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

    # V011 — limpeza do cooldown do grupo grátis.
    removidos = 0
    for k in list(v11_gratis_enviados_por_jogo.keys()):
        ts = float(v11_gratis_enviados_por_jogo.get(k, 0) or 0)
        if agora - ts > V11_FREE_COOLDOWN_JOGO_HORAS * 3600:
            v11_gratis_enviados_por_jogo.pop(k, None)
            removidos += 1
    if removidos > 0:
        v17_salvar_estado()  # V17 — persiste após limpeza de expirados

    # Pendências FT precisam durar mais que a janela curta, pois aguardam confirmação até ~81/82.
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

    # campos calculados
    liga: str = "NEUTRA"
    lado_favorito: str = "DESCONHECIDO"
    odd_favorito: float = 0.0
    lado_zebra: str = "DESCONHECIDO"
    lado_dominante: str = "EQUILIBRADO"
    lado_pressionante: str = "DESCONHECIDO"
    valor_pos_evento_classe: str = "SEM_VALOR_ESPECIAL"
    valor_pos_evento_motivo: str = ""
    protecao_ia_ativa: bool = False
    # Confiança do parser: quantos campos críticos vieram com valor real (0–8).
    # Usado para detectar silenciosamente falhas de formato da CornerPro.
    parser_confianca: int = 0
    parser_observacoes: List[str] = field(default_factory=list)
    fluxo_decisao: str = "NORMAL"
    fluxo_motivo: str = ""

    # V011 — campos auxiliares para logs/CSV/roteamento.
    previsao_over05_ht: float = 0.0
    grupo_gratuito: str = "NAO"
    motivo_grupo_gratuito: str = ""
    alavancagem: str = "NAO"
    motivo_alavancagem: str = ""
    australia_leitura: str = ""
    motivo_australia: str = ""
    destino_final: str = ""

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
    """Detector defensivo de VOLUME_FT no texto bruto/cabeçalho.

    Aceita VOLUME_FT, VOLUME FT, VOLUME-FT, VOLUMEFT, BOT_VOLUME_FT.
    Usado em duas camadas: detector principal e fail-safe antes do processamento.
    """
    raw = remover_acentos(texto or "").upper()
    primeiros = raw[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", primeiros)
    return bool(
        "VOLUMEFT" in compacto
        or "BOTVOLUMEFT" in compacto
        or re.search(r"\b(?:BOT\s*)?VOLUME\s*[-_ ]*\s*FT\b", primeiros)
    )


def contem_pos70_bruto(texto: str) -> bool:
    """V27 — identifica alertas POS70/Pós 70 antes do score.

    POS70 foi considerado redundante na auditoria operacional. A flag
    HABILITAR_V27_POS70_OFF permite desligar essa porta sem mexer nos demais FT.
    """
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
    """V27 — identifica HT_MODERADO para transformá-lo em HT_ALAVANCAGEM em observação."""
    raw = remover_acentos(texto or "").upper()[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", raw)
    return "HTMODERADO" in compacto or bool(re.search(r"\bHT\s*[-_ ]*MODERADO\b", raw))


def contem_sniper_bruto(texto: str) -> bool:
    """SNIPER V2 — identifica o bot 🎯 SNIPER sem depender do emoji.

    Aceita: SNIPER, SNIPER_FT, 🎯 SNIPER, BOT_SNIPER.
    """
    raw = remover_acentos(texto or "").upper()[:350].replace("_", " ")
    compacto = re.sub(r"[^A-Z0-9]+", "", raw)
    return "SNIPER" in compacto or bool(re.search(r"\b(?:BOT\s*)?SNIPER\s*[-_ ]*(?:FT|2T)?\b", raw))


def detectar_estrategia(texto: str) -> str:
    """Detecta a estratégia de forma robusta, priorizando a linha do bot.

    V20 — correção crítica:
    - normalizar() remove underscore, então VOLUME_FT podia virar VOLUMEFT;
    - a presença de "Intervalo" no alerta podia empurrar fallback para HT;
    - agora o detector lê primeiro a linha "Alerta Estratégia" e usa uma
      versão compacta sem espaços/underscore/hífen para reconhecer variações.

    Exemplos aceitos para Volume:
    VOLUME_FT, VOLUME FT, VOLUME-FT, VOLUMEFT, BOT_VOLUME_FT, BOT VOLUME FT.
    """
    raw = remover_acentos(texto or "").upper()
    raw = raw.replace("_", " ")
    raw = re.sub(r"\s+", " ", raw)

    # Usa primeiro a linha/cabeçalho da estratégia, para não confundir com
    # "Intervalo", "1ºP", "2ºP" ou outros termos do corpo do alerta.
    estrategia_txt = raw
    m = re.search(
        r"ALERTA\s+ESTRATEGIA\s*:\s*(.*?)(?:\s+JOGO\s*:|\s+COMPETICAO\s*:|\s+TEMPO\s*:|$)",
        raw,
        re.IGNORECASE,
    )
    if m and m.group(1).strip():
        estrategia_txt = m.group(1).strip()

    # Remove emojis/símbolos e deixa só letras/números para matching definitivo.
    compacto = re.sub(r"[^A-Z0-9]+", "", estrategia_txt)
    compacto_full = re.sub(r"[^A-Z0-9]+", "", raw)

    # V20 — VOLUME_FT tem prioridade absoluta antes de HT/FT normal.
    # Isso evita VOLUME_FT cair em ALFA_HT/ALFA_FT por causa de underscore removido
    # ou por causa do texto "Intervalo" no corpo do alerta.
    if contem_volume_ft_bruto(estrategia_txt) or contem_volume_ft_bruto(raw[:350]):
        return "VOLUME_FT"

    # SNIPER V2 — bot separado do FT genérico.
    if contem_sniper_bruto(estrategia_txt) or contem_sniper_bruto(raw[:350]):
        return "SNIPER_FT"

    # Confirmações têm prioridade depois de Volume/Sniper.
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

    # Radares canônicos.
    if "ARCEHT" in compacto or compacto == "ARCE":
        return "ARCE_HT"
    if "CHAMAFT" in compacto or compacto == "CHAMA":
        return "CHAMA_FT"

    # Nomes antigos/variações de HT.
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

    # Nomes antigos/variações de FT.
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

    # Fallback por período só no texto completo, como último recurso.
    # Não usa "Intervalo" para forçar HT se o minuto depois corrigir para FT.
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
    """SNIPER V2 — leitura FT separada, sem interferir em CHAMA/VOLUME/ARCE."""
    return estrategia == "SNIPER_FT"


def eh_confirmacao(estrategia: str) -> bool:
    return estrategia in {"ALFA_HT_CONFIRMACAO", "ALFA_FT_CONFIRMACAO"}


def eh_volume_ft(estrategia: str) -> bool:
    """V014 — radar interno VOLUME_FT."""
    return estrategia == "VOLUME_FT"


# =========================================================
# V007 — NORMALIZAÇÃO DE PERÍODO / PARSER SEGURO
# =========================================================

def corrigir_estrategia_por_minuto(estrategia: str, tempo: int, observacoes: List[str]) -> str:
    """Minuto é autoridade para HT/FT.

    Não mata o jogo por erro bobo de rótulo. Corrige internamente e registra
    observação para auditoria. Isso evita cliente receber FT como HT e evita
    perder jogo bom por nomenclatura da CornerPro.
    """
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
    """Aceita pequena divergência de feed (ex.: tempo 81 e último gol 82)."""
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
    """True quando o último gol foi do lado que já vencia antes do gol.

    Exemplos True: 1x0→2x0, 2x0→3x0, 2x1→3x1.
    Exemplo False: 0x0→1x0, 0x1→1x1, 0x2→1x2.
    """
    lado = m.ultimo_gol_lado
    if lado not in {"CASA", "FORA"}:
        return False
    gols_pro, gols_contra = gols_lado(m, lado)
    if gols_pro <= 0:
        return False
    # Placar antes do último gol: lado tinha um gol a menos.
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
    # Empatou: antes perdia por 1, depois empate.
    if antes_pro < gols_contra and gols_pro == gols_contra:
        return True
    # Reduziu: antes perdia por 2+, depois ainda perde por menos.
    if antes_pro < gols_contra and gols_pro < gols_contra:
        return True
    return False


def gol_recente_do_pressionante(m: "Metricas", janela: int = 5) -> bool:
    return gol_recente(m, janela) and m.ultimo_gol_lado == m.lado_pressionante and m.lado_pressionante in {"CASA", "FORA"}


def gol_recente_pressionante_aumentou_vantagem(m: "Metricas", janela: int = 5) -> bool:
    return gol_recente_do_pressionante(m, janela) and ultimo_gol_aumentou_vantagem(m)


def gol_recente_pressionante_resolveu_confirmacao(m: "Metricas", janela: int = 5) -> bool:
    """Regra mais rígida para BOT_FT CONFIRMAÇÃO.

    Na confirmação, se o gol saiu praticamente em cima do alerta e foi do lado
    pressionante, a entrada morre quando esse gol deixa o pressionante vencendo
    ou amplia vantagem. Exceções: empate/redução de desvantagem.
    """
    if not gol_recente_do_pressionante(m, janela):
        return False
    if ultimo_gol_empatou_ou_reduziu(m):
        return False
    return ultimo_gol_deixou_lado_vencendo(m) or ultimo_gol_aumentou_vantagem(m)


def competicao_base_bloqueada(m: "Metricas") -> bool:
    """Bloqueia apenas U18/U19/U20/Sub-18/Sub-19/Sub-20.

    Não bloqueia U21, Reserves, B, C ou II.
    """
    texto = remover_acentos(f"{m.competicao} {m.jogo}").upper()
    padroes = [
        r"\bU\s*-?\s*(18|19|20)\b",
        r"\bSUB\s*-?\s*(18|19|20)\b",
        r"\bSUB\s*(18|19|20)\b",
        r"\bUNDER\s*-?\s*(18|19|20)\b",
    ]
    return any(re.search(p, texto) for p in padroes)


def massacre_contextual_ht(m: "Metricas", lado: str, pos_gol_recente: bool = False) -> Tuple[bool, str]:
    """HT/AHT premium: baixa quantidade, muita qualidade.

    Não é score novo; é portão de qualidade. Se houver gol recente, exige
    números ainda mais fortes porque não existe bot de confirmação no HT.
    """
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

    # Gol cedo da zebra pode ser positivo, mas ainda precisa de reação real do favorito.
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
        return tuple(float(x.replace(",", ".")) for x in m.groups())  # type: ignore
    except Exception:
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
    # V25 — separa links colados antes do regex (ex: "rnkjghttps://")
    texto_sep = re.sub(r"(https?://)", r" \1", texto)
    for link in re.findall(r"https?://[^\s]+", texto_sep, re.IGNORECASE):
        link = link.strip().rstrip(".")
        if "bet365" in link.lower() and not bet365:
            bet365 = link
        if "cornerprobet" in link.lower() and "/analysis/" in link.lower() and not corner:
            corner = link
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


def calcular_confianca_parser(m: "Metricas") -> int:
    """Conta quantos campos críticos vieram com valor real do parser.

    Detecta silenciosamente falhas de formato da CornerPro.
    Se a CornerPro mudar texto/emojis, o parser retorna zeros e o sistema
    continua funcionando aparentemente normal — mas com dados falsos.

    Campos avaliados (1 ponto cada, máximo 8):
      ataques_perigosos, remates_baliza, ultimos5, ultimos10,
      xg, chance_golo, odds, pressao_alfa

    Score < 4: alerta nos logs — possível falha de parser.
    Score 0–2: crítico — entrada provavelmente sem dados reais.
    """
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



# =========================================================
# TIMES DE ELITE DE TRANSIÇÃO
# =========================================================
# Lista herdada da base central. Esses times, quando empatando ou perdendo,
# costumam manter valor ofensivo mesmo quando um ou outro número não está perfeito.
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
    # Aplica apenas se o time elite está empatando ou perdendo.
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
    """Diferença de AP pela ótica do lado analisado.

    Evita o erro de interpretar AP casa-fora como negativo quando o favorito/
    pressionante joga fora e domina o jogo.
    """
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
    """HT-1 fica fora do V12 definitivo.

    Mantemos a função apenas para compatibilidade interna, mas ela não altera
    nenhuma decisão nesta versão. O único ajuste HT ativo é o HT-2:
    ap_diff pela ótica do lado avaliado.
    """
    return False


def ht_bonus_super_fav_vencendo_v12_2(m: Metricas) -> Tuple[bool, str]:
    """V12_2 — bônus cirúrgico para super favorito vencendo por 1 no HT.

    Não é atalho de aprovação. Só aplica quando:
    - HT até 37';
    - favorito fora odd <= 1.55 OU favorito casa odd <= 1.30;
    - favorito vencendo por exatamente 1 gol;
    - favorito é o lado pressionante;
    - pressão extrema confirmada.
    """
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
    """Valida se o placar ainda dá valor real para over.

    Essa função recupera a filosofia do main.py antigo: pressão não basta.
    O sistema precisa saber se o contexto emocional ainda pede gol ou se a
    pressão já foi paga pelo jogo.
    """
    gc, gf = extrair_gols_placar(m.placar)
    if gc is None or gf is None:
        return True, "PLACAR_DESCONHECIDO"

    total_gols = gc + gf
    dif = abs(gc - gf)
    fav = m.lado_favorito
    perdendo = lado_perdendo(m)
    vencendo = lado_vencendo(m)

    # HT: ainda aceita 1x0/0x1, mas evita placar muito aberto cedo sem massacre.
    if eh_ht(m.estrategia):
        if total_gols >= 3 and dif >= 2:
            if lado == vencendo and pressao_extrema_lado(m, lado) and consequencia_real_lado(m, lado):
                return True, "HT_MASSACRE_CONTINUA_MESMO_PLACAR_ABERTO"
            return False, "HT_PLACAR_ABERTO_DEMAIS_SEM_VALOR"
        return True, "HT_CONTEXTO_VIVO"

    # FT: empate, diferença de 1 gol e favorito não vencendo são cenários vivos.
    if vencendo == "EMPATE":
        return True, "FT_EMPATE_CONTEXTO_VIVO"
    if dif <= 1:
        return True, "FT_PLACAR_APERTADO"
    if fav in {"CASA", "FORA"} and perdendo == fav:
        return True, "FT_FAVORITO_ATRAS_DO_PLACAR"

    # Se o lado que está vencendo por 2+ é também o pressionante, só aceita
    # quando existe massacre vivo comprovado; caso contrário é placar resolvido.
    if dif >= 2 and lado == vencendo:
        if pressao_extrema_lado(m, lado) and consequencia_real_lado(m, lado):
            return True, "MASSACRE_CONTINUA_VIVO"
        return False, "PLACAR_RESOLVIDO_SEM_FOME"

    # Se o lado pressionante está perdendo por 2+, ainda há urgência, mas exige
    # pressão real para não virar entrada emocional vazia.
    if dif >= 2 and lado == perdendo:
        if pressao_viva_lado(m, lado) and consequencia_real_lado(m, lado):
            return True, "TIME_ATRAS_PRECISA_REAGIR_COM_PRESSAO"
        return False, "TIME_ATRAS_SEM_PRESSAO_REAL"

    return True, "CONTEXTO_NEUTRO_VIVO"


def consequencia_minima_emocional(m: Metricas, lado: str) -> bool:
    """Consequência mínima para exceções contextuais.

    Mesmo quando há gol contra o fluxo ou favorito não vencendo, o jogo precisa
    provar pelo menos algum caminho real de gol. Isso evita liberar contexto
    emocional sem finalização nenhuma.
    """
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
    """Retorna classe, motivo, ajuste_score, proteger_ia.

    A função não duplica bônus em excesso. Ela é o núcleo humano:
    quem pressionava, quem marcou e se a pressão ainda vale.
    """
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

    # Favorito não vencendo e pressão sustentada: caso Rīgas / Donaufeld.
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

    # Sem gol recente, não há valor pós-evento de gol.
    if not ultimo or minutos > 12 or ultimo_lado == "DESCONHECIDO":
        return "SEM_VALOR_ESPECIAL", "SEM_GOL_RECENTE_RELEVANTE", 0, fav_press and fav_nv

    # Gol contra fluxo: zebra/adversário marcou enquanto o pressionante/favorito segue vivo.
    if ultimo_lado != lado and pressao_viva_lado(m, lado):
        if fav == lado or ultimo_lado == zebra:
            ajuste = 10 if m.odd_favorito and m.odd_favorito <= 1.30 else 7
            return "GOL_CONTRA_FLUXO_VALORIZA", "ZEBRA_MARCOU_E_PRESSIONANTE_SEGUE_VIVO", ajuste, True
        return "GOL_CONTRA_FLUXO_VALORIZA", "ADVERSARIO_MARCOU_E_PRESSAO_CONTINUA", 5, True

    # Gol do lado pressionante.
    if ultimo_lado == lado:
        # V010 — quando o gol deixou o pressionante vencendo e o U5/RB/IP já
        # caíram, força PRESSAO_PREMIADA_MORREU mesmo que U10 ainda esteja alto
        # (U10 carrega pressão pré-gol por até 10 minutos — contaminação real).
        if gol_recente_do_pressionante(m, janela=5) and ultimo_gol_deixou_lado_vencendo(m) and pressao_pos_gol_esfriou(m, lado):
            return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_U10_CONTAMINADO_U5_CAIU", -14, False
        if pressao_morta_lado(m, lado):
            return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_E_RITMO_CAIU", -14, False
        if pressao_viva_lado(m, lado):
            return "PRESSAO_PREMIADA_MAS_CONTINUA", "GOL_PREMIOU_MAS_PRESSAO_CONTINUA", -2, True
        return "PRESSAO_PREMIADA_MORREU", "GOL_PREMIOU_PRESSAO_SEM_CONTINUIDADE_CLARA", -8, False

    return "SEM_VALOR_ESPECIAL", "GOL_RECENTE_SEM_LEITURA_ESPECIAL", 0, False


def pressao_pos_gol_esfriou(m: "Metricas", lado: str) -> bool:
    """V10 FINAL — detecta pressão que esfriou após gol premiado.

    O objetivo é evitar que o U10, ainda contaminado pela pressão antes do gol,
    faça o sistema tratar como viva uma pressão que morreu no pós-gol.

    Critério conservador: só considera que esfriou quando U5, finalização, cantos
    e IP do lado pressionante estão baixos ao mesmo tempo.
    """
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
    """Pressão extrema real, usada como exceção de elite no funil.

    Esta função precisa existir no escopo global porque várias camadas do score
    chamam pressao_extrema_lado(). Sem ela, o bot compila, mas quebra em runtime
    quando chega um alerta que passa por esses caminhos.
    """
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
    # V28 — alarga janela FT: 62-81' retorna bônus pleno.
    # Alertas chegam até 77'; penalizar 80' era prematuro.
    if HABILITAR_V28 and HABILITAR_V28_RELOGIO_FT:
        if 62 <= m.tempo <= 81:
            return 7
        if 82 <= m.tempo <= 85:
            return 3
        if m.tempo > 85:
            return -5
        return 1
    # V27 e anterior
    if 62 <= m.tempo <= 80:
        return 7
    if 81 <= m.tempo <= 85:
        return 3
    if m.tempo > 85:
        return -5
    return 1


def score_confirmacao(m: Metricas, chave: str) -> Tuple[int, str]:
    if not eh_confirmacao(m.estrategia):
        return 0, "NAO_E_CONFIRMACAO"
    anterior = ultimas_leituras_por_jogo.get(chave)
    if not anterior:
        return 2, "CONFIRMACAO_SEM_HISTORICO"
    old: Metricas = anterior.get("metricas")  # type: ignore
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
    if ip_lado(m, lado)["pico"] >= ip_lado(old, lado)["pico"]:
        pontos += 2
        motivos.append("ip_manteve_ou_subiu")
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
        # Exceção para super favorito não vencendo com IP absurdo.
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
    """V009 — freio contextual de inflação do score.

    Esta camada NÃO muda parser, fluxo, confirmação, gol recente ou funil.
    Ela só corrige a régua: jogos bons continuam podendo passar, mas nem todo
    aprovado comum pode virar 90/96.

    Filosofia:
    - massacre real / super favorito em campo: pode manter 92–96;
    - jogo aberto produtivo: teto moderado;
    - liga UNDER + baixa produção: teto forte;
    - pressão recente fraca/finalização baixa: teto preventivo.
    """
    if not HABILITAR_SCORE_V9:
        return score, "SCORE_V9_DESATIVADO"
    if eh_ht(m.estrategia):
        # HT já passa pelo funil premium de massacre; não mexer aqui.
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

    # Massacre real: preserva elite. Exemplo: Kongsvinger.
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

    # Jogos abertos são bons, mas não devem morar na mesma prateleira do massacre.
    if cenario == "CAOS_PRODUTIVO":
        cap = min(cap, V9_CAP_JOGO_ABERTO)
        motivos.append("JOGO_ABERTO_NAO_MASSACRE")

    # Liga UNDER precisa provar mais; se vier com xG/chance baixa, teto forte.
    if m.liga == "UNDER":
        cap = min(cap, V9_CAP_LIGA_UNDER)
        motivos.append("LIGA_UNDER")
        if d["xg"] < 0.45 and d["chance"] < 8 and d["rb"] <= 1:
            cap = min(cap, V9_CAP_LIGA_UNDER_FRACA)
            motivos.append("UNDER_COM_PRODUCAO_BAIXA")

    # Finalização baixa: evita MP/Fénix/Liniers virarem 90+ sem cara elite.
    if d["rb"] <= 1 and d["chance"] < 8 and d["xg"] < 0.45:
        cap = min(cap, V9_CAP_FINALIZACAO_BAIXA)
        motivos.append("FINALIZACAO_BAIXA")

    # Pressão recente fraca: domínio acumulado não basta no FT.
    if d["u5"] <= 2 and d["u10"] <= 5 and not elite_contextual:
        cap = min(cap, V9_CAP_PRESSAO_RECENTE_FRACA)
        motivos.append("PRESSAO_RECENTE_FRACA")

    # Aprovado comum: passou no funil, mas sem massacre nem elite contextual.
    if not motivos and not massacre_extremo and not elite_contextual:
        cap = min(cap, V9_CAP_APROVADO_COMUM)
        motivos.append("APROVADO_COMUM_SEM_ELITE")

    if score > cap:
        return cap, "SCORE_V9_TETO_" + "+".join(motivos)
    return score, "SCORE_V9_SEM_REDUCAO_" + ("+".join(motivos) if motivos else "FORTE")



def finalização_minima_lado(m: Metricas, lado: str) -> bool:
    """Exige pelo menos um sinal de finalização real do lado pressionante.

    Impede que contexto emocional forte (valor_forte=True) libere jogos
    onde não há nenhuma tentativa real de gol — apenas volume ofensivo
    sem ruptura da defesa adversária.

    Critério: RB >= 1 OU RDA >= 1 OU xG >= 0.15 OU Chance >= 5.
    Mais permissivo que consequencia_minima_emocional() de propósito:
    é o piso mínimo absoluto, não a trava completa.
    """
    d = dados_lado(m, lado)
    return d["rb"] >= 1 or d["rda"] >= 1 or d["xg"] >= 0.15 or d["chance"] >= 5


# =========================================================
# CAMADA DE CENÁRIO FT — V005
# Classificação única que agrupa todas as decisões FT em um
# único ponto de controle. Não toca HT, parser, Telegram.
#
# Cenários classificados:
#   ALFA_REAL          → passa com score cheio
#   FAKE_PRESSURE      → bloqueia no funil (RB=0 + chance baixa + xG baixo)
#   PERDEDOR_REAGINDO  → libera se pressão + consequência + xGL ≥ 0.50
#   PLACAR_RESOLVIDO   → bloqueia vencedor por 2+ sem pressão extrema
#   U5_MORTO           → bloqueia no funil (U5 ≤ 1 AND U10 ≤ 4)
#   FAVORITO_VENCENDO_FACIL → bônus de favoritismo reduzido
# =========================================================

# Constante de xGL mínimo para perdedor reagindo
_XGL_MIN_PERDEDOR_REAGINDO = 0.50


def v27_continuidade_pos_gol(m: Metricas) -> Tuple[bool, str]:
    """V27 — valida se o jogo continuou vivo depois de gol recente.

    Resolve o problema central visto na auditoria: dados acumulados bons antes
    do gol continuarem inflando o alerta quando, depois do gol, o jogo morreu.
    Como não temos timeline perfeita pós-gol, usamos sinais disponíveis:
    - U5/U10 do lado pressionante;
    - remate/pressão recente;
    - canto posterior ao gol;
    - IP ainda forte.
    """
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

    # Só endurece de verdade quando o gol recente é do lado que podia esfriar o jogo:
    # pressionante/favorito/vencedor. Gol contra fluxo pode abrir o jogo.
    if not (gol_lado_vencedor or gol_do_press or gol_do_fav):
        return True, "V27_GOL_NAO_PREMIOU_PRESSIONANTE"

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

    if delta <= 3:
        # Gol muito recente: exige mais cuidado porque odds/ritmo podem estar instáveis.
        ok = provas >= 3
    else:
        ok = provas >= 2

    motivo = (
        f"V27_CONT_POS_GOL provas={provas} delta={delta} lado={lado} "
        f"u5={d['u5']} u10={d['u10']} rb={d['rb']} rl={d['rl']} "
        f"chance={d['chance']} xg={d['xg']:.2f} cantos_pos={len(cantos_pos_gol)} "
        f"ip={ip['pico']} gol_vencedor={gol_lado_vencedor} gol_press={gol_do_press} gol_fav={gol_do_fav}"
    )
    return ok, motivo if ok else "BLOQUEIO_" + motivo


def v27_empate_tem_prova_suficiente(m: Metricas, lado: str) -> Tuple[bool, str]:
    """V27 — empate FT precisa de 2 de 4 provas reais."""
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
    """V27 — lado perdendo por 1 só passa se há reação viva + consequência."""
    if not HABILITAR_V27_PERDEDOR_UM_EXIGE_REACAO:
        return True, "V27_PERDEDOR_UM_OFF"
    if not eh_ft(m.estrategia):
        return True, "V27_NAO_FT"
    if diferenca_placar(m) != 1 or lado_vencendo(m) in {"EMPATE", "DESCONHECIDO"}:
        return True, "V27_NAO_PERDE_1"
    if lado != lado_perdendo(m):
        return True, "V27_LADO_NAO_E_PERDEDOR"
    d = dados_lado(m, lado)
    pressao_dupla = d["u5"] >= 3 and d["u10"] >= 6
    reacao_viva = pressao_dupla or (pressao_viva_lado(m, lado) and consequencia_real_lado(m, lado))
    motivo = f"V27_PERDEDOR_1 reacao={reacao_viva} u5={d['u5']} u10={d['u10']} consequencia={consequencia_real_lado(m,lado)}"
    return bool(reacao_viva), motivo


def v27_favorito_vencendo_1x0_exige_extra(m: Metricas, lado: str) -> Tuple[bool, str]:
    """V27 — 1x0/0x1 no FT é tratado como vantagem curta potencialmente morta.

    2x1/3x2 não são endurecidos aqui porque já carregam evidência de jogo aberto.
    """
    if not HABILITAR_V27_REFINO_PLACAR_FT:
        return True, "V27_PLACAR_OFF"
    if not eh_ft(m.estrategia):
        return True, "V27_NAO_FT"
    total = total_gols_placar(m)
    if total != 1 or diferenca_placar(m) != 1 or lado != lado_vencendo(m):
        return True, "V27_NAO_1X0_VENCEDOR"
    d = dados_lado(m, lado)
    ip = ip_lado(m, lado)
    ok = (
        d["u5"] >= 5
        and d["u10"] >= 9
        and (d["rb"] >= 2 or d["chance"] >= 10 or d["xg"] >= 0.45)
        and (ip["pico"] >= 22 or ip["c18"] >= 2)
    )
    motivo = f"V27_1X0_EXTRA ok={ok} u5={d['u5']} u10={d['u10']} rb={d['rb']} chance={d['chance']} xg={d['xg']:.2f} ip={ip['pico']}"
    return ok, motivo




def continuidade_pos_gol(m: "Metricas", lado: str) -> Tuple[bool, str]:
    """V28 — Versão direta da verificacao de continuidade pos-gol por lado especifico.

    Complementa v27_continuidade_pos_gol() que analisa o contexto geral.
    Esta funcao verifica especificamente o lado indicado com criterio mais simples.
    Usada internamente no cenario FAV_VENCE_1_GOL_RECENTE_MORTO.
    """
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
    """V28 — ajuste contextual leve baseado em favoritismo, xG e pressao recente.

    Nao substitui o score. Adiciona pequeno ajuste (+4/+2/-1/-3) para aproximar
    o sistema do DNA dos melhores greens historicos (odd + xG + U15/L15).
    """
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
    """V28 — liga UNDER so passa se for realmente extraordinaria.

    Tres caminhos aceitos (qualquer um basta):
      1. RB forte + xG solido
      2. Chance alta + pressao dupla recente
      3. Pressao extrema + consequencia + IP elite
    """
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
    codigo: str          # código do cenário (ex: "ALFA_REAL")
    bloqueia: bool       # True = funil deve barrar
    teto_score: int      # score máximo permitido (100 = sem teto)
    bonus_fav_cap: int   # cap do bônus de favoritismo (99 = sem cap)
    motivo: str          # motivo para log/auditoria


def classificar_cenario_ft(m: Metricas) -> CenarioFT:
    """Camada única de cenário FT.

    Chamada APENAS para FT. Retorna um CenarioFT que o funil
    e o score_python_contextual usam para tomar decisões.
    Não mexe em nada do HT, parser ou Telegram.
    """
    if eh_ht(m.estrategia):
        # Segurança: nunca deve ser chamada para HT, mas retorna neutro se for.
        return CenarioFT("NAO_FT", False, 100, 99, "CHAMADA_INDEVIDA_HT")

    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return CenarioFT("SEM_LADO", False, 100, 99, "SEM_LADO_PRESSIONANTE")

    # ── V28 — Bloqueio forte de liga UNDER antes de qualquer cenario ─────────
    # Taxa historica: 36.4% — sem justificativa para manter no funil.
    # Excecao: prova extraordinaria (rb+xG ou chance+pressao ou extremo+IP).
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

    # ── V27: continuidade pós-gol e refino contextual antes dos cenários clássicos ──
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

    # ── CENÁRIO 1: Jogo morto FT ─────────────────────────────────────────────
    # Lógica trazida do código antigo (4.436 linhas): jogo realmente morto
    # exige ausência total nas quatro dimensões simultaneamente.
    # U5=0 E U10=0 E RB=0 E xG<0.10 — não é pressão baixa, é ausência total.
    # Mais preciso que U5 ≤ 1: um jogo pode ter U5=1 e ainda ter xG real.
    if d["u5"] == 0 and d["u10"] == 0 and d["rb"] == 0 and d["xg"] < 0.10:
        return CenarioFT(
            "JOGO_MORTO_FT", True, 78, 99,
            f"U5=0_U10=0_RB=0_XG={d['xg']:.2f}_AUSENCIA_TOTAL"
        )

    # ── CENÁRIO 2: Fake pressure — RB=0 + chance baixa + xG baixo ──────────
    # Volume territorial sem nenhuma finalização real = ilusão de pressão.
    if d["rb"] == 0 and d["chance"] < 8 and d["xg"] < 0.25:
        return CenarioFT(
            "FAKE_PRESSURE", True, 76, 99,
            f"RB=0_CHANCE={d['chance']}_XG={d['xg']:.2f}_SEM_FINALIZACAO_REAL"
        )

    # ── CENÁRIO 3: Referência de pressão dupla ──────────────────────────────
    # U5 e U10 juntos elevam confiança — mas não são portão de bloqueio.
    # Um jogo com RB=4, xG=0.90 não deve morrer por U10=5.
    # pressao_dupla é usada pelos cenários seguintes como sinal, não como trava.
    pressao_dupla = d["u5"] >= 3 and d["u10"] >= 6

    # ── CENÁRIO 4: Caos produtivo — jogo aberto com gols dos dois lados ────
    # 3x2, 2x2, 4x3: diferença ≤ 1 com volume alto de gols.
    # Não é placar resolvido — os dois times mostraram que sabem marcar.
    # Entra ANTES do bloqueio de placar aberto por essa razão.
    if total_gols >= 4 and dif <= 1:
        return CenarioFT(
            "CAOS_PRODUTIVO", False, 100, 99,
            f"JOGO_ABERTO_{gc}x{gf}_DIF={dif}_TOTAL_GOLS={total_gols}"
        )

    # ── CENÁRIO 5: Placar aberto — vencedor sem pressão extrema ─────────────
    # Vencedor por 2+ não tem fome real. Só passa se pressão extrema.
    if dif >= 2 and lado == vencendo:
        if not pressao_extrema_lado(m, lado):
            return CenarioFT(
                "PLACAR_RESOLVIDO", True, 78, 3,
                f"VENCENDO_{gc}x{gf}_SEM_PRESSAO_EXTREMA_PARA_AMPLIAR"
            )
        # Pressão extrema: passa mas bônus de favoritismo reduzido.
        return CenarioFT(
            "VENCEDOR_EXTREMO", False, 100, 5,
            f"VENCENDO_{gc}x{gf}_PRESSAO_EXTREMA_CONTINUA"
        )

    # ── CENÁRIO 6: Placar aberto — perdedor reagindo ─────────────────────────
    # Perdedor por 2+ libera SE: pressão + consequência + xGL ≥ 0.50.
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

    # ── CENÁRIO 7: Favorito vencendo fácil (dif=1, mas xGL baixo) ───────────
    # Favorito vencendo por 1 com xGL < 0.55: pressão histórica fraca.
    # Não bloqueia, mas reduz bônus de favoritismo.
    if dif == 1 and lado == vencendo and xgl_lado < 0.55:
        return CenarioFT(
            "FAVORITO_VENCENDO_XGL_FRACO", False, 100, 4,
            f"VENCENDO_1_GOL_XGL={xgl_lado:.2f}_HISTORICO_FRACO"
        )

    # ── ALFA REAL ────────────────────────────────────────────────────────────
    return CenarioFT("ALFA_REAL", False, 100, 99, "CENARIO_IDEAL_CONFIRMADO")


def liga_under_exige_prova_extra(m: Metricas) -> bool:
    """V010 — identifica ligas UNDER/Sul-Americanas que precisam de prova extra.

    Argentina excluída: comportamento melhor na auditoria operacional.
    México incluído mesmo não estando na lista UNDER formal — historicamente
    problemático nas auditorias.
    """
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
    """V010 — exige consequência real comprovada para ligas UNDER/rígidas.

    Cinco caminhos aceitos (qualquer um basta):
      1. RB forte + pressão recente viva
      2. xG alto + chance alta
      3. Pressão elite (U5/U10/IP) + consequência real
      4. Área + chance (domínio de área comprovado)
      5. Pressão extrema + xG + RB mínimo
    """
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
# FUNIL OBRIGATÓRIO HÍBRIDO — GOAT V004 + V005 + V006
# Unificação definitiva: melhor do V003 corrigido + melhor do V003 produção.
#
# Do V003 produção:
#   - contexto_emocional_vivo() com lógica apurada (TIME_ATRAS_SEM_PRESSAO_REAL,
#     HT_MASSACRE_CONTINUA, FT_FAVORITO_ATRAS_DO_PLACAR)
#   - consequencia_minima_emocional() com limiares HT/FT separados
#   - FT: excecao_contextual_valida = (valor_forte OR super_fav+extremo) AND consequencia_minima
#   - HT: trava de favorito fraco odd > 1.50 sem massacre
#
# Do V003 corrigido:
#   - valor_forte_validado: valor_forte só conta se há finalização_minima_lado()
#     (impede contexto emocional sem nenhum remate/xG passar pelo funil)
#   - motivo específico FUNIL_FT_VALOR_FORTE_SEM_FINALIZACAO para auditoria
#
# V005: classificar_cenario_ft() entra antes do bloco FT do funil.
#
# V006: HT exige que favorito seja o lado pressionante (bloqueia zebra dominando)
# =========================================================

def funil_obrigatorio_hibrido(m: Metricas) -> Tuple[bool, int, str, Dict[str, Any]]:
    """Portão antes do score aditivo.

    Filosofia: RADAR → VALIDAÇÃO → TRAVAS → CLASSIFICAÇÃO.
    Um jogo precisa provar pressão viva, consequência real, contexto emocional
    vivo E finalização mínima antes de chegar ao score aditivo.
    """
    lado = m.lado_pressionante
    detalhes: Dict[str, Any] = {}

    # ── Bloqueios imediatos ──────────────────────────────────────────────────
    if m.liga == "PERIGOSA":
        return False, 72, "FUNIL_LIGA_PERIGOSA", detalhes
    if lado not in {"CASA", "FORA"}:
        return False, 72, "FUNIL_SEM_LADO_PRESSIONANTE", detalhes
    if vermelho_contra_pressionante(m):
        return False, 74, "FUNIL_VERMELHO_CONTRA_PRESSIONANTE", detalhes

    # ── V010 — Prova extra para ligas UNDER/rígidas ──────────────────────────
    # Roda antes das avaliações base para bloquear cedo e registrar motivo claro.
    # Argentina excluída. México e Sul-Americanas problemáticas incluídas.
    if liga_under_exige_prova_extra(m):
        under_ok, under_motivo = prova_extra_liga_under(m, lado)
        detalhes["under_prova_extra"] = under_ok
        detalhes["under_prova_extra_motivo"] = under_motivo
        if not under_ok:
            return False, 78, f"FUNIL_UNDER_SEM_PROVA_EXTRA | {under_motivo}", detalhes

    # ── Avaliações base ──────────────────────────────────────────────────────
    pressao      = pressao_viva_lado(m, lado)
    consequencia = consequencia_real_lado(m, lado)
    extremo      = pressao_extrema_lado(m, lado)
    fav_nao_vence = favorito_nao_vencendo(m) and m.lado_favorito == lado
    super_fav     = bool(m.odd_favorito and m.odd_favorito <= 1.30 and fav_nao_vence)

    # valor_forte cru: contexto emocional favorável.
    valor_forte = m.valor_pos_evento_classe in {
        "GOL_CONTRA_FLUXO_VALORIZA",
        "FAVORITO_NAO_VENCE_PRESSAO_SUSTENTADA",
        "PRESSAO_PREMIADA_MAS_CONTINUA",
    }

    # [V003 corrigido] valor_forte só é operacional com finalização mínima.
    # Sem remate/xG/chance, contexto emocional não converte — é fake pressure emocional.
    tem_finalizacao      = finalização_minima_lado(m, lado)
    valor_forte_validado = valor_forte and tem_finalizacao

    # [V003 produção] consequência mínima com limiares HT/FT separados.
    consequencia_minima = consequencia_minima_emocional(m, lado)

    # [V003 produção] contexto emocional apurado — avalia fome pelo placar.
    emocional_vivo, motivo_emocional = contexto_emocional_vivo(m, lado)

    detalhes.update({
        "pressao_viva":               pressao,
        "consequencia":               consequencia,
        "pressao_extrema":            extremo,
        "favorito_nao_vence":         fav_nao_vence,
        "super_favorito_nao_vence":   super_fav,
        "valor_pos_evento_forte":     valor_forte,
        "valor_forte_validado":       valor_forte_validado,
        "tem_finalizacao_minima":     tem_finalizacao,
        "consequencia_minima_emocional": consequencia_minima,
        "contexto_emocional_vivo":    emocional_vivo,
        "motivo_emocional":           motivo_emocional,
    })

    # Pressão morta bloqueia antes de qualquer outro check.
    if pressao_morta_lado(m, lado):
        return False, 74, "FUNIL_PRESSAO_MORTA", detalhes

    # Contexto emocional morto bloqueia.
    # Exceção única: super favorito não vencendo com pressão extrema tem fome
    # institucional suficiente para sobrepor o contexto de placar.
    if not emocional_vivo and not (super_fav and extremo):
        return False, 76, f"FUNIL_CONTEXTO_EMOCIONAL_MORTO_{motivo_emocional}", detalhes

    # ── HT ──────────────────────────────────────────────────────────────────
    # HT precisa parecer massacre: pressão + consequência.
    # Favorito fraco (odd > 1.50) bloqueado sem números de massacre.
    if eh_ht(m.estrategia):
        # V007 — HT/AHT é camada premium. Não buscamos volume.
        # Só passa com massacre contextual real. Se houve gol recente, a régua sobe.
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

    # ── FT ──────────────────────────────────────────────────────────────────
    # [V005] Camada de cenário FT — decisão única antes de qualquer lógica FT.
    cenario = classificar_cenario_ft(m)
    detalhes["cenario_ft"] = cenario.codigo
    detalhes["cenario_ft_motivo"] = cenario.motivo

    if cenario.bloqueia:
        return False, cenario.teto_score, f"FUNIL_FT_CENARIO_{cenario.codigo}", detalhes

    if not pressao:
        return False, 76, "FUNIL_FT_SEM_PRESSAO_VIVA", detalhes

    # [V003 produção + V003 corrigido] exceção contextual exige:
    # (valor_forte_validado OU super_fav+extremo) AND consequencia_minima.
    # valor_forte sem finalização não é exceção — é fake pressure emocional.
    excecao_contextual_valida = (
        (valor_forte_validado or (super_fav and extremo))
        and consequencia_minima
    )

    if not consequencia and not excecao_contextual_valida:
        # Motivo específico para auditoria: saber se falhou por falta de finalização
        # ou por ausência total de consequência.
        if valor_forte and not tem_finalizacao:
            return False, 78, "FUNIL_FT_VALOR_FORTE_SEM_FINALIZACAO", detalhes
        return False, 78, "FUNIL_FT_SEM_CONSEQUENCIA_MINIMA", detalhes

    # [V003 produção] favorito fraco/equilibrado só passa com números extremos
    # ou valor_forte_validado com consequência mínima.
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
    # O bônus de time elite já é aplicado dentro de score_favoritismo().
    # Aqui chamamos novamente apenas para registrar o motivo no log/CSV, sem somar de novo.
    _, elite_motivo = bonus_time_elite_transicao(m)
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
    # V28 — DNA projetado: ajuste leve baseado em favoritismo + xG + pressao recente.
    # Nao altera o score central; apenas adiciona ou remove ate 4 pontos.
    dna_ajuste, dna_motivo = v28_dna_projetado(m, m.lado_pressionante)
    if HABILITAR_V28 and HABILITAR_V28_DNA_PROJETADO:
        componentes["dna_v28"] = dna_ajuste

    score_bruto = base + sum(componentes.values())

    if eh_ht(m.estrategia):
        # Bônus para pressão extrema no HT — massacre confirmado.
        # A penalidade de -2 foi removida: o funil já exigiu pressão viva
        # e consequência real. Punir novamente por não ser extremo é
        # redundância estatística. Calibração: corte HT ajustado para 86.
        if m.lado_pressionante in {"CASA", "FORA"} and pressao_extrema_lado(m, m.lado_pressionante):
            score_bruto += 4

        bonus_super_fav_ok, bonus_super_fav_motivo = ht_bonus_super_fav_vencendo_v12_2(m)
        if bonus_super_fav_ok:
            score_bruto += 5
        # HT-3 (bônus Over 0.5HT histórico) — reservado para versão futura com auditoria específica.
    else:
        # [V005] Aplica cap de bônus de favoritismo definido pelo cenário FT.
        cenario_ft = classificar_cenario_ft(m)
        if cenario_ft.bonus_fav_cap < 99:
            componentes["favoritismo"] = min(componentes["favoritismo"], cenario_ft.bonus_fav_cap)
            # Recalcula score_bruto com bônus de favoritismo limitado.
            score_bruto = base + sum(componentes.values())

        if favorito_nao_vencendo(m) and m.lado_favorito == m.lado_pressionante:
            score_bruto += 4

    score = clamp(score_bruto, 0, 96)  # teto Python: nunca 100

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
# IA AUDITORA / PROTEÇÃO DA IA
# =========================================================

def montar_prompt_ia(m: Metricas, decisao_py: DecisaoPython) -> str:
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
Estratégia: {m.estrategia}
Jogo: {m.jogo}
Competição: {m.competicao}
Minuto: {m.tempo}
Placar: {m.placar}
Mercado: {m.mercado}
Liga: {m.liga}
Odds: {m.odds}
Favorito: {m.lado_favorito} odd {m.odd_favorito}
Dominante: {m.lado_dominante}
Pressionante: {m.lado_pressionante}
Último gol: {m.ultimo_gol}' {m.ultimo_gol_lado}
AP: {m.ataques_perigosos}
U5: {m.ultimos5}
U10: {m.ultimos10}
Cantos: {m.cantos}
RB: {m.remates_baliza}
Remates lado: {m.remates_lado}
RDA: {m.remates_dentro_area}
Chance gol: {m.chance_golo}
xG: {m.xg}
IP: {m.pressao_alfa}
Valor pós-evento: {m.valor_pos_evento_classe} | {m.valor_pos_evento_motivo}
Score Python: {decisao_py.score}
Motivo Python: {decisao_py.motivo}
""".strip()


async def consultar_openai(m: Metricas, decisao_py: DecisaoPython) -> Tuple[str, int, str]:
    if not OPENAI_HABILITADO or not OPENAI_API_KEY:
        return "APROVAR", decisao_py.score, "OPENAI_DESATIVADA_USANDO_SCORE_PYTHON"

    prompt = montar_prompt_ia(m, decisao_py)
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
        conf = pegar_numero(r"CONFIANCA\s*=\s*(\d+)", content, decisao_py.score)
        if conf == decisao_py.score:
            conf = pegar_numero(r"CONFIANÇA\s*=\s*(\d+)", content, decisao_py.score)
        motivo = content[:250]
        log(f"🤖 OpenAI | {m.jogo} | {decisao} | confiança={conf}")
        return decisao, clamp(conf, 0, 95), motivo  # teto IA: nunca passa de 95
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

    # Impedimentos de proteção.
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
    """
    Define o título exibido ao cliente usando o minuto como autoridade.

    Correção crítica:
    - Se a CornerPro mandar estratégia HT/ARCE_HT por engano aos 70+,
      o cliente NÃO pode receber "PRIMEIRO TEMPO".
    - 46' em diante = SEGUNDO TEMPO.
    - Antes de 46' = PRIMEIRO TEMPO.
    """
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
    """Mensagem para o canal interno/completo (@ALFA_CON).

    Diferença para o canal grátis/principal:
    - mantém todo o corpo da mensagem igual;
    - troca apenas o cabeçalho visual para identificar a família do bot;
    - não altera parser, score, IA, filtros, grupo grátis ou canal público.
    """
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
    """Retorna janela e limite do grupo grátis.

    J1: 06:30–12:00 | J2: 12:00–18:00 | J3: 18:00–23:00.
    Fora disso retorna (None, 0).
    """
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
    v17_salvar_estado()  # V17 — persiste no disco
    asyncio.create_task(v26_salvar_estado_telegram())  # V26 — persiste no Telegram como backup


def jogo_em_cooldown_gratis_v11(chave_jogo: str) -> bool:
    ts = float(v11_gratis_enviados_por_jogo.get(chave_jogo, 0) or 0)
    return ts > 0 and time.time() - ts < V11_FREE_COOLDOWN_JOGO_HORAS * 3600


def favorito_vencendo_por_um_extremo_v18(m: Metricas, score: int = 0) -> Tuple[bool, str]:
    """V18 — exceção rara para favorito vencendo por 1.

    Regra oficial:
    - padrão do grupo grátis e do VOLUME_FT: favorito empatando ou perdendo;
    - se o favorito já vence, só pode passar vencendo por 1 e em cenário
      EXTREMAMENTE absurdo;
    - gol recente do favorito continua bloqueando, porque pode ser pressão
      premiada;
    - score 90+ é exigido quando a função recebe score já calculado
      (grupo grátis); no VOLUME_FT, que roda antes do score, a prova vem
      apenas dos dados brutos.
    """
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

    # Prioridade normal do grupo grátis: favorito perdendo ou empatando.
    if vencedor != fav and vencedor != "EMPATE":
        return True, "CENARIO_A_FAVORITO_PERDENDO"
    if vencedor == "EMPATE":
        return True, "CENARIO_B_FAVORITO_EMPATANDO"

    # V18 — favorito vencendo deixa de ser cenário comum.
    # Só passa vencendo por 1 em cenário realmente absurdo.
    ok_c, motivo_c = favorito_vencendo_por_um_extremo_v18(m, score=score)
    if ok_c:
        return True, "CENARIO_C_EXTREMO_" + motivo_c
    return False, motivo_c

def liga_australiana_v11(m: Metricas) -> bool:
    texto = remover_acentos(f"{m.competicao} {m.jogo}").lower()
    return "australia" in texto


def leitura_australia_v11(m: Metricas) -> Tuple[str, str]:
    """Leitura especial de ligas australianas/abertas.

    Não bloqueia o canal completo. Apenas informa cautela para grupo grátis e
    ALAVANCAGEM quando placar elástico + gol recente indicam pressão premiada.
    """
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
    """Referência dos antigos três pilares; usado para ALAVANCAGEM, não para aprovar."""
    if lado not in {"CASA", "FORA"}:
        return False, "SEM_LADO"
    d = dados_lado(m, lado)
    p1 = bool(m.odd_favorito and m.odd_favorito <= 1.55)
    p2 = bool(d["rb"] >= 2 or d["chance"] >= 9)
    p3 = bool(d["u5"] >= 4 and d["u10"] >= 8)
    ok = p1 and p2 and p3
    return ok, f"p1_odd={p1}|p2_rb_chance={p2}|p3_u5u10={p3}"


def selo_alavancagem_v11(m: Metricas, score: int) -> Tuple[bool, str]:
    """ALAVANCAGEM é classificação contextual rara, não aprovação."""
    if not HABILITAR_V11_ALAVANCAGEM:
        return False, "V11_ALAVANCAGEM_DESATIVADA"
    # V27/V28 — ALAVANCAGEM somente FT. HT nao tem contexto suficiente para selo de elite.
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
        # V25 — HT: alavancagem só se favorito estiver PERDENDO (não empatando)
        # e for massacre extremo. Contexto de desespero real.
        gc, gf = extrair_gols_placar(m.placar)
        if gc is None or gf is None:
            return False, "HT_PLACAR_INVALIDO"
        gols_fav = gc if fav == "CASA" else gf
        gols_adv = gf if fav == "CASA" else gc
        if gols_fav >= gols_adv:
            return False, f"HT_FAVORITO_NAO_PERDE | {m.placar}"
        massacre_ok, massacre_motivo = massacre_contextual_ht(m, lado, pos_gol_recente=False)
        # Exige massacre E pilares — critério mais rígido que antes.
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
    """Rótulo visual técnico apenas para canais de auditoria.

    Não altera parser, score, canal normal nem grupo grátis.
    """
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
]


def registrar_csv(m: Metricas, decisao_py: DecisaoPython, decisao_ia: DecisaoIA, score_medio: int, decisao_final: str, motivo: str) -> None:
    garantir_csv()
    row = {
        "data_hora": now_iso(),
        "jogo": m.jogo,
        "estrategia": m.estrategia,
        "minuto": m.tempo,
        "placar": m.placar,
        "mercado": m.mercado,
        "score_python": decisao_py.score,
        "decisao_ia": decisao_ia.decisao,
        "ia_original": decisao_ia.confianca_original,
        "ia_corrigida": decisao_ia.confianca_corrigida,
        "score_medio": score_medio,
        "lado_favorito": m.lado_favorito,
        "odd_favorito": m.odd_favorito,
        "lado_pressionante": m.lado_pressionante,
        "ultimo_gol_lado": m.ultimo_gol_lado,
        "valor_pos_evento_classe": m.valor_pos_evento_classe,
        "valor_pos_evento_motivo": m.valor_pos_evento_motivo,
        "protecao_ia_ativa": m.protecao_ia_ativa,
        "liga": m.liga,
        "decisao_final": decisao_final,
        "motivo_bloqueio": motivo,
        "parser_confianca": m.parser_confianca,
        "parser_observacoes": " | ".join(m.parser_observacoes),
        "fluxo_decisao": m.fluxo_decisao,
        "fluxo_motivo": m.fluxo_motivo,
        "teto_v9": decisao_py.detalhes.get("teto_v9", ""),
        "grupo_gratuito": m.grupo_gratuito,
        "motivo_grupo_gratuito": m.motivo_grupo_gratuito,
        "alavancagem": m.alavancagem,
        "motivo_alavancagem": m.motivo_alavancagem,
        "australia_leitura": m.australia_leitura,
        "motivo_australia": m.motivo_australia,
        "destino_final": m.destino_final,
        "modo_teste": str(MODO_TESTE),
        "previsao_over05_ht": m.previsao_over05_ht,
        "resultado_manual": "",
        "cornerpro": m.cornerpro,
        "bet365": m.bet365,
    }
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)


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
            log(f"⚠️ TypeNotFoundError envio tent={tentativa}/{max_tentativas} | reconectando")
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
    """Regra do primeiro alerta FT.

    Quando o pressionante marcou recentemente e com esse gol ficou vencendo
    (seja saindo de empate ou ampliando vantagem já existente), não enviamos
    direto e não matamos. Aguardamos BOT_FT CONFIRMAÇÃO.

    V010 — corrigido: antes usava gol_recente_pressionante_aumentou_vantagem(),
    que só pegava ampliação de vantagem (1x0→2x0, 2x1→3x1) mas não pegava
    o caso de empate→vitória (0x0→1x0, 1x1→2x1, 2x2→3x2).
    Bug real: Cienciano x Sporting Cristal — gol no 69' fez 3x2, alerta no 73'
    com pressão pré-gol ainda contaminando U10, score chegou a 90%.

    Casos que DEVEM cair em espera (pressionante vencendo após o gol):
      0x0 → 1x0 | 1x1 → 2x1 | 2x2 → 3x2 | 1x0 → 2x0 | 2x1 → 3x1

    Casos que NÃO devem cair em espera (pressionante não vencendo):
      0x1 → 1x1  (empatou — ainda tem jogo)
      0x2 → 1x2  (reduziu — ainda perdendo)
      gol da zebra / gol contra o pressionante
    """
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


def confirmacao_isolada_valida(m: Metricas) -> Tuple[bool, str]:
    """FT_CONFIRMAÇÃO sem CHAMA anterior só entra em urgência nova.

    Ex.: favorito empatando/perdendo ou sofreu gol contra fluxo dentro da janela.
    Limite operacional: até 82'.
    """
    if not eh_confirmacao(m.estrategia) or not eh_ft(m.estrategia):
        return False, "NAO_E_FT_CONFIRMACAO"
    if int(m.tempo or 0) > 82:
        return False, f"CONFIRMACAO_FORA_JANELA_{m.tempo}"
    fav = m.lado_favorito
    lado = m.lado_pressionante
    if fav not in {"CASA", "FORA"} or lado not in {"CASA", "FORA"}:
        return False, "SEM_FAVORITO_OU_PRESSIONANTE"

    # Favorito pressionando e ainda não venceu/resolvido.
    if fav == lado and favorito_nao_vencendo(m) and pressao_viva_lado(m, lado) and consequencia_minima_emocional(m, lado):
        return True, "CONF_ISOLADA_FAVORITO_NAO_VENCE_COM_PRESSAO"

    # Gol contra o favorito/pressionante pode criar urgência nova.
    if gol_recente(m, janela=6) and m.ultimo_gol_lado != lado and fav == lado and pressao_viva_lado(m, lado):
        return True, "CONF_ISOLADA_GOL_CONTRA_FLUXO_GEROU_URGENCIA"

    return False, "CONF_ISOLADA_SEM_URGENCIA_NOVA"


def comparar_alertas_confirmacao(old: Metricas, novo: Metricas) -> Tuple[bool, str, Dict[str, Any]]:
    """Compara alerta 1 e alerta 2.

    Retorna True só quando a confirmação realmente confirmou continuidade.
    Também bloqueia confirmação com gol recente do pressionante que deixou/ manteve
    o lado vencendo, pois no minuto 80/81 não há tempo para provar nova pressão.
    """
    detalhes: Dict[str, Any] = {}
    lado = old.lado_pressionante if old.lado_pressionante in {"CASA", "FORA"} else novo.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "CONF_SEM_LADO_COMPARAVEL", detalhes

    # Na confirmação, gol recente do pressionante resolvendo/deixando vencendo mata.
    if gol_recente_pressionante_resolveu_confirmacao(novo, janela=5):
        return False, (
            f"CONF_GOL_RECENTE_PRESSIONANTE_RESOLVEU | ultimo={novo.ultimo_gol}' {novo.ultimo_gol_lado} | "
            f"placar={novo.placar} | press={novo.lado_pressionante}"
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
        # queda forte em U5/U10/Chance/AP pesa mais.
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

    # Mantém/libera quando há confirmação objetiva de continuidade. O bot de
    # confirmação da CornerPro já é mais exigente, mas exigimos prova mínima.
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
    # Espera até ~82' com piso/teto operacional. Como o Telegram chega em tempo real,
    # usamos minutos de jogo como aproximação em segundos.
    tempo = int(m.tempo or 0)
    if tempo >= 82:
        return 90
    return max(120, min(600, (82 - tempo + 1) * 60))

# =========================================================
# SNIPER V2 — LEITURA SEPARADA FT
# Módulo isolado: não altera CHAMA, VOLUME, ARCE, score central nem IA V28.
# Filosofia: favorito forte + necessidade + pressão madura no fim do jogo.
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
    """True quando o gol recente pagou a pressão do favorito/pressionante."""
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


def filtro_sniper_ft_v2(m: "Metricas") -> Tuple[bool, str]:
    """Porta própria do 🎯 SNIPER.

    Não cria score novo e não substitui o V28. Apenas impede que o Sniper
    entre quando o contexto já perdeu valor. Se passar, segue pelo fluxo
    normal Python + IA V28.
    """
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
        if d["u5"] >= 4: provas += 1
        if d["u10"] >= 8: provas += 1
        if d["rb"] >= 2 or d["chance"] >= 10 or d["xg"] >= 0.40: provas += 1
        if ip["pico"] >= 22 or ip["c18"] >= 2: provas += 1
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
# Porta isolada antes do score normal.
# Reprovados morrem silenciosamente (CSV/log apenas).
# Aprovados seguem fluxo ALFA normal com mensagem pública ALFA.
# Não toca: score, IA, CHAMA, ARCE, HT, FT principal, grupo grátis, alavancagem, V13 HTML.
# =========================================================

def volume_ft_favorito_vencendo_extremo_v21(m: "Metricas") -> Tuple[bool, str]:
    """V21 — exceção raríssima do VOLUME_FT para favorito já vencendo.

    Regra oficial do bot Volume:
    - favorito empatando ou perdendo segue para os filtros normais;
    - favorito vencendo por 1 ou mais gols só passa se o jogo estiver
      EXTREMAMENTE vivo e o favorito ainda estiver amassando agora;
    - AP acumulado não basta; precisa pressão recente + finalização real + IP +
      domínio do lado certo.

    Esta função é usada somente na porta VOLUME_FT. Não altera score geral,
    ALFA_FT, CHAMA_FT, HT, IA nem grupo grátis.
    """
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

    # Gol de vantagem recente do favorito tende a ser pressão premiada.
    if m.ultimo_gol and m.ultimo_gol_lado == fav and minutos_desde_ultimo_gol(m) < V11_FREE_GOL_VANTAGEM_MIN:
        return False, f"V21_GOL_VANTAGEM_RECENTE_{minutos_desde_ultimo_gol(m)}MIN"

    d = dados_lado(m, fav)
    op = lado_oposto(fav)
    od = dados_lado(m, op)
    ip = ip_lado(m, fav)
    ap_diff = ap_diff_lado(m, fav)

    # O favorito precisa estar melhor AGORA, não só no acumulado.
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
        # Vencendo por 2+ precisa ser ainda mais raro. Só libera quando parece
        # massacre vivo/colapso do adversário, não placar confortável.
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
    """V21 — regra de placar do VOLUME_FT.

    Prioridade do bot volume:
    - favorito empatando ou perdendo = pode seguir para os próximos filtros;
    - favorito vencendo por 1+ = bloqueia, exceto em cenário EXTREMO real;
    - válido somente para VOLUME_FT, sem alterar score geral.
    """
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
    """Só bloqueia gol recente se colocou o favorito vencendo.
    Não bloqueia: favorito empatou, zebra marcou, favorito continua perdendo.
    """
    if not gol_recente(m, janela=5):
        return True, "VOLUME_FT_SEM_GOL_RECENTE"
    fav = m.lado_favorito
    lado_gol = m.ultimo_gol_lado
    # Gol do favorito que deixou ele vencendo — mata o contexto.
    if lado_gol == fav and ultimo_gol_deixou_lado_vencendo(m):
        return False, f"VOLUME_FT_GOL_RECENTE_FAV_VENCENDO | {m.ultimo_gol}' {lado_gol} | {m.placar}"
    return True, f"VOLUME_FT_GOL_RECENTE_OK | {m.ultimo_gol}' {lado_gol} | {m.placar}"


def _volume_ft_pressao_viva(m: "Metricas") -> Tuple[bool, str]:
    """U10 e U5 com vantagem clara para o lado pressionante."""
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE"
    idx = 0 if lado == "CASA" else 1
    u10_fav = m.ultimos10[idx]
    u10_adv = m.ultimos10[1 - idx]
    u5_fav = m.ultimos5[idx]
    u5_adv = m.ultimos5[1 - idx]
    # U10: diferença mínima de 4. U5: diferença mínima de 2.
    if (u10_fav - u10_adv) < 4:
        return False, f"VOLUME_FT_U10_INSUFICIENTE | {u10_fav}x{u10_adv}"
    if (u5_fav - u5_adv) < 2:
        return False, f"VOLUME_FT_U5_INSUFICIENTE | {u5_fav}x{u5_adv}"
    return True, f"VOLUME_FT_PRESSAO_VIVA | U10={u10_fav}x{u10_adv} U5={u5_fav}x{u5_adv}"


def _volume_ft_ataques_perigosos(m: "Metricas") -> Tuple[bool, str]:
    """Ataques perigosos com vantagem clara para o lado pressionante."""
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE_AP"
    idx = 0 if lado == "CASA" else 1
    ap_fav = m.ataques_perigosos[idx]
    ap_adv = m.ataques_perigosos[1 - idx]
    # Diferença mínima de 8 e razão mínima de 1.4x.
    if (ap_fav - ap_adv) < 8:
        return False, f"VOLUME_FT_AP_DIFERENCA_INSUFICIENTE | {ap_fav}x{ap_adv}"
    if ap_adv > 0 and (ap_fav / ap_adv) < 1.4:
        return False, f"VOLUME_FT_AP_RAZAO_INSUFICIENTE | {ap_fav}x{ap_adv}"
    return True, f"VOLUME_FT_AP_OK | {ap_fav}x{ap_adv}"


def _volume_ft_remates(m: "Metricas") -> Tuple[bool, str]:
    """Remates confirmando ofensiva real."""
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE_RB"
    idx = 0 if lado == "CASA" else 1
    rb_fav = m.remates_baliza[idx]
    rb_adv = m.remates_baliza[1 - idx]
    rl_fav = m.remates_lado[idx]
    # Exige mínimo de remates à baliza OU remates totais significativos.
    if rb_fav < 2 and rl_fav < 4:
        return False, f"VOLUME_FT_REMATES_INSUFICIENTES | RB={rb_fav} RL={rl_fav}"
    if rb_fav <= rb_adv and rb_fav < 3:
        return False, f"VOLUME_FT_RB_SEM_VANTAGEM | {rb_fav}x{rb_adv}"
    return True, f"VOLUME_FT_REMATES_OK | RB={rb_fav}x{rb_adv} RL={rl_fav}"


def _volume_ft_chance_golo(m: "Metricas") -> Tuple[bool, str]:
    """Chance de gol mínima para o lado pressionante."""
    lado = m.lado_pressionante
    if lado not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_LADO_PRESSIONANTE_CHANCE"
    idx = 0 if lado == "CASA" else 1
    chance = m.chance_golo[idx]
    if chance < 8:
        return False, f"VOLUME_FT_CHANCE_INSUFICIENTE | chance={chance}"
    return True, f"VOLUME_FT_CHANCE_OK | chance={chance}"


def _volume_ft_favorito_pressionando(m: "Metricas") -> Tuple[bool, str]:
    """Favorito deve ser o lado pressionante/dominante."""
    fav = m.lado_favorito
    press = m.lado_pressionante
    if fav not in {"CASA", "FORA"} or press not in {"CASA", "FORA"}:
        return False, "VOLUME_FT_SEM_FAV_OU_PRESS"
    if fav != press:
        return False, f"VOLUME_FT_FAV_NAO_PRESSIONANTE | fav={fav} press={press}"
    return True, f"VOLUME_FT_FAV_PRESSIONANTE | {fav}"


def _volume_ft_minuto(m: "Metricas") -> Tuple[bool, str]:
    """Janela ideal 65–80. Após 85 bloqueia."""
    t = int(m.tempo or 0)
    if t > 85:
        return False, f"VOLUME_FT_FORA_JANELA_TARDIO | {t}'"
    if t < 65:
        return False, f"VOLUME_FT_FORA_JANELA_CEDO | {t}'"
    return True, f"VOLUME_FT_MINUTO_OK | {t}'"


def filtro_volume_ft(m: "Metricas") -> Tuple[bool, str]:
    """Porta exclusiva do VOLUME_FT. Aplica todos os filtros em sequência.
    Retorna (True, motivo) se passou. (False, motivo) se bloqueado.
    """
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
# Camada completamente isolada. Zero impacto no score/funil.
# Ponto de inserção: após registrar_csv() e enviar_auditoria().
# Bots excluídos dos reprovados: BOT_VOLUME_FT, BOT_VOLUME_HT (futuros).
# =========================================================

import json
import threading as _threading

# Diretório de dados: Railway Volume /data com fallback para pasta local.
def _data_dir() -> Path:
    d = Path(os.getenv("DATA_DIR", "/data"))
    if not d.exists():
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            d = Path("auditoria_html")
            d.mkdir(parents=True, exist_ok=True)
    return d

_v13_lock = _threading.Lock()

# Estratégias excluídas dos reprovados (bots de volume).
_V13_ESTRATEGIAS_EXCLUIDAS_REPROV = {"BOT_VOLUME_FT", "BOT_VOLUME_HT", "VOLUME_FT"}


def _v13_arquivo_json(tipo: str, dt: Optional[datetime] = None) -> Path:
    """Retorna caminho do JSON diário. tipo = 'aprovados' ou 'reprovados'."""
    dt = dt or datetime.now()
    nome = f"{tipo}_{dt.strftime('%Y_%m_%d')}.json"
    return _data_dir() / nome


def _v13_arquivo_html(tipo: str, dt: Optional[datetime] = None) -> Path:
    dt = dt or datetime.now()
    nome = f"{tipo}_{dt.strftime('%Y_%m_%d')}.html"
    return _data_dir() / nome


def v13_registrar(m: "Metricas", score_medio: int, aprovado: bool, motivo: str) -> None:
    """Registra alerta no JSON diário e regera o HTML imediatamente."""
    if not HABILITAR_V13_AUDITORIA_HTML:
        return
    # Reprovados de bots de volume não entram no HTML de auditoria.
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
            arq.write_text(json.dumps(registros, ensure_ascii=False, indent=2), encoding="utf-8")
            v13_gerar_html(tipo)
    except Exception as e:
        log(f"⚠️ V13 registrar erro | {type(e).__name__}: {e}")


def v13_gerar_html(tipo: str, dt: Optional[datetime] = None) -> Path:
    """Gera HTML diário interativo a partir do JSON.

    V22 — restaura o padrão oficial de auditoria COUTIPS:
    - botões GREEN / RED por jogo;
    - marcação salva no próprio navegador via localStorage;
    - contadores de marcados/greens/reds/pendentes;
    - exportação JSON e CSV das marcações;
    - link CornerPro preservado.

    Observação: como o HTML é aberto localmente no navegador/Telegram, a marcação
    fica salva no aparelho/navegador usado para auditar, não volta automaticamente
    para o Railway. Para enviar resultado, usar os botões de exportação.
    """
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
    cor = "#00c853" if tipo == "aprovados" else "#e53935"
    data_fmt = dt.strftime("%d/%m/%Y")
    total = len(registros)
    storage_key = f"coutips_auditoria_{tipo}_{dt.strftime('%Y_%m_%d')}"

    linhas_html = []
    for i, r in enumerate(registros):
        link = str(r.get("cornerpro", "") or "")
        link_tag = f'<a href="{html.escape(link)}" target="_blank" rel="noopener">🔗 CornerPro</a>' if link else "—"
        motivo_txt = f'<div class="motivo">{html.escape(str(r.get("motivo", "") or ""))}</div>' if r.get("motivo") else ""
        bot = html.escape(str(r.get("bot", "") or ""))
        jogo = html.escape(str(r.get("jogo", "") or ""))
        minuto = html.escape(str(r.get("minuto", "") or ""))
        placar = html.escape(str(r.get("placar", "") or ""))
        score = html.escape(str(r.get("score", "") or ""))
        liga = html.escape(str(r.get("liga", "") or ""))
        uid_base = f"{r.get('bot','')}|{r.get('jogo','')}|{r.get('minuto','')}|{r.get('placar','')}|{r.get('score','')}|{i}"
        # UID curto e seguro para HTML/JS. Evita aspas, espaços e acentos quebrando botões.
        uid = hashlib.sha1(uid_base.encode("utf-8", errors="ignore")).hexdigest()[:16]
        linhas_html.append(f"""
        <div class="item" data-id="{uid}" data-bot="{bot}" data-jogo="{jogo}" data-minuto="{minuto}" data-placar="{placar}" data-score="{score}" data-liga="{liga}" data-cornerpro="{html.escape(link, quote=True)}">
          <div class="topline">
            <span class="badge">{bot}</span>
            <strong>{jogo}</strong>
          </div>
          <div class="meta">⏱ {minuto}' &nbsp;|&nbsp; 📊 {placar} &nbsp;|&nbsp; Score: <b>{score}%</b> &nbsp;|&nbsp; Liga: {liga} &nbsp;|&nbsp; {link_tag}</div>
          {motivo_txt}
          <div class="actions">
            <button type="button" class="btn green" data-action="GREEN" data-id="{uid}">✅ GREEN</button>
            <button type="button" class="btn red" data-action="RED" data-id="{uid}">❌ RED</button>
            <button type="button" class="btn clear" data-action="CLEAR" data-id="{uid}">↩ LIMPAR</button>
            <span class="status" id="status-{uid}">PENDENTE</span>
          </div>
        </div>""")

    corpo = "\n".join(linhas_html) if linhas_html else "<p class='vazio'>Nenhum registro ainda.</p>"

    conteudo = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,user-scalable=no">
<title>COUTIPS {titulo} — {data_fmt}</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d0d0d;color:#e0e0e0;padding:16px;padding-bottom:90px}}
  h1{{color:{cor};font-size:1.25rem;margin-bottom:4px}}
  .sub{{color:#999;font-size:0.85rem;margin-bottom:12px}}
  .toolbar{{position:sticky;top:0;z-index:20;background:#101010;border:1px solid #272727;border-radius:10px;padding:10px;margin-bottom:12px;box-shadow:0 8px 20px rgba(0,0,0,.35)}}
  .stats{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px}}
  .pill{{background:#1b1b1b;border:1px solid #333;border-radius:999px;padding:5px 9px;font-size:.78rem;color:#ddd}}
  .pill.green{{border-color:#00c853;color:#69f0ae}}
  .pill.red{{border-color:#e53935;color:#ff8a80}}
  .pill.pending{{border-color:#ffb300;color:#ffd54f}}
  .item{{background:#181818;border-left:4px solid {cor};border-radius:8px;padding:11px 12px;margin:8px 0;font-size:0.88rem;line-height:1.55}}
  .item.green-marked{{box-shadow:0 0 0 1px rgba(0,200,83,.5);border-left-color:#00c853}}
  .item.red-marked{{box-shadow:0 0 0 1px rgba(229,57,53,.55);border-left-color:#e53935}}
  .topline{{display:flex;align-items:center;gap:7px;flex-wrap:wrap;margin-bottom:3px}}
  .badge{{display:inline-block;background:#1e3a5f;color:#90caf9;padding:2px 8px;border-radius:10px;font-size:0.72rem;font-weight:800}}
  .meta{{color:#ddd}}
  .motivo{{color:#ff8a65;font-size:0.8rem;margin-top:4px;word-break:break-word}}
  a{{color:#ffb300;text-decoration:none}}
  a:hover{{text-decoration:underline}}
  .actions{{display:flex;gap:7px;align-items:center;flex-wrap:wrap;margin-top:10px}}
  .btn{{border:0;border-radius:8px;padding:8px 10px;font-weight:800;font-size:.78rem;cursor:pointer;color:#fff}}
  .btn.green{{background:#0b8f3a}}
  .btn.red{{background:#b3261e}}
  .btn.clear{{background:#333;color:#ddd}}
  .btn.export{{background:#263238;color:#fff;border:1px solid #455a64}}
  .status{{font-size:.78rem;font-weight:900;color:#ffb300;margin-left:2px}}
  .status.green{{color:#69f0ae}}
  .status.red{{color:#ff8a80}}
  .vazio{{color:#888;margin-top:16px}}
</style>
</head>
<body>
<h1>📋 COUTIPS — {titulo}</h1>
<div class="sub">{data_fmt} &nbsp;|&nbsp; {total} registro(s)</div>
<div class="toolbar">
  <div class="stats">
    <span class="pill">Total: <b id="total">{total}</b></span>
    <span class="pill green">GREEN: <b id="greens">0</b></span>
    <span class="pill red">RED: <b id="reds">0</b></span>
    <span class="pill pending">Pendentes: <b id="pendentes">{total}</b></span>
  </div>
  <div class="actions">
    <button type="button" class="btn export" onclick="exportarJSON()">⬇ Exportar JSON</button>
    <button type="button" class="btn export" onclick="exportarCSV()">⬇ Exportar CSV</button>
  </div>
</div>
{corpo}
<script>
const STORAGE_KEY = {json.dumps(storage_key)};
function carregar() {{
  try {{ return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{{}}'); }} catch(e) {{ return {{}}; }}
}}
function salvar(dados) {{
  localStorage.setItem(STORAGE_KEY, JSON.stringify(dados));
}}
function marcarResultado(id, resultado) {{
  const dados = carregar();
  dados[id] = resultado;
  salvar(dados);
  aplicarEstado();
}}
function limparResultado(id) {{
  const dados = carregar();
  delete dados[id];
  salvar(dados);
  aplicarEstado();
}}
function aplicarEstado() {{
  const dados = carregar();
  let greens = 0, reds = 0;
  document.querySelectorAll('.item').forEach(item => {{
    const id = item.dataset.id;
    const res = dados[id];
    const st = document.getElementById('status-' + id);
    item.classList.remove('green-marked','red-marked');
    st.classList.remove('green','red');
    if (res === 'GREEN') {{
      greens++; item.classList.add('green-marked'); st.textContent = '✅ GREEN'; st.classList.add('green');
    }} else if (res === 'RED') {{
      reds++; item.classList.add('red-marked'); st.textContent = '❌ RED'; st.classList.add('red');
    }} else {{
      st.textContent = 'PENDENTE';
    }}
  }});
  const total = document.querySelectorAll('.item').length;
  document.getElementById('greens').textContent = greens;
  document.getElementById('reds').textContent = reds;
  document.getElementById('pendentes').textContent = total - greens - reds;
}}
function coletarResultados() {{
  const dados = carregar();
  return Array.from(document.querySelectorAll('.item')).map(item => {{
    const id = item.dataset.id;
    return {{
      id: id,
      resultado: dados[id] || '',
      bot: item.dataset.bot || '',
      jogo: item.dataset.jogo || '',
      minuto: item.dataset.minuto || '',
      placar: item.dataset.placar || '',
      score: item.dataset.score || '',
      liga: item.dataset.liga || '',
      cornerpro: item.dataset.cornerpro || ''
    }};
  }});
}}
function baixar(nome, conteudo, tipo) {{
  const blob = new Blob([conteudo], {{type: tipo}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = nome; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
}}
function exportarJSON() {{
  baixar('coutips_{tipo}_{dt.strftime('%Y_%m_%d')}_resultados.json', JSON.stringify(coletarResultados(), null, 2), 'application/json;charset=utf-8');
}}
function csvEscape(v) {{
  v = String(v ?? '');
  return '"' + v.replaceAll('"', '""') + '"';
}}
function exportarCSV() {{
  const rows = coletarResultados();
  const header = ['resultado','bot','jogo','minuto','placar','score','liga','cornerpro'];
  const csv = [header.join(',')].concat(rows.map(r => header.map(h => csvEscape(r[h])).join(','))).join('\n');
  baixar('coutips_{tipo}_{dt.strftime('%Y_%m_%d')}_resultados.csv', csv, 'text/csv;charset=utf-8');
}}
document.addEventListener('click', function(e) {{
  const btn = e.target.closest('button[data-action][data-id]');
  if (!btn) return;
  const id = btn.dataset.id;
  const action = btn.dataset.action;
  if (action === 'GREEN' || action === 'RED') {{
    marcarResultado(id, action);
  }} else if (action === 'CLEAR') {{
    limparResultado(id);
  }}
}});
aplicarEstado();
</script>
</body>
</html>"""

    try:
        arq_html.write_text(conteudo, encoding="utf-8")
        log(f"📄 V22 HTML interativo gerado | {arq_html.name} | {total} registros")
    except Exception as e:
        log(f"⚠️ V13 gerar_html erro | {type(e).__name__}: {e}")
    return arq_html

async def v13_enviar_htmls_telegram() -> None:
    """Envia os HTMLs de aprovados e reprovados do dia anterior para @ALFA_CON às 00:05."""
    if not HABILITAR_V13_AUDITORIA_HTML:
        return
    ontem = datetime.now() - timedelta(days=1)
    canal = CONFIRMATION_CHANNEL
    for tipo in ("aprovados", "reprovados"):
        arq_html = _v13_arquivo_html(tipo, ontem)
        arq_json = _v13_arquivo_json(tipo, ontem)
        # Garante que o HTML final está atualizado antes de enviar.
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
    """Loop que dispara o envio dos HTMLs todo dia às 00:05."""
    log("🕐 V13 agendador iniciado — envio diário às 00:05")
    while True:
        try:
            agora = datetime.now()
            # Próxima execução às 00:05 do dia seguinte.
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
# DECISÃO / PROCESSAMENTO
# =========================================================

def chave_alerta_unica(texto: str) -> str:
    limpo = remover_acentos(texto)
    jogo = extrair_jogo(limpo)
    tempo = extrair_tempo(limpo)
    resultado = extrair_resultado(limpo)
    estrategia = detectar_estrategia(limpo)
    # V25 — HT: chave sem minuto para bloquear mesmo jogo em minutos diferentes.
    # FT mantém minuto na chave porque tem bot de confirmação (dois alertas válidos).
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
    """Compatibilidade legada.

    No V11 o canal completo recebe tudo aprovado. O grupo grátis é um envio
    adicional, controlado por elegivel_grupo_gratuito_v11().
    """
    return COMPLETE_CHANNEL


async def registrar_bloqueio_fluxo(m: Metricas, motivo: str, decisao: str = "REPROVADO", score: int = 0) -> None:
    """Registra bloqueios/esperas que acontecem antes do score/IA."""
    decisao_py = DecisaoPython(score=score, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo, detalhes={})
    decisao_ia = DecisaoIA(decisao="BLOQUEAR", confianca_original=score, confianca_corrigida=score, motivo="FLUXO_PRE_SCORE", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
    registrar_csv(m, decisao_py, decisao_ia, score, decisao, motivo)
    await enviar_auditoria(m, score, score, score, False, motivo)


async def processar_alerta(alerta: Alerta) -> None:
    m = alerta.metricas
    chave = alerta.chave_jogo

    # V20 — fail-safe definitivo: se o texto bruto diz VOLUME_FT, mas qualquer
    # etapa anterior classificou como ALFA_HT/ALFA_FT, força VOLUME_FT antes
    # de passar pelo score. Isso impede que Volume caia no fluxo normal.
    if contem_volume_ft_bruto(m.texto_bruto) and m.estrategia != "VOLUME_FT":
        antigo = m.estrategia
        m.estrategia = "VOLUME_FT"
        m.parser_observacoes.append(f"V20_FORCE_VOLUME_FT:{antigo}->VOLUME_FT")
        preencher_contexto_calculado(m)
        log(f"🧭 V20 FORCE_VOLUME_FT | {antigo}->VOLUME_FT | {m.jogo} | {m.tempo}'")

    # SNIPER V2 — fail-safe: se o texto bruto tiver SNIPER, força a família separada.
    if contem_sniper_bruto(m.texto_bruto) and m.estrategia != "SNIPER_FT":
        antigo = m.estrategia
        m.estrategia = "SNIPER_FT"
        m.parser_observacoes.append(f"SNIPER_FORCE:{antigo}->SNIPER_FT")
        preencher_contexto_calculado(m)
        log(f"🎯 SNIPER FORCE | {antigo}->SNIPER_FT | {m.jogo} | {m.tempo}'")

    # =====================================================
    # V007 — bloqueios e roteamentos antes do score/IA
    # =====================================================

    # Base sem mercado operacional: não desperdiça análise.
    if HABILITAR_BLOQUEIO_BASE_SEM_MERCADO and competicao_base_bloqueada(m):
        m.fluxo_decisao = "BLOQUEADO_BASE_SEM_MERCADO"
        m.fluxo_motivo = "U18_U19_U20_SUB18_SUB19_SUB20"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"⛔ {motivo} | {m.jogo} | {m.competicao}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        return

    # Parser crítico: não mata por rótulo corrigível, mas segura dado quebrado.
    if HABILITAR_BLOQUEIO_PARSER_CRITICO and m.parser_confianca <= PARSER_CONFIANCA_CRITICA:
        m.fluxo_decisao = "BLOQUEADO_PARSER_CRITICO"
        m.fluxo_motivo = f"confianca={m.parser_confianca}/8"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"⛔ {motivo} | {m.jogo}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        return

    # V27 — POS70 OFF: bot redundante após auditoria. Bloqueia antes do score.
    if HABILITAR_V27_POS70_OFF and contem_pos70_bruto(m.texto_bruto):
        m.fluxo_decisao = "V27_POS70_DESATIVADO"
        m.fluxo_motivo = "POS70_OFF_REDUCAO_VOLUME_REDUNDANCIA"
        motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
        log(f"🔇 {motivo} | {m.jogo}")
        await registrar_bloqueio_fluxo(m, motivo, score=0)
        return

    # V27/V28 — HT_MODERADO: taxa 53% — bloqueio total no V28.
    # V27 modo observacao preservado via flag; V28 bloqueia completamente.
    if contem_ht_moderado_bruto(m.texto_bruto):
        if HABILITAR_V28 and HABILITAR_V28_HT_MODERADO_BLOQUEIO:
            # V28 — bloqueio total: HT_MODERADO descontinuado
            m.fluxo_decisao = "V28_HT_MODERADO_BLOQUEADO"
            m.fluxo_motivo = "HT_MODERADO_DESCONTINUADO_V28_TAXA_53PCT"
            motivo = f"{m.fluxo_decisao} | {m.fluxo_motivo}"
            log(f"⛔ {motivo} | {m.jogo}")
            await registrar_bloqueio_fluxo(m, motivo, score=0)
            return
        elif HABILITAR_V27_HT_MODERADO_ALAV_OBS:
            # V27 modo observacao: passa pelo score mas nao vai ao canal principal
            m.fluxo_decisao = "V27_HT_MODERADO_ALAVANCAGEM_OBSERVACAO"
            lado_ht = m.lado_pressionante
            pos_gol_ht = gol_recente_do_pressionante(m, janela=3)
            ht_ok, ht_motivo = massacre_contextual_ht(m, lado_ht, pos_gol_recente=pos_gol_ht)
            m.fluxo_motivo = ht_motivo
            if not ht_ok:
                motivo = f"V27_HT_MODERADO_BLOQUEADO_SEM_MASSACRE | {ht_motivo}"
                log(f"⛔ {motivo} | {m.jogo}")
                await registrar_bloqueio_fluxo(m, motivo, score=0)
                return
            log(f"🧪 V27 HT_MODERADO PASSOU COMO HT_ALAVANCAGEM_OBS | {m.jogo} | {ht_motivo}")

    # SNIPER V2 — porta própria antes do score normal.
    # Reprovados morrem silenciosamente no CSV/log, igual filosofia do Volume.
    # Aprovados seguem o fluxo normal Python + IA V28, sem mexer no CHAMA/VOLUME/ARCE.
    if eh_sniper_ft(m.estrategia):
        ok_sniper, motivo_sniper = filtro_sniper_ft_v2(m)
        if not ok_sniper:
            m.fluxo_decisao = "SNIPER_V2_BLOQUEADO"
            m.fluxo_motivo = motivo_sniper
            decisao_py_sniper = DecisaoPython(score=0, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_sniper, detalhes={})
            decisao_ia_sniper = DecisaoIA(decisao="BLOQUEAR", confianca_original=0, confianca_corrigida=0, motivo="SNIPER_V2_FILTRO", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
            registrar_csv(m, decisao_py_sniper, decisao_ia_sniper, 0, "REPROVADO", motivo_sniper)
            log(f"🔇 SNIPER V2 BLOQUEADO SILENCIOSO | {motivo_sniper} | {m.jogo}")
            return
        m.fluxo_decisao = "SNIPER_V2_APROVADO_PARA_SCORE"
        m.fluxo_motivo = motivo_sniper
        log(f"🎯 SNIPER V2 FILTRO PASSOU | {motivo_sniper} | {m.jogo}")

    # V014 — VOLUME_FT: porta exclusiva antes do score normal.
    # Reprovados morrem silenciosamente — só CSV/log interno, sem canal de reprovados.
    if HABILITAR_VOLUME_FT and eh_volume_ft(m.estrategia):
        ok_vol, motivo_vol = filtro_volume_ft(m)
        if not ok_vol:
            m.fluxo_decisao = "VOLUME_FT_BLOQUEADO"
            m.fluxo_motivo = motivo_vol
            # Registra no CSV para rastreio interno. Sem envio para canais de reprovados.
            decisao_py_vol = DecisaoPython(score=0, aprovado_pre_ia=False, status="REPROVADO", motivo=motivo_vol, detalhes={})
            decisao_ia_vol = DecisaoIA(decisao="BLOQUEAR", confianca_original=0, confianca_corrigida=0, motivo="VOLUME_FT_FILTRO", protecao_ativa=False, protecao_motivo="SEM_PROTECAO")
            registrar_csv(m, decisao_py_vol, decisao_ia_vol, 0, "REPROVADO", motivo_vol)
            log(f"🔇 VOLUME_FT BLOQUEADO SILENCIOSO | {motivo_vol} | {m.jogo}")
            return
        # Passou o filtro: continua no fluxo normal do V13 como ALFA_FT.
        log(f"🟢 VOLUME_FT FILTRO PASSOU | {motivo_vol} | {m.jogo}")
        m.fluxo_decisao = "VOLUME_FT_APROVADO"  # V14 — preserva rastreio para auditoria futura
        m.estrategia = "ALFA_FT"  # Mensagem pública sai como ALFA, não como VOLUME_FT.

    # V026 — FAV_NAO_PRESSIONANTE: penaliza score FT onde favorito não é o lado pressionante.
    # Modo auditoria: não bloqueia — registra motivo e aplica penalidade no score.
    # O score ainda pode aprovar dependendo da margem. Permite observar antes de decidir.
    if HABILITAR_V26_FAV_NAO_PRESSIONANTE and eh_ft(m.estrategia) and not eh_confirmacao(m.estrategia):
        fav_v26 = m.lado_favorito
        press_v26 = m.lado_pressionante
        if fav_v26 in {"CASA", "FORA"} and press_v26 in {"CASA", "FORA"} and fav_v26 != press_v26:
            motivo_nao_press = f"V26_FAV_NAO_PRESSIONANTE_PENALIDADE={V26_FAV_NAO_PRESSIONANTE_PENALIDADE} | fav={fav_v26} press={press_v26}"
            log(f"⚠️ V26 FAV_NAO_PRESSIONANTE | penalidade=-{V26_FAV_NAO_PRESSIONANTE_PENALIDADE} | {m.jogo} | {motivo_nao_press}")
            # Registra na flag de observação da métrica para rastreio no CSV.
            m.fluxo_motivo = (m.fluxo_motivo or "") + f" | {motivo_nao_press}"
            # A penalidade é aplicada no score Python antes da IA.
            # Usamos um atributo auxiliar que calcular_score_python considera.
            m.penalidade_v26_fav_nao_press = V26_FAV_NAO_PRESSIONANTE_PENALIDADE

    # BOT_FT CONFIRMAÇÃO: se existe gatilho anterior pendente, comparar obrigatoriamente.
    if HABILITAR_CONFIRMACAO_V2 and eh_confirmacao(m.estrategia) and eh_ft(m.estrategia):
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
                return
        else:
            # Confirmação isolada só entra com urgência nova. E ainda assim morre se o gol recente
            # do pressionante deixou o lado vencendo/resolvido em cima do alerta.
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
                return

            ok_iso, motivo_iso = confirmacao_isolada_valida(m)
            m.fluxo_decisao = "CONFIRMACAO_ISOLADA_APROVADA_PARA_SCORE" if ok_iso else "CONFIRMACAO_ISOLADA_BLOQUEADA"
            m.fluxo_motivo = motivo_iso
            log(f"🧪 FT_CONF_ISOLADA | ok={ok_iso} | {m.jogo} | {motivo_iso}")
            if not ok_iso:
                await registrar_bloqueio_fluxo(m, f"{m.fluxo_decisao} | {motivo_iso}", score=0)
                ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
                return

    # Primeiro alerta FT com gol recente do pressionante aumentando vantagem: aguarda confirmação.
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
        # Registra como observação operacional, sem enviar ao canal principal.
        await registrar_bloqueio_fluxo(m, f"AGUARDANDO_CONFIRMACAO | {motivo_aguardar}", decisao="AGUARDANDO", score=0)
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": 0, "recebido_em": time.time()}
        return

    # =====================================================
    # Fluxo normal já existente: Python + IA + envio
    # =====================================================
    decisao_py = score_python_contextual(m, chave)
    log(
        f"📊 PY_PROCESSADO | {m.estrategia} | Gol={decisao_py.score}% | {m.jogo} | "
        f"Liga={m.liga} | Fav={m.lado_favorito}/{m.odd_favorito} | Press={m.lado_pressionante} | Valor={m.valor_pos_evento_classe} | Fluxo={m.fluxo_decisao}"
    )

    decisao_ia_txt, confianca_ia, motivo_ia = await consultar_openai(m, decisao_py)
    decisao_ia = calcular_protecao_ia(m, decisao_py, decisao_ia_txt, confianca_ia)

    # Bloqueio crítico só depois da proteção contextual.
    if decisao_ia.confianca_corrigida <= 45 and not decisao_ia.protecao_ativa:
        score_medio = round((decisao_py.score + decisao_ia.confianca_corrigida) / 2)
        motivo = f"IA_BLOQUEIO_CRITICO | {motivo_ia}"
        log(f"⛔ BLOQUEADO IA CRITICA | IA={decisao_ia.confianca_corrigida}% | {m.jogo}")
        registrar_csv(m, decisao_py, decisao_ia, score_medio, "REPROVADO", motivo)
        await enviar_auditoria(m, decisao_py.score, decisao_ia.confianca_corrigida, score_medio, False, motivo)
        v13_registrar(m, score_medio, False, motivo)  # V13
        ultimas_leituras_por_jogo[chave] = {"metricas": m, "score": decisao_py.score, "recebido_em": time.time()}
        return

    score_medio = clamp((decisao_py.score + decisao_ia.confianca_corrigida) / 2)

    # V026 — aplicar penalidade FAV_NAO_PRESSIONANTE no score_medio.
    penalidade_v26 = getattr(m, 'penalidade_v26_fav_nao_press', 0)
    if penalidade_v26:
        score_medio_original = score_medio
        score_medio = max(0, score_medio - penalidade_v26)
        log(f"📉 V26 penalidade aplicada | score {score_medio_original}% → {score_medio}% | -{penalidade_v26} | {m.jogo}")

    # V27 — teto forte para liga UNDER em qualquer fluxo aprovado.
    if HABILITAR_V27_UNDER_TETO_82 and m.liga == "UNDER" and score_medio > V27_UNDER_TETO_SCORE:
        score_under_original = score_medio
        score_medio = V27_UNDER_TETO_SCORE
        m.fluxo_motivo = (m.fluxo_motivo or "") + f" | V27_UNDER_TETO_{V27_UNDER_TETO_SCORE}:{score_under_original}->{score_medio}"
        log(f"🟡 V27 UNDER teto aplicado | score {score_under_original}% → {score_medio}% | {m.jogo}")

    corte = corte_por_estrategia(m.estrategia)
    aprovado = score_medio >= corte and decisao_py.status != "REPROVADO"

    # Confirmação forte só pode furar canal técnico quando não foi bloqueada pelo fluxo V007.
    if eh_confirmacao(m.estrategia) and score_medio >= 92:
        aprovado = True

    # Pré-calcula campos V11 para CSV antes do registro.
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
    v13_registrar(m, score_medio, aprovado, motivo_final)  # V13

    # V27 — HT_MODERADO em observação: mesmo aprovado, não vai para canal principal.
    # No V28 este bloco nunca e alcancado porque o HT_MODERADO e bloqueado antes.
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

    # V011 — canal completo recebe tudo que foi aprovado.
    alavanca_ok, alavanca_motivo = selo_alavancagem_v11(m, score_medio)
    m.alavancagem = "SIM" if alavanca_ok else "NAO"
    m.motivo_alavancagem = alavanca_motivo

    # Canal completo/interno recebe visual técnico por família de bot.
    # Grupo grátis/principal continua usando formatar_alerta_cliente() normal.
    mensagem_completa = formatar_alerta_canal_completo(m, score_medio, alavancagem=alavanca_ok)
    canal_completo = destino_principal(m, score_medio)
    await enfileirar_envio(canal_completo, mensagem_completa)
    destinos = [canal_completo]

    # V011 — grupo grátis é vitrine: limitado, sem selo, sem confirmações.
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
    log(
        f"✅ ENVIADO/ENFILEIRADO V11 | {m.estrategia} | score={score_medio}% | "
        f"destinos={m.destino_final} | gratis={m.grupo_gratuito}:{m.motivo_grupo_gratuito} | "
        f"alfa={m.alavancagem}:{m.motivo_alavancagem} | {m.jogo}"
    )

async def janela_decisao(chave: str) -> None:
    await asyncio.sleep(JANELA_DECISAO_SEGUNDOS)
    async with lock_jogo(chave):
        alertas = pendentes_por_jogo.pop(chave, [])
        tarefas_decisao.pop(chave, None)
    if not alertas:
        return
    # Usa o alerta mais recente da janela.
    alerta = sorted(alertas, key=lambda a: a["recebido_em"])[-1]
    await processar_alerta(alerta["alerta"])


def auditoria_autorizada(event: events.NewMessage.Event) -> bool:
    """V24 — restringe o comando /auditoria.

    Aceita:
    - mensagens OUTGOING do próprio usuário;
    - sender_id ou chat_id em AUDITORIA_CHAT_IDS;
    - posts de canal sem sender_id (sender_id=None): aceita se AUDITORIA_CHAT_IDS
      está configurado — assumindo que só o dono posta no canal monitorado.
    """
    try:
        if bool(getattr(event, "out", False)) or bool(getattr(event, "outgoing", False)):
            return True
        chat_id = int(getattr(event, "chat_id", 0) or 0)
        sender_id = getattr(event, "sender_id", None)
        # Post de canal: sender_id é None. Se AUDITORIA_CHAT_IDS está configurado,
        # aceita — o canal é privado e só o dono envia mensagens.
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

        # V19 — comando /auditoria restrito.
        # Por padrão, só comando OUTGOING do próprio usuário recebe HTML.
        # Incoming externo só recebe se chat_id/sender_id estiver em AUDITORIA_CHAT_IDS.
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
# WATCHDOG / MAIN
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


async def main() -> None:
    global tarefa_envio
    logar_versao_inicial()
    garantir_csv()
    v17_carregar_estado()  # V17 — carrega do disco (fallback local)

    tarefa_envio = asyncio.create_task(trabalhador_fila_envio())
    asyncio.create_task(watchdog())
    if HABILITAR_V13_AUDITORIA_HTML:
        asyncio.create_task(v13_agendador())  # V13 — envio diário às 00:05

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        await receber_mensagem(event)

    @client.on(events.MessageEdited(incoming=True))
    async def handler_edit(event):
        # V19 — edições da CornerPro não reprocessam alerta.
        # Evita duplicar CSV/HTML/OpenAI quando a mensagem original é corrigida.
        return

    # V24 — handler para comando auditoria em canal.
    # Escuta mensagens outgoing normais.
    @client.on(events.NewMessage(outgoing=True))
    async def handler_outgoing(event):
        try:
            texto = (event.raw_text or "").strip().lower()
            if texto in ("auditoria", "/auditoria"):
                await receber_mensagem(event)
        except Exception as e:
            log(f"⚠️ V24 handler_outgoing erro | {type(e).__name__}: {e}")

    # V24 — captura posts de canal via handler incoming geral.
    # auditoria_autorizada() filtra por ID — só o dono recebe os HTMLs.
    # O handler incoming já existia; o filtro por texto e ID é feito dentro de receber_mensagem.

    log("🚀 INICIANDO BOT")
    await client.start()
    log("✅ TELEGRAM CONECTADO COM SUCESSO")
    # V26 — carrega estado do Telegram após conexão (backup que sobrevive a restarts)
    await v26_carregar_estado_telegram()
    log(f"🤖 OpenAI {'ATIVA' if OPENAI_HABILITADO and OPENAI_API_KEY else 'DESATIVADA'} ({OPENAI_MODEL})")
    log(f"📡 Canais ativos: principal={TARGET_CHANNEL} | confirmação={CONFIRMATION_CHANNEL}")
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
