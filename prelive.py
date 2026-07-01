# -*- coding: utf-8 -*-
"""
COUTIPS / ALFA — PRÉ-LIVE (processo separado do main.py)

Isolado do sistema ao vivo em 30/06/2026, depois do crash de 27/06/2026
(AuthKeyDuplicatedError) ter derrubado o ao vivo por causa de uma falha
que era do pré-live. A partir desta versão, pré-live e ao vivo rodam em
processos Python diferentes, com sessões Telegram diferentes — uma falha
aqui nunca mais chega perto do ao vivo.

Roda como um serviço PRÓPRIO no Railway, separado do serviço do main.py.
Usa a variável SESSION_STRING_PRELIVE (não SESSION_STRING — essa é do
ao vivo). Exige replicas=1 nas configurações do Railway, igual ao ao vivo.
"""

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
try:
    from telethon.errors.common import TypeNotFoundError
except Exception:
    class TypeNotFoundError(Exception):
        pass

import fcntl
import os
import re
import json
import time
import random
import asyncio
import traceback
import unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

VERSAO_COUTIPS = "ALFA_COUTIPS_2026_06_30_PRELIVE_ISOLADO_V1"


# =========================================================
# LOG
# =========================================================
def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def logar_versao_inicial() -> None:
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log(f"🚀 VERSAO_COUTIPS_ATIVA = {VERSAO_COUTIPS}")
    log("✅ Sistema: PRÉ-LIVE V2 (scraper TheoBorges + market engine), isolado do ao vivo")
    log("✅ Sessão Telegram própria (SESSION_STRING_PRELIVE) — nunca compete com o ao vivo")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


# =========================================================
# CONFIGURAÇÃO (variáveis de ambiente)
# =========================================================
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
try:
    API_ID = int(API_ID_RAW)
except Exception:
    API_ID = 0

SESSION_STRING_PRELIVE = os.getenv("SESSION_STRING_PRELIVE", "").strip()

CANAL_MULTIPLAS_PRELIVE = os.getenv("CANAL_MULTIPLAS_PRELIVE", "")   # https://t.me/Pre_cout
CANAL_LOGS_PRELIVE = os.getenv("CANAL_LOGS_PRELIVE", "")             # https://t.me/Cout_aud

ODD_MAXIMA_FAVORITO_PRELIVE = float(os.getenv("ODD_MAXIMA_FAVORITO_PRELIVE", "2.20"))

PRELIVE_SCORE_MINIMO = int(os.getenv("PRELIVE_SCORE_MINIMO", "85"))
PRELIVE_MEDIA_MINIMA = int(os.getenv("PRELIVE_MEDIA_MINIMA", "85"))
PRELIVE_QTD_MINIMA_JOGOS = int(os.getenv("PRELIVE_QTD_MINIMA_JOGOS", "5"))
PRELIVE_QTD_MINIMA_ABSOLUTA = int(os.getenv("PRELIVE_QTD_MINIMA_ABSOLUTA", "3"))
PRELIVE_MAXIMO_JOGOS = int(os.getenv("PRELIVE_MAXIMO_JOGOS", "8"))
PRELIVE_DOMINANCIA_MINIMA = int(os.getenv("PRELIVE_DOMINANCIA_MINIMA", "3"))

TEOLOGIN_EMAIL = os.getenv("TEOLOGIN_EMAIL", "")
TEOLOGIN_SENHA = os.getenv("TEOLOGIN_SENHA", "")

PESO_MATCHUP_OFENSIVO = float(os.getenv("PESO_MATCHUP_OFENSIVO", "0.20"))
PESO_MATCHUP_DEFENSIVO = float(os.getenv("PESO_MATCHUP_DEFENSIVO", "0.10"))
PESO_DNA_ARCE = float(os.getenv("PESO_DNA_ARCE", "0.20"))
PESO_DNA_CHAMA = float(os.getenv("PESO_DNA_CHAMA", "0.20"))
PESO_CONSISTENCIA = float(os.getenv("PESO_CONSISTENCIA", "0.15"))
PESO_CONTEXTO = float(os.getenv("PESO_CONTEXTO", "0.15"))

HORA_AGENDADOR_PRELIVE = os.getenv("HORA_AGENDADOR_PRELIVE", "08:00")  # HH:MM


def validar_env() -> None:
    faltando = []
    if not API_ID:
        faltando.append("API_ID")
    if not API_HASH:
        faltando.append("API_HASH")
    if not SESSION_STRING_PRELIVE:
        faltando.append("SESSION_STRING_PRELIVE")
    if not TEOLOGIN_EMAIL:
        faltando.append("TEOLOGIN_EMAIL")
    if not TEOLOGIN_SENHA:
        faltando.append("TEOLOGIN_SENHA")
    if faltando:
        log(f"❌ Variáveis de ambiente faltando: {', '.join(faltando)}")
        raise SystemExit(1)
    if not CANAL_MULTIPLAS_PRELIVE:
        log("⚠️ CANAL_MULTIPLAS_PRELIVE não configurado — múltiplas vão cair em 'me' (chat próprio)")


# Client Telegram PRÓPRIO do pré-live — sessão diferente do ao vivo.
client = TelegramClient(StringSession(SESSION_STRING_PRELIVE), API_ID, API_HASH)


# =========================================================
# HELPERS GERAIS
# =========================================================
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


def clamp(valor: float, minimo: int = 0, maximo: int = 100) -> int:
    return max(minimo, min(maximo, int(round(valor))))


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
    if not liga:
        return False
    l = remover_acentos(liga).lower()
    return any(remover_acentos(termo) in l for termo in _TORNEIOS_CAMPO_NEUTRO)


def eh_amistoso(liga: str) -> bool:
    if not liga:
        return False
    l = remover_acentos(liga).lower()
    return any(remover_acentos(termo) in l for termo in _TERMOS_AMISTOSO)


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
# DATACLASSES
# =========================================================
@dataclass
class HistoricoTimePreLive:
    nome: str = ""
    total_jogos: int = 0
    escanteios_media: float = 0.0
    escanteios_sofridos_media: float = 0.0
    gols_media: float = 0.0
    gols_sofridos_media: float = 0.0
    btts_percent: float = 0.0
    over_05_ht_percent: float = 0.0
    over_15_ft_percent: float = 0.0
    over_25_ft_percent: float = 0.0
    cartoes_media: float = 0.0
    cartoes_sofridos_media: float = 0.0
    confiabilidade: str = "BAIXA"
    dados_completos: bool = False
    escopo_dados: str = "last10"


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
    marcou_primeiro: float = 0.0
    sofreu_primeiro: float = 0.0
    nao_sofreu: float = 0.0
    falhou_marcar: float = 0.0
    posse: float = 0.0
    league_reliability: float = 80.0
    team_reliability: float = 80.0
    gols_ht_casa: float = 0.0
    gols_ht_fora: float = 0.0
    sofre_ht_casa: float = 0.0
    sofre_ht_fora: float = 0.0
    over_05_ht_casa: float = 0.0
    over_05_ht_fora: float = 0.0
    over_15_ft_casa: float = 0.0
    over_15_ft_fora: float = 0.0
    over_25_ft_casa: float = 0.0
    over_25_ft_fora: float = 0.0
    btts_casa: float = 0.0
    btts_fora: float = 0.0
    escanteios_favor_casa: float = 0.0
    escanteios_favor_fora: float = 0.0
    escanteios_contra_casa: float = 0.0
    escanteios_contra_fora: float = 0.0
    vitorias_casa: float = 0.0
    vitorias_fora: float = 0.0


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
    dados_raw: Dict[str, Any] = field(default_factory=dict)
    odd_favorito: float = 0.0
    lado_favorito: str = ""


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
    mercado_original: str = ""
    valor_casa: float = 0.0
    valor_fora: float = 0.0
    media: float = 0.0
    matchup_ofensivo: float = 0.0
    matchup_defensivo: float = 0.0
    dna_arce: float = 0.0
    dna_chama: float = 0.0
    consistencia: float = 0.0
    contexto: float = 0.0


@dataclass
class MultiplePreLive:
    nome: str = ""
    mercados: List[MercadoPreLive] = field(default_factory=list)
    score_medio: float = 0.0
    descricao: str = ""
    num_ancoras: int = 0
    num_complementares: int = 0
    confianca_baixa: bool = False


def _mapear_estatisticas_prelive(estat: dict) -> dict:
    normalizado = {}
    mapeamento = {
        "gols_marcados": "gols_marcados", "gols_sofridos": "gols_sofridos",
        "xg": "xg", "xga": "xga", "finalizacoes": "finalizacoes",
        "finalizacoes_alvo": "finalizacoes_alvo", "finalizacoes_por_gol": "finalizacoes_por_gol",
        "finalizacoes_sofridas": "finalizacoes_sofridas", "escanteios_favor": "escanteios_favor",
        "escanteios_contra": "escanteios_contra", "escanteios_total": "escanteios_total",
        "mais_escanteios": "mais_escanteios", "over_05": "over_05_ht", "over_15": "over_15_ft",
        "over_25": "over_25_ft", "over_35": "over_35_ft", "over_45": "over_45_ft",
        "over_55": "over_55_ft", "over_05_ht": "over_05_ht", "over_15_ht": "over_15_ht",
        "over_25_ht": "over_25_ht", "over_05_2t": "over_05_2t", "over_15_2t": "over_15_2t",
        "over_25_2t": "over_25_2t", "under_05": "under_05", "under_15": "under_15",
        "under_25": "under_25", "under_35": "under_35", "under_45": "under_45",
        "under_55": "under_55", "escanteios_25": "escanteios_25", "escanteios_35": "escanteios_35",
        "escanteios_45": "escanteios_45", "escanteios_55": "escanteios_55",
        "escanteios_65": "escanteios_65", "escanteios_75": "escanteios_75",
        "escanteios_85": "escanteios_85", "escanteios_95": "escanteios_95",
        "escanteios_105": "escanteios_105", "btts": "btts", "ambas_marcam": "btts",
        "posse": "posse", "posse_de_bola": "posse", "vitorias": "vitorias",
        "derrotas": "derrotas", "marcou_primeiro": "marcou_primeiro",
        "nao_sofreu": "nao_sofreu", "falhou_marcar": "falhou_marcar",
        "sofreu_primeiro": "sofreu_primeiro", "sem_gols": "sem_gols",
        "ampliou_placar": "ampliou_placar", "sofreu_empate": "sofreu_empate",
        "venceu_apos_abrir": "venceu_apos_abrir", "empatou_apos_abrir": "empatou_apos_abrir",
        "perdeu_apos_abrir": "perdeu_apos_abrir", "buscou_empate": "buscou_empate",
        "sofreu_novamente": "sofreu_novamente", "virou_venceu": "virou_venceu",
        "empatou_apos_sair_atras": "empatou_apos_sair_atras",
        "perdeu_apos_sair_atras": "perdeu_apos_sair_atras", "gols_ht": "gols_ht",
        "gols_ft": "gols_ft", "sofre_ht": "sofre_ht", "sofre_ft": "sofre_ft",
        "media_gols": "media_gols", "media_gols_sofridos": "media_gols_sofridos",
        "media_total_gols": "media_total_gols", "media_gols_marcados": "media_gols",
        "over_05_ht_casa": "over_05_ht_casa", "over_05_ht_fora": "over_05_ht_fora",
        "over_15_ft_casa": "over_15_ft_casa", "over_15_ft_fora": "over_15_ft_fora",
        "over_25_ft_casa": "over_25_ft_casa", "over_25_ft_fora": "over_25_ft_fora",
        "btts_casa": "btts_casa", "btts_fora": "btts_fora",
        "escanteios_favor_casa": "escanteios_favor_casa",
        "escanteios_favor_fora": "escanteios_favor_fora",
        "escanteios_contra_casa": "escanteios_contra_casa",
        "escanteios_contra_fora": "escanteios_contra_fora",
        "vitorias_casa": "vitorias_casa", "vitorias_fora": "vitorias_fora",
        "cartoes_recebidos": "cartoes_recebidos", "cartoes_adversarios": "cartoes_adversarios",
        "cartoes_total": "cartoes_total", "cartoes_05": "cartoes_05", "cartoes_15": "cartoes_15",
        "cartoes_25": "cartoes_25", "cartoes_35": "cartoes_35", "cartoes_45": "cartoes_45",
        "cartoes_55": "cartoes_55", "cartoes_65": "cartoes_65", "total_jogos": "total_jogos",
        "ppj": "ppj", "posicao": "posicao",
    }
    for key, value in estat.items():
        if key in mapeamento:
            normalizado[mapeamento[key]] = value
        else:
            normalizado[key] = value

    campos_obrigatorios = [
        "gols_marcados", "gols_sofridos", "xg", "xga", "over_05_ht", "over_15_ft",
        "over_25_ft", "btts", "posse", "vitorias", "derrotas", "marcou_primeiro",
        "nao_sofreu", "falhou_marcar", "finalizacoes", "escanteios_favor",
        "escanteios_contra", "gols_ht", "gols_ft", "sofre_ht", "sofre_ft",
        "over_05_ht_casa", "over_05_ht_fora", "over_15_ft_casa", "over_15_ft_fora",
        "over_25_ft_casa", "over_25_ft_fora", "btts_casa", "btts_fora",
        "escanteios_favor_casa", "escanteios_favor_fora", "escanteios_contra_casa",
        "escanteios_contra_fora", "vitorias_casa", "vitorias_fora",
    ]
    for campo in campos_obrigatorios:
        if campo not in normalizado:
            normalizado[campo] = 0.0
    return normalizado


# =========================================================
# SCRAPER V2
# =========================================================
class TeoBorgesScraperPreLiveV2:
    def __init__(self):
        self.logado = False
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        self.client = None
        self._cache_abas = {}
        self._cache_jogos = {}

    async def login(self) -> bool:
        if not TEOLOGIN_EMAIL or not TEOLOGIN_SENHA:
            log("❌ TEOLOGIN_EMAIL ou TEOLOGIN_SENHA não configurados!")
            return False
        log("🔑 Tentando login no clube.theoborges.com (V2)")
        try:
            self.client = httpx.AsyncClient(headers=self.headers, follow_redirects=True, timeout=30.0)
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
            login_data = {"email": TEOLOGIN_EMAIL, "password": TEOLOGIN_SENHA}
            if csrf_token:
                login_data["_token"] = csrf_token
            resp = await self.client.post(
                "https://clube.theoborges.com/login", data=login_data,
                headers={"Content-Type": "application/x-www-form-urlencoded",
                         "Referer": "https://clube.theoborges.com/login"}
            )
            if "login" in str(resp.url).lower() and "dashboard" not in str(resp.url).lower() and "matches" not in str(resp.url).lower():
                log("❌ Login falhou! Verifique email e senha.")
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
        cache_key = f"jogos_{dia}"
        if cache_key in self._cache_jogos:
            log(f"📦 Usando cache para jogos de {dia}")
            return self._cache_jogos[cache_key]
        try:
            urls_tentar = [
                f"https://clube.theoborges.com/matches?dia={dia}",
                f"https://clube.theoborges.com/matches?date={dia}",
                f"https://clube.theoborges.com/matches/{dia}",
            ]
            links = []
            for url in urls_tentar:
                log(f"🔄 Tentando buscar jogos em: {url}")
                try:
                    resp = await self.client.get(url, timeout=60.0)
                    if resp.status_code == 200:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        for selector in [".match-row", ".match-item", "a[href*='/game/']", ".game-link"]:
                            for link in soup.select(selector):
                                href = link.get("href")
                                if href and "/game/" in href:
                                    if href.startswith("http"):
                                        full_url = href
                                    elif href.startswith("//"):
                                        full_url = f"https:{href}"
                                    else:
                                        full_url = f"https://clube.theoborges.com{href}"
                                    if full_url not in links:
                                        links.append(full_url)
                        if links:
                            log(f"📊 Encontrados {len(links)} links em {url}")
                            break
                except Exception as e:
                    log(f"⚠️ Erro ao tentar {url}: {e}")
                    continue
                await asyncio.sleep(2)
            self._cache_jogos[cache_key] = links
            log(f"📊 Total de {len(links)} links de jogos para {dia}")
            return links
        except Exception as e:
            log(f"❌ Erro ao buscar jogos: {type(e).__name__}: {e}")
            return []

    async def _refazer_com_scope(self, url: str, scope: str, timeout: int = 60) -> Optional[BeautifulSoup]:
        try:
            separador = "&" if "?" in url else "?"
            url_scope = f"{url}{separador}stats_scope={scope}"
            resp = await self.client.get(url_scope, timeout=timeout)
            if resp.status_code != 200:
                return None
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            log(f"⚠️ Erro ao refazer requisição com scope={scope}: {type(e).__name__}: {e}")
            return None

    def _label_para_slug(self, label_norm: str, card_titulo: str = "") -> Optional[str]:
        l = label_norm.strip()
        m = re.search(r"(\d+)[.,]5", l)
        if m:
            numero = m.group(1)
            if "cartao" in l or "cartoes" in l:
                return f"cartoes_{numero}5"
            if "escanteio" in l or "canto" in l:
                if "sofreu" in l or "contra" in l:
                    return f"escanteios_contra_{numero}5"
                return f"escanteios_{numero}5"
            if "gol" in l:
                return f"under_{numero}5" if "under" in l else f"over_{numero}5"
        tabela = {
            "marcou primeiro": "marcou_primeiro", "sofreu primeiro": "sofreu_primeiro",
            "nao sofreu gols": "nao_sofreu", "falhou em marcar": "falhou_marcar",
            "ambas marcam": "btts", "media de posse de bola": "posse",
            "pontos por jogo": "ppj", "total de jogos": "total_jogos",
            "gols marcados": "gols_marcados", "gols sofridos": "gols_sofridos",
            "media gols marcados": "media_gols", "media gols sofridos": "media_gols_sofridos",
            "media total de gols": "media_total_gols", "finalizacoes por jogo": "finalizacoes",
            "finalizacoes no alvo": "finalizacoes_alvo",
            "finalizacoes por gol marcado": "finalizacoes_por_gol",
            "expected goals (xg) - a favor": "xg", "expected goals (xg) - contra": "xga",
            "media de escanteios a favor": "escanteios_favor",
            "media de escanteios contra": "escanteios_contra",
            "media total de escanteios": "escanteios_total",
            "terminou a partida com mais escanteios": "mais_escanteios",
            "ampliou o placar": "ampliou_placar", "sofreu empate": "sofreu_empate",
            "venceu apos abrir": "venceu_apos_abrir", "empatou apos abrir": "empatou_apos_abrir",
            "perdeu apos abrir": "perdeu_apos_abrir", "buscou empate": "buscou_empate",
            "sofreu novamente": "sofreu_novamente", "virou e venceu": "virou_venceu",
            "empatou apos sair atras": "empatou_apos_sair_atras",
            "perdeu apos sair atras": "perdeu_apos_sair_atras",
            "media recebidos": "cartoes_recebidos", "media dos adversarios": "cartoes_adversarios",
            "sem gols": "sem_gols", "vitoria": "vitorias", "derrota": "derrotas",
            "media total": "cartoes_total",
        }
        if l in tabela:
            return tabela[l]
        for chave in sorted(tabela.keys(), key=len, reverse=True):
            if chave in l:
                return tabela[chave]
        return None

    def _slug_especial(self, titulo_card_norm: str, sub_id: str, label_norm: str) -> Optional[str]:
        if "total de gols" in titulo_card_norm and "over" in sub_id and "1t" not in sub_id and "2t" not in sub_id:
            if "over 1.5 gols" in label_norm:
                return "over_15_ft"
            if "over 2.5 gols" in label_norm:
                return "over_25_ft"
        if "gols no 1" in titulo_card_norm and "1t" in sub_id:
            if "over 0.5 gols" in label_norm:
                return "over_05_ht"
        return None

    def _parse_pagina_completa(self, soup: BeautifulSoup) -> Dict:
        dados = {"estatisticas": {"casa": {}, "fora": {}}, "odds": {}}
        abas = soup.select(".tab-content") or [soup]
        for aba in abas:
            aba_id = aba.get("id", "geral")
            cards = aba.select(".game-card") or [aba]
            for card in cards:
                titulo_elem = card.select_one(".pg-tstable-colheader-label")
                titulo_card = titulo_elem.text.strip() if titulo_elem else ""
                paineis = card.select(".pg-stat-tab-panel")
                if paineis:
                    for painel in paineis:
                        sub_id = painel.get("id", "")
                        self._extrair_linhas_card(painel, aba_id, titulo_card, sub_id, dados, agregado=False)
                else:
                    self._extrair_linhas_card(card, aba_id, titulo_card, "", dados, agregado=True)
        odds = self._extrair_odds_soup(soup)
        if odds:
            dados["odds"] = odds
        return dados

    def _extrair_linhas_card(self, container, aba_id, titulo_card, sub_id, dados, agregado) -> None:
        titulo_norm = remover_acentos(titulo_card or "").lower()
        for row in container.select(".pg-tstable-row"):
            label_elem = row.select_one(".pg-tstable-label")
            if not label_elem:
                continue
            label = remover_acentos(label_elem.text.strip().lower())
            valores = row.select(".pgcv")
            if len(valores) < 2:
                continue
            val_casa = self._parse_valor(valores[0].text.strip())
            val_fora = self._parse_valor(valores[1].text.strip())
            partes = [p for p in (aba_id, titulo_norm, sub_id, label) if p]
            chave_composta = "__".join(partes)
            dados["estatisticas"]["casa"][chave_composta] = val_casa
            dados["estatisticas"]["fora"][chave_composta] = val_fora
            slug_especial = self._slug_especial(titulo_norm, sub_id, label)
            if slug_especial:
                dados["estatisticas"]["casa"][slug_especial] = val_casa
                dados["estatisticas"]["fora"][slug_especial] = val_fora
            if agregado:
                slug = self._label_para_slug(label, titulo_card)
                if slug and slug not in dados["estatisticas"]["casa"]:
                    dados["estatisticas"]["casa"][slug] = val_casa
                    dados["estatisticas"]["fora"][slug] = val_fora

    def _extrair_odds_soup(self, soup: BeautifulSoup) -> Dict:
        odds = {}
        html_texto = str(soup)
        seletores = [
            ".pg-odd", ".odds", ".pg-odds", ".card-match-odds", ".match-odds",
            ".game-odds", ".event-odds", "div[class*='odd']", "span[class*='odd']"
        ]
        for seletor in seletores:
            for elem in soup.select(seletor):
                text = elem.text.strip()
                if "x" in text:
                    parts = text.split("x")
                    if len(parts) >= 3:
                        try:
                            odd_casa = float(parts[0].strip().replace(",", "."))
                            odd_empate = float(parts[1].strip().replace(",", "."))
                            odd_fora = float(parts[2].strip().replace(",", "."))
                            if odd_casa > 0 and odd_fora > 0:
                                odds["1X2"] = {"casa": odd_casa, "empate": odd_empate, "fora": odd_fora}
                                return odds
                        except Exception:
                            pass
        padrao_odds = r"(\d+\.\d+)\s*[xX×]\s*(\d+\.\d+)\s*[xX×]\s*(\d+\.\d+)"
        match = re.search(padrao_odds, html_texto)
        if match:
            try:
                odd_casa, odd_empate, odd_fora = float(match.group(1)), float(match.group(2)), float(match.group(3))
                if odd_casa > 0 and odd_fora > 0:
                    odds["1X2"] = {"casa": odd_casa, "empate": odd_empate, "fora": odd_fora}
                    return odds
            except Exception:
                pass
        padrao_casa = r"(?:Casa|Home)\s*[:=]\s*(\d+\.\d+)"
        padrao_empate = r"(?:Empate|Draw)\s*[:=]\s*(\d+\.\d+)"
        padrao_fora = r"(?:Fora|Away)\s*[:=]\s*(\d+\.\d+)"
        odd_casa = re.search(padrao_casa, html_texto, re.IGNORECASE)
        odd_empate = re.search(padrao_empate, html_texto, re.IGNORECASE)
        odd_fora = re.search(padrao_fora, html_texto, re.IGNORECASE)
        if odd_casa and odd_empate and odd_fora:
            try:
                odds["1X2"] = {
                    "casa": float(odd_casa.group(1)),
                    "empate": float(odd_empate.group(1)),
                    "fora": float(odd_fora.group(1)),
                }
                return odds
            except Exception:
                pass
        return odds

    def _parse_valor(self, valor: str) -> float:
        valor = valor.strip().replace(",", ".").replace("%", "").replace("'", "")
        try:
            return float(valor)
        except Exception:
            return 0.0

    def _classificar_confiabilidade(self, total_jogos: float) -> Tuple[str, bool]:
        total = int(total_jogos or 0)
        if total >= 8:
            return "ALTA", True
        if total >= 4:
            return "MEDIA", True
        if total >= 1:
            return "BAIXA", True
        return "BAIXA", False

    def _construir_historico(self, dados_completos: Dict) -> Tuple[HistoricoTimePreLive, HistoricoTimePreLive]:
        hist_casa = HistoricoTimePreLive()
        hist_fora = HistoricoTimePreLive()
        estat_casa = dados_completos.get("estatisticas", {}).get("casa", {})
        estat_fora = dados_completos.get("estatisticas", {}).get("fora", {})
        escopo_usado = dados_completos.get("escopo_usado", "last10")

        hist_casa.nome = dados_completos.get("time_casa", "")
        hist_casa.gols_media = estat_casa.get("media_gols", 0)
        hist_casa.gols_sofridos_media = estat_casa.get("media_gols_sofridos", 0)
        hist_casa.escanteios_media = estat_casa.get("escanteios_favor", 0)
        hist_casa.escanteios_sofridos_media = estat_casa.get("escanteios_contra", 0)
        hist_casa.btts_percent = estat_casa.get("btts", 0)
        hist_casa.over_25_ft_percent = estat_casa.get("over_25_ft", 0)
        hist_casa.over_05_ht_percent = estat_casa.get("over_05_ht", 0)
        hist_casa.over_15_ft_percent = estat_casa.get("over_15_ft", 0)
        hist_casa.total_jogos = int(estat_casa.get("total_jogos", 0) or 0)
        hist_casa.confiabilidade, hist_casa.dados_completos = self._classificar_confiabilidade(hist_casa.total_jogos)
        hist_casa.escopo_dados = escopo_usado

        hist_fora.nome = dados_completos.get("time_fora", "")
        hist_fora.gols_media = estat_fora.get("media_gols", 0)
        hist_fora.gols_sofridos_media = estat_fora.get("media_gols_sofridos", 0)
        hist_fora.escanteios_media = estat_fora.get("escanteios_favor", 0)
        hist_fora.escanteios_sofridos_media = estat_fora.get("escanteios_contra", 0)
        hist_fora.btts_percent = estat_fora.get("btts", 0)
        hist_fora.over_25_ft_percent = estat_fora.get("over_25_ft", 0)
        hist_fora.over_05_ht_percent = estat_fora.get("over_05_ht", 0)
        hist_fora.over_15_ft_percent = estat_fora.get("over_15_ft", 0)
        hist_fora.total_jogos = int(estat_fora.get("total_jogos", 0) or 0)
        hist_fora.confiabilidade, hist_fora.dados_completos = self._classificar_confiabilidade(hist_fora.total_jogos)
        hist_fora.escopo_dados = escopo_usado
        return hist_casa, hist_fora

    def _sugerir_mercado_especifico(self, media: float, tipo: str) -> Tuple[str, float]:
        if tipo == "escanteios":
            if media >= 10.5:
                return "Over 10.5 Escanteios FT", 10.5
            elif media >= 9.5:
                return "Over 9.5 Escanteios FT", 9.5
            elif media >= 8.5:
                return "Over 8.5 Escanteios FT", 8.5
            elif media >= 7.5:
                return "Over 7.5 Escanteios FT", 7.5
            elif media >= 6.5:
                return "Over 6.5 Escanteios FT", 6.5
            else:
                return "Under 6.5 Escanteios FT", 6.5
        elif tipo == "gols":
            if media >= 3.5:
                return "Over 3.5 Gols FT", 3.5
            elif media >= 2.5:
                return "Over 2.5 Gols FT", 2.5
            elif media >= 1.5:
                return "Over 1.5 Gols FT", 1.5
            else:
                return "Under 1.5 Gols FT", 1.5
        elif tipo == "btts":
            return ("BTTS (Ambas Marcam)", 0) if media >= 60 else ("BTTS Não (Ambas Não Marcam)", 0)
        elif tipo == "ht":
            return ("Over 0.5 HT", 0) if media >= 0.8 else ("Under 0.5 HT", 0)
        return "Mercado não identificado", 0

    async def extrair_dados_jogo(self, url: str) -> Optional[Dict]:
        await asyncio.sleep(random.uniform(1.5, 3.0))
        if url in self._cache_abas:
            log(f"📦 Usando cache para {url}")
            return self._cache_abas[url]
        dados_completos = {
            "time_casa": "", "time_fora": "", "liga": "", "url": url,
            "estatisticas": {"casa": {}, "fora": {}}, "odds": {}, "aba": {},
        }
        try:
            resp_base = await self.client.get(url, timeout=60.0)
            if resp_base.status_code != 200:
                return None
            soup_base = BeautifulSoup(resp_base.text, "html.parser")
            times = soup_base.select_one(".card-match-teams")
            if times:
                time_casa_elem = times.select_one(".card-match-teams-block.home .card-match-teams-name")
                time_fora_elem = times.select_one(".card-match-teams-block.away .card-match-teams-name")
                if time_casa_elem:
                    dados_completos["time_casa"] = time_casa_elem.text.strip()
                if time_fora_elem:
                    dados_completos["time_fora"] = time_fora_elem.text.strip()
            liga_elem = soup_base.select_one(".card-match-competition")
            if liga_elem:
                dados_completos["liga"] = liga_elem.text.strip()
            odds_base = self._extrair_odds_soup(soup_base)
            if odds_base:
                dados_completos["odds"] = odds_base

            dados_pagina = self._parse_pagina_completa(soup_base)
            escopo_usado = "last10"

            def _min_jogos(dp: Dict) -> int:
                tc = dp.get("estatisticas", {}).get("casa", {}).get("total_jogos", 0)
                tf = dp.get("estatisticas", {}).get("fora", {}).get("total_jogos", 0)
                return min(int(tc or 0), int(tf or 0))

            if dados_pagina and _min_jogos(dados_pagina) < 8:
                soup_5 = await self._refazer_com_scope(url, "last5")
                if soup_5:
                    dados_5 = self._parse_pagina_completa(soup_5)
                    if dados_5 and _min_jogos(dados_5) > _min_jogos(dados_pagina):
                        dados_pagina = dados_5
                        escopo_usado = "last5"

            if dados_pagina and _min_jogos(dados_pagina) < 3:
                soup_comp = await self._refazer_com_scope(url, "competition")
                if soup_comp:
                    dados_comp = self._parse_pagina_completa(soup_comp)
                    if dados_comp and _min_jogos(dados_comp) > _min_jogos(dados_pagina):
                        dados_pagina = dados_comp
                        escopo_usado = "competicao_completa"

            dados_completos["escopo_usado"] = escopo_usado
            if dados_pagina:
                dados_completos["estatisticas"] = dados_pagina["estatisticas"]
                if dados_pagina.get("odds"):
                    dados_completos["odds"].update(dados_pagina["odds"])

            odds_1x2 = dados_completos.get("odds", {}).get("1X2", {})
            odd_casa = odds_1x2.get("casa", 0)
            odd_fora = odds_1x2.get("fora", 0)
            if odd_casa and odd_fora:
                if odd_casa < odd_fora:
                    dados_completos["lado_favorito"] = "CASA"
                    dados_completos["odd_favorito"] = odd_casa
                else:
                    dados_completos["lado_favorito"] = "FORA"
                    dados_completos["odd_favorito"] = odd_fora

            hist_casa, hist_fora = self._construir_historico(dados_completos)
            dados_completos["historico_casa"] = hist_casa
            dados_completos["historico_fora"] = hist_fora

            estat_casa = dados_completos["estatisticas"]["casa"]
            estat_fora = dados_completos["estatisticas"]["fora"]

            esc_casa = hist_casa.escanteios_media if hist_casa.escanteios_media else estat_casa.get("escanteios_favor", 0)
            esc_fora = hist_fora.escanteios_sofridos_media if hist_fora.escanteios_sofridos_media else estat_fora.get("escanteios_contra", 0)
            media_escanteios = (esc_casa + esc_fora) / 2 if (esc_casa + esc_fora) > 0 else 0

            gols_casa = hist_casa.gols_media if hist_casa.gols_media else estat_casa.get("gols_marcados", 0)
            gols_fora = hist_fora.gols_sofridos_media if hist_fora.gols_sofridos_media else estat_fora.get("gols_sofridos", 0)
            media_gols = (gols_casa + gols_fora) / 2 if (gols_casa + gols_fora) > 0 else 0

            btts_casa = hist_casa.btts_percent if hist_casa.btts_percent else estat_casa.get("btts", 0)
            btts_fora = hist_fora.btts_percent if hist_fora.btts_percent else estat_fora.get("btts", 0)
            media_btts = (btts_casa + btts_fora) / 2 if (btts_casa + btts_fora) > 0 else 0

            ht_casa = hist_casa.over_05_ht_percent if hist_casa.over_05_ht_percent else estat_casa.get("over_05_ht", 0)
            ht_fora = hist_fora.over_05_ht_percent if hist_fora.over_05_ht_percent else estat_fora.get("over_05_ht", 0)
            media_ht = (ht_casa + ht_fora) / 2 if (ht_casa + ht_fora) > 0 else 0

            dados_completos["media_escanteios"] = media_escanteios
            dados_completos["media_gols"] = media_gols
            dados_completos["media_btts"] = media_btts
            dados_completos["media_ht"] = media_ht

            mercado_esc, _ = self._sugerir_mercado_especifico(media_escanteios, "escanteios")
            mercado_gols, _ = self._sugerir_mercado_especifico(media_gols, "gols")
            mercado_btts, _ = self._sugerir_mercado_especifico(media_btts, "btts")
            mercado_ht, _ = self._sugerir_mercado_especifico(media_ht, "ht")
            dados_completos["mercados_sugeridos"] = {
                "escanteios": mercado_esc, "gols": mercado_gols,
                "btts": mercado_btts, "ht": mercado_ht,
            }

            self._cache_abas[url] = dados_completos
            return dados_completos
        except Exception as e:
            log(f"⚠️ Erro ao extrair dados de {url}: {type(e).__name__}: {e}")
            return None

    async def close(self):
        if self.client:
            await self.client.aclose()


# =========================================================
# FEATURE NORMALIZER / BUILDER V2
# =========================================================
class FeatureNormalizerPreLiveV2:
    @staticmethod
    def percent(value: float) -> float:
        if value is None:
            return 0.0
        return value / 100 if value > 1 else value

    @staticmethod
    def ratio(value: float) -> float:
        return float(value or 0)

    @staticmethod
    def count(value: float) -> float:
        return float(value or 0)

    @staticmethod
    def odds(value: float) -> float:
        if value is None or value <= 0:
            return 0.0
        return float(value)


class FeatureBuilderPreLiveV2:
    def __init__(self):
        self.normalizer = FeatureNormalizerPreLiveV2()
        self._cache: Dict[str, Dict] = {}

    def build(self, jogo: JogoPreLive) -> Dict[str, Any]:
        key = jogo.url
        if key in self._cache:
            return self._cache[key]
        casa, fora = jogo.estatisticas_casa, jogo.estatisticas_fora
        hist_casa = getattr(jogo, "historico_casa", None)
        hist_fora = getattr(jogo, "historico_fora", None)
        features = {
            "ataque_casa": self.normalizer.ratio(casa.gols_marcados),
            "ataque_fora": self.normalizer.ratio(fora.gols_marcados),
            "xg_casa": self.normalizer.ratio(casa.xg),
            "xg_fora": self.normalizer.ratio(fora.xg),
            "defesa_casa": self.normalizer.ratio(casa.gols_sofridos),
            "defesa_fora": self.normalizer.ratio(fora.gols_sofridos),
            "xga_casa": self.normalizer.ratio(casa.xga),
            "xga_fora": self.normalizer.ratio(fora.xga),
            "over_05_ht_casa": self.normalizer.percent(casa.over_05_ht),
            "over_05_ht_fora": self.normalizer.percent(fora.over_05_ht),
            "over_15_ft_casa": self.normalizer.percent(casa.over_15_ft),
            "over_15_ft_fora": self.normalizer.percent(fora.over_15_ft),
            "over_25_ft_casa": self.normalizer.percent(casa.over_25_ft),
            "over_25_ft_fora": self.normalizer.percent(fora.over_25_ft),
            "btts_casa": self.normalizer.percent(casa.btts),
            "btts_fora": self.normalizer.percent(fora.btts),
            "escanteios_favor_casa": self.normalizer.count(casa.escanteios_favor),
            "escanteios_favor_fora": self.normalizer.count(fora.escanteios_favor),
            "escanteios_contra_casa": self.normalizer.count(casa.escanteios_contra),
            "escanteios_contra_fora": self.normalizer.count(fora.escanteios_contra),
            "vitorias_casa": self.normalizer.percent(casa.vitorias),
            "vitorias_fora": self.normalizer.percent(fora.vitorias),
            "marcou_primeiro_casa": self.normalizer.percent(casa.marcou_primeiro),
            "marcou_primeiro_fora": self.normalizer.percent(fora.marcou_primeiro),
            "nao_sofreu_casa": self.normalizer.percent(casa.nao_sofreu),
            "nao_sofreu_fora": self.normalizer.percent(fora.nao_sofreu),
            "posse_casa": self.normalizer.percent(casa.posse),
            "posse_fora": self.normalizer.percent(fora.posse),
            "gols_ht_casa": self.normalizer.count(casa.gols_ht),
            "gols_ht_fora": self.normalizer.count(fora.gols_ht),
            "gols_ft_casa": self.normalizer.count(casa.gols_ft),
            "gols_ft_fora": self.normalizer.count(fora.gols_ft),
            "lado_favorito": jogo.lado_favorito,
            "odd_favorito": self.normalizer.odds(jogo.odd_favorito),
            "liga": jogo.liga,
            "posicao_casa": casa.posicao or 10,
            "posicao_fora": fora.posicao or 10,
            "team_reliability_casa": casa.team_reliability or 80.0,
            "team_reliability_fora": fora.team_reliability or 80.0,
        }
        if hist_casa:
            features["hist_escanteios_casa"] = hist_casa.escanteios_media
            features["hist_gols_casa"] = hist_casa.gols_media
            features["hist_btts_casa"] = hist_casa.btts_percent
            features["hist_total_jogos_casa"] = hist_casa.total_jogos
        if hist_fora:
            features["hist_escanteios_fora"] = hist_fora.escanteios_media
            features["hist_gols_fora"] = hist_fora.gols_media
            features["hist_btts_fora"] = hist_fora.btts_percent
            features["hist_total_jogos_fora"] = hist_fora.total_jogos
        self._cache[key] = features
        return features

    def clear_cache(self) -> None:
        self._cache = {}


class MarketEnginePreLiveV2:
    def __init__(self):
        self.feature_builder = FeatureBuilderPreLiveV2()
        # 30/06: removida instanciação morta de PreLiveScorer (V1) — nunca
        # era chamada por nenhum método daqui, só ocupava memória.

    def calcular_mercados(self, jogo: JogoPreLive) -> List[MercadoPreLive]:
        features = self.feature_builder.build(jogo)
        mercados = []
        dados_raw = getattr(jogo, "dados_raw", {}) or {}
        media_esc = dados_raw.get("media_escanteios", 0)
        media_gols = dados_raw.get("media_gols", 0)
        media_btts = dados_raw.get("media_btts", 0)
        media_ht = dados_raw.get("media_ht", 0)
        mercados_sugeridos = dados_raw.get("mercados_sugeridos", {})

        nome_sugerido_esc = mercados_sugeridos.get("escanteios", "Escanteios")
        score_esc = self._calc_escanteios(features)
        if score_esc > 0:
            m = MercadoPreLive(nome="Escanteios", jogo=jogo, score=score_esc, detalhes={"media": media_esc})
            m.nome_sugerido = nome_sugerido_esc
            m.estatistica_media = media_esc
            mercados.append(m)

        nome_sugerido_gols = mercados_sugeridos.get("gols", "Over 2.5 Gols FT")
        score_gols = self._calc_over_25_ft(features)
        if score_gols > 0:
            m = MercadoPreLive(nome="Gols", jogo=jogo, score=score_gols, detalhes={"media": media_gols})
            m.nome_sugerido = nome_sugerido_gols
            m.estatistica_media = media_gols
            mercados.append(m)

        nome_sugerido_btts = mercados_sugeridos.get("btts", "BTTS")
        score_btts = self._calc_btts(features)
        if score_btts > 0:
            m = MercadoPreLive(nome="BTTS", jogo=jogo, score=score_btts, detalhes={"media": media_btts})
            m.nome_sugerido = nome_sugerido_btts
            m.estatistica_media = media_btts
            mercados.append(m)

        nome_sugerido_ht = mercados_sugeridos.get("ht", "Over 0.5 HT")
        score_ht = self._calc_over_05_ht(features)
        if score_ht > 0:
            m = MercadoPreLive(nome="Over 0.5 HT", jogo=jogo, score=score_ht, detalhes={"media": media_ht})
            m.nome_sugerido = nome_sugerido_ht
            m.estatistica_media = media_ht
            mercados.append(m)

        if not eh_campo_neutro(jogo.liga):
            score_casa = self._calc_vitoria_casa(features, jogo)
            if score_casa > 0:
                m = MercadoPreLive(nome="Vitória Casa", jogo=jogo, score=score_casa, detalhes={})
                m.nome_sugerido = "Vitória Casa"
                mercados.append(m)
            score_fora = self._calc_vitoria_fora(features, jogo)
            if score_fora > 0:
                m = MercadoPreLive(nome="Vitória Fora", jogo=jogo, score=score_fora, detalhes={})
                m.nome_sugerido = "Vitória Fora"
                mercados.append(m)

        ajuste_liga = liga_ajuste(classificar_liga(jogo.liga))
        penalidade_amistoso = -8 if eh_amistoso(jogo.liga) else 0
        ajuste_total = ajuste_liga + penalidade_amistoso
        if ajuste_total:
            for m in mercados:
                m.score = clamp(m.score + ajuste_total, 0, 100)
        return mercados

    def _calc_vitoria_casa(self, f, jogo) -> float:
        favoritismo = 70 if f.get("lado_favorito") == "CASA" else 30
        score = (f.get("ataque_casa", 0) * 0.35 + f.get("defesa_fora", 0) * 0.25 +
                 f.get("xg_casa", 0) * 0.20 + f.get("btts_casa", 0) * 0.05 + favoritismo * 0.15)
        return clamp(score, 0, 100)

    def _calc_vitoria_fora(self, f, jogo) -> float:
        favoritismo = 70 if f.get("lado_favorito") == "FORA" else 30
        score = (f.get("defesa_casa", 0) * 0.35 + f.get("ataque_fora", 0) * 0.25 +
                 f.get("xg_fora", 0) * 0.20 + f.get("btts_fora", 0) * 0.05 + favoritismo * 0.15)
        return clamp(score, 0, 100)

    def _calc_over_05_ht(self, f) -> float:
        casa_ataca_fora_sofre = (f.get("xg_casa", 0) + f.get("xga_fora", 0)) / 2
        fora_ataca_casa_sofre = (f.get("xg_fora", 0) + f.get("xga_casa", 0)) / 2
        cruzamento = (casa_ataca_fora_sofre + fora_ataca_casa_sofre) / 2
        contexto = (f.get("over_05_ht_casa", 0) + f.get("over_05_ht_fora", 0)) / 2
        score = cruzamento * 20 + contexto * 40
        return clamp(score, 0, 100)

    def _calc_over_25_ft(self, f) -> float:
        casa_ataca_fora_sofre = (f.get("xg_casa", 0) + f.get("xga_fora", 0)) / 2
        fora_ataca_casa_sofre = (f.get("xg_fora", 0) + f.get("xga_casa", 0)) / 2
        cruzamento = (casa_ataca_fora_sofre + fora_ataca_casa_sofre) / 2
        contexto = (f.get("over_25_ft_casa", 0) + f.get("over_25_ft_fora", 0) +
                    f.get("btts_casa", 0) + f.get("btts_fora", 0)) / 4
        score = cruzamento * 22 + contexto * 35
        return clamp(score, 0, 100)

    def _calc_btts(self, f) -> float:
        casa_marca = (f.get("xg_casa", 0) + f.get("xga_fora", 0)) / 2
        fora_marca = (f.get("xg_fora", 0) + f.get("xga_casa", 0)) / 2
        cruzamento = min(casa_marca, fora_marca)
        contexto = (f.get("btts_casa", 0) + f.get("btts_fora", 0)) / 2
        score = cruzamento * 28 + contexto * 45
        return clamp(score, 0, 100)

    def _calc_escanteios(self, f) -> float:
        casa_ataca_fora_sofre = (f.get("escanteios_favor_casa", 0) + f.get("escanteios_contra_fora", 0)) / 2
        fora_ataca_casa_sofre = (f.get("escanteios_favor_fora", 0) + f.get("escanteios_contra_casa", 0)) / 2
        cruzamento = (casa_ataca_fora_sofre + fora_ataca_casa_sofre) / 2
        return clamp(cruzamento * 10, 0, 100)


class CandidateSelectorPreLiveV2:
    def __init__(self):
        self.engine = MarketEnginePreLiveV2()

    def selecionar_candidatos(self, jogos: List[JogoPreLive]) -> Dict[str, Any]:
        todos_mercados = []
        for jogo in jogos:
            todos_mercados.extend(self.engine.calcular_mercados(jogo))

        melhores_por_jogo = {}
        for m in todos_mercados:
            chave = f"{m.jogo.time_casa} x {m.jogo.time_fora}"
            melhores_por_jogo.setdefault(chave, []).append(m)

        melhores_4_por_jogo = {}
        for chave, mercados in melhores_por_jogo.items():
            ordenados = sorted(mercados, key=lambda x: x.score, reverse=True)
            melhores_4_por_jogo[chave] = ordenados[:4]
            if len(ordenados) >= 2 and (ordenados[0].score - ordenados[1].score) < PRELIVE_DOMINANCIA_MINIMA:
                melhores_4_por_jogo[chave] = [m for m in ordenados if m.score >= 85][:4]

        jogos_aprovados = []
        for chave, mercados in melhores_4_por_jogo.items():
            if mercados and mercados[0].score >= PRELIVE_SCORE_MINIMO:
                jogos_aprovados.append({
                    "chave": chave, "melhor_mercado": mercados[0],
                    "todos_mercados": mercados, "score": mercados[0].score,
                })
        jogos_aprovados.sort(key=lambda x: x["score"], reverse=True)
        return {
            "jogos_aprovados": jogos_aprovados,
            "total_jogos": len(jogos),
            "total_aprovados": len(jogos_aprovados),
        }


class MultipleBuilderPreLiveV2:
    def construir_multiplas(self, jogos_aprovados: List[Dict]) -> List[MultiplePreLive]:
        total = len(jogos_aprovados)
        if total < PRELIVE_QTD_MINIMA_ABSOLUTA:
            log(f"⚠️ Apenas {total} jogos aprovados. Piso absoluto: {PRELIVE_QTD_MINIMA_ABSOLUTA} — nenhuma múltipla")
            return []
        if total < PRELIVE_QTD_MINIMA_JOGOS:
            log(f"🟡 Apenas {total} jogos aprovados (abaixo do ideal {PRELIVE_QTD_MINIMA_JOGOS}) — mandando SAFE com confiança reduzida")
            mercados_reduzidos = [j["melhor_mercado"] for j in jogos_aprovados]
            score_medio = sum(m.score for m in mercados_reduzidos) / len(mercados_reduzidos)
            safe_reduzida = MultiplePreLive(
                nome="SAFE", mercados=mercados_reduzidos, score_medio=score_medio,
                descricao="Mercados mais confiáveis de cada jogo — CONFIANÇA REDUZIDA (poucos jogos hoje)",
                num_ancoras=len(jogos_aprovados), num_complementares=0, confianca_baixa=True,
            )
            return [safe_reduzida]

        if total >= 8:
            num_ancoras, num_complementares = 4, 4
        elif total == 7:
            num_ancoras, num_complementares = 4, 3
        elif total == 6:
            num_ancoras, num_complementares = 3, 3
        else:
            num_ancoras, num_complementares = 2, 3

        ancoras = jogos_aprovados[:num_ancoras]
        complementares = jogos_aprovados[num_ancoras:num_ancoras + num_complementares]
        if len(complementares) < 1:
            log(f"⚠️ Não há complementares suficientes. Apenas {len(ancoras)} âncoras.")
            return []

        multiplas = []
        safe = self._montar_multipla("SAFE", ancoras, complementares, 0)
        if safe:
            multiplas.append(safe)
        pro1 = self._montar_multipla("PRO1", ancoras, complementares, 1)
        if pro1 and len(pro1.mercados) >= PRELIVE_QTD_MINIMA_JOGOS:
            multiplas.append(pro1)
        pro2 = self._montar_multipla("PRO2", ancoras, complementares, 2)
        if pro2 and len(pro2.mercados) >= PRELIVE_QTD_MINIMA_JOGOS:
            multiplas.append(pro2)
        diamond = self._montar_multipla_diamond("DIAMOND", ancoras, complementares)
        if diamond and len(diamond.mercados) >= PRELIVE_QTD_MINIMA_JOGOS:
            multiplas.append(diamond)
        return multiplas

    def _montar_multipla(self, nome, ancoras, complementares, idx) -> Optional[MultiplePreLive]:
        mercados = [a["melhor_mercado"] for a in ancoras]
        for c in complementares:
            todos = c["todos_mercados"]
            mercados.append(todos[idx] if idx < len(todos) else todos[0])
        if len(mercados) < PRELIVE_QTD_MINIMA_JOGOS:
            return None
        score_medio = sum(m.score for m in mercados) / len(mercados)
        return MultiplePreLive(
            nome=nome, mercados=mercados, score_medio=score_medio,
            descricao=self._get_descricao(nome), num_ancoras=len(ancoras),
            num_complementares=len(complementares)
        )

    def _montar_multipla_diamond(self, nome, ancoras, complementares) -> Optional[MultiplePreLive]:
        mercados = [a["melhor_mercado"] for a in ancoras]
        for i, c in enumerate(complementares):
            todos = c["todos_mercados"]
            if len(todos) >= 3 and i % 3 == 0:
                mercados.append(todos[2])
            elif len(todos) >= 2 and i % 2 == 0:
                mercados.append(todos[1])
            else:
                mercados.append(todos[0])
        if len(mercados) < PRELIVE_QTD_MINIMA_JOGOS:
            return None
        score_medio = sum(m.score for m in mercados) / len(mercados)
        return MultiplePreLive(
            nome=nome, mercados=mercados, score_medio=score_medio,
            descricao="Combinação diversificada para maior retorno",
            num_ancoras=len(ancoras), num_complementares=len(complementares)
        )

    def _get_descricao(self, nome: str) -> str:
        return {
            "SAFE": "Mercados mais confiáveis de cada jogo",
            "PRO1": "Segunda melhor combinação de mercados",
            "PRO2": "Terceira melhor combinação de mercados",
            "DIAMOND": "Combinação diversificada para maior retorno",
        }.get(nome, nome)


# =========================================================
# FORMATAÇÃO E ROTINA PRINCIPAL
# =========================================================
def formatar_multipla_prelive_v2(multiple: MultiplePreLive) -> str:
    linhas = [
        f"📊 **{multiple.nome}**" + (" ⚠️ CONFIANÇA REDUZIDA" if multiple.confianca_baixa else ""),
        f"📈 Média: **{multiple.score_medio:.1f}%**",
        f"📝 {multiple.descricao}",
        f"🔹 {multiple.num_ancoras} Âncoras | {multiple.num_complementares} Complementares",
        "", "---", "",
    ]
    for i, m in enumerate(multiple.mercados, 1):
        is_ancora = i <= multiple.num_ancoras
        prefixo = "⚓" if is_ancora else "🔸"
        nome_mercado = getattr(m, "nome_sugerido", None) or m.nome
        linhas.append(
            f"{prefixo} {i}. **{m.jogo.time_casa} x {m.jogo.time_fora}**\n"
            f"   🎯 {nome_mercado} | Score: {m.score:.1f}%"
        )
        media = getattr(m, "estatistica_media", 0)
        if media:
            linhas.append(f"   📊 Média: {media:.1f}")
        if i <= 5 and m.detalhes:
            linhas.append(f"   📊 Conf: {m.score:.0f}%")
    linhas.extend([
        "", "---",
        f"✅ Total: {len(multiple.mercados)} jogos",
        f"🏆 Liga: {multiple.mercados[0].jogo.liga if multiple.mercados else 'N/A'}",
        "", "📝 SIGA SUA GESTÃO DE BANCA", "⛔ APOSTE COM RESPONSABILIDADE",
    ])
    return "\n".join(linhas)


# =========================================================
# MONITOR DE SAÚDE DO SCRAPER (30/06)
#
# Cada execução do pré-live raspa o TheoBorges sob condições que mudam
# (site fora do ar, formato de página alterado, jogos sem dado suficiente).
# Sem isso registrado nem o operador, nem a IA que ele consulta sobre
# melhorias, sabia distinguir "o motor de score está ruim" de "o scraper
# não está achando dado pra trabalhar". Esse monitor não muda nenhuma
# decisão — só registra o que aconteceu na raspagem, pra virar relatório.
# =========================================================
class MonitorSaudeScraperPreLive:
    def __init__(self):
        self.inicio = time.time()
        self.jogos_encontrados = 0
        self.jogos_processados = 0
        self.paginas_com_erro = 0
        self.descartados_odd_alta = 0
        self.descartados_sem_estatisticas = 0
        self.fallback_last10 = 0
        self.fallback_last5 = 0
        self.fallback_competicao_completa = 0

    def registrar_fallback(self, escopo_usado: str) -> None:
        if escopo_usado == "last5":
            self.fallback_last5 += 1
        elif escopo_usado == "competicao_completa":
            self.fallback_competicao_completa += 1
        else:
            self.fallback_last10 += 1

    def duracao_segundos(self) -> float:
        return round(time.time() - self.inicio, 1)

    def relatorio_texto(self) -> str:
        duracao = self.duracao_segundos()
        return (
            "🩺 <b>Monitor do Scraper Pré-Live</b>\n"
            f"Jogos encontrados: {self.jogos_encontrados}\n"
            f"Jogos processados com sucesso: {self.jogos_processados}\n"
            f"Páginas com erro: {self.paginas_com_erro}\n"
            f"Descartados (odd alta): {self.descartados_odd_alta}\n"
            f"Descartados (sem estatísticas): {self.descartados_sem_estatisticas}\n"
            f"Fallback usado — Últimos 10: {self.fallback_last10} | "
            f"Últimos 5: {self.fallback_last5} | Competição completa: {self.fallback_competicao_completa}\n"
            f"Tempo total da raspagem: {duracao}s"
        )


async def varrer_site_theoborges_prelive_v2() -> None:
    log("🚀 INICIANDO SISTEMA PRÉ-LIVE V2")
    scraper = TeoBorgesScraperPreLiveV2()
    monitor = MonitorSaudeScraperPreLive()

    if not await scraper.login():
        log("❌ Falha no login. Encerrando.")
        await client.send_message("me", "❌ Falha no login no clube.theoborges.com.")
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
    monitor.jogos_encontrados = len(todos_links)

    jogos = []
    for url in todos_links[:30]:
        dados = await scraper.extrair_dados_jogo(url)
        if not dados:
            monitor.paginas_com_erro += 1
            continue

        odd_favorito = dados.get("odd_favorito", 0)
        if odd_favorito and odd_favorito > ODD_MAXIMA_FAVORITO_PRELIVE:
            log(f"⏭️ Jogo {dados['time_casa']} x {dados['time_fora']} descartado: odd favorito {odd_favorito} > {ODD_MAXIMA_FAVORITO_PRELIVE}")
            monitor.descartados_odd_alta += 1
            continue

        estat_casa = dados["estatisticas"].get("casa", {})
        estat_fora = dados["estatisticas"].get("fora", {})
        if not estat_casa and not estat_fora:
            log(f"⚠️ Jogo {dados['time_casa']} x {dados['time_fora']} sem estatísticas")
            monitor.descartados_sem_estatisticas += 1
            continue

        monitor.registrar_fallback(dados.get("escopo_usado", "last10"))

        estat_casa_obj = EstatisticasTimePreLive(
            nome=dados["time_casa"],
            gols_marcados=estat_casa.get("gols_marcados", 0), gols_sofridos=estat_casa.get("gols_sofridos", 0),
            xg=estat_casa.get("xg", 0), xga=estat_casa.get("xga", 0),
            over_05_ht=estat_casa.get("over_05_ht", 0), over_15_ft=estat_casa.get("over_15_ft", 0),
            over_25_ft=estat_casa.get("over_25_ft", 0), btts=estat_casa.get("btts", 0),
            escanteios_favor=estat_casa.get("escanteios_favor", 0), escanteios_contra=estat_casa.get("escanteios_contra", 0),
            finalizacoes=estat_casa.get("finalizacoes", 0), finalizacoes_sofridas=estat_casa.get("finalizacoes_sofridas", 0),
            gols_ht=estat_casa.get("gols_ht", 0), gols_ft=estat_casa.get("gols_ft", 0),
            sofre_ht=estat_casa.get("sofre_ht", 0), sofre_ft=estat_casa.get("sofre_ft", 0),
            vitorias=estat_casa.get("vitorias", 0), derrotas=estat_casa.get("derrotas", 0),
            marcou_primeiro=estat_casa.get("marcou_primeiro", 0), nao_sofreu=estat_casa.get("nao_sofreu", 0),
            posse=estat_casa.get("posse", 0), team_reliability=80.0, forma=0,
            over_05_ht_casa=estat_casa.get("over_05_ht_casa", 0), over_05_ht_fora=estat_casa.get("over_05_ht_fora", 0),
            over_15_ft_casa=estat_casa.get("over_15_ft_casa", 0), over_15_ft_fora=estat_casa.get("over_15_ft_fora", 0),
            over_25_ft_casa=estat_casa.get("over_25_ft_casa", 0), over_25_ft_fora=estat_casa.get("over_25_ft_fora", 0),
            btts_casa=estat_casa.get("btts_casa", 0), btts_fora=estat_casa.get("btts_fora", 0),
            escanteios_favor_casa=estat_casa.get("escanteios_favor_casa", 0), escanteios_favor_fora=estat_casa.get("escanteios_favor_fora", 0),
            escanteios_contra_casa=estat_casa.get("escanteios_contra_casa", 0), escanteios_contra_fora=estat_casa.get("escanteios_contra_fora", 0),
            vitorias_casa=estat_casa.get("vitorias_casa", 0), vitorias_fora=estat_casa.get("vitorias_fora", 0),
        )

        estat_fora_obj = EstatisticasTimePreLive(
            nome=dados["time_fora"],
            gols_marcados=estat_fora.get("gols_marcados", 0), gols_sofridos=estat_fora.get("gols_sofridos", 0),
            xg=estat_fora.get("xg", 0), xga=estat_fora.get("xga", 0),
            over_05_ht=estat_fora.get("over_05_ht", 0), over_15_ft=estat_fora.get("over_15_ft", 0),
            over_25_ft=estat_fora.get("over_25_ft", 0), btts=estat_fora.get("btts", 0),
            escanteios_favor=estat_fora.get("escanteios_favor", 0), escanteios_contra=estat_fora.get("escanteios_contra", 0),
            finalizacoes=estat_fora.get("finalizacoes", 0), finalizacoes_sofridas=estat_fora.get("finalizacoes_sofridas", 0),
            gols_ht=estat_fora.get("gols_ht", 0), gols_ft=estat_fora.get("gols_ft", 0),
            sofre_ht=estat_fora.get("sofre_ht", 0), sofre_ft=estat_fora.get("sofre_ft", 0),
            vitorias=estat_fora.get("vitorias", 0), derrotas=estat_fora.get("derrotas", 0),
            marcou_primeiro=estat_fora.get("marcou_primeiro", 0), nao_sofreu=estat_fora.get("nao_sofreu", 0),
            posse=estat_fora.get("posse", 0), team_reliability=80.0, forma=0,
            over_05_ht_casa=estat_fora.get("over_05_ht_casa", 0), over_05_ht_fora=estat_fora.get("over_05_ht_fora", 0),
            over_15_ft_casa=estat_fora.get("over_15_ft_casa", 0), over_15_ft_fora=estat_fora.get("over_15_ft_fora", 0),
            over_25_ft_casa=estat_fora.get("over_25_ft_casa", 0), over_25_ft_fora=estat_fora.get("over_25_ft_fora", 0),
            btts_casa=estat_fora.get("btts_casa", 0), btts_fora=estat_fora.get("btts_fora", 0),
            escanteios_favor_casa=estat_fora.get("escanteios_favor_casa", 0), escanteios_favor_fora=estat_fora.get("escanteios_favor_fora", 0),
            escanteios_contra_casa=estat_fora.get("escanteios_contra_casa", 0), escanteios_contra_fora=estat_fora.get("escanteios_contra_fora", 0),
            vitorias_casa=estat_fora.get("vitorias_casa", 0), vitorias_fora=estat_fora.get("vitorias_fora", 0),
        )

        hist_casa = dados.get("historico_casa", HistoricoTimePreLive())
        hist_fora = dados.get("historico_fora", HistoricoTimePreLive())

        jogo = JogoPreLive(
            time_casa=dados["time_casa"], time_fora=dados["time_fora"], liga=dados["liga"],
            data="", horario="", estatisticas_casa=estat_casa_obj, estatisticas_fora=estat_fora_obj,
            odds=dados.get("odds", {}), url=dados["url"], dados_raw=dados,
            odd_favorito=dados.get("odd_favorito", 0), lado_favorito=dados.get("lado_favorito", ""),
        )
        jogo.historico_casa = hist_casa
        jogo.historico_fora = hist_fora
        jogos.append(jogo)
        monitor.jogos_processados += 1

    log(f"📊 Dados extraídos de {len(jogos)} jogos")
    await scraper.close()

    if len(jogos) < 2:
        log("❌ Poucos jogos com dados disponíveis")
        await client.send_message("me", "❌ Dados insuficientes para montar múltiplas.")
        await _enviar_relatorio_monitor(monitor)
        return

    selector = CandidateSelectorPreLiveV2()
    resultado = selector.selecionar_candidatos(jogos)
    log(f"📊 {resultado['total_aprovados']} jogos aprovados (Score ≥ {PRELIVE_SCORE_MINIMO})")

    if resultado["total_aprovados"] < PRELIVE_QTD_MINIMA_ABSOLUTA:
        log(f"❌ Menos de {PRELIVE_QTD_MINIMA_ABSOLUTA} jogos aprovados (piso absoluto)")
        await client.send_message(
            "me",
            f"❌ Apenas {resultado['total_aprovados']} jogos aprovados. Piso mínimo absoluto: {PRELIVE_QTD_MINIMA_ABSOLUTA}."
        )
        await _enviar_relatorio_monitor(monitor)
        return

    builder = MultipleBuilderPreLiveV2()
    multiplas = builder.construir_multiplas(resultado["jogos_aprovados"])

    if not multiplas:
        log("❌ Nenhuma múltipla construída")
        await client.send_message("me", "❌ Não foi possível construir múltiplas.")
        await _enviar_relatorio_monitor(monitor)
        return

    log(f"✅ {len(multiplas)} múltiplas construídas")

    destino = CANAL_MULTIPLAS_PRELIVE or "me"
    for multiple in multiplas:
        msg = formatar_multipla_prelive_v2(multiple)
        await client.send_message(destino, msg)
        log(f"📤 {multiple.nome} enviada para {destino}")
        await asyncio.sleep(1)

    msg_top = "📋 MERCADOS POR JOGO (TOP 4)\n\n"
    for jogo_info in resultado["jogos_aprovados"][:PRELIVE_MAXIMO_JOGOS]:
        chave = jogo_info["chave"]
        mercados = jogo_info["todos_mercados"]
        msg_top += f"🏟️ {chave}\n"
        for i, m in enumerate(mercados[:4], 1):
            medalha = ["🥇", "🥈", "🥉", "4️⃣"][i - 1]
            nome_sugerido = getattr(m, "nome_sugerido", None) or m.nome
            media = getattr(m, "estatistica_media", 0)
            estat_media = f" | média {media:.1f}" if media else ""
            msg_top += f"{medalha} {nome_sugerido} ... {m.score:.1f}%{estat_media}\n"
        msg_top += "\n"
    msg_top += f"📊 TOTAL: {len(resultado['jogos_aprovados'])} jogos | {sum(len(j['todos_mercados']) for j in resultado['jogos_aprovados'])} mercados analisados"

    await client.send_message(destino, msg_top)
    log("✅ Processo Pré-Live V2 concluído!")
    await _enviar_relatorio_monitor(monitor)


async def _enviar_relatorio_monitor(monitor: "MonitorSaudeScraperPreLive") -> None:
    """Manda o relatório técnico do scraper pro canal de logs (não pro
    canal de clientes) — separação de canal igual já existe no ao vivo,
    pra cliente nunca ver detalhe técnico de raspagem."""
    try:
        destino = CANAL_LOGS_PRELIVE or "me"
        await client.send_message(destino, monitor.relatorio_texto(), parse_mode="html")
    except Exception as e:
        log(f"⚠️ Falha ao enviar relatório do monitor: {e}")


# =========================================================
# AGENDADOR (envio automático diário) + LOG TÉCNICO
# =========================================================
async def agendador_prelive() -> None:
    """Roda a varredura automaticamente, uma vez por dia, no horário
    configurado em HORA_AGENDADOR_PRELIVE (padrão 08:00)."""
    try:
        hora, minuto = (int(x) for x in HORA_AGENDADOR_PRELIVE.split(":"))
    except Exception:
        hora, minuto = 8, 0
    log(f"🕐 Agendador pré-live iniciado — envio diário às {hora:02d}:{minuto:02d}")
    while True:
        try:
            agora = datetime.now()
            proximo = agora.replace(hour=hora, minute=minuto, second=0, microsecond=0)
            if agora >= proximo:
                proximo += timedelta(days=1)
            espera = (proximo - agora).total_seconds()
            log(f"🕐 Próximo envio pré-live em {int(espera // 3600)}h{int((espera % 3600) // 60)}m")
            await asyncio.sleep(espera)
            await varrer_site_theoborges_prelive_v2()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log(f"⚠️ Agendador pré-live erro | {type(e).__name__}: {e}")
            await asyncio.sleep(60)


# =========================================================
# PROTEÇÃO CONTRA INSTÂNCIA DUPLICADA
# =========================================================
def verificar_instancia_unica():
    """Lock próprio do pré-live — caminho diferente do lock do ao vivo,
    nunca disputam o mesmo arquivo."""
    lock_path = "/tmp/alfa_prelive.lock"
    try:
        lock_file = open(lock_path, "w")
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        log(f"✅ Instância única do pré-live confirmada (PID {os.getpid()})")
        return lock_file
    except IOError:
        log("❌ OUTRA INSTÂNCIA DO PRÉ-LIVE JÁ ESTÁ RODANDO NESTE HOST — encerrando este processo.")
        log("   Verifique no Railway se há mais de 1 réplica ativa em Settings.")
        raise SystemExit(1)


async def _notificar_crash_telegram(motivo: str, detalhe: str = "") -> None:
    try:
        canal = CANAL_LOGS_PRELIVE or CANAL_MULTIPLAS_PRELIVE
        if not canal:
            return
        agora = time.strftime("%H:%M:%S")
        det = (detalhe[:200] + "...") if len(detalhe) > 200 else detalhe
        msg = (
            f"🚨 <b>ALERTA OPERACIONAL — PRÉ-LIVE</b>\n"
            f"⚠️ Processo caiu e está reiniciando\n"
            f"🕐 {agora}\n"
            f"📋 Motivo: {motivo}"
            + (f"\n💬 {det}" if det else "")
        )
        cli_tmp = TelegramClient(StringSession(SESSION_STRING_PRELIVE), API_ID, API_HASH)
        await cli_tmp.connect()
        await cli_tmp.send_message(canal, msg, parse_mode="html")
        await cli_tmp.disconnect()
    except Exception as e:
        log(f"⚠️ Não consegui notificar crash do pré-live no Telegram: {e}")


async def _notificar_sessao_morta_telegram(tentativas: int) -> None:
    motivo = (
        f"AuthKeyDuplicatedError persistente após {tentativas} tentativas. "
        f"A sessão Telegram do PRÉ-LIVE foi revogada permanentemente — retry não resolve. "
        f"É necessário gerar uma SESSION_STRING_PRELIVE nova e garantir replicas=1 no "
        f"serviço do pré-live no Railway antes de redeployar."
    )
    log(f"🛑 {motivo}")
    await _notificar_crash_telegram("Sessão Telegram do pré-live morta — processo parado", motivo)


# =========================================================
# MAIN
# =========================================================
async def main() -> None:
    validar_env()
    logar_versao_inicial()

    asyncio.create_task(agendador_prelive())

    @client.on(events.NewMessage(incoming=True))
    async def handler_prelive(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/prelive":
            log("📩 /prelive RECEBIDO (INCOMING)")
            await varrer_site_theoborges_prelive_v2()

    @client.on(events.NewMessage(from_users="me"))
    async def handler_prelive_me(event):
        texto = event.raw_text or ""
        if texto.strip().lower() == "/prelive":
            log("📩 /prelive RECEBIDO (CHAT PRIVADO)")
            await varrer_site_theoborges_prelive_v2()

    log("🚀 INICIANDO PRÉ-LIVE (processo separado)")

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

    log("✅ TELEGRAM CONECTADO COM SUCESSO (PRÉ-LIVE)")
    log(f"📡 Canal Múltiplas Pré-Live={CANAL_MULTIPLAS_PRELIVE or 'NÃO CONFIGURADO'}")
    log(f"📡 Canal Logs Pré-Live={CANAL_LOGS_PRELIVE or 'NÃO CONFIGURADO'}")
    log("📡 Comando disponível: /prelive (varredura manual)")

    await client.run_until_disconnected()


if __name__ == "__main__":
    _lock = verificar_instancia_unica()

    AUTHKEY_FALHAS_MAX = 8
    falhas_authkey_consecutivas = 0
    tentativa = 0

    while True:
        tentativa += 1
        try:
            log(f"🚀 INICIANDO PRÉ-LIVE | tentativa #{tentativa}")
            asyncio.run(main())
            log("ℹ️ main() retornou normalmente. Reiniciando em 5s...")
            falhas_authkey_consecutivas = 0
            time.sleep(5)
        except KeyboardInterrupt:
            log("🛑 Pré-live encerrado manualmente.")
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
                    try:
                        asyncio.run(_notificar_sessao_morta_telegram(falhas_authkey_consecutivas))
                    except Exception:
                        pass
                    log("🛑 PARANDO O PROCESSO — gere uma SESSION_STRING_PRELIVE nova e confirme replicas=1 no Railway.")
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
                log(f"❌ ERRO FATAL NO PRÉ-LIVE: {e}")
                log(traceback.format_exc())
                try:
                    asyncio.run(_notificar_crash_telegram(f"{type(e).__name__}", erro_str))
                except Exception:
                    pass
                log("🔁 Reiniciando em 10s...")
                time.sleep(10)
