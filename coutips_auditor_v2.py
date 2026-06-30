#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
COUTIPS AUDITOR V2 - Sistema de Inteligência do ALFA
================================================================================
Versão: 2.0.0
Autor: COUTIPS Development Team

EVOLUÇÃO DO V1 PARA V2:
- Snapshot completo com todos os scores e componentes
- Contexto do jogo (favorito, pressão, domínio, intensidade)
- Classificações expandidas (PRESSAO_CONVERTIDA, PRESSAO_CONTINUOU, etc.)
- Resultado operacional detalhado (GREEN_EXCELENTE, RED_EVITAVEL, etc.)
- Learning Engine (identificação de padrões estatísticos)
- Relatórios inteligentes com conclusões automáticas
- Consultas avançadas para responder perguntas complexas

FILOSOFIA: O Auditor V2 é um SISTEMA DE INTELIGÊNCIA.
Nunca altera decisões do ALFA. Apenas observa, registra, analisa e gera conhecimento.
================================================================================
"""

import os
import sys
import json
import re
import unicodedata
import sqlite3
import time
import uuid
import threading
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple, Set
from abc import ABC, abstractmethod
from contextlib import contextmanager
from collections import defaultdict
from enum import Enum

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

@dataclass
class Config:
    """Configuração central do sistema V2"""
    
    # Database
    db_path: str = "audit_database_v2.db"
    
    # Telegram
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # Chaves de API
    api_keys: Dict[str, str] = field(default_factory=lambda: {
        "api1": os.getenv("API1_KEY", "your_api_key_1"),
        "api2": os.getenv("API2_KEY", "your_api_key_2"),
        "api3": os.getenv("API3_KEY", "your_api_key_3"),
    })
    
    # URLs das APIs
    api_urls: Dict[str, str] = field(default_factory=lambda: {
        "api1": "https://api.football-data.org/v4",
        "api2": "https://api2.example.com/v1",
        "api3": "https://api3.example.com/v1",
    })
    
    # Ordem de prioridade dos providers
    # ATUALIZADO (30/06): CornerPro foi reintroduzido na cascata, agora com
    # arquitetura própria de segurança — sessão via cookies (não scraping de
    # login), espera obrigatória após o fim do jogo, intervalo mínimo entre
    # requisições, e circuit-breaker que desliga o provider sozinho se a
    # sessão expirar (ver CornerProProvider). É a fonte com cobertura mais
    # próxima de 100% dos jogos que o ALFA realmente analisa, porque é a
    # MESMA fonte que já alimenta o ao vivo. Fica em primeiro na prioridade;
    # se falhar (ou estiver fora da janela de espera), a cascata cai pros
    # providers seguintes normalmente, sem travar a auditoria do jogo.
    provider_priority: List[str] = field(default_factory=lambda: [
        "cornerpro", "api1", "api2", "api3", "sofascore", "flashscore"
    ])

    # --- CornerPro: cookies de sessão (não é login automático) ---
    # A sessão da CornerPro não expira por inatividade nem ao fechar a aba —
    # só com logout manual. Por isso o auditor usa os cookies de uma sessão
    # já autenticada pelo operador, em vez de tentar automatizar login.
    cornerpro_phpsessid: str = os.getenv("CORNERPRO_PHPSESSID", "")
    cornerpro_token: str = os.getenv("CORNERPRO_TOKEN", "")

    # Nunca duas requisições à CornerPro ao mesmo tempo, e nunca em rajada —
    # regra de segurança definida junto com o operador pra não arriscar a
    # sessão que o AO VIVO também depende (mesma conta, fontes diferentes).
    cornerpro_intervalo_minimo_segundos: int = int(os.getenv("CORNERPRO_INTERVALO_MINIMO_SEGUNDOS", "45"))

    # Espera depois do fim do jogo antes de tentar buscar o resultado:
    # (90 - minuto_do_alerta) + margem, em minutos.
    cornerpro_margem_minutos: int = int(os.getenv("CORNERPRO_MARGEM_MINUTOS", "25"))

    # Circuit-breaker: depois de N falhas de autenticação (401/403)
    # CONSECUTIVAS, o provider para de tentar e avisa o canal — em vez de
    # martelar uma sessão que claramente expirou.
    cornerpro_max_falhas_auth_consecutivas: int = int(os.getenv("CORNERPRO_MAX_FALHAS_AUTH", "3"))

    # Canal/chat pra avisos operacionais do CornerProProvider (níveis 2-4 de
    # erro). Se vazio, cai no telegram_chat_id geral do auditor.
    cornerpro_alert_chat_id: str = os.getenv("CORNERPRO_ALERT_CHAT_ID", "")
    
    # Limites
    queue_batch_size: int = 50
    max_retry_attempts: int = 3
    request_timeout: int = 30
    
    # Learning Engine
    learning_min_samples: int = 50
    learning_analysis_period_days: int = 90
    
    # Limiares para classificações
    threshold_pressure_convert: float = 0.6
    threshold_goal_high: int = 3
    threshold_corner_high: int = 10


# Configuração global
config = Config()


# ============================================================================
# ENUMS E CONSTANTES
# ============================================================================

class ClassificationType(Enum):
    """Classificações de contexto expandidas"""
    PRESSAO_CONVERTIDA = "PRESSAO_CONVERTIDA"
    PRESSAO_CONTINUOU = "PRESSAO_CONTINUOU"
    PRESSAO_PREMIADA = "PRESSAO_PREMIADA"
    PRESSAO_MORREU = "PRESSAO_MORREU"
    PRESSAO_FALSA = "PRESSAO_FALSA"
    PRESSAO_ANTIGA = "PRESSAO_ANTIGA"
    GOL_CONTRA_FLUXO = "GOL_CONTRA_FLUXO"
    CAOS = "CAOS"
    SEM_GOL = "SEM_GOL"


class OperationalResult(Enum):
    """Resultados operacionais expandidos"""
    GREEN_EXCELENTE = "GREEN_EXCELENTE"
    GREEN_NORMAL = "GREEN_NORMAL"
    GREEN_SOFRIDO = "GREEN_SOFRIDO"
    GREEN_PERDIDO_EXCELENTE = "GREEN_PERDIDO_EXCELENTE"
    GREEN_PERDIDO_NORMAL = "GREEN_PERDIDO_NORMAL"
    RED_JUSTIFICAVEL = "RED_JUSTIFICAVEL"
    RED_EVITAVEL = "RED_EVITAVEL"
    RED_BOBO = "RED_BOBO"
    RED_EVITADO_JUSTIFICAVEL = "RED_EVITADO_JUSTIFICAVEL"
    RED_EVITADO_AFORTUNADO = "RED_EVITADO_AFORTUNADO"


class GameContext(Enum):
    """Contexto do jogo no momento da decisão"""
    FAVORITO_VENCENDO = "FAVORITO_VENCENDO"
    FAVORITO_EMPATANDO = "FAVORITO_EMPATANDO"
    FAVORITO_PERDENDO = "FAVORITO_PERDENDO"
    ZEBRA_VENCENDO = "ZEBRA_VENCENDO"
    ZEBRA_EMPATANDO = "ZEBRA_EMPATANDO"
    ZEBRA_PERDENDO = "ZEBRA_PERDENDO"
    EQUILIBRADO = "EQUILIBRADO"


class PressureType(Enum):
    """Tipo de pressão identificada"""
    PRESSIONANTE = "PRESSIONANTE"
    DOMINIO_TERRITORIAL = "DOMINIO_TERRITORIAL"
    MELHORES_OPORTUNIDADES = "MELHORES_OPORTUNIDADES"
    MAIOR_INTENSIDADE = "MAIOR_INTENSIDADE"
    NENHUM = "NENHUM"


# ============================================================================
# BANCO DE DADOS V2
# ============================================================================

class AuditDatabaseV2:
    """Gerenciador do banco de dados V2 com tabelas expandidas"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.db_path
        self._init_tables()
        self._init_indices()
    
    @contextmanager
    def get_connection(self):
        """Context manager para conexões com o banco"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_tables(self):
        """Cria todas as tabelas necessárias (V2 expandido)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Tabela principal de partidas auditadas (expandida)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_id TEXT UNIQUE NOT NULL,
                    fixture_id TEXT,
                    match_id TEXT,
                    league TEXT,
                    country TEXT,
                    season TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    match_date TEXT,
                    match_time TEXT,
                    market TEXT,
                    period TEXT,
                    
                    -- SNAPSHOT COMPLETO (V2)
                    snapshot_raw TEXT,          -- Dados brutos recebidos
                    snapshot_processed TEXT,    -- Dados tratados
                    decision_type TEXT,
                    approved BOOLEAN,
                    decision_minute INTEGER,
                    decision_reasons TEXT,
                    
                    -- SCORES COMPLETOS (V2)
                    score_total REAL,
                    score_base REAL,
                    score_pressure REAL,
                    score_ip REAL,
                    score_chance_gol REAL,
                    score_shots REAL,
                    score_rb REAL,
                    score_contexto REAL,
                    score_favoritismo REAL,
                    penalties TEXT,
                    bonuses TEXT,
                    locks_triggered TEXT,
                    score_components TEXT,
                    
                    -- CONTEXTO DO JOGO (V2)
                    favorite_team TEXT,
                    favorite_odd REAL,
                    pressure_team TEXT,
                    territorial_dominance TEXT,
                    best_opportunities TEXT,
                    offensive_intensity TEXT,
                    emotional_situation TEXT,
                    game_context TEXT,
                    pressure_type TEXT,
                    
                    -- RESULTADOS REAIS
                    ft_score_home INTEGER,
                    ft_score_away INTEGER,
                    ht_score_home INTEGER,
                    ht_score_away INTEGER,
                    goal_after_alert BOOLEAN,
                    goal_team TEXT,
                    goal_minute INTEGER,
                    goal_is_favorite BOOLEAN,
                    goal_is_pressure BOOLEAN,
                    goal_side TEXT,              -- LADO DO GOL (V2)
                    
                    -- CLASSIFICAÇÕES V2
                    classification TEXT,          -- ClassificationType
                    classification_detailed TEXT, -- Detalhamento da classificação
                    operational_result TEXT,      -- OperationalResult
                    green BOOLEAN,
                    
                    -- ESTATÍSTICAS DA PARTIDA
                    shots_home INTEGER,
                    shots_away INTEGER,
                    shots_on_target_home INTEGER,
                    shots_on_target_away INTEGER,
                    corners_home INTEGER,
                    corners_away INTEGER,
                    possession_home INTEGER,
                    possession_away INTEGER,
                    xg_home REAL,
                    xg_away REAL,
                    attacks_home INTEGER,
                    attacks_away INTEGER,
                    dangerous_attacks_home INTEGER,
                    dangerous_attacks_away INTEGER,
                    yellow_cards_home INTEGER,
                    yellow_cards_away INTEGER,
                    red_cards_home INTEGER,
                    red_cards_away INTEGER,
                    
                    -- METADADOS
                    audit_source TEXT,
                    audited_at TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            
            # Tabela de relatórios inteligentes (V2)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_type TEXT,
                    report_date TEXT,
                    report_data TEXT,
                    conclusions TEXT,
                    generated_at TEXT
                )
            """)
            
            # Tabela de estatísticas agregadas (V2)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_date TEXT,
                    stat_type TEXT,
                    stat_key TEXT,
                    stat_value REAL,
                    stat_metadata TEXT,
                    updated_at TEXT,
                    UNIQUE(stat_date, stat_type, stat_key)
                )
            """)
            
            # Tabela de aprendizado (V2 - NEW)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_learning (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    learning_type TEXT,
                    learning_key TEXT,
                    learning_value REAL,
                    sample_size INTEGER,
                    confidence REAL,
                    metadata TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    UNIQUE(learning_type, learning_key)
                )
            """)
            
            # Tabela de padrões identificados (V2 - NEW)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_type TEXT,
                    pattern_name TEXT,
                    pattern_description TEXT,
                    conditions TEXT,
                    success_rate REAL,
                    sample_size INTEGER,
                    confidence REAL,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            
            # Tabela de logs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    log_type TEXT,
                    log_message TEXT,
                    log_data TEXT,
                    created_at TEXT
                )
            """)
            
            # Tabela de fila de processamento
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_id TEXT UNIQUE NOT NULL,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            
            conn.commit()
    
    def _init_indices(self):
        """Cria índices para melhor performance"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            indices = [
                "CREATE INDEX IF NOT EXISTS idx_decision_id ON audit_matches(decision_id)",
                "CREATE INDEX IF NOT EXISTS idx_match_date ON audit_matches(match_date)",
                "CREATE INDEX IF NOT EXISTS idx_classification ON audit_matches(classification)",
                "CREATE INDEX IF NOT EXISTS idx_green ON audit_matches(green)",
                "CREATE INDEX IF NOT EXISTS idx_approved ON audit_matches(approved)",
                "CREATE INDEX IF NOT EXISTS idx_audited_at ON audit_matches(audited_at)",
                "CREATE INDEX IF NOT EXISTS idx_league ON audit_matches(league)",
                "CREATE INDEX IF NOT EXISTS idx_period ON audit_matches(period)",
                "CREATE INDEX IF NOT EXISTS idx_queue_status ON audit_queue(status)",
                "CREATE INDEX IF NOT EXISTS idx_stats_date ON audit_statistics(stat_date)",
                "CREATE INDEX IF NOT EXISTS idx_learning_type ON audit_learning(learning_type)",
                "CREATE INDEX IF NOT EXISTS idx_patterns_type ON audit_patterns(pattern_type)",
                "CREATE INDEX IF NOT EXISTS idx_game_context ON audit_matches(game_context)",
                "CREATE INDEX IF NOT EXISTS idx_operational_result ON audit_matches(operational_result)",
                "CREATE INDEX IF NOT EXISTS idx_score_total ON audit_matches(score_total)",
            ]
            
            for index in indices:
                cursor.execute(index)
            
            conn.commit()
    
    # ===== MÉTODOS CRUD V2 =====
    
    def save_snapshot_v2(self, decision_id: str, snapshot: Dict[str, Any]) -> bool:
        """Salva um snapshot completo da decisão (V2)"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Extrair scores
                scores = snapshot.get('scores', {})
                
                # Extrair contexto
                context = snapshot.get('context', {})
                
                cursor.execute("""
                    INSERT OR REPLACE INTO audit_matches (
                        decision_id, fixture_id, match_id, league, country, season,
                        home_team, away_team, match_date, match_time, market, period,
                        snapshot_raw, snapshot_processed, decision_type, approved, 
                        decision_minute, decision_reasons,
                        score_total, score_base, score_pressure, score_ip, 
                        score_chance_gol, score_shots, score_rb, score_contexto,
                        score_favoritismo, penalties, bonuses, locks_triggered, 
                        score_components,
                        favorite_team, favorite_odd, pressure_team, 
                        territorial_dominance, best_opportunities, offensive_intensity,
                        emotional_situation, game_context, pressure_type,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    decision_id,
                    snapshot.get('fixture_id'),
                    snapshot.get('match_id'),
                    snapshot.get('league'),
                    snapshot.get('country'),
                    snapshot.get('season'),
                    snapshot.get('home_team'),
                    snapshot.get('away_team'),
                    snapshot.get('match_date'),
                    snapshot.get('match_time'),
                    snapshot.get('market'),
                    snapshot.get('period', 'FT'),
                    json.dumps(snapshot, ensure_ascii=False),  # snapshot_raw
                    json.dumps(snapshot.get('processed', {}), ensure_ascii=False),  # snapshot_processed
                    snapshot.get('decision_type', 'live'),
                    1 if snapshot.get('approved') else 0,
                    snapshot.get('decision_minute'),
                    json.dumps(snapshot.get('decision_reasons', []), ensure_ascii=False),
                    scores.get('total'),
                    scores.get('base'),
                    scores.get('pressure'),
                    scores.get('ip'),
                    scores.get('chance_gol'),
                    scores.get('shots'),
                    scores.get('rb'),
                    scores.get('contexto'),
                    scores.get('favoritismo'),
                    json.dumps(scores.get('penalties', []), ensure_ascii=False),
                    json.dumps(scores.get('bonuses', []), ensure_ascii=False),
                    json.dumps(scores.get('locks', []), ensure_ascii=False),
                    json.dumps(scores.get('components', {}), ensure_ascii=False),
                    context.get('favorite_team'),
                    context.get('favorite_odd'),
                    context.get('pressure_team'),
                    context.get('territorial_dominance'),
                    context.get('best_opportunities'),
                    context.get('offensive_intensity'),
                    context.get('emotional_situation'),
                    context.get('game_context'),
                    context.get('pressure_type'),
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                ))
                
                conn.commit()
                return True
        except Exception as e:
            self.log_error(f"Erro ao salvar snapshot V2: {e}", {'decision_id': decision_id})
            return False
    
    def update_match_result_v2(self, decision_id: str, result_data: Dict[str, Any]) -> bool:
        """Atualiza uma partida com o resultado real e classificações V2"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE audit_matches SET
                        ft_score_home = ?,
                        ft_score_away = ?,
                        ht_score_home = ?,
                        ht_score_away = ?,
                        goal_after_alert = ?,
                        goal_team = ?,
                        goal_minute = ?,
                        goal_is_favorite = ?,
                        goal_is_pressure = ?,
                        goal_side = ?,
                        classification = ?,
                        classification_detailed = ?,
                        operational_result = ?,
                        green = ?,
                        shots_home = ?,
                        shots_away = ?,
                        shots_on_target_home = ?,
                        shots_on_target_away = ?,
                        corners_home = ?,
                        corners_away = ?,
                        possession_home = ?,
                        possession_away = ?,
                        xg_home = ?,
                        xg_away = ?,
                        attacks_home = ?,
                        attacks_away = ?,
                        dangerous_attacks_home = ?,
                        dangerous_attacks_away = ?,
                        yellow_cards_home = ?,
                        yellow_cards_away = ?,
                        red_cards_home = ?,
                        red_cards_away = ?,
                        audit_source = ?,
                        audited_at = ?,
                        updated_at = ?
                    WHERE decision_id = ?
                """, (
                    result_data.get('ft_score_home'),
                    result_data.get('ft_score_away'),
                    result_data.get('ht_score_home'),
                    result_data.get('ht_score_away'),
                    1 if result_data.get('goal_after_alert') else 0,
                    result_data.get('goal_team'),
                    result_data.get('goal_minute'),
                    1 if result_data.get('goal_is_favorite') else 0,
                    1 if result_data.get('goal_is_pressure') else 0,
                    result_data.get('goal_side'),
                    result_data.get('classification'),
                    result_data.get('classification_detailed'),
                    result_data.get('operational_result'),
                    1 if result_data.get('green') else 0,
                    result_data.get('shots_home', 0),
                    result_data.get('shots_away', 0),
                    result_data.get('shots_on_target_home', 0),
                    result_data.get('shots_on_target_away', 0),
                    result_data.get('corners_home', 0),
                    result_data.get('corners_away', 0),
                    result_data.get('possession_home', 0),
                    result_data.get('possession_away', 0),
                    result_data.get('xg_home', 0.0),
                    result_data.get('xg_away', 0.0),
                    result_data.get('attacks_home', 0),
                    result_data.get('attacks_away', 0),
                    result_data.get('dangerous_attacks_home', 0),
                    result_data.get('dangerous_attacks_away', 0),
                    result_data.get('yellow_cards_home', 0),
                    result_data.get('yellow_cards_away', 0),
                    result_data.get('red_cards_home', 0),
                    result_data.get('red_cards_away', 0),
                    result_data.get('audit_source', 'unknown'),
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                    decision_id
                ))
                
                conn.commit()
                return True
        except Exception as e:
            self.log_error(f"Erro ao atualizar resultado V2: {e}", {'decision_id': decision_id})
            return False
    
    # ===== MÉTODOS DE CONSULTA AVANÇADA V2 =====
    
    def get_match_by_decision_id(self, decision_id: str) -> Optional[Dict[str, Any]]:
        """Busca uma partida pelo ID da decisão"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM audit_matches WHERE decision_id = ?", (decision_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_pending_matches(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retorna partidas pendentes de auditoria"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_matches 
                WHERE ft_score_home IS NULL 
                AND audited_at IS NULL
                ORDER BY created_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_unprocessed_queue(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retorna itens não processados da fila"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_queue 
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_resumo_hoje(self) -> Dict[str, Any]:
        """Resumo do dia (30/06) — pensado pra responder rápido, a qualquer
        hora, à pergunta 'como está indo a auditoria hoje?'. Diferente do
        get_stats_summary_v2 (que é histórico acumulado desde sempre), este
        filtra só pelo dia de hoje."""
        hoje = datetime.now().strftime("%Y-%m-%d")
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM audit_matches WHERE date(created_at) = ?", (hoje,)
            )
            registrados_hoje = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM audit_matches WHERE date(created_at) = ? AND audited_at IS NOT NULL",
                (hoje,)
            )
            auditados_hoje = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM audit_matches WHERE date(created_at) = ? AND approved = 1",
                (hoje,)
            )
            aprovados_hoje = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM audit_matches WHERE date(created_at) = ? AND approved = 0",
                (hoje,)
            )
            reprovados_hoje = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM audit_matches WHERE date(created_at) = ? AND green = 1",
                (hoje,)
            )
            greens_hoje = cursor.fetchone()[0] or 0

            cursor.execute(
                "SELECT COUNT(*) FROM audit_matches WHERE date(created_at) = ? AND green = 0 AND audited_at IS NOT NULL",
                (hoje,)
            )
            reds_hoje = cursor.fetchone()[0] or 0

            cursor.execute("SELECT COUNT(*) FROM audit_queue WHERE status = 'pending'")
            pendentes_total = cursor.fetchone()[0] or 0

            cursor.execute("SELECT COUNT(*) FROM audit_queue WHERE status = 'failed'")
            falhados_total = cursor.fetchone()[0] or 0

        return {
            "registrados_hoje": registrados_hoje,
            "auditados_hoje": auditados_hoje,
            "aprovados_hoje": aprovados_hoje,
            "reprovados_hoje": reprovados_hoje,
            "greens_hoje": greens_hoje,
            "reds_hoje": reds_hoje,
            "pendentes_na_fila": pendentes_total,
            "falhados_na_fila": falhados_total,
        }
    
    # ===== CONSULTAS INTELIGENTES V2 =====
    
    def query_performance_by_score_range(self, min_score: float, max_score: float) -> Dict[str, Any]:
        """Performance por faixa de score"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE score_total BETWEEN ? AND ?
                AND audited_at IS NOT NULL
            """, (min_score, max_score))
            row = cursor.fetchone()
            return dict(row) if row else {'total': 0, 'greens': 0, 'success_rate': 0}
    
    def query_performance_by_context(self, context: str) -> Dict[str, Any]:
        """Performance por contexto do jogo"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE game_context = ?
                AND audited_at IS NOT NULL
            """, (context,))
            row = cursor.fetchone()
            return dict(row) if row else {'total': 0, 'greens': 0, 'success_rate': 0}
    
    def query_pressure_conversion_by_score(self, min_score: float = None) -> List[Dict[str, Any]]:
        """Conversão de pressão por faixa de score"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            where_clause = ""
            if min_score is not None:
                where_clause = f"AND score_total >= {min_score}"
            
            cursor.execute(f"""
                SELECT 
                    classification,
                    COUNT(*) as total,
                    SUM(CASE WHEN goal_after_alert = 1 AND goal_is_pressure = 1 THEN 1 ELSE 0 END) as pressure_converted,
                    ROUND(SUM(CASE WHEN goal_after_alert = 1 AND goal_is_pressure = 1 THEN 1 ELSE 0 END) * 100.0 / 
                          COUNT(CASE WHEN goal_after_alert = 1 THEN 1 ELSE NULL END), 2) as conversion_rate
                FROM audit_matches
                WHERE audited_at IS NOT NULL
                {where_clause}
                GROUP BY classification
                ORDER BY conversion_rate DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def query_favorite_performance(self) -> Dict[str, Any]:
        """Performance do favorito em diferentes situações"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Favorito vencendo
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE game_context = 'FAVORITO_VENCENDO'
                AND audited_at IS NOT NULL
            """)
            winning = cursor.fetchone()
            
            # Favorito empatando
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE game_context = 'FAVORITO_EMPATANDO'
                AND audited_at IS NOT NULL
            """)
            drawing = cursor.fetchone()
            
            # Favorito perdendo
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE game_context = 'FAVORITO_PERDENDO'
                AND audited_at IS NOT NULL
            """)
            losing = cursor.fetchone()
            
            return {
                'vencendo': dict(winning) if winning else {'total': 0, 'greens': 0, 'success_rate': 0},
                'empatando': dict(drawing) if drawing else {'total': 0, 'greens': 0, 'success_rate': 0},
                'perdendo': dict(losing) if losing else {'total': 0, 'greens': 0, 'success_rate': 0}
            }
    
    def query_goal_side_analysis(self) -> List[Dict[str, Any]]:
        """Análise do lado do gol"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    goal_side,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE goal_after_alert = 1
                AND goal_side IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY goal_side
                ORDER BY success_rate DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def query_operational_result_analysis(self) -> List[Dict[str, Any]]:
        """Análise de resultados operacionais"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    operational_result,
                    COUNT(*) as total,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM audit_matches WHERE audited_at IS NOT NULL), 2) as percentage
                FROM audit_matches
                WHERE operational_result IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY operational_result
                ORDER BY total DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_stats_summary_v2(self) -> Dict[str, Any]:
        """Resumo estatístico completo V2"""
        stats = {}
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Total de jogos
            cursor.execute("SELECT COUNT(*) FROM audit_matches")
            stats['total'] = cursor.fetchone()[0] or 0
            
            # Total auditados
            cursor.execute("SELECT COUNT(*) FROM audit_matches WHERE audited_at IS NOT NULL")
            stats['audited'] = cursor.fetchone()[0] or 0
            
            # Por período
            cursor.execute("SELECT period, COUNT(*) FROM audit_matches GROUP BY period")
            stats['by_period'] = {row[0] or 'unknown': row[1] for row in cursor.fetchall()}
            
            # Por contexto
            cursor.execute("""
                SELECT game_context, COUNT(*) FROM audit_matches 
                WHERE game_context IS NOT NULL 
                GROUP BY game_context
            """)
            stats['by_context'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Por classificação
            cursor.execute("""
                SELECT classification, COUNT(*) FROM audit_matches 
                WHERE classification IS NOT NULL 
                GROUP BY classification
            """)
            stats['by_classification'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Por resultado operacional
            cursor.execute("""
                SELECT operational_result, COUNT(*) FROM audit_matches 
                WHERE operational_result IS NOT NULL 
                GROUP BY operational_result
            """)
            stats['by_operational_result'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Taxa de acerto geral
            cursor.execute("SELECT SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) FROM audit_matches")
            total_greens = cursor.fetchone()[0] or 0
            stats['success_rate'] = round(total_greens / stats['total'] * 100, 2) if stats['total'] > 0 else 0
            
            # Score médio
            cursor.execute("SELECT AVG(score_total) FROM audit_matches WHERE score_total IS NOT NULL")
            stats['avg_score'] = round(cursor.fetchone()[0] or 0, 2)
            
            # Score por contexto
            cursor.execute("""
                SELECT 
                    game_context,
                    AVG(score_total) as avg_score,
                    COUNT(*) as total
                FROM audit_matches
                WHERE game_context IS NOT NULL AND score_total IS NOT NULL
                GROUP BY game_context
            """)
            stats['score_by_context'] = [dict(row) for row in cursor.fetchall()]
            
            return stats
    
    def add_to_queue(self, decision_id: str) -> bool:
        """Adiciona uma decisão à fila de processamento"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO audit_queue (decision_id, status, created_at, updated_at)
                    VALUES (?, 'pending', ?, ?)
                """, (
                    decision_id,
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            self.log_error(f"Erro ao adicionar à fila: {e}", {'decision_id': decision_id})
            return False
    
    def update_queue_status(self, decision_id: str, status: str, error: str = None) -> bool:
        """Atualiza o status de um item na fila"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE audit_queue 
                    SET status = ?, attempts = attempts + 1, error_message = ?, updated_at = ?
                    WHERE decision_id = ?
                """, (
                    status,
                    error,
                    datetime.now().isoformat(),
                    decision_id
                ))
                conn.commit()
                return True
        except Exception as e:
            self.log_error(f"Erro ao atualizar fila: {e}", {'decision_id': decision_id})
            return False
    
    def log_error(self, message: str, data: Dict[str, Any] = None):
        """Registra um erro no log"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO audit_logs (log_type, log_message, log_data, created_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    'error',
                    message,
                    json.dumps(data, ensure_ascii=False) if data else None,
                    datetime.now().isoformat()
                ))
                conn.commit()
        except Exception:
            pass
    
    def log_info(self, message: str, data: Dict[str, Any] = None):
        """Registra uma informação no log"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO audit_logs (log_type, log_message, log_data, created_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    'info',
                    message,
                    json.dumps(data, ensure_ascii=False) if data else None,
                    datetime.now().isoformat()
                ))
                conn.commit()
        except Exception:
            pass


# ============================================================================
# MATCHING POR NOME — peça que faltava no V1/V2 originais.
#
# O pipeline do Projeto ALFA nunca tem o ID interno de nenhum provider
# (football-data.org, SofaScore, etc.) — só tem nome dos times + data,
# que vêm do CornerPro via Telegram. Sem isso, get_match_result(match_id)
# nunca ia achar nada de verdade. Esta função resolve "achar o jogo certo
# pelo nome", com a mesma régua de segurança discutida no projeto: nome
# dos dois times bate de forma forte + mesma data — senão, descarta
# (silêncio é mais seguro que confirmar resultado do jogo errado).
# ============================================================================

def _normalizar_nome_time(nome: str) -> str:
    if not nome:
        return ""
    txt = unicodedata.normalize("NFD", str(nome))
    txt = "".join(c for c in txt if unicodedata.category(c) != "Mn")
    txt = txt.lower().strip()
    txt = re.sub(r"\b(fc|cf|sc|afc|ac|club|the)\b", "", txt)
    txt = re.sub(r"[^a-z0-9 ]", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _similaridade_nome(a: str, b: str) -> float:
    """Similaridade simples por sobreposição de palavras (0.0 a 1.0)."""
    na, nb = _normalizar_nome_time(a), _normalizar_nome_time(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    sa, sb = set(na.split()), set(nb.split())
    if not sa or not sb:
        return 0.0
    inter = sa & sb
    return len(inter) / max(len(sa), len(sb))


def encontrar_id_por_nomes(candidatos: List[Dict[str, Any]], home_team: str, away_team: str,
                            limiar: float = 0.6) -> Optional[str]:
    """
    Recebe uma lista de jogos candidatos (cada um com 'id', 'home', 'away')
    e devolve o id do jogo certo, só se:
    - o melhor candidato bate forte nos dois nomes (>= limiar nos dois lados)
    - não existe um segundo candidato quase tão bom (ambiguidade -> descarta)
    """
    pontuados = []
    for c in candidatos:
        sim_h = _similaridade_nome(home_team, c.get("home", ""))
        sim_a = _similaridade_nome(away_team, c.get("away", ""))
        if sim_h >= limiar and sim_a >= limiar:
            pontuados.append((sim_h + sim_a, c.get("id")))

    if not pontuados:
        return None
    pontuados.sort(key=lambda x: x[0], reverse=True)
    if len(pontuados) >= 2 and (pontuados[0][0] - pontuados[1][0]) < 0.15:
        return None  # ambíguo — dois candidatos quase iguais, melhor não arriscar
    return pontuados[0][1]


# ============================================================================
# PROVIDERS (MANTIDOS DO V1)
# ============================================================================

class BaseProvider(ABC):
    """Interface base para todos os providers de dados"""
    
    @abstractmethod
    def get_match_result(self, match_id: str, fixture_id: str = None) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def get_match_statistics(self, match_id: str) -> Dict[str, Any]:
        pass
    
    def find_match_by_teams(self, home_team: str, away_team: str, match_date: str) -> Optional[str]:
        """
        Tenta achar o id do jogo desse provider usando só nome dos times +
        data — é o que o pipeline do ALFA realmente tem disponível (nunca
        tem o ID nativo de nenhum provider externo). Implementação default
        retorna None (provider não suporta busca por nome); cada provider
        concreto que tiver um endpoint de busca real deve sobrescrever.
        """
        return None
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def priority(self) -> int:
        pass


class APIProvider(BaseProvider):
    """Provider para APIs gratuitas"""
    
    def __init__(self, api_key: str = None, api_url: str = None, name: str = "api1"):
        self.api_key = api_key
        self.api_url = api_url or config.api_urls.get(name, "https://api.football-data.org/v4")
        self._provider_name = name
        self._priority = config.provider_priority.index(name) + 1 if name in config.provider_priority else 99
    
    @property
    def provider_name(self) -> str:
        return self._provider_name
    
    @property
    def priority(self) -> int:
        return self._priority
    
    def get_match_result(self, match_id: str, fixture_id: str = None) -> Dict[str, Any]:
        try:
            url = f"{self.api_url}/matches/{fixture_id or match_id}"
            headers = {"X-Auth-Token": self.api_key} if self.api_key else {}
            headers["User-Agent"] = "Mozilla/5.0 (compatible; CoutipsAuditor/2.0)"
            
            response = requests.get(url, headers=headers, timeout=config.request_timeout)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_response(data)
        except Exception as e:
            return {"error": str(e), "source": self.provider_name}
    
    def find_match_by_teams(self, home_team: str, away_team: str, match_date: str) -> Optional[str]:
        """
        football-data.org permite buscar jogos por intervalo de data
        (dateFrom/dateTo). Busca os jogos do dia e casa pelo nome dos
        times — só essa API tem esse endpoint documentado de verdade
        entre os providers gratuitos da cascata.
        """
        if not match_date:
            return None
        try:
            url = f"{self.api_url}/matches"
            params = {"dateFrom": match_date, "dateTo": match_date}
            headers = {"X-Auth-Token": self.api_key} if self.api_key else {}
            headers["User-Agent"] = "Mozilla/5.0 (compatible; CoutipsAuditor/2.0)"
            
            response = requests.get(url, headers=headers, params=params, timeout=config.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            candidatos = []
            for m in data.get("matches", []):
                candidatos.append({
                    "id": m.get("id"),
                    "home": (m.get("homeTeam") or {}).get("name", ""),
                    "away": (m.get("awayTeam") or {}).get("name", ""),
                })
            return encontrar_id_por_nomes(candidatos, home_team, away_team)
        except Exception:
            return None
    
    def get_match_statistics(self, match_id: str) -> Dict[str, Any]:
        try:
            url = f"{self.api_url}/matches/{match_id}/statistics"
            headers = {"X-Auth-Token": self.api_key} if self.api_key else {}
            headers["User-Agent"] = "Mozilla/5.0 (compatible; CoutipsAuditor/2.0)"
            
            response = requests.get(url, headers=headers, timeout=config.request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def _parse_response(self, data: Dict) -> Dict[str, Any]:
        match = data.get('match', data)
        score = match.get('score', {})
        home_team = match.get('homeTeam', {})
        away_team = match.get('awayTeam', {})
        
        return {
            'source': self.provider_name,
            'ft_score_home': score.get('fullTime', {}).get('homeTeam'),
            'ft_score_away': score.get('fullTime', {}).get('awayTeam'),
            'ht_score_home': score.get('halfTime', {}).get('homeTeam'),
            'ht_score_away': score.get('halfTime', {}).get('awayTeam'),
            'home_team': home_team.get('name'),
            'away_team': away_team.get('name'),
            'status': match.get('status'),
            'utc_date': match.get('utcDate')
        }


class SofaScoreProvider(BaseProvider):
    """Provider para SofaScore"""
    
    def __init__(self):
        self.base_url = "https://api.sofascore.com/api/v1"
        self._provider_name = "sofascore"
        self._priority = config.provider_priority.index("sofascore") + 1
    
    @property
    def provider_name(self) -> str:
        return self._provider_name
    
    @property
    def priority(self) -> int:
        return self._priority
    
    def get_match_result(self, match_id: str, fixture_id: str = None) -> Dict[str, Any]:
        try:
            url = f"{self.base_url}/event/{match_id}"
            headers = {"User-Agent": "Mozilla/5.0 (compatible; CoutipsAuditor/2.0)"}
            time.sleep(0.5)  # cuidado de fonte não-oficial: nunca em rajada
            response = requests.get(url, headers=headers, timeout=config.request_timeout)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_response(data)
        except Exception as e:
            return {"error": str(e), "source": self.provider_name}
    
    def find_match_by_teams(self, home_team: str, away_team: str, match_date: str) -> Optional[str]:
        """
        Busca via endpoint de busca do SofaScore (não-oficial, mas público
        e bastante usado). Filtra os resultados por tipo "event" e checa
        nome dos dois times. Sem ID prévio nenhum, é a única forma de achar
        o jogo certo nessa fonte.
        """
        try:
            query = f"{home_team} {away_team}".strip()
            if not query:
                return None
            url = f"{self.base_url}/search/all"
            headers = {"User-Agent": "Mozilla/5.0 (compatible; CoutipsAuditor/2.0)"}
            time.sleep(0.5)
            response = requests.get(url, headers=headers, params={"q": query}, timeout=config.request_timeout)
            response.raise_for_status()
            data = response.json()
            
            candidatos = []
            for item in data.get("results", []):
                entity = item.get("entity", {}) if isinstance(item, dict) else {}
                if item.get("type") != "event":
                    continue
                home = (entity.get("homeTeam") or {}).get("name", "")
                away = (entity.get("awayTeam") or {}).get("name", "")
                candidatos.append({"id": entity.get("id"), "home": home, "away": away})
            return encontrar_id_por_nomes(candidatos, home_team, away_team)
        except Exception:
            return None
    
    def get_match_statistics(self, match_id: str) -> Dict[str, Any]:
        try:
            url = f"{self.base_url}/event/{match_id}/statistics"
            headers = {"User-Agent": "Mozilla/5.0 (compatible; CoutipsAuditor/2.0)"}
            time.sleep(0.5)
            response = requests.get(url, headers=headers, timeout=config.request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def _parse_response(self, data: Dict) -> Dict[str, Any]:
        event = data.get('event', {})
        home_score = event.get('homeScore', {})
        away_score = event.get('awayScore', {})
        
        return {
            'source': self.provider_name,
            'ft_score_home': home_score.get('current'),
            'ft_score_away': away_score.get('current'),
            'ht_score_home': home_score.get('period1'),
            'ht_score_away': away_score.get('period1'),
            'status': event.get('status', {}).get('description'),
            'start_time': event.get('startTimestamp')
        }


class FlashScoreProvider(BaseProvider):
    """Provider para FlashScore.

    HONESTO (29/06): esse provider está como estava no V1 — a URL base
    (flashscore.com/api) não é um endpoint público real, e _parse_response
    sempre devolve placar None independente da resposta. Ou seja, hoje ele
    nunca confirma resultado nenhum; só cai pro próximo provider da
    cascata sem erro. Deixei na cascata (não quebra nada, é inofensivo),
    mas não conte com ele até alguém substituir por um endpoint real.
    """
    
    def __init__(self):
        self.base_url = "https://flashscore.com/api"
        self._provider_name = "flashscore"
        self._priority = config.provider_priority.index("flashscore") + 1
    
    @property
    def provider_name(self) -> str:
        return self._provider_name
    
    @property
    def priority(self) -> int:
        return self._priority
    
    def get_match_result(self, match_id: str, fixture_id: str = None) -> Dict[str, Any]:
        try:
            url = f"{self.base_url}/match/{match_id}"
            response = requests.get(url, timeout=config.request_timeout)
            response.raise_for_status()
            return self._parse_response(response.text)
        except Exception as e:
            return {"error": str(e), "source": self.provider_name}
    
    def get_match_statistics(self, match_id: str) -> Dict[str, Any]:
        try:
            url = f"{self.base_url}/match/{match_id}/statistics"
            response = requests.get(url, timeout=config.request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def _parse_response(self, data: str) -> Dict[str, Any]:
        return {
            'source': self.provider_name,
            'ft_score_home': None,
            'ft_score_away': None,
            'ht_score_home': None,
            'ht_score_away': None,
            'status': 'unknown'
        }


# ============================================================================
# CORNERPRO PROVIDER — adicionado em 30/06/2026
#
# Mesma fonte que já alimenta o AO VIVO via Telegram. A página de jogo da
# CornerPro guarda, depois que a partida termina, um JSON completo dentro
# de um bloco <script> Next.js (self.__next_f.push). Esta classe não faz
# login automatizado — usa os cookies de uma sessão já autenticada pelo
# operador (PHPSESSID + token), porque a sessão da CornerPro não expira
# por inatividade, só com logout manual confirmado pelo operador.
#
# Quatro camadas de segurança, todas combinadas com o operador antes de
# escrever este código:
#   1. Espera obrigatória: (90 - minuto_do_alerta) + margem antes de tentar.
#   2. Intervalo mínimo entre requisições (nunca em rajada, nunca paralelo).
#   3. Sistema de 4 níveis de erro (ver _registrar_falha / _registrar_sucesso).
#   4. Circuit-breaker: some auth falha demais, o provider se desliga sozinho
#      até o operador renovar os cookies — nunca martela uma sessão morta.
# ============================================================================

class CornerProProvider(BaseProvider):
    """Provider para a CornerPro (cornerprobet.com), via cookies de sessão."""

    _ultima_requisicao_ts: float = 0.0  # compartilhado por todas as instâncias
    _lock = threading.Lock()

    def __init__(self):
        self._provider_name = "cornerpro"
        self._priority = (
            config.provider_priority.index("cornerpro") + 1
            if "cornerpro" in config.provider_priority else 99
        )
        self._telegram = None  # criado sob demanda, só quando precisa avisar
        # Estado do circuit-breaker e dos níveis de erro por jogo. Em
        # memória, mas o nível 2 (falha por jogo) também se apoia no
        # contador 'attempts' que o AuditDatabaseV2 já mantém na fila —
        # não depende só de memória pra não se perder num restart.
        self._falhas_auth_consecutivas = 0
        self._circuito_aberto = False
        self._falhas_por_decisao: Dict[str, int] = {}

    @property
    def provider_name(self) -> str:
        return self._provider_name

    @property
    def priority(self) -> int:
        return self._priority

    def _get_telegram(self) -> "AuditTelegramV2":
        if self._telegram is None:
            self._telegram = AuditTelegramV2()
        return self._telegram

    def _avisar_canal(self, mensagem: str) -> None:
        try:
            chat_id_original = config.telegram_chat_id
            if config.cornerpro_alert_chat_id:
                config.telegram_chat_id = config.cornerpro_alert_chat_id
            self._get_telegram().send_message(mensagem, parse_mode="HTML")
            config.telegram_chat_id = chat_id_original
        except Exception:
            pass  # aviso falhar nunca pode derrubar a auditoria em si

    def _cookies(self) -> Dict[str, str]:
        return {
            "PHPSESSID": config.cornerpro_phpsessid,
            "token": config.cornerpro_token,
        }

    def _sessao_configurada(self) -> bool:
        return bool(config.cornerpro_phpsessid and config.cornerpro_token)

    def _respeitar_intervalo_minimo(self) -> None:
        """Nunca duas requisições à CornerPro em rajada — espera o tempo
        que faltar desde a última requisição de QUALQUER instância."""
        with CornerProProvider._lock:
            agora = time.time()
            decorrido = agora - CornerProProvider._ultima_requisicao_ts
            faltante = config.cornerpro_intervalo_minimo_segundos - decorrido
            if faltante > 0:
                time.sleep(faltante)
            CornerProProvider._ultima_requisicao_ts = time.time()

    def _pronto_para_buscar(self, match: Dict[str, Any]) -> bool:
        """(90 - minuto_do_alerta) + margem, contado a partir do momento em
        que a decisão foi registrada na fila (proxy confiável do horário
        real do alerta, já que o registro acontece poucos segundos depois)."""
        try:
            minuto = int(match.get("decision_minute") or 0)
            criado_em_str = match.get("created_at") or match.get("queue_created_at")
            if not criado_em_str:
                return True  # sem como calcular, não bloqueia — deixa a
                              # cascata seguir e o próprio jogo decidir
            criado_em = datetime.fromisoformat(criado_em_str)
            minutos_espera = max(0, 90 - minuto) + config.cornerpro_margem_minutos
            pronto_em = criado_em + timedelta(minutes=minutos_espera)
            return datetime.now() >= pronto_em
        except Exception:
            return True

    @staticmethod
    def _extrair_json_apos_chave(texto: str, chave: str) -> Optional[Any]:
        """Acha '"chave":' no texto e extrai o bloco JSON balanceado que
        vem depois (objeto {} ou array []), faz json.loads só desse trecho.
        Resiliente ao formato React Server Components da Next.js, que não
        é um JSON único — é vários fragmentos de texto escapado.
        """
        idx = texto.find(f'"{chave}":')
        if idx == -1:
            return None
        pos = idx + len(f'"{chave}":')
        while pos < len(texto) and texto[pos] in " \t\n":
            pos += 1
        if pos >= len(texto) or texto[pos] not in "{[":
            return None
        abre = texto[pos]
        fecha = "}" if abre == "{" else "]"
        profundidade = 0
        fim = None
        dentro_string = False
        escapando = False
        for i in range(pos, len(texto)):
            c = texto[i]
            if escapando:
                escapando = False
                continue
            if c == "\\":
                escapando = True
                continue
            if c == '"':
                dentro_string = not dentro_string
                continue
            if dentro_string:
                continue
            if c == abre:
                profundidade += 1
            elif c == fecha:
                profundidade -= 1
                if profundidade == 0:
                    fim = i + 1
                    break
        if fim is None:
            return None
        bloco = texto[pos:fim]
        try:
            return json.loads(bloco)
        except Exception:
            return None

    def _extrair_resultado_da_pagina(self, html: str) -> Dict[str, Any]:
        """Concatena os fragmentos self.__next_f.push([1,"...]) e procura o
        objeto 'game' dentro do texto já desescapado. Devolve status + scores
        se conseguir; nunca inventa valor — se não achar com segurança,
        devolve erro de parse e a cascata segue pro próximo provider.
        """
        fragmentos = re.findall(r'self\.__next_f\.push\(\[1,\s*"((?:[^"\\]|\\.)*)"\]\)', html)
        if not fragmentos:
            return {"error": "parse_fail", "detalhe": "nenhum fragmento __next_f encontrado"}

        texto_completo = ""
        for frag in fragmentos:
            try:
                texto_completo += frag.encode("utf-8").decode("unicode_escape")
            except Exception:
                texto_completo += frag

        game = self._extrair_json_apos_chave(texto_completo, "game")
        if not isinstance(game, dict):
            return {"error": "parse_fail", "detalhe": "bloco 'game' não encontrado/inválido"}

        status = game.get("status")
        scores = game.get("scores") or {}

        def _par(valor) -> Tuple[Optional[int], Optional[int]]:
            if isinstance(valor, (list, tuple)) and len(valor) >= 2:
                try:
                    return int(valor[0]), int(valor[1])
                except Exception:
                    return None, None
            if isinstance(valor, dict):
                try:
                    return int(valor.get("home")), int(valor.get("away"))
                except Exception:
                    return None, None
            return None, None

        ft_home, ft_away = _par(scores.get("FT") or scores.get("ft"))
        ht_home, ht_away = _par(scores.get("HT") or scores.get("ht"))

        if status != 3 or ft_home is None:
            # Jogo ainda não terminou (ou terminou e ainda não conseguimos
            # ler o placar com segurança) — não é erro, é "ainda não dá".
            return {"error": "not_finished", "status_bruto": status}

        return {
            "source": self.provider_name,
            "ft_score_home": ft_home,
            "ft_score_away": ft_away,
            "ht_score_home": ht_home,
            "ht_score_away": ht_away,
            "status": "finished",
        }

    def get_match_result(self, match_id: str, fixture_id: str = None,
                          match_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        match_context = match_context or {}

        if not self._sessao_configurada():
            return {"error": "sem_sessao", "source": self.provider_name}

        if self._circuito_aberto:
            return {"error": "circuito_aberto", "source": self.provider_name}

        if not self._pronto_para_buscar(match_context):
            return {"error": "aguardando_fim_jogo", "source": self.provider_name}

        url = match_id or match_context.get("cornerpro_url")
        if not url:
            return {"error": "sem_url", "source": self.provider_name}

        self._respeitar_intervalo_minimo()

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            }
            response = requests.get(
                url, headers=headers, cookies=self._cookies(),
                timeout=config.request_timeout,
            )

            if response.status_code in (401, 403):
                self._registrar_falha_auth(match_context.get("decision_id", ""))
                return {"error": "auth", "source": self.provider_name, "status_code": response.status_code}

            response.raise_for_status()
            resultado = self._extrair_resultado_da_pagina(response.text)

            if resultado.get("error"):
                self._registrar_falha_nivel1(match_context.get("decision_id", ""), resultado.get("error"))
                return resultado

            self._registrar_sucesso()
            return resultado

        except requests.exceptions.RequestException as e:
            self._registrar_falha_nivel1(match_context.get("decision_id", ""), str(e))
            return {"error": "network", "source": self.provider_name, "detalhe": str(e)}

    def find_match_by_teams(self, home_team: str, away_team: str, match_date: str) -> Optional[str]:
        # Não se aplica — a CornerPro é acessada direto pela URL do alerta
        # (já vem certa, sem ambiguidade de nome), nunca por busca.
        return None

    def get_match_statistics(self, match_id: str) -> Dict[str, Any]:
        return {"error": "nao_implementado"}

    # --- Sistema de 4 níveis de erro ---

    def _registrar_sucesso(self) -> None:
        self._falhas_auth_consecutivas = 0
        self._falhas_por_decisao.clear()

    def _registrar_falha_nivel1(self, decision_id: str, detalhe: str) -> None:
        """Nível 1 — falha isolada (timeout, site fora do ar, parse falhou
        numa execução). Não avisa nada; a fila tenta de novo no próprio
        ciclo seguinte (5 minutos), e se na 2ª vez falhar de novo entra no
        nível 2."""
        if not decision_id:
            return
        contagem = self._falhas_por_decisao.get(decision_id, 0) + 1
        self._falhas_por_decisao[decision_id] = contagem
        if contagem >= 2:
            self._avisar_canal(
                f"⚠️ <b>Auditor CornerPro — jogo não auditado</b>\n"
                f"Decisão: <code>{decision_id}</code>\n"
                f"Motivo: {detalhe}\n"
                f"Duas tentativas falharam. Marcado como não auditado por essa fonte "
                f"— a cascata segue tentando as outras fontes normalmente."
            )

    def _registrar_falha_auth(self, decision_id: str) -> None:
        """Nível 3/4 — falha de autenticação (401/403). Acumula falhas
        CONSECUTIVAS (de qualquer jogo); ao atingir o limite, abre o
        circuito (para de tentar) e avisa em linguagem simples como
        renovar a sessão."""
        self._falhas_auth_consecutivas += 1
        limite = config.cornerpro_max_falhas_auth_consecutivas
        if self._falhas_auth_consecutivas < limite:
            return
        if self._circuito_aberto:
            return
        self._circuito_aberto = True
        self._avisar_canal(
            "🛑 <b>Auditor CornerPro pausado — sessão provavelmente expirou</b>\n\n"
            f"{self._falhas_auth_consecutivas} tentativas seguidas foram recusadas pela "
            "CornerPro (erro de login/sessão).\n\n"
            "<b>O que fazer:</b>\n"
            "1. Abra o navegador e entre normalmente no site da CornerPro.\n"
            "2. Abra as Ferramentas do Desenvolvedor (F12) → aba Application/Armazenamento → Cookies.\n"
            "3. Copie os valores novos de <code>PHPSESSID</code> e <code>token</code>.\n"
            "4. Atualize as variáveis <code>CORNERPRO_PHPSESSID</code> e <code>CORNERPRO_TOKEN</code> no Railway.\n"
            "5. Reinicie o serviço do auditor.\n\n"
            "A auditoria continua funcionando com as outras fontes (sem interrupção), "
            "só esta fonte específica está pausada até a sessão ser renovada."
        )


# (Removida a nota de "CornerPro fora da cascata" — ver bloco acima.)


# ============================================================================
# LEARNING ENGINE (V2 - NOVA CAMADA)
# ============================================================================

class LearningEngine:
    """
    Learning Engine - Identifica padrões estatísticos automaticamente.
    NUNCA altera decisões. Apenas registra conhecimento.
    """
    
    def __init__(self, db: 'AuditDatabaseV2' = None):
        self.db = db or AuditDatabaseV2()
        self._patterns = {}
        self._last_analysis = None
    
    def analyze(self, force: bool = False) -> Dict[str, Any]:
        """
        Executa análise de padrões e aprendizado.
        Retorna os padrões identificados.
        """
        # Verificar se já foi analisado recentemente
        if not force and self._last_analysis:
            time_since = (datetime.now() - self._last_analysis).total_seconds()
            if time_since < 3600:  # 1 hora
                return self._patterns
        
        self._last_analysis = datetime.now()
        
        # Executar todas as análises
        self._analyze_score_performance()
        self._analyze_league_performance()
        self._analyze_context_performance()
        self._analyze_pressure_conversion()
        self._analyze_operational_results()
        self._analyze_goal_side_patterns()
        self._analyze_favorite_performance()
        self._analyze_temporal_patterns()
        
        # Salvar padrões no banco
        self._save_patterns()
        
        return self._patterns
    
    def _analyze_score_performance(self):
        """Analisa performance por faixa de score"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Score 80-84
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE score_total BETWEEN 80 AND 84
                AND audited_at IS NOT NULL
            """)
            row = cursor.fetchone()
            if row and row[0] > 0:
                self._patterns['score_80_84'] = {
                    'total': row[0],
                    'greens': row[1] or 0,
                    'success_rate': round((row[1] or 0) / row[0] * 100, 2)
                }
            
            # Score 85-89
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE score_total BETWEEN 85 AND 89
                AND audited_at IS NOT NULL
            """)
            row = cursor.fetchone()
            if row and row[0] > 0:
                self._patterns['score_85_89'] = {
                    'total': row[0],
                    'greens': row[1] or 0,
                    'success_rate': round((row[1] or 0) / row[0] * 100, 2)
                }
            
            # Score 90+
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE score_total >= 90
                AND audited_at IS NOT NULL
            """)
            row = cursor.fetchone()
            if row and row[0] > 0:
                self._patterns['score_90_plus'] = {
                    'total': row[0],
                    'greens': row[1] or 0,
                    'success_rate': round((row[1] or 0) / row[0] * 100, 2)
                }
            
            # Score por IP
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN score_ip >= 24 THEN 'IP_ALTO'
                        WHEN score_ip >= 18 THEN 'IP_MEDIO'
                        ELSE 'IP_BAIXO'
                    END as ip_level,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE score_ip IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY ip_level
            """)
            self._patterns['ip_performance'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['ip_performance'].append({
                        'level': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
    
    def _analyze_league_performance(self):
        """Analisa performance por liga"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    league,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE league IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY league
                HAVING total >= 10
                ORDER BY (greens * 1.0 / total) DESC
                LIMIT 10
            """)
            self._patterns['top_leagues'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['top_leagues'].append({
                        'league': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
            
            cursor.execute("""
                SELECT 
                    league,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE league IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY league
                HAVING total >= 10
                ORDER BY (greens * 1.0 / total) ASC
                LIMIT 10
            """)
            self._patterns['worst_leagues'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['worst_leagues'].append({
                        'league': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
    
    def _analyze_context_performance(self):
        """Analisa performance por contexto do jogo"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    game_context,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE game_context IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY game_context
            """)
            self._patterns['context_performance'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['context_performance'].append({
                        'context': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
    
    def _analyze_pressure_conversion(self):
        """Analisa conversão de pressão"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    classification,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE classification IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY classification
            """)
            self._patterns['pressure_performance'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['pressure_performance'].append({
                        'classification': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
    
    def _analyze_operational_results(self):
        """Analisa distribuição de resultados operacionais"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    operational_result,
                    COUNT(*) as total
                FROM audit_matches
                WHERE operational_result IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY operational_result
                ORDER BY total DESC
            """)
            self._patterns['operational_distribution'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['operational_distribution'].append({
                        'result': row[0],
                        'total': row[1]
                    })
            
            # Calcular percentual de erros evitáveis
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN operational_result = 'RED_EVITAVEL' THEN 1 ELSE 0 END) as avoidable
                FROM audit_matches
                WHERE operational_result IS NOT NULL
                AND audited_at IS NOT NULL
                AND approved = 1
            """)
            row = cursor.fetchone()
            if row and row[0] > 0:
                self._patterns['avoidable_errors'] = {
                    'total': row[0],
                    'avoidable': row[1] or 0,
                    'avoidable_rate': round((row[1] or 0) / row[0] * 100, 2)
                }
    
    def _analyze_goal_side_patterns(self):
        """Analisa padrões do lado do gol"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    goal_side,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE goal_after_alert = 1
                AND goal_side IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY goal_side
            """)
            self._patterns['goal_side_patterns'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['goal_side_patterns'].append({
                        'side': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
    
    def _analyze_favorite_performance(self):
        """Analisa performance do favorito"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Performance geral do favorito
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    SUM(CASE WHEN goal_after_alert = 1 AND goal_is_favorite = 1 THEN 1 ELSE 0 END) as favorite_goals
                FROM audit_matches
                WHERE audited_at IS NOT NULL
            """)
            row = cursor.fetchone()
            if row and row[0] > 0:
                self._patterns['favorite_overall'] = {
                    'total': row[0],
                    'greens': row[1] or 0,
                    'success_rate': round((row[1] or 0) / row[0] * 100, 2),
                    'favorite_goals': row[2] or 0
                }
            
            # Favorito por odd
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN favorite_odd < 1.5 THEN 'ODD_BAIXA'
                        WHEN favorite_odd < 2.0 THEN 'ODD_MEDIA'
                        ELSE 'ODD_ALTA'
                    END as odd_level,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE favorite_odd IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY odd_level
            """)
            self._patterns['favorite_by_odd'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['favorite_by_odd'].append({
                        'odd_level': row[0],
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
    
    def _analyze_temporal_patterns(self):
        """Analisa padrões temporais (horário, dia da semana)"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Performance por hora do dia
            cursor.execute("""
                SELECT 
                    strftime('%H', match_time) as hour,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE match_time IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY hour
                HAVING total >= 5
                ORDER BY (greens * 1.0 / total) DESC
            """)
            self._patterns['best_hours'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['best_hours'].append({
                        'hour': f"{row[0]}:00",
                        'total': row[1],
                        'greens': row[2] or 0,
                        'success_rate': round((row[2] or 0) / row[1] * 100, 2)
                    })
            
            # Performance por dia da semana
            cursor.execute("""
                SELECT 
                    strftime('%w', match_date) as day_of_week,
                    CASE strftime('%w', match_date)
                        WHEN '0' THEN 'Domingo'
                        WHEN '1' THEN 'Segunda'
                        WHEN '2' THEN 'Terça'
                        WHEN '3' THEN 'Quarta'
                        WHEN '4' THEN 'Quinta'
                        WHEN '5' THEN 'Sexta'
                        WHEN '6' THEN 'Sábado'
                    END as day_name,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE match_date IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY day_of_week
                HAVING total >= 5
                ORDER BY (greens * 1.0 / total) DESC
            """)
            self._patterns['best_days'] = []
            for row in cursor.fetchall():
                if row[0] and row[1] > 0:
                    self._patterns['best_days'].append({
                        'day': row[1],
                        'total': row[2],
                        'greens': row[3] or 0,
                        'success_rate': round((row[3] or 0) / row[2] * 100, 2)
                    })
    
    def _save_patterns(self):
        """Salva padrões identificados no banco"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            for pattern_type, data in self._patterns.items():
                if isinstance(data, list):
                    for item in data:
                        # Criar chave única para o padrão
                        key = f"{pattern_type}_{item.get('league', item.get('context', item.get('classification', item.get('side', item.get('hour', item.get('day', 'unknown'))))))}"
                        if 'level' in item:
                            key = f"{pattern_type}_{item['level']}"
                        elif 'odd_level' in item:
                            key = f"{pattern_type}_{item['odd_level']}"
                        
                        cursor.execute("""
                            INSERT OR REPLACE INTO audit_learning (
                                learning_type, learning_key, learning_value, sample_size, 
                                confidence, metadata, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            pattern_type,
                            key,
                            item.get('success_rate', item.get('total', 0)),
                            item.get('total', 0),
                            min(1.0, item.get('total', 0) / 100),  # Confidence baseado no tamanho da amostra
                            json.dumps(item, ensure_ascii=False),
                            datetime.now().isoformat(),
                            datetime.now().isoformat()
                        ))
                else:
                    cursor.execute("""
                        INSERT OR REPLACE INTO audit_learning (
                            learning_type, learning_key, learning_value, sample_size, 
                            confidence, metadata, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        pattern_type,
                        pattern_type,
                        data.get('success_rate', data.get('total', 0)),
                        data.get('total', 0),
                        min(1.0, data.get('total', 0) / 100),
                        json.dumps(data, ensure_ascii=False),
                        datetime.now().isoformat(),
                        datetime.now().isoformat()
                    ))
            
            conn.commit()
    
    def get_pattern(self, pattern_type: str) -> Optional[Dict[str, Any]]:
        """Retorna um padrão específico"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_learning
                WHERE learning_type = ?
                ORDER BY confidence DESC
            """, (pattern_type,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows] if rows else None
    
    def get_all_patterns(self) -> Dict[str, Any]:
        """Retorna todos os padrões identificados"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_learning
                ORDER BY learning_type, confidence DESC
            """)
            patterns = defaultdict(list)
            for row in cursor.fetchall():
                patterns[row['learning_type']].append(dict(row))
            return dict(patterns)
    
    def generate_insights(self) -> List[str]:
        """Gera insights automáticos baseados nos padrões identificados"""
        insights = []
        
        # Analisar padrões de score
        score_patterns = self.get_pattern('score_80_84')
        if score_patterns and score_patterns[0].get('success_rate', 0) > 65:
            insights.append(f"Jogos com Score 80-84 apresentam taxa de acerto de {score_patterns[0]['success_rate']:.1f}%")
        
        score_patterns = self.get_pattern('score_85_89')
        if score_patterns and score_patterns[0].get('success_rate', 0) > 70:
            insights.append(f"Jogos com Score 85-89 apresentam taxa de acerto de {score_patterns[0]['success_rate']:.1f}%")
        
        # Analisar ligas
        top_leagues = self.get_pattern('top_leagues')
        if top_leagues and len(top_leagues) > 0:
            best = top_leagues[0]
            insights.append(f"Melhor liga: {best.get('metadata', {}).get('league', 'Unknown')} com {best.get('success_rate', 0):.1f}% de acerto")
        
        # Analisar contexto
        context_perf = self.get_pattern('context_performance')
        if context_perf:
            for ctx in context_perf:
                metadata = json.loads(ctx.get('metadata', '{}'))
                if metadata.get('success_rate', 0) > 65:
                    insights.append(f"Contexto {metadata.get('context', 'Unknown')}: {metadata.get('success_rate', 0):.1f}% de acerto ({metadata.get('total', 0)} jogos)")
        
        # Analisar pressão
        pressure_perf = self.get_pattern('pressure_performance')
        if pressure_perf:
            for p in pressure_perf:
                metadata = json.loads(p.get('metadata', '{}'))
                if metadata.get('classification') == 'PRESSAO_CONVERTIDA' and metadata.get('success_rate', 0) > 70:
                    insights.append(f"Pressão Convertida: {metadata.get('success_rate', 0):.1f}% de acerto")
        
        # Analisar erros evitáveis
        avoidable = self.get_pattern('avoidable_errors')
        if avoidable and avoidable[0].get('avoidable_rate', 0) > 30:
            insights.append(f"⚠️ {avoidable[0]['avoidable_rate']:.1f}% dos erros são evitáveis")
        
        return insights


# ============================================================================
# GERENCIADOR PRINCIPAL V2
# ============================================================================

class AuditManagerV2:
    """Gerencia todo o processo de auditoria V2"""
    
    def __init__(self, db: AuditDatabaseV2 = None):
        self.db = db or AuditDatabaseV2()
        self.providers = []
        self.learning = LearningEngine(db)
        self._init_providers()
        self._running = False
    
    def _init_providers(self):
        """Inicializa os providers na ordem de prioridade"""
        self.providers = []

        if 'cornerpro' in config.provider_priority:
            self.providers.append(CornerProProvider())

        for name in ['api1', 'api2', 'api3']:
            if name in config.provider_priority:
                self.providers.append(
                    APIProvider(
                        api_key=config.api_keys.get(name, ''),
                        api_url=config.api_urls.get(name),
                        name=name
                    )
                )
        
        if 'sofascore' in config.provider_priority:
            self.providers.append(SofaScoreProvider())
        
        if 'flashscore' in config.provider_priority:
            self.providers.append(FlashScoreProvider())
        
        self.providers.sort(key=lambda p: p.priority)
    
    def registrar_decisao_v2(self, snapshot: Dict[str, Any]) -> str:
        """
        Registra uma decisão do sistema principal (V2 - Snapshot Completo)
        """
        decision_id = snapshot.get('decision_id') or str(uuid.uuid4())
        snapshot['decision_id'] = decision_id
        
        # Salvar snapshot completo
        self.db.save_snapshot_v2(decision_id, snapshot)
        
        # Adicionar à fila
        self.db.add_to_queue(decision_id)
        
        self.db.log_info(
            f"Decisão V2 registrada: {decision_id}",
            {
                'decision_id': decision_id,
                'approved': snapshot.get('approved'),
                'score_total': snapshot.get('scores', {}).get('total'),
                'league': snapshot.get('league')
            }
        )
        
        return decision_id
    
    def processar_auditoria_v2(self, decision_id: str) -> bool:
        """Processa a auditoria de uma decisão (V2)"""
        try:
            match = self.db.get_match_by_decision_id(decision_id)
            
            if not match:
                raise ValueError(f"Decisão {decision_id} não encontrada")
            
            if match.get('audited_at') is not None:
                self.db.log_info(f"Decisão {decision_id} já auditada")
                return True
            
            # Buscar resultado real
            result = self._fetch_match_result(match)
            
            if not result or result.get('ft_score_home') is None:
                self.db.update_queue_status(decision_id, 'pending', "Aguardando resultado")
                return False
            
            # Classificar resultado (V2)
            classification = self._classify_result_v2(match, result)
            
            # Atualizar banco
            self.db.update_match_result_v2(decision_id, classification)
            
            # Atualizar fila
            self.db.update_queue_status(decision_id, 'completed')
            
            # Disparar aprendizado
            self.learning.analyze(force=False)
            
            self.db.log_info(
                f"Auditoria V2 concluída: {decision_id}",
                {
                    'decision_id': decision_id,
                    'classification': classification.get('classification'),
                    'operational_result': classification.get('operational_result'),
                    'green': classification.get('green'),
                    'source': classification.get('audit_source')
                }
            )
            
            return True
            
        except Exception as e:
            self.db.log_error(
                f"Erro ao processar auditoria V2 {decision_id}: {e}",
                {'decision_id': decision_id, 'error': str(e)}
            )
            self.db.update_queue_status(decision_id, 'failed', str(e))
            return False
    
    def _fetch_match_result(self, match: Dict[str, Any]) -> Dict[str, Any]:
        """Busca o resultado real da partida.

        CORRIGIDO (29/06): o pipeline do ALFA nunca chega aqui com um
        match_id/fixture_id nativo de algum provider — só tem nome dos
        times + data (vindo do CornerPro via Telegram). Antes de
        get_match_result, tenta resolver o ID certo via find_match_by_teams
        em cada provider; se não achar com segurança, pula esse provider
        e tenta o próximo, sem nunca arriscar confirmar resultado de jogo
        errado.

        ATUALIZADO (30/06): a CornerPro é tratada como caso especial — não
        precisa resolver nome (a URL já vem certa do próprio alerta, salva
        dentro do snapshot bruto como 'cornerpro_url') e recebe um contexto
        extra (minuto da decisão + horário de registro) pra calcular a
        janela de espera antes de tentar buscar o resultado.
        """
        match_id = match.get('match_id')
        fixture_id = match.get('fixture_id')
        home_team = match.get('home_team', '')
        away_team = match.get('away_team', '')
        match_date = match.get('match_date', '')

        try:
            snapshot_bruto = json.loads(match.get('snapshot_raw') or '{}')
        except Exception:
            snapshot_bruto = {}
        cornerpro_url = snapshot_bruto.get('cornerpro_url') or ''

        for provider in self.providers:
            try:
                if provider.provider_name == "cornerpro":
                    if not cornerpro_url:
                        continue  # sem link salvo no alerta original — pula pro próximo provider
                    match_context = {
                        "decision_id": match.get('decision_id', ''),
                        "decision_minute": match.get('decision_minute'),
                        "created_at": match.get('created_at'),
                        "cornerpro_url": cornerpro_url,
                    }
                    result = provider.get_match_result(cornerpro_url, match_context=match_context)
                    if result and result.get('ft_score_home') is not None:
                        result['source'] = provider.provider_name
                        return result
                    continue  # 'aguardando_fim_jogo'/'auth'/'not_finished' etc. — cascata segue

                id_resolvido = fixture_id or match_id
                if not id_resolvido and home_team and away_team:
                    id_resolvido = provider.find_match_by_teams(home_team, away_team, match_date)
                    if not id_resolvido:
                        continue  # esse provider não achou o jogo com segurança — tenta o próximo

                result = provider.get_match_result(id_resolvido or '', fixture_id)
                
                if result and result.get('ft_score_home') is not None:
                    result['source'] = provider.provider_name
                    return result
            except Exception as e:
                self.db.log_error(
                    f"Provider {provider.provider_name} falhou: {e}",
                    {'match_id': match_id}
                )
                continue
        
        return None
    
    def _classify_result_v2(self, match: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """Classifica o resultado da partida (V2)"""
        
        # Resultados do jogo
        ft_home = result.get('ft_score_home', 0)
        ft_away = result.get('ft_score_away', 0)
        ht_home = result.get('ht_score_home', 0)
        ht_away = result.get('ht_score_away', 0)
        
        # Dados da decisão
        approved = bool(match.get('approved', False))
        decision_minute = match.get('decision_minute', 0)
        
        try:
            snapshot = json.loads(match.get('snapshot_raw', '{}'))
            scores = json.loads(match.get('score_components', '{}'))
        except:
            snapshot = {}
            scores = {}
        
        # Contexto
        favorite = snapshot.get('favorite', 'home')
        pressure_team = snapshot.get('pressure_team', 'home')
        game_context = match.get('game_context', 'EQUILIBRADO')
        
        # Verificar gols após alerta
        goals_after_alert = []
        for goal in result.get('goals', []):
            goal_minute = goal.get('minute', '0')
            try:
                if '+' in str(goal_minute):
                    minute_parts = str(goal_minute).split('+')
                    minute = int(minute_parts[0])
                else:
                    minute = int(goal_minute)
            except:
                minute = 0
            
            if minute > decision_minute:
                goals_after_alert.append({
                    'minute': goal_minute,
                    'team': goal.get('team', 'unknown'),
                    'player': goal.get('player', '')
                })
        
        goal_after_alert = len(goals_after_alert) > 0
        goal_team = None
        goal_minute = None
        
        if goal_after_alert:
            first_goal = goals_after_alert[0]
            goal_team = first_goal.get('team')
            goal_minute = first_goal.get('minute')
        
        # ===== CLASSIFICAÇÃO DE CONTEXTO (V2) =====
        classification = ClassificationType.SEM_GOL.value
        
        if goal_after_alert:
            if goal_team == pressure_team:
                # Pressão convertida
                classification = ClassificationType.PRESSAO_CONVERTIDA.value
            else:
                classification = ClassificationType.GOL_CONTRA_FLUXO.value
        
        # Classificação detalhada
        classification_detailed = classification
        if classification == ClassificationType.PRESSAO_CONVERTIDA.value:
            # Subclassificações de pressão
            score_total = match.get('score_total', 0)
            if score_total >= 90:
                classification_detailed = ClassificationType.PRESSAO_PREMIADA.value
            elif score_total >= 85:
                classification_detailed = ClassificationType.PRESSAO_CONTINUOU.value
            elif score_total >= 80:
                classification_detailed = ClassificationType.PRESSAO_MORREU.value
            else:
                classification_detailed = ClassificationType.PRESSAO_FALSA.value
        
        # ===== LADO DO GOL (V2) =====
        goal_side = None
        if goal_team:
            if goal_team == favorite:
                goal_side = 'FAVORITO'
            elif goal_team == pressure_team:
                goal_side = 'PRESSIONANTE'
            else:
                goal_side = 'ZEBRA'
        
        # ===== RESULTADO OPERACIONAL (V2) =====
        operational_result = None
        
        if approved and goal_after_alert:
            # GREEN
            if goal_team == pressure_team:
                operational_result = OperationalResult.GREEN_EXCELENTE.value
            else:
                operational_result = OperationalResult.GREEN_SOFRIDO.value
        elif approved and not goal_after_alert:
            # RED
            score_total = match.get('score_total', 0)
            if score_total >= 85:
                operational_result = OperationalResult.RED_EVITAVEL.value
            elif score_total >= 80:
                operational_result = OperationalResult.RED_JUSTIFICAVEL.value
            else:
                operational_result = OperationalResult.RED_BOBO.value
        elif not approved and goal_after_alert:
            # GREEN Perdido
            if goal_team == pressure_team:
                operational_result = OperationalResult.GREEN_PERDIDO_EXCELENTE.value
            else:
                operational_result = OperationalResult.GREEN_PERDIDO_NORMAL.value
        elif not approved and not goal_after_alert:
            # RED Evitado
            score_total = match.get('score_total', 0)
            if score_total >= 85:
                operational_result = OperationalResult.RED_EVITADO_JUSTIFICAVEL.value
            else:
                operational_result = OperationalResult.RED_EVITADO_AFORTUNADO.value
        
        # GREEN or RED
        green = False
        if approved:
            green = goal_after_alert and goal_team == pressure_team
        else:
            green = goal_after_alert and goal_team == pressure_team
        
        goal_is_favorite = goal_team == favorite if goal_team else False
        goal_is_pressure = goal_team == pressure_team if goal_team else False
        
        return {
            'ft_score_home': ft_home,
            'ft_score_away': ft_away,
            'ht_score_home': ht_home,
            'ht_score_away': ht_away,
            'goal_after_alert': goal_after_alert,
            'goal_team': goal_team,
            'goal_minute': goal_minute,
            'goal_is_favorite': goal_is_favorite,
            'goal_is_pressure': goal_is_pressure,
            'goal_side': goal_side,
            'classification': classification,
            'classification_detailed': classification_detailed,
            'operational_result': operational_result,
            'green': green,
            'shots_home': result.get('shots_home', 0),
            'shots_away': result.get('shots_away', 0),
            'shots_on_target_home': result.get('shots_on_target_home', 0),
            'shots_on_target_away': result.get('shots_on_target_away', 0),
            'corners_home': result.get('corners_home', 0),
            'corners_away': result.get('corners_away', 0),
            'possession_home': result.get('possession_home', 0),
            'possession_away': result.get('possession_away', 0),
            'xg_home': result.get('xg_home', 0.0),
            'xg_away': result.get('xg_away', 0.0),
            'attacks_home': result.get('attacks_home', 0),
            'attacks_away': result.get('attacks_away', 0),
            'dangerous_attacks_home': result.get('dangerous_attacks_home', 0),
            'dangerous_attacks_away': result.get('dangerous_attacks_away', 0),
            'yellow_cards_home': result.get('yellow_cards_home', 0),
            'yellow_cards_away': result.get('yellow_cards_away', 0),
            'red_cards_home': result.get('red_cards_home', 0),
            'red_cards_away': result.get('red_cards_away', 0),
            'audit_source': result.get('source', 'unknown')
        }
    
    def processar_fila(self, limit: int = None) -> int:
        """Processa todos os itens da fila"""
        limit = limit or config.queue_batch_size
        items = self.db.get_unprocessed_queue(limit)
        
        processed = 0
        for item in items:
            if self.processar_auditoria_v2(item['decision_id']):
                processed += 1
            time.sleep(0.5)
        
        return processed
    
    def start(self):
        """Inicia o processamento contínuo"""
        self._running = True
        self.db.log_info("Auditor V2 iniciado", {'status': 'running'})
    
    def stop(self):
        """Para o processamento contínuo"""
        self._running = False
        self.db.log_info("Auditor V2 parado", {'status': 'stopped'})


# ============================================================================
# RELATÓRIOS INTELIGENTES V2
# ============================================================================

class AuditReportsV2:
    """Gera relatórios inteligentes com conclusões automáticas (V2)"""
    
    def __init__(self, db: AuditDatabaseV2 = None, learning: LearningEngine = None):
        self.db = db or AuditDatabaseV2()
        self.learning = learning or LearningEngine(db)
    
    def generate_daily_report_v2(self, date: datetime = None) -> Tuple[str, List[str]]:
        """Gera relatório diário com conclusões automáticas"""
        if date is None:
            date = datetime.now()
        
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        # Estatísticas do dia
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN approved = 1 THEN 1 ELSE 0 END) as approved,
                    SUM(CASE WHEN approved = 0 THEN 1 ELSE 0 END) as rejected,
                    SUM(CASE WHEN green = 1 AND approved = 1 THEN 1 ELSE 0 END) as greens,
                    SUM(CASE WHEN green = 0 AND approved = 1 THEN 1 ELSE 0 END) as reds,
                    SUM(CASE WHEN green = 1 AND approved = 0 THEN 1 ELSE 0 END) as greens_lost,
                    SUM(CASE WHEN green = 0 AND approved = 0 THEN 1 ELSE 0 END) as reds_avoided,
                    AVG(score_total) as avg_score
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at < ?
            """, (start_date.isoformat(), end_date.isoformat()))
            
            row = cursor.fetchone()
            total = row[0] or 0
            greens = row[3] or 0
            success_rate = (greens / total * 100) if total > 0 else 0
        
        # Gerar conclusões automáticas
        conclusions = self._generate_daily_conclusions(total, success_rate, date)
        
        report = f"""
╔══════════════════════════════════════════════════════════════╗
║              RELATÓRIO DIÁRIO INTELIGENTE V2               ║
║                    {date.strftime('%d/%m/%Y')}                         ║
╚══════════════════════════════════════════════════════════════╝

📊 RESUMO DO DIA
───────────────────────────────────────────────────────────────
  Total de Jogos Auditados: {total}
  Taxa de Acerto: {success_rate:.1f}%
  Score Médio: {row[7] or 0:.1f}

✅ DECISÕES APROVADAS
───────────────────────────────────────────────────────────────
  Aprovados: {row[1] or 0}
  └─ GREEN: {row[3] or 0}
  └─ RED: {row[4] or 0}

❌ DECISÕES REPROVADAS
───────────────────────────────────────────────────────────────
  Reprovados: {row[2] or 0}
  └─ GREEN Perdido: {row[5] or 0}
  └─ RED Evitado: {row[6] or 0}

📈 ANÁLISE DE CONTEXTO
───────────────────────────────────────────────────────────────
"""
        
        # Contextos do dia
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    classification,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at < ?
                AND classification IS NOT NULL
                GROUP BY classification
            """, (start_date.isoformat(), end_date.isoformat()))
            
            for row in cursor.fetchall():
                if row[0]:
                    rate = (row[2] or 0) / row[1] * 100 if row[1] > 0 else 0
                    report += f"  {row[0]}: {row[2] or 0}/{row[1]} ({rate:.1f}%)\n"
        
        # Adicionar conclusões
        if conclusions:
            report += "\n💡 CONCLUSÕES AUTOMÁTICAS\n───────────────────────────────────────────────────────────────\n"
            for i, conclusion in enumerate(conclusions[:5], 1):
                report += f"  {i}. {conclusion}\n"
        
        report += f"""
───────────────────────────────────────────────────────────────
📅 Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}
"""
        
        return report, conclusions
    
    def _generate_daily_conclusions(self, total: int, success_rate: float, date: datetime) -> List[str]:
        """Gera conclusões automáticas para o relatório diário"""
        conclusions = []
        
        # BUG CORRIGIDO (29/06): start_date/end_date eram usados mais abaixo
        # sem nunca terem sido definidos nesta função — isso quebraria o
        # relatório diário com NameError toda vez que tentasse achar o
        # melhor contexto do dia.
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        # Performance do dia
        if total > 0:
            if success_rate > 65:
                conclusions.append(f"✅ Desempenho acima da média: {success_rate:.1f}% de acerto")
            elif success_rate > 50:
                conclusions.append(f"📊 Desempenho dentro da média: {success_rate:.1f}% de acerto")
            else:
                conclusions.append(f"⚠️ Desempenho abaixo da média: {success_rate:.1f}% de acerto")
        
        # Comparação com período anterior
        prev_date = date - timedelta(days=1)
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at < ?
            """, (prev_date.replace(hour=0, minute=0, second=0).isoformat(), 
                  prev_date.replace(hour=23, minute=59, second=59).isoformat()))
            row = cursor.fetchone()
            prev_total = row[0] or 0
            prev_greens = row[1] or 0
            prev_rate = (prev_greens / prev_total * 100) if prev_total > 0 else 0
            
            if prev_total > 0 and total > 0:
                diff = success_rate - prev_rate
                if diff > 5:
                    conclusions.append(f"📈 Melhora de {diff:.1f}% em relação ao dia anterior")
                elif diff < -5:
                    conclusions.append(f"📉 Queda de {abs(diff):.1f}% em relação ao dia anterior")
        
        # Melhor contexto do dia
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    classification,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at < ?
                AND classification IS NOT NULL
                GROUP BY classification
                HAVING total >= 3
                ORDER BY (greens * 1.0 / total) DESC
                LIMIT 1
            """, (start_date.isoformat(), end_date.isoformat()))
            row = cursor.fetchone()
            if row:
                rate = (row[2] or 0) / row[1] * 100 if row[1] > 0 else 0
                conclusions.append(f"🏆 Melhor contexto: {row[0]} ({rate:.1f}% de acerto)")
        
        return conclusions
    
    def generate_weekly_report_v2(self) -> Tuple[str, List[str]]:
        """Gera relatório semanal inteligente"""
        today = datetime.now()
        start_date = today - timedelta(days=7)
        
        report = f"""
╔══════════════════════════════════════════════════════════════╗
║             RELATÓRIO SEMANAL INTELIGENTE V2               ║
║              {start_date.strftime('%d/%m')} - {today.strftime('%d/%m/%Y')}              ║
╚══════════════════════════════════════════════════════════════╝

📊 RESUMO DA SEMANA
───────────────────────────────────────────────────────────────
"""
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    AVG(score_total) as avg_score,
                    SUM(CASE WHEN classification = 'PRESSAO_CONVERTIDA' THEN 1 ELSE 0 END) as pressure_conv,
                    SUM(CASE WHEN classification = 'GOL_CONTRA_FLUXO' THEN 1 ELSE 0 END) as counter_goal,
                    SUM(CASE WHEN operational_result = 'GREEN_EXCELENTE' THEN 1 ELSE 0 END) as green_excelente
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
            """, (start_date.isoformat(), today.isoformat()))
            
            row = cursor.fetchone()
            total = row[0] or 0
            greens = row[1] or 0
            success_rate = (greens / total * 100) if total > 0 else 0
            
            report += f"""
  Total de Jogos: {total}
  Taxa de Acerto: {success_rate:.1f}%
  Score Médio: {row[2] or 0:.1f}
  
  📈 Indicadores de Qualidade:
  ├─ Pressão Convertida: {row[3] or 0}
  ├─ Gol Contra o Fluxo: {row[4] or 0}
  └─ GREEN Excelente: {row[5] or 0}

📊 EVOLUÇÃO DIÁRIA
───────────────────────────────────────────────────────────────
"""
            
            # Evolução diária
            for i in range(7):
                day = today - timedelta(days=i)
                day_start = day.replace(hour=0, minute=0, second=0)
                day_end = day_start + timedelta(days=1)
                
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                    FROM audit_matches
                    WHERE audited_at >= ? AND audited_at < ?
                """, (day_start.isoformat(), day_end.isoformat()))
                
                row = cursor.fetchone()
                day_total = row[0] or 0
                day_greens = row[1] or 0
                day_rate = (day_greens / day_total * 100) if day_total > 0 else 0
                
                bar = '█' * int(day_rate / 5) + '░' * (20 - int(day_rate / 5))
                report += f"  {day.strftime('%a')} {bar} {day_rate:.1f}% ({day_greens}/{day_total})\n"
        
        # Gerar conclusões
        conclusions = self._generate_weekly_conclusions(total, success_rate, start_date, today)
        
        if conclusions:
            report += "\n💡 CONCLUSÕES AUTOMÁTICAS\n───────────────────────────────────────────────────────────────\n"
            for i, conclusion in enumerate(conclusions, 1):
                report += f"  {i}. {conclusion}\n"
        
        report += f"""
───────────────────────────────────────────────────────────────
📅 Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}
"""
        
        return report, conclusions

    def generate_biweekly_report_v2(self) -> Tuple[str, List[str]]:
        """Gera relatório quinzenal (30/06) — mesma estrutura do semanal,
        janela de 15 dias em vez de 7. Pedido explícito do operador:
        fechamento diário, semanal, quinzenal e mensal, todos com o mesmo
        nível de detalhe (aprovados/reprovados, green/red, evolução)."""
        today = datetime.now()
        start_date = today - timedelta(days=15)

        report = f"""
╔══════════════════════════════════════════════════════════════╗
║            RELATÓRIO QUINZENAL INTELIGENTE V2               ║
║              {start_date.strftime('%d/%m')} - {today.strftime('%d/%m/%Y')}              ║
╚══════════════════════════════════════════════════════════════╝

📊 RESUMO DA QUINZENA
───────────────────────────────────────────────────────────────
"""

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    AVG(score_total) as avg_score,
                    SUM(CASE WHEN classification = 'PRESSAO_CONVERTIDA' THEN 1 ELSE 0 END) as pressure_conv,
                    SUM(CASE WHEN classification = 'GOL_CONTRA_FLUXO' THEN 1 ELSE 0 END) as counter_goal,
                    SUM(CASE WHEN operational_result = 'GREEN_EXCELENTE' THEN 1 ELSE 0 END) as green_excelente,
                    SUM(CASE WHEN approved = 1 THEN 1 ELSE 0 END) as aprovados,
                    SUM(CASE WHEN approved = 0 THEN 1 ELSE 0 END) as reprovados
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
            """, (start_date.isoformat(), today.isoformat()))

            row = cursor.fetchone()
            total = row[0] or 0
            greens = row[1] or 0
            success_rate = (greens / total * 100) if total > 0 else 0

            report += f"""
  Total Auditado: {total}
  Aprovados: {row[6] or 0} | Reprovados: {row[7] or 0}
  Taxa de Acerto (dos auditados): {success_rate:.1f}%
  Score Médio: {row[2] or 0:.1f}

  📈 Indicadores de Qualidade:
  ├─ Pressão Convertida: {row[3] or 0}
  ├─ Gol Contra o Fluxo: {row[4] or 0}
  └─ GREEN Excelente: {row[5] or 0}

📊 EVOLUÇÃO POR SEMANA (dentro da quinzena)
───────────────────────────────────────────────────────────────
"""
            for semana in range(2):
                semana_fim = today - timedelta(days=7 * semana)
                semana_inicio = semana_fim - timedelta(days=7)
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                    FROM audit_matches
                    WHERE audited_at >= ? AND audited_at < ?
                """, (semana_inicio.isoformat(), semana_fim.isoformat()))
                r = cursor.fetchone()
                s_total = r[0] or 0
                s_greens = r[1] or 0
                s_rate = (s_greens / s_total * 100) if s_total > 0 else 0
                bar2 = '█' * int(s_rate / 5) + '░' * (20 - int(s_rate / 5))
                rotulo = "Semana mais recente" if semana == 0 else "Semana anterior"
                report += f"  {rotulo:<20} {bar2} {s_rate:.1f}% ({s_greens}/{s_total})\n"

        conclusions_quinzenal = self._generate_weekly_conclusions(total, success_rate, start_date, today)

        if conclusions_quinzenal:
            report += "\n💡 CONCLUSÕES AUTOMÁTICAS\n───────────────────────────────────────────────────────────────\n"
            for i, conclusion in enumerate(conclusions_quinzenal, 1):
                report += f"  {i}. {conclusion}\n"

        report += f"""
───────────────────────────────────────────────────────────────
📅 Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}
"""

        return report, conclusions_quinzenal
    
    def _generate_weekly_conclusions(self, total: int, success_rate: float, start_date: datetime, end_date: datetime) -> List[str]:
        """Gera conclusões automáticas para o relatório semanal"""
        conclusions = []
        
        # Performance geral
        if total > 0:
            if success_rate > 60:
                conclusions.append(f"✅ Semana positiva: {success_rate:.1f}% de acerto")
            elif success_rate > 50:
                conclusions.append(f"📊 Semana dentro da média: {success_rate:.1f}% de acerto")
            else:
                conclusions.append(f"⚠️ Semana negativa: {success_rate:.1f}% de acerto")
        
        # Comparação com semana anterior
        prev_start = start_date - timedelta(days=7)
        prev_end = start_date - timedelta(days=1)
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
            """, (prev_start.isoformat(), prev_end.isoformat()))
            
            row = cursor.fetchone()
            prev_total = row[0] or 0
            prev_greens = row[1] or 0
            prev_rate = (prev_greens / prev_total * 100) if prev_total > 0 else 0
            
            if prev_total > 0 and total > 0:
                diff = success_rate - prev_rate
                if diff > 3:
                    conclusions.append(f"📈 Evolução positiva: +{diff:.1f}% em relação à semana anterior")
                elif diff < -3:
                    conclusions.append(f"📉 Queda de {abs(diff):.1f}% em relação à semana anterior")
        
        # Melhor score da semana
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    score_total,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
                AND score_total IS NOT NULL
                GROUP BY score_total
                HAVING total >= 3
                ORDER BY (greens * 1.0 / total) DESC
                LIMIT 1
            """, (start_date.isoformat(), end_date.isoformat()))
            
            row = cursor.fetchone()
            if row:
                rate = (row[2] or 0) / row[1] * 100 if row[1] > 0 else 0
                conclusions.append(f"🎯 Score de destaque: {row[0]} ({rate:.1f}% de acerto)")
        
        # Contexto dominante
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    classification,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
                AND classification IS NOT NULL
                GROUP BY classification
                HAVING total >= 5
                ORDER BY (greens * 1.0 / total) DESC
                LIMIT 1
            """, (start_date.isoformat(), end_date.isoformat()))
            
            row = cursor.fetchone()
            if row:
                rate = (row[2] or 0) / row[1] * 100 if row[1] > 0 else 0
                conclusions.append(f"🏆 Contexto dominante: {row[0]} ({rate:.1f}% de acerto)")
        
        return conclusions
    
    def generate_monthly_report_v2(self) -> Tuple[str, List[str]]:
        """Gera relatório mensal inteligente"""
        today = datetime.now()
        start_date = today.replace(day=1)
        
        report = f"""
╔══════════════════════════════════════════════════════════════╗
║             RELATÓRIO MENSAL INTELIGENTE V2                ║
║                {start_date.strftime('%B %Y')}                      ║
╚══════════════════════════════════════════════════════════════╝

📈 INDICADORES MENSAIS
───────────────────────────────────────────────────────────────
"""
        
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    SUM(CASE WHEN green = 0 AND approved = 1 THEN 1 ELSE 0 END) as reds,
                    SUM(CASE WHEN green = 1 AND approved = 0 THEN 1 ELSE 0 END) as greens_lost,
                    SUM(CASE WHEN green = 0 AND approved = 0 THEN 1 ELSE 0 END) as reds_avoided,
                    AVG(score_total) as avg_score,
                    SUM(CASE WHEN classification = 'PRESSAO_CONVERTIDA' THEN 1 ELSE 0 END) as pressure_conv
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
            """, (start_date.isoformat(), today.isoformat()))
            
            row = cursor.fetchone()
            total = row[0] or 0
            greens = row[1] or 0
            success_rate = (greens / total * 100) if total > 0 else 0
            
            report += f"""
  Total de Jogos: {total}
  Taxa de Acerto: {success_rate:.1f}%
  Score Médio: {row[5] or 0:.1f}
  Pressão Convertida: {row[6] or 0}

✅ RESULTADOS
───────────────────────────────────────────────────────────────
  GREEN: {greens}
  RED: {row[2] or 0}
  GREEN Perdido: {row[3] or 0}
  RED Evitado: {row[4] or 0}

📊 RANKING DE CONTEXTOS
───────────────────────────────────────────────────────────────
"""
            
            cursor.execute("""
                SELECT 
                    classification,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE audited_at >= ? AND audited_at <= ?
                AND classification IS NOT NULL
                GROUP BY classification
                HAVING total >= 5
                ORDER BY success_rate DESC
            """, (start_date.isoformat(), today.isoformat()))
            
            for row in cursor.fetchall():
                if row[0]:
                    report += f"  {row[0]}: {row[2]}/{row[1]} ({row[3]:.1f}%)\n"
        
        # Análise de aprendizado
        insights = self.learning.generate_insights()
        
        if insights:
            report += "\n💡 INSIGHTS DO APRENDIZADO\n───────────────────────────────────────────────────────────────\n"
            for i, insight in enumerate(insights[:5], 1):
                report += f"  {i}. {insight}\n"
        
        report += f"""
───────────────────────────────────────────────────────────────
📅 Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}
"""
        
        return report, insights


# ============================================================================
# TELEGRAM V2
# ============================================================================

class AuditTelegramV2:
    """Envia relatórios inteligentes via Telegram (V2)"""
    
    def __init__(self):
        self.bot_token = config.telegram_bot_token
        self.chat_id = config.telegram_chat_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.enabled = bool(self.bot_token and self.chat_id)
    
    def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        if not self.enabled:
            print("⚠️ Telegram não configurado")
            return False
        
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': parse_mode
            }
            
            response = requests.post(url, json=payload, timeout=config.request_timeout)
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"❌ Erro ao enviar mensagem: {e}")
            return False
    
    def send_report(self, report: str, conclusions: List[str] = None, title: str = None) -> bool:
        """Envia um relatório com conclusões"""
        if title:
            message = f"<b>{title}</b>\n\n<pre>{report}</pre>"
        else:
            message = f"<pre>{report}</pre>"
        
        if len(message) > 4096:
            parts = []
            current_part = ""
            for line in report.split('\n'):
                if len(current_part) + len(line) + 1 > 3800:
                    parts.append(current_part)
                    current_part = line + '\n'
                else:
                    current_part += line + '\n'
            if current_part:
                parts.append(current_part)
            
            success = True
            for i, part in enumerate(parts):
                if title and i == 0:
                    msg = f"<b>{title}</b>\n\n<pre>{part}</pre>"
                else:
                    msg = f"<pre>{part}</pre>"
                if not self.send_message(msg):
                    success = False
            return success
        
        return self.send_message(message)
    
    def send_insights(self, insights: List[str]) -> bool:
        """Envia insights gerados pelo Learning Engine"""
        if not insights:
            return True
        
        message = "💡 <b>INSIGHTS DO SISTEMA</b>\n\n"
        for i, insight in enumerate(insights, 1):
            message += f"{i}. {insight}\n"
        
        return self.send_message(message)
    
    def send_startup_message(self) -> bool:
        msg = f"""
🤖 <b>COUTIPS AUDITOR V2</b> iniciado!

📅 Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}
📊 Status: <b>ATIVO</b>

🧠 <b>NOVAS FUNCIONALIDADES V2:</b>
  • Snapshot completo com todos os scores
  • Contexto do jogo (favorito, pressão, domínio)
  • 9 classificações de contexto expandidas
  • 10 resultados operacionais detalhados
  • Learning Engine com análise de padrões
  • Relatórios inteligentes com conclusões automáticas
  • Consultas avançadas por score, contexto e padrões

📈 Relatórios programados:
  • Diário: 23:00
  • Semanal: Segunda 06:00
  • Mensal: Dia 1
"""
        return self.send_message(msg)


# ============================================================================
# SCHEDULER V2
# ============================================================================

class AuditSchedulerV2:
    """Executa auditorias e relatórios inteligentes (V2)"""
    
    def __init__(self):
        self.db = AuditDatabaseV2()
        self.manager = AuditManagerV2(self.db)
        self.learning = LearningEngine(self.db)
        self.reports = AuditReportsV2(self.db, self.learning)
        self.telegram = AuditTelegramV2()
        self._running = False
        self._thread = None
        self._last_processed = datetime.now()
    
    def start(self):
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        
        self.db.log_info("Scheduler V2 iniciado", {'status': 'running'})
        self.telegram.send_startup_message()
    
    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self.db.log_info("Scheduler V2 parado", {'status': 'stopped'})
    
    def _run(self):
        last_daily = None
        last_weekly = None
        last_biweekly = None
        last_monthly = None
        last_learning = None
        last_queue = datetime.now()
        
        while self._running:
            now = datetime.now()
            
            # Processar fila a cada 5 minutos
            if (now - last_queue).seconds >= 300:
                self._process_queue()
                last_queue = now
            
            # Learning Engine - análise diária às 02:00
            if now.hour == 2 and now.minute == 0:
                if last_learning is None or last_learning.date() != now.date():
                    self._run_learning()
                    last_learning = now
            
            # Relatório diário às 23:30 — pedido explícito do operador (30/06):
            # fechamento do dia com aprovados/reprovados e green/red.
            if now.hour == 23 and now.minute == 30:
                if last_daily is None or last_daily.date() != now.date():
                    self._send_daily_report()
                    last_daily = now
            
            # Relatório semanal às 06:00 de Segunda-feira
            if now.weekday() == 0 and now.hour == 6 and now.minute == 0:
                if last_weekly is None or (now - last_weekly).days >= 7:
                    self._send_weekly_report()
                    last_weekly = now

            # Relatório quinzenal (30/06) — a cada 15 dias corridos, às 06:30,
            # sem depender de dia da semana específico (diferente do semanal).
            if now.hour == 6 and now.minute == 30:
                if last_biweekly is None or (now - last_biweekly).days >= 15:
                    self._send_biweekly_report()
                    last_biweekly = now
            
            # Relatório mensal no dia 1 às 08:00
            if now.day == 1 and now.hour == 8 and now.minute == 0:
                if last_monthly is None or last_monthly.month != now.month:
                    self._send_monthly_report()
                    last_monthly = now
            
            time.sleep(30)
    
    def _process_queue(self):
        try:
            processed = self.manager.processar_fila()
            if processed > 0:
                self.db.log_info(f"Processados {processed} itens da fila", {'count': processed})
        except Exception as e:
            self.db.log_error(f"Erro ao processar fila: {e}")
    
    def _run_learning(self):
        try:
            patterns = self.learning.analyze(force=True)
            insights = self.learning.generate_insights()
            
            if insights:
                self.telegram.send_insights(insights)
                self.db.log_info("Learning Engine atualizado", {'insights': len(insights)})
        except Exception as e:
            self.db.log_error(f"Erro no Learning Engine: {e}")
    
    def _send_daily_report(self):
        try:
            report, conclusions = self.reports.generate_daily_report_v2()
            self.db.save_report('daily_v2', report)
            self.telegram.send_report(report, conclusions, "📊 RELATÓRIO DIÁRIO V2")
            
            if conclusions:
                self.telegram.send_insights(conclusions)
            
            self.db.log_info("Relatório diário V2 enviado")
        except Exception as e:
            self.db.log_error(f"Erro ao enviar relatório diário V2: {e}")
    
    def _send_weekly_report(self):
        try:
            report, conclusions = self.reports.generate_weekly_report_v2()
            self.db.save_report('weekly_v2', report)
            self.telegram.send_report(report, conclusions, "📊 RELATÓRIO SEMANAL V2")
            self.db.log_info("Relatório semanal V2 enviado")
        except Exception as e:
            self.db.log_error(f"Erro ao enviar relatório semanal V2: {e}")

    def _send_biweekly_report(self):
        try:
            report, conclusions = self.reports.generate_biweekly_report_v2()
            self.db.save_report('biweekly_v2', report)
            self.telegram.send_report(report, conclusions, "📊 RELATÓRIO QUINZENAL V2")
            self.db.log_info("Relatório quinzenal V2 enviado")
        except Exception as e:
            self.db.log_error(f"Erro ao enviar relatório quinzenal V2: {e}")
    
    def _send_monthly_report(self):
        try:
            report, insights = self.reports.generate_monthly_report_v2()
            self.db.save_report('monthly_v2', report)
            self.telegram.send_report(report, insights, "📊 RELATÓRIO MENSAL V2")
            self.db.log_info("Relatório mensal V2 enviado")
        except Exception as e:
            self.db.log_error(f"Erro ao enviar relatório mensal V2: {e}")


# ============================================================================
# CONSULTAS AVANÇADAS V2
# ============================================================================

class AuditQueriesV2:
    """Módulo de consultas avançadas V2"""
    
    def __init__(self, db: AuditDatabaseV2 = None, learning: LearningEngine = None):
        self.db = db or AuditDatabaseV2()
        self.learning = learning or LearningEngine(db)
    
    # ===== CONSULTAS V1 (mantidas) =====
    
    def get_all_greens(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_matches 
                WHERE green = 1 
                ORDER BY audited_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_reds(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_matches 
                WHERE green = 0 AND approved = 1
                ORDER BY audited_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_greens_lost(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_matches 
                WHERE green = 1 AND approved = 0
                ORDER BY audited_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_reds_avoided(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM audit_matches 
                WHERE green = 0 AND approved = 0
                ORDER BY audited_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    # ===== CONSULTAS AVANÇADAS V2 =====
    
    def query_performance_by_score_range(self, min_score: float, max_score: float) -> Dict[str, Any]:
        """Performance por faixa de score"""
        return self.db.query_performance_by_score_range(min_score, max_score)
    
    def query_performance_by_context(self, context: str) -> Dict[str, Any]:
        """Performance por contexto do jogo"""
        return self.db.query_performance_by_context(context)
    
    def query_pressure_conversion_by_score(self, min_score: float = None) -> List[Dict[str, Any]]:
        """Conversão de pressão por faixa de score"""
        return self.db.query_pressure_conversion_by_score(min_score)
    
    def query_favorite_performance(self) -> Dict[str, Any]:
        """Performance do favorito em diferentes situações"""
        return self.db.query_favorite_performance()
    
    def query_goal_side_analysis(self) -> List[Dict[str, Any]]:
        """Análise do lado do gol"""
        return self.db.query_goal_side_analysis()
    
    def query_operational_result_analysis(self) -> List[Dict[str, Any]]:
        """Análise de resultados operacionais"""
        return self.db.query_operational_result_analysis()
    
    def query_best_score_range(self) -> Dict[str, Any]:
        """Identifica a melhor faixa de score"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN score_total < 80 THEN '< 80'
                        WHEN score_total < 85 THEN '80-84'
                        WHEN score_total < 90 THEN '85-89'
                        ELSE '90+'
                    END as score_range,
                    COUNT(*) as total,
                    SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) as greens,
                    ROUND(SUM(CASE WHEN green = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
                FROM audit_matches
                WHERE score_total IS NOT NULL
                AND audited_at IS NOT NULL
                GROUP BY score_range
                HAVING total >= 10
                ORDER BY success_rate DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def query_avoidable_errors_analysis(self) -> Dict[str, Any]:
        """Análise de erros evitáveis"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN operational_result = 'RED_EVITAVEL' THEN 1 ELSE 0 END) as avoidable,
                    SUM(CASE WHEN operational_result = 'RED_EVITAVEL' AND score_total > 85 THEN 1 ELSE 0 END) as high_score_avoidable
                FROM audit_matches
                WHERE approved = 1
                AND audited_at IS NOT NULL
            """)
            row = cursor.fetchone()
            
            return {
                'total': row[0] or 0,
                'avoidable': row[1] or 0,
                'avoidable_rate': round((row[1] or 0) / (row[0] or 1) * 100, 2),
                'high_score_avoidable': row[2] or 0,
                'high_score_avoidable_rate': round((row[2] or 0) / (row[0] or 1) * 100, 2)
            }
    
    def query_learning_patterns(self, pattern_type: str = None) -> Dict[str, Any]:
        """Retorna padrões identificados pelo Learning Engine"""
        if pattern_type:
            patterns = self.learning.get_pattern(pattern_type)
            return {pattern_type: patterns} if patterns else {}
        else:
            return self.learning.get_all_patterns()
    
    def query_insights(self) -> List[str]:
        """Retorna insights gerados pelo Learning Engine"""
        return self.learning.generate_insights()
    
    def get_stats_summary_v2(self) -> Dict[str, Any]:
        """Resumo estatístico completo V2"""
        return self.db.get_stats_summary_v2()


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

# ============================================================================
# RESUMO DIÁRIO SOB DEMANDA (30/06) — usado pelo comando /status_auditor
# ============================================================================

def formatar_resumo_diario_auditor(manager: "AuditManagerV2") -> str:
    """Monta a mensagem de status do auditor pra responder, a qualquer
    momento do dia, "como está indo a auditoria hoje?". Pensado pra
    acompanhamento de perto enquanto o CornerProProvider ainda é novo —
    nenhuma decisão do ALFA depende deste comando, é só visibilidade."""
    resumo = manager.db.get_resumo_hoje()

    cornerpro = next((p for p in manager.providers if p.provider_name == "cornerpro"), None)
    if cornerpro is None:
        linha_cornerpro = "CornerPro: não está na cascata (provider_priority sem 'cornerpro')"
    elif not cornerpro._sessao_configurada():
        linha_cornerpro = "CornerPro: ⚠️ cookies não configurados (CORNERPRO_PHPSESSID/CORNERPRO_TOKEN vazios)"
    elif cornerpro._circuito_aberto:
        linha_cornerpro = "CornerPro: 🛑 PAUSADO (sessão provavelmente expirada — renovar cookies)"
    elif cornerpro._falhas_auth_consecutivas > 0:
        linha_cornerpro = f"CornerPro: 🟡 ativo, com {cornerpro._falhas_auth_consecutivas} falha(s) de autenticação recente(s)"
    else:
        linha_cornerpro = "CornerPro: ✅ ativo, sem falhas registradas"

    pct_green = (
        round(resumo["greens_hoje"] / (resumo["greens_hoje"] + resumo["reds_hoje"]) * 100, 1)
        if (resumo["greens_hoje"] + resumo["reds_hoje"]) > 0 else None
    )
    linha_green = (
        f"Green hoje: {resumo['greens_hoje']} | Red hoje: {resumo['reds_hoje']}"
        + (f" ({pct_green}% green)" if pct_green is not None else "")
    )

    return (
        "🧠 <b>STATUS DO AUDITOR — HOJE</b>\n\n"
        f"Alertas registrados hoje: {resumo['registrados_hoje']}\n"
        f"Já auditados (resultado confirmado): {resumo['auditados_hoje']}\n"
        f"Aprovados hoje: {resumo['aprovados_hoje']} | Reprovados hoje: {resumo['reprovados_hoje']}\n"
        f"{linha_green}\n\n"
        f"Esperando resultado na fila: {resumo['pendentes_na_fila']}\n"
        f"Com falha na fila: {resumo['falhados_na_fila']}\n\n"
        f"{linha_cornerpro}\n\n"
        "<i>Auditoria roda continuamente o dia todo (fila checada a cada 5 min) — "
        "não é num horário fixo só. O relatório completo diário sai às 23:00.</i>"
    )


def main():
    """Função principal do sistema V2"""
    
    print("=" * 70)
    print("        🧠 COUTIPS AUDITOR V2 - Sistema de Inteligência")
    print("=" * 70)
    print()
    
    # Inicializar componentes
    db = AuditDatabaseV2()
    manager = AuditManagerV2(db)
    learning = LearningEngine(db)
    scheduler = AuditSchedulerV2()
    queries = AuditQueriesV2(db, learning)
    
    # Verificar configuração
    print("📋 VERIFICANDO CONFIGURAÇÃO:")
    print(f"  • Banco de dados: {config.db_path}")
    print(f"  • Telegram: {'✅ Configurado' if config.telegram_bot_token else '❌ Não configurado'}")
    print(f"  • Providers: {len(manager.providers)}")
    for p in manager.providers:
        print(f"    - {p.provider_name} (prioridade {p.priority})")
    print()
    
    # Iniciar scheduler
    print("📅 Iniciando scheduler V2...")
    scheduler.start()
    
    print()
    print("✅ SISTEMA DE INTELIGÊNCIA V2 ATIVO!")
    print()
    print("🧠 NOVIDADES DO V2:")
    print("  1. Snapshot completo com todos os scores")
    print("  2. Contexto do jogo (favorito, pressão, domínio)")
    print("  3. 9 classificações de contexto expandidas")
    print("  4. 10 resultados operacionais detalhados")
    print("  5. Learning Engine - análise automática de padrões")
    print("  6. Relatórios inteligentes com conclusões automáticas")
    print("  7. Consultas avançadas por score, contexto e padrões")
    print()
    
    # Mostrar exemplo de uso
    print("📝 EXEMPLO DE REGISTRO DE DECISÃO V2:")
    print("   from coutips_auditor_v2 import AuditManagerV2")
    print("   auditor = AuditManagerV2()")
    print("   snapshot = {")
    print("       'scores': {")
    print("           'total': 87.5,")
    print("           'base': 45.0,")
    print("           'pressure': 22.5,")
    print("           'ip': 26.0,")
    print("           'chance_gol': 8.5,")
    print("           'shots': 7.5,")
    print("           'rb': 4.0,")
    print("           'contexto': 6.0,")
    print("           'favoritismo': 2.5")
    print("       },")
    print("       'context': {")
    print("           'favorite_team': 'home',")
    print("           'favorite_odd': 1.28,")
    print("           'pressure_team': 'home',")
    print("           'game_context': 'FAVORITO_VENCENDO'")
    print("       }")
    print("   })")
    print("   decision_id = auditor.registrar_decisao_v2(snapshot)")
    print()
    
    # Menu interativo
    while True:
        print("\n" + "=" * 70)
        print("MENU PRINCIPAL V2")
        print("=" * 70)
        print("  1. Registrar decisão (exemplo)")
        print("  2. Processar fila")
        print("  3. Ver estatísticas V2")
        print("  4. Ver análise por contexto")
        print("  5. Ver análise por score")
        print("  6. Ver padrões do Learning Engine")
        print("  7. Ver insights")
        print("  8. Ver erros evitáveis")
        print("  9. Exportar dados para CSV")
        print(" 10. Sair")
        print()
        
        try:
            option = input("Escolha uma opção: ").strip()
            
            if option == '1':
                # Registrar decisão de exemplo
                snapshot = {
                    'fixture_id': '123456',
                    'match_id': 'exemplo_v2_001',
                    'league': 'Premier League',
                    'country': 'Inglaterra',
                    'season': '2024/25',
                    'home_team': 'Manchester City',
                    'away_team': 'Arsenal',
                    'match_date': datetime.now().strftime('%Y-%m-%d'),
                    'match_time': datetime.now().strftime('%H:%M'),
                    'market': '1x2',
                    'period': 'FT',
                    'decision_type': 'live',
                    'approved': True,
                    'decision_minute': 56,
                    'decision_reasons': ['Pressão alta', 'Posse de bola'],
                    'scores': {
                        'total': 87.5,
                        'base': 45.0,
                        'pressure': 22.5,
                        'ip': 26.0,
                        'chance_gol': 8.5,
                        'shots': 7.5,
                        'rb': 4.0,
                        'contexto': 6.0,
                        'favoritismo': 2.5,
                        'penalties': [],
                        'bonuses': [],
                        'locks': [],
                        'components': {
                            'attacks': 12.0,
                            'dangerous_attacks': 10.5,
                            'possession': 8.0,
                            'xg': 6.0
                        }
                    },
                    'context': {
                        'favorite_team': 'home',
                        'favorite_odd': 1.28,
                        'pressure_team': 'home',
                        'territorial_dominance': 'home',
                        'best_opportunities': 'home',
                        'offensive_intensity': 'high',
                        'emotional_situation': 'calm',
                        'game_context': 'FAVORITO_VENCENDO',
                        'pressure_type': 'PRESSIONANTE'
                    },
                    'odds': {'home': 1.28, 'draw': 6.00, 'away': 6.00}
                }
                
                decision_id = manager.registrar_decisao_v2(snapshot)
                print(f"✅ Decisão V2 registrada: {decision_id}")
                
            elif option == '2':
                processed = manager.processar_fila()
                print(f"✅ Processados {processed} itens da fila")
                
            elif option == '3':
                stats = queries.get_stats_summary_v2()
                print("\n📊 ESTATÍSTICAS V2:")
                print(f"  Total de jogos: {stats.get('total', 0)}")
                print(f"  Auditados: {stats.get('audited', 0)}")
                print(f"  Taxa de acerto: {stats.get('success_rate', 0):.1f}%")
                print(f"  Score médio: {stats.get('avg_score', 0):.1f}")
                
                if 'by_context' in stats:
                    print("\n  Por contexto:")
                    for ctx, count in stats['by_context'].items():
                        print(f"    {ctx}: {count}")
                
                if 'by_classification' in stats:
                    print("\n  Por classificação:")
                    for cls, count in stats['by_classification'].items():
                        print(f"    {cls}: {count}")
                
                if 'by_operational_result' in stats:
                    print("\n  Por resultado operacional:")
                    for result, count in stats['by_operational_result'].items():
                        print(f"    {result}: {count}")
                
            elif option == '4':
                print("\n📊 ANÁLISE POR CONTEXTO:")
                contexts = ['FAVORITO_VENCENDO', 'FAVORITO_EMPATANDO', 'FAVORITO_PERDENDO', 
                           'ZEBRA_VENCENDO', 'ZEBRA_EMPATANDO', 'ZEBRA_PERDENDO', 'EQUILIBRADO']
                
                for ctx in contexts:
                    perf = queries.query_performance_by_context(ctx)
                    if perf.get('total', 0) > 0:
                        print(f"  {ctx}: {perf['greens']}/{perf['total']} ({perf['success_rate']:.1f}%)")
                
            elif option == '5':
                print("\n📊 ANÁLISE POR SCORE:")
                ranges = [
                    ('< 80', 0, 79),
                    ('80-84', 80, 84),
                    ('85-89', 85, 89),
                    ('90+', 90, 999)
                ]
                
                for label, min_s, max_s in ranges:
                    perf = queries.query_performance_by_score_range(min_s, max_s)
                    if perf.get('total', 0) > 0:
                        print(f"  {label}: {perf['greens']}/{perf['total']} ({perf['success_rate']:.1f}%)")
                
            elif option == '6':
                patterns = queries.query_learning_patterns()
                print("\n🧠 PADRÕES IDENTIFICADOS:")
                
                for pattern_type, data in patterns.items():
                    print(f"\n  {pattern_type}:")
                    if isinstance(data, list):
                        for item in data[:5]:
                            metadata = json.loads(item.get('metadata', '{}'))
                            if 'league' in metadata:
                                print(f"    - {metadata.get('league')}: {metadata.get('success_rate', 0):.1f}% ({metadata.get('total', 0)} jogos)")
                            elif 'context' in metadata:
                                print(f"    - {metadata.get('context')}: {metadata.get('success_rate', 0):.1f}% ({metadata.get('total', 0)} jogos)")
                            elif 'classification' in metadata:
                                print(f"    - {metadata.get('classification')}: {metadata.get('success_rate', 0):.1f}% ({metadata.get('total', 0)} jogos)")
                            else:
                                print(f"    - {item.get('learning_key')}: {item.get('learning_value')} (n={item.get('sample_size')})")
                
            elif option == '7':
                insights = queries.query_insights()
                print("\n💡 INSIGHTS DO SISTEMA:")
                for i, insight in enumerate(insights, 1):
                    print(f"  {i}. {insight}")
                
            elif option == '8':
                avoidable = queries.query_avoidable_errors_analysis()
                print("\n🛑 ANÁLISE DE ERROS EVITÁVEIS:")
                print(f"  Total de REDs: {avoidable['total']}")
                print(f"  Erros evitáveis: {avoidable['avoidable']} ({avoidable['avoidable_rate']:.1f}%)")
                print(f"  Erros evitáveis com score alto: {avoidable['high_score_avoidable']} ({avoidable['high_score_avoidable_rate']:.1f}%)")
                
            elif option == '9':
                filename = f"audit_export_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                import csv
                
                with db.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM audit_matches ORDER BY id DESC")
                    columns = [description[0] for description in cursor.description]
                    
                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(columns)
                        
                        for row in cursor.fetchall():
                            writer.writerow(row)
                
                print(f"✅ Dados exportados para {filename}")
                
            elif option == '10':
                print("\n👋 Encerrando o sistema V2...")
                scheduler.stop()
                print("✅ Sistema encerrado!")
                break
                
            else:
                print("❌ Opção inválida")
                
        except KeyboardInterrupt:
            print("\n\n👋 Encerrando o sistema V2...")
            scheduler.stop()
            print("✅ Sistema encerrado!")
            break
        except Exception as e:
            print(f"❌ Erro: {e}")


# ============================================================================
# PONTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    main()
