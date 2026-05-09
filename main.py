from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import os
import re
import time

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
TARGET_CHANNEL = "@CoutipsIPS"

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID ou API_HASH não configurado.")

API_ID = int(API_ID)

client = TelegramClient("coutips_ips_session", API_ID, API_HASH)

# =========================================================
# NOVA ESTRUTURA OFICIAL COUTIPS
# =========================================================
# HT_PREMIUM   = antigo ARCE HT
# HT_MODERADO  = antigo ColtHT / IPS HT
# FT_PREMIUM   = antigo CHAMA 3.0
# FT_MODERADO  = antigo Coltips Pós-70 / IPS FT
# =========================================================

CORTES = {
    "HT_PREMIUM": 75,
    "HT_MODERADO": 73,
    "FT_PREMIUM": 78,
    "FT_MODERADO": 76,
}

BOTS_PREMIUM = ["HT_PREMIUM", "FT_PREMIUM"]
BOTS_MODERADOS = ["HT_MODERADO", "FT_MODERADO"]

PRIORIDADE = {
    "HT_PREMIUM": ["HT_MODERADO"],
    "FT_PREMIUM": ["FT_MODERADO"],
}

COOLDOWN_SEGUNDOS = 600
CACHE_MAX_SEGUNDOS = 3600

ultimos_jogos = {}


# =========================================================
# FUNÇÕES BASE
# =========================================================

def normalizar(texto):
    return texto.replace("*", "").replace("_", "").replace("**", "").strip()


def limpar_linha(texto):
    return re.sub(r"\s+", " ", texto).strip()


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


def extrair_link_bet365(texto):
    m = re.search(r"https?://(?:www\.)?bet365[^\s*]+", texto, re.IGNORECASE)
    if m:
        return m.group(0).strip()
    return ""


# =========================================================
# DETECÇÃO NOVA DOS BOTS
# =========================================================

def detectar_estrategia(texto):
    t = texto.upper()

    # Premium HT — antigo ARCE HT
    if "HT_PREMIUM" in t or "ARCE" in t:
        return "HT_PREMIUM"

    # Moderado HT — antigo ColtHT / IPS HT
    if "HT_MODERADO" in t or "COLTHT" in t or "IPS HT" in t:
        return "HT_MODERADO"

    # Premium FT — antigo CHAMA 3.0
    if "FT_PREMIUM" in t or "CHAMA" in t:
        return "FT_PREMIUM"

    # Moderado FT — antigo Coltips Pós-70 / IPS FT
    if "FT_MODERADO" in t or "IPS FT" in t or "PÓS-70" in t or "POS-70" in t:
        return "FT_MODERADO"

    # Segurança: se o texto for claramente HT
    if "1ºT" in t or "1T" in t or "INTERVALO" in t:
        return "HT_MODERADO"

    # Padrão final
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

    ultimos5 = pegar_par(r"Ultimos 5':\s*(\d+).*?-\s*(\d+)", texto_limpo)
    ultimos10 = pegar_par(r"Ultimos 10':\s*(\d+).*?-\s*(\d+)", texto_limpo)

    ultimo_gol = pegar_numero(r"Último golo:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Ultimo golo:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Último gol:\s*(\d+)", texto_limpo, 0)
    if ultimo_gol == 0:
        ultimo_gol = pegar_numero(r"Ultimo gol:\s*(\d+)", texto_limpo, 0)

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
    posse_casa, posse_fora = metricas["posse"]
    ataques_casa, ataques_fora = metricas["ataques"]

    ap_total = ap_casa + ap_fora
    rb_total = rb_casa + rb_fora
    cantos_total = cantos_casa + cantos_fora
    ataques_total = ataques_casa + ataques_fora

    score = 0

    if ap_total >= 18:
        score += 4
    if ap_total >= 25:
        score += 4
    if ap_total >= 35:
        score += 4

    if abs(ap_casa - ap_fora) >= 8:
        score += 5
    if abs(ap_casa - ap_fora) >= 15:
        score += 5

    if ataques_total >= 70:
        score += 3
    if ataques_total >= 90:
        score += 3

    if rb_total >= 3:
        score += 5
    if rb_total >= 5:
        score += 5

    if abs(rb_casa - rb_fora) >= 3:
        score += 4

    if cantos_total >= 5:
        score += 4
    if cantos_total >= 8:
        score += 3

    if abs(cantos_casa - cantos_fora) >= 3:
        score += 3

    if abs(posse_casa - posse_fora) >= 12:
        score += 3

    return score


def momentum_score(metricas):
    u5_casa, u5_fora = metricas["ultimos5"]
    u10_casa, u10_fora = metricas["ultimos10"]

    u5_total = u5_casa + u5_fora
    u10_total = u10_casa + u10_fora

    score = 0

    if u5_total >= 3:
        score += 4
    if u5_total >= 5:
        score += 4

    if u10_total >= 7:
        score += 5
    if u10_total >= 10:
        score += 4

    if abs(u10_casa - u10_fora) >= 4:
        score += 4

    if abs(u5_casa - u5_fora) >= 3:
        score += 3

    return score


def relogio_score(metricas, estrategia):
    tempo = metricas["tempo"]

    if estrategia in ["HT_PREMIUM", "HT_MODERADO"]:
        if 25 <= tempo <= 35:
            return 9
        if 36 <= tempo <= 42:
            return 5
        if 18 <= tempo <= 24:
            return 2
        if tempo < 18:
            return -10
        return -4

    if estrategia in ["FT_PREMIUM", "FT_MODERADO"]:
        if 68 <= tempo <= 76:
            return 9
        if 77 <= tempo <= 80:
            return 4
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

    penalidade = 0

    if ap_total >= 25 and rb_total <= 1:
        penalidade -= 10

    if ap_total >= 18 and rb_total == 0:
        penalidade -= 12

    if ap_total >= 25 and remates_lado_total <= 2 and rb_total <= 1:
        penalidade -= 6

    if u10_total <= 3 and u5_total <= 1:
        penalidade -= 7

    return penalidade


def score_por_tipo_bot(metricas, estrategia):
    score = 50

    score += dominio_score(metricas)
    score += momentum_score(metricas)
    score += relogio_score(metricas, estrategia)
    score += risco_gol_recente(metricas)
    score += fake_pressure_penalty(metricas)

    fav = favorito_score(metricas)

    if estrategia == "HT_PREMIUM":
        score += fav
        score += 6

    elif estrategia == "HT_MODERADO":
        score += int(fav * 0.7)
        score += 3

    elif estrategia == "FT_PREMIUM":
        score += int(fav * 0.6)
        score += 6

    elif estrategia == "FT_MODERADO":
        score += int(fav * 0.4)
        score += 2

    return score


def ajuste_confianca(metricas, estrategia, score):
    rb_total = sum(metricas["remates_baliza"])
    ap_total = sum(metricas["ataques_perigosos"])
    u5_total = sum(metricas["ultimos5"])
    u10_total = sum(metricas["ultimos10"])

    # Reduz score inflado sem finalização
    if score >= 88 and rb_total <= 1:
        score -= 8

    # FT precisa ser mais rígido porque o relógio pesa
    if estrategia in ["FT_PREMIUM", "FT_MODERADO"]:
        if metricas["tempo"] >= 81 and u5_total <= 2:
            score -= 8
        if rb_total == 0:
            score -= 7

    # Premium precisa de contexto mais limpo
    if estrategia in ["HT_PREMIUM", "FT_PREMIUM"]:
        if ap_total < 18:
            score -= 5
        if u10_total < 6:
            score -= 5

    # Moderado pode passar com volume, mas não com jogo morto
    if estrategia in ["HT_MODERADO", "FT_MODERADO"]:
        if u10_total <= 3:
            score -= 6

    return score


def calcular_score(texto, estrategia):
    metricas = extrair_metricas(texto)

    score = score_por_tipo_bot(metricas, estrategia)
    score = ajuste_confianca(metricas, estrategia, score)

    score = int(max(0, min(score, 95)))

    return score, metricas


# =========================================================
# ANTI-DUPLICIDADE / PRIORIDADE
# =========================================================

def limpar_cache():
    agora = time.time()
    for chave in list(ultimos_jogos.keys()):
        if agora - ultimos_jogos[chave] > CACHE_MAX_SEGUNDOS:
            del ultimos_jogos[chave]


def verificar_prioridade(jogo, estrategia):
    limpar_cache()
    agora = time.time()

    chave_atual = f"{jogo}_{estrategia}"

    if chave_atual in ultimos_jogos:
        if agora - ultimos_jogos[chave_atual] <= COOLDOWN_SEGUNDOS:
            return False, "Bloqueado por duplicidade do mesmo bot"

    # Se for moderado e já houve premium recente no mesmo jogo, bloqueia
    for bot_premium, bots_bloqueados in PRIORIDADE.items():
        if estrategia in bots_bloqueados:
            chave_premium = f"{jogo}_{bot_premium}"
            if chave_premium in ultimos_jogos:
                if agora - ultimos_jogos[chave_premium] <= COOLDOWN_SEGUNDOS:
                    return False, f"Bloqueado por prioridade: {bot_premium}"

    # Se for premium, ele pode passar mesmo se moderado já apareceu antes.
    # Isso preserva a prioridade do sinal mais forte.
    ultimos_jogos[chave_atual] = agora

    return True, "OK"


# =========================================================
# CLASSIFICAÇÃO
# =========================================================

def faixa_score(score, estrategia):
    if estrategia == "HT_PREMIUM":
        if score >= 89:
            return "ELITE HT"
        if score >= 82:
            return "PREMIUM HT"
        return "BOA HT"

    if estrategia == "HT_MODERADO":
        if score >= 88:
            return "ELITE HT"
        if score >= 81:
            return "PREMIUM HT"
        return "BOA HT"

    if estrategia == "FT_PREMIUM":
        if score >= 92:
            return "DIAMANTE / ELITE"
        if score >= 85:
            return "PREMIUM FT"
        return "BOA FT"

    if estrategia == "FT_MODERADO":
        if score >= 90:
            return "ELITE FT"
        if score >= 84:
            return "PREMIUM FT"
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
# HANDLER PRINCIPAL
# =========================================================

@client.on(events.NewMessage)
async def handler(event):
    if event.out:
        return

    texto = event.raw_text or ""

    if not texto.strip():
        return

    if not mensagem_valida(texto):
        print("ℹ️ Mensagem ignorada: não parece alerta CornerPro.")
        return

    try:
        print("\n📩 NOVA MENSAGEM RECEBIDA:")
        print(texto)

        estrategia = detectar_estrategia(texto)
        jogo = extrair_jogo(texto)

        score, metricas = calcular_score(texto, estrategia)
        corte = CORTES.get(estrategia, 76)

        print(f"🎯 Estratégia detectada: {estrategia}")
        print(f"⚽ Jogo: {jogo}")
        print(f"⏱ Tempo: {metricas['tempo']}'")
        print(f"📊 Placar: {metricas['placar']}")
        print(f"🎯 Mercado calculado: {metricas['mercado']}")
        print(f"🔥 Score calculado: {score}%")
        print(f"📌 Corte mínimo: {corte}%")

        permitido, motivo = verificar_prioridade(jogo, estrategia)

        if not permitido:
            print(f"⛔ BLOQUEADO: {motivo}")
            return

        if score < corte:
            print("⛔ BLOQUEADO: score abaixo do corte")
            return

        mensagem_final = montar_mensagem(jogo, estrategia, score, metricas)

        try:
            await client.send_message(TARGET_CHANNEL, mensagem_final)
            print("✅ MENSAGEM ENVIADA PARA O CANAL")

        except FloodWaitError as e:
            print(f"⛔ FLOOD WAIT: aguardar {e.seconds} segundos.")

        except Exception as e:
            print(f"❌ ERRO AO ENVIAR MENSAGEM: {e}")

    except Exception as e:
        print(f"❌ ERRO NO PROCESSAMENTO DO ALERTA: {e}")


print("🚀 COUTIPS ONLINE - SCORE CONTEXTUAL ATIVO")
print("📊 Estratégias ativas: HT_PREMIUM | HT_MODERADO | FT_PREMIUM | FT_MODERADO")
print(f"📡 Canal destino: {TARGET_CHANNEL}")

client.start()

print("✅ TELEGRAM CONECTADO COM SUCESSO")

client.run_until_disconnected()