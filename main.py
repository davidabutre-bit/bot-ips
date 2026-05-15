"""
BOT COUTIPS – Pipeline CornerPro → OpenAI → Telegram (Modo Teste)
Recebe alertas do CornerPro, envia para OpenAI calcular score contextual COUTIPS/ALFA,
recebe resultado, adiciona cabeçalho e envia para canal de teste @ALFA_CON.
"""

import os
import json
import hashlib
from telethon import TelegramClient, events
import openai

# ------------------------
# VARIÁVEIS DE AMBIENTE
# ------------------------
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHANNEL_CONFIRMACAO = os.getenv("TARGET_CHANNEL_CONFIRMACAO")  # canal de teste

# Cortes e cooldown
CORTE_GOL_HT = int(os.getenv("CORTE_GOL_HT", "87"))
CORTE_GOL_FT = int(os.getenv("CORTE_GOL_FT", "82"))
CORTE_CONFIRMACAO_GOL_HT = int(os.getenv("CORTE_CONFIRMACAO_GOL_HT", "82"))
CORTE_CONFIRMACAO_GOL_FT = int(os.getenv("CORTE_CONFIRMACAO_GOL_FT", "82"))
CORTE_OBSERVACAO_JANELA = int(os.getenv("CORTE_OBSERVACAO_JANELA", "80"))
COOLDOWN_SEGUNDOS = int(os.getenv("COOLDOWN_SEGUNDOS", "600"))
CACHE_MAX_SEGUNDOS = int(os.getenv("CACHE_MAX_SEGUNDOS", "3600"))

# ------------------------
# INICIALIZAÇÃO TELEGRAM
# ------------------------
client = TelegramClient("coutips_v2_session", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ------------------------
# INICIALIZAÇÃO OPENAI
# ------------------------
openai.api_key = OPENAI_API_KEY

# ------------------------
# ANTI-DUPLICIDADE
# ------------------------
mensagens_processadas = set()

def gerar_hash_mensagem(texto):
    return hashlib.md5(texto.encode("utf-8")).hexdigest()

# ------------------------
# FUNÇÃO DE CÁLCULO CONTEXTUAL COM OPENAI
# ------------------------
async def calcular_score_contextual(alerta):
    """
    Recebe alerta CornerPro, envia para OpenAI e retorna JSON:
    score (0-100), chance (%) e decisão ENTRA/NÃO ENTRA, observações
    """
    prompt = f"""
    Você é o motor contextual COUTIPS/ALFA. Recebi o seguinte alerta:
    {alerta}

    Analise de acordo com regras oficiais COUTIPS/ALFA:
    1. Pressão convertível e sustentada
    2. IP e ataques recentes
    3. Remates e remates à baliza
    4. Gol recente e continuidade pós-gol
    5. Score contextual (0-100)
    6. Chance Coutips de Gol (%)
    7. Entrada: ENTRA ou NÃO ENTRA
    8. Observações: gol recente, lado do favorito, pressão, continuidade

    Retorne **apenas JSON** com: score, chance, decisao, observacoes
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        resposta_texto = response["choices"][0]["message"]["content"]
        resultado = json.loads(resposta_texto)
    except Exception as e:
        resultado = {
            "score": 0,
            "chance": 0,
            "decisao": "NÃO ENTRA",
            "observacoes": str(e)
        }

    return resultado

# ------------------------
# LISTENER PARA ALERTAS DO CORNERPRO
# ------------------------
@client.on(events.NewMessage(chats="@GrupoPonteCornerPro"))  # substitua pelo seu grupo ponte
async def listener(event):
    alerta = event.message.message
    hash_alerta = gerar_hash_mensagem(alerta)

    if hash_alerta in mensagens_processadas:
        return
    mensagens_processadas.add(hash_alerta)

    resultado = await calcular_score_contextual(alerta)

    # Monta mensagem final
    mensagem_final = (
        f"🟡 COUTIPS ALERTA (MODO TESTE)\n"
        f"Score: {resultado['score']} / 100\n"
        f"Chance Coutips: {resultado['chance']}%\n"
        f"Decisão: {resultado['decisao']}\n"
        f"Observações: {resultado['observacoes']}"
    )

    # ------------------------
    # ENVIO SOMENTE PARA CANAL DE TESTE
    # ------------------------
    await client.send_message(TARGET_CHANNEL_CONFIRMACAO, mensagem_final)

# ------------------------
# INICIAR BOT
# ------------------------
print("Bot Python COUTIPS ativo (modo teste). Escutando alertas CornerPro...")
client.run_until_disconnected()
