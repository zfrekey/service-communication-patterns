"""Código compartilhado: conexão, declaração das filas e mensagem padrão.

Producer e consumer declaram as filas pelo mesmo `declarar_fila`. Redeclarar uma
fila existente com argumentos diferentes fecha o canal com PRECONDITION_FAILED.
"""
import json
import os
import random

import pika

TIPOS = ["pix", "cartao", "boleto"]

# Argumentos de cada fila usada fora do roteamento. Todas clássicas e duráveis.
FILAS = {
    "trabalho": {},                            # balanceamento e ack
    "duravel": {},                             # persistente x transiente
    "prioridade": {"x-max-priority": 10},      # prioridade só existe em fila clássica com esse argumento
}


def conectar():
    # Padrão: broker local. Em duas máquinas: RABBITMQ_HOST=<ip do broker> e o usuário criado no compose.
    credenciais = pika.PlainCredentials(os.environ.get("RABBITMQ_USER", "guest"),
                                        os.environ.get("RABBITMQ_PASS", "guest"))
    return pika.BlockingConnection(pika.ConnectionParameters(os.environ.get("RABBITMQ_HOST", "localhost"),
                                                             credentials=credenciais))


def declarar_fila(canal, nome):
    # Fila fora de FILAS (ex.: "nao-existe" no teste de publisher confirm) não é declarada.
    if nome in FILAS:
        canal.queue_declare(queue=nome, durable=True, arguments=FILAS[nome] or None)


def pagamento(i):
    return {"id": i, "valor": round(random.uniform(10, 500), 2), "tipo": TIPOS[i % len(TIPOS)]}


def ler_tudo(canal, fila):
    """Esvazia a fila com basic_get (o "poll" da aula: nunca bloqueia) e devolve os corpos."""
    corpos = []
    while True:
        method, _, corpo = canal.basic_get(queue=fila, auto_ack=True)
        if method is None:
            return corpos
        corpos.append(json.loads(corpo))


def preparar_roteamento(canal, exchange, tipo, consumidores="BCD"):
    """Declara a exchange e as filas `<tipo>.B`, `<tipo>.C`... já vazias. Devolve os nomes das filas."""
    canal.exchange_declare(exchange=exchange, exchange_type=tipo, durable=True)
    filas = {}
    for c in consumidores:
        nome = f"{tipo}.{c}"
        canal.queue_declare(queue=nome, durable=True)
        canal.queue_purge(queue=nome)
        filas[c] = nome
    return filas


def imprimir_matriz(canal, filas, rotulos, titulo="Mensagem"):
    """Lê cada fila até esvaziar e imprime "mensagem x fila que recebeu"."""
    recebidas = {c: {m["rotulo"] for m in ler_tudo(canal, fila)} for c, fila in filas.items()}
    largura = max(len(titulo), *(len(r) for r in rotulos))
    print()
    print(f"{titulo:<{largura}}  " + "  ".join(f"{c:^3}" for c in filas))
    for r in rotulos:
        marcas = "  ".join(f"{'✔' if r in recebidas[c] else '':^3}" for c in filas)
        print(f"{r:<{largura}}  {marcas}")
