"""Headers exchange: roteia pelos headers da mensagem, não pela chave.

B exige todos os pares (x-match: all), C aceita qualquer um (x-match: any).
Pares do binding: tipo=pix, alto=sim.

Uso (de dentro de rabbitmq/): python -m roteamento.headers
"""
import json

import pika

from comum import conectar, imprimir_matriz, preparar_roteamento

EXCHANGE = "pagamentos.headers"
PARES = {"tipo": "pix", "alto": "sim"}
BINDINGS = {"B": "all", "C": "any"}
MENSAGENS = [
    {"tipo": "pix", "alto": "sim"},
    {"tipo": "pix", "alto": "nao"},
    {"tipo": "cartao", "alto": "sim"},
    {"tipo": "boleto", "alto": "nao"},
]


def main():
    conexao = conectar()
    canal = conexao.channel()
    filas = preparar_roteamento(canal, EXCHANGE, "headers", consumidores="BC")

    for c, modo in BINDINGS.items():
        canal.queue_bind(queue=filas[c], exchange=EXCHANGE, arguments={"x-match": modo, **PARES})
        print(f"[bind] {EXCHANGE} --x-match={modo} {PARES}--> {filas[c]}")

    rotulos = []
    for headers in MENSAGENS:
        rotulo = ", ".join(f"{k}={v}" for k, v in headers.items())
        rotulos.append(rotulo)
        canal.basic_publish(exchange=EXCHANGE, routing_key="", body=json.dumps({"rotulo": rotulo}),
                            properties=pika.BasicProperties(headers=headers))
        print(f"[A] publicou com headers {rotulo}")

    imprimir_matriz(canal, filas, rotulos, titulo="Headers")
    conexao.close()


if __name__ == "__main__":
    main()
