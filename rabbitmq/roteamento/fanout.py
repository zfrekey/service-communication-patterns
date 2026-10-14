"""Fanout exchange: entrega para todas as filas ligadas e ignora a chave.

B, C e D ligados sem chave. As duas mensagens saem com chave "qualquer-coisa"
e chegam nas três filas.

Uso (de dentro de rabbitmq/): python -m roteamento.fanout
"""
import json

from comum import conectar, imprimir_matriz, preparar_roteamento

EXCHANGE = "pagamentos.fanout"
MENSAGENS = ["pagamento 1", "pagamento 2"]


def main():
    conexao = conectar()
    canal = conexao.channel()
    filas = preparar_roteamento(canal, EXCHANGE, "fanout")

    for fila in filas.values():
        canal.queue_bind(queue=fila, exchange=EXCHANGE)
        print(f"[bind] {EXCHANGE} -----> {fila}")

    for rotulo in MENSAGENS:
        canal.basic_publish(exchange=EXCHANGE, routing_key="qualquer-coisa", body=json.dumps({"rotulo": rotulo}))
        print(f"[A] publicou '{rotulo}' com chave qualquer-coisa (ignorada)")

    imprimir_matriz(canal, filas, MENSAGENS)
    conexao.close()


if __name__ == "__main__":
    main()
