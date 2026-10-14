"""Direct exchange: roteia pela chave exata.

B <- pix, C <- cartao, D <- pix e boleto. Uma fila pode ter vários bindings, e a
mesma chave ligada a várias filas funciona como fanout (pix chega em B e D).

Uso (de dentro de rabbitmq/): python -m roteamento.direct
"""
import json

from comum import conectar, imprimir_matriz, preparar_roteamento

EXCHANGE = "pagamentos.direct"
BINDINGS = {"B": ["pix"], "C": ["cartao"], "D": ["pix", "boleto"]}
CHAVES = ["pix", "cartao", "boleto"]


def main():
    conexao = conectar()
    canal = conexao.channel()
    filas = preparar_roteamento(canal, EXCHANGE, "direct")

    for c, chaves in BINDINGS.items():
        for chave in chaves:
            canal.queue_bind(queue=filas[c], exchange=EXCHANGE, routing_key=chave)
            print(f"[bind] {EXCHANGE} --{chave}--> {filas[c]}")

    for chave in CHAVES:
        canal.basic_publish(exchange=EXCHANGE, routing_key=chave, body=json.dumps({"rotulo": chave}))
        print(f"[A] publicou com chave {chave}")

    imprimir_matriz(canal, filas, CHAVES, titulo="Chave")
    conexao.close()


if __name__ == "__main__":
    main()
