"""Topic exchange: roteia por padrão de chave.

Chave pagamento.<tipo>.<status>. `*` casa exatamente uma palavra, `#` casa zero ou mais.
B <- pagamento.pix.*, C <- *.*.negado, D <- #.

Uso (de dentro de rabbitmq/): python -m roteamento.topic
"""
import json

from comum import conectar, imprimir_matriz, preparar_roteamento

EXCHANGE = "pagamentos.topic"
BINDINGS = {"B": "pagamento.pix.*", "C": "*.*.negado", "D": "#"}
CHAVES = ["pagamento.pix.aprovado", "pagamento.pix.negado", "pagamento.cartao.aprovado", "pagamento.boleto.negado"]


def main():
    conexao = conectar()
    canal = conexao.channel()
    filas = preparar_roteamento(canal, EXCHANGE, "topic")

    for c, padrao in BINDINGS.items():
        canal.queue_bind(queue=filas[c], exchange=EXCHANGE, routing_key=padrao)
        print(f"[bind] {EXCHANGE} --{padrao}--> {filas[c]}")

    for chave in CHAVES:
        canal.basic_publish(exchange=EXCHANGE, routing_key=chave, body=json.dumps({"rotulo": chave}))
        print(f"[A] publicou com chave {chave}")

    imprimir_matriz(canal, filas, CHAVES, titulo="Chave")
    conexao.close()


if __name__ == "__main__":
    main()
