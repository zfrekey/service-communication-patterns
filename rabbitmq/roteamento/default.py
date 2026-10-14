"""Default exchange (""): toda fila já nasce ligada a ela com a chave igual ao próprio nome.

"Mandar direto para a fila" também passa por uma exchange. Chave que não é nome
de fila: a mensagem é descartada em silêncio (sem publisher confirm ninguém avisa).

Uso (de dentro de rabbitmq/): python -m roteamento.default
"""
import json

from comum import conectar, ler_tudo

FILA = "default.B"


def main():
    conexao = conectar()
    canal = conexao.channel()
    canal.queue_declare(queue=FILA, durable=True)
    canal.queue_purge(queue=FILA)

    for chave in [FILA, "nao-existe"]:
        canal.basic_publish(exchange="", routing_key=chave, body=json.dumps({"rotulo": chave}))
        print(f'[A] publicou com exchange="" e chave {chave}')

    recebidas = ler_tudo(canal, FILA)
    print(f"\nfila {FILA} recebeu {len(recebidas)} mensagem(ns): {[m['rotulo'] for m in recebidas]}")
    print("a mensagem com chave nao-existe sumiu sem erro (ver producer.py --confirm)")
    conexao.close()


if __name__ == "__main__":
    main()
