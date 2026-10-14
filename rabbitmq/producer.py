"""Serviço A: publica pagamentos numa fila pela default exchange.

Por padrão a mensagem é persistente (delivery_mode=2). Com --confirm, o broker
confirma cada publicação e devolve as que não chegaram a nenhuma fila (mandatory).

Uso: python producer.py [--fila trabalho] [--quantidade 10] [--inicio 1] [--intervalo 0]
                        [--transiente] [--confirm] [--prioridade-aleatoria]
"""
import argparse
import json
import random
import time

import pika
from pika.exceptions import NackError, UnroutableError

from comum import FILAS, conectar, declarar_fila, pagamento


def main():
    parser = argparse.ArgumentParser(description="Serviço A: producer RabbitMQ")
    parser.add_argument("--fila", default="trabalho", help=f"uma de {list(FILAS)} (ou outra, para testar o --confirm)")
    parser.add_argument("--quantidade", type=int, default=10)
    parser.add_argument("--inicio", type=int, default=1, help="id do primeiro pagamento")
    parser.add_argument("--intervalo", type=float, default=0, help="segundos entre envios")
    parser.add_argument("--transiente", action="store_true", help="delivery_mode=1 (some no restart do broker)")
    parser.add_argument("--confirm", action="store_true", help="publisher confirms + mandatory=True")
    parser.add_argument("--prioridade-aleatoria", action="store_true", help="priority aleatória de 0 a 9")
    args = parser.parse_args()

    conexao = conectar()
    canal = conexao.channel()
    declarar_fila(canal, args.fila)
    if args.confirm:
        # A partir daqui basic_publish espera o ack do broker e lança exceção se falhar.
        canal.confirm_delivery()

    modo = "transiente" if args.transiente else "persistente"
    for i in range(args.inicio, args.inicio + args.quantidade):
        p = pagamento(i)
        prioridade = random.randint(0, 9) if args.prioridade_aleatoria else None
        propriedades = pika.BasicProperties(delivery_mode=1 if args.transiente else 2, priority=prioridade)
        texto_prioridade = f" prioridade {prioridade}" if prioridade is not None else ""
        try:
            canal.basic_publish(exchange="", routing_key=args.fila, body=json.dumps(p),
                                properties=propriedades, mandatory=args.confirm)
            print(f"[A] pagamento {p['id']:>3} ({p['tipo']:<6}){texto_prioridade} {modo} -> fila {args.fila}",
                  flush=True)
        except UnroutableError:
            print(f"[A] ERRO pagamento {p['id']}: nenhuma fila recebeu (UnroutableError)", flush=True)
        except NackError:
            print(f"[A] ERRO pagamento {p['id']}: broker recusou (NackError)", flush=True)
        time.sleep(args.intervalo)

    conexao.close()


if __name__ == "__main__":
    main()
