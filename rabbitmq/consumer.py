"""Serviços B, C, D: consomem uma fila, competindo com os outros consumers dela.

O RabbitMQ faz round-robin entre consumers, não entre processos: --consumers 3
cria 3 consumers no mesmo canal e dá peso 3 a este processo.

Uso: python consumer.py [--nome B] [--fila trabalho] [--consumers 1] [--prefetch P]
                        [--processamento 0] [--auto-ack] [--cair-apos N]
"""
import argparse
import json
import os
import time
from collections import Counter

from comum import FILAS, conectar, declarar_fila


def main():
    parser = argparse.ArgumentParser(description="Serviço B/C/D: consumer RabbitMQ")
    parser.add_argument("--nome", default="B")
    parser.add_argument("--fila", default="trabalho", choices=list(FILAS))
    parser.add_argument("--consumers", type=int, default=1, help="consumers neste processo (peso no round-robin)")
    parser.add_argument("--prefetch", type=int, default=0, help="máximo de mensagens sem ack por consumer (0 = sem limite)")
    parser.add_argument("--processamento", type=float, default=0,
                        help="segundos simulando trabalho (bem abaixo de 60: o callback bloqueia os heartbeats)")
    parser.add_argument("--auto-ack", action="store_true", help="ack na entrega (at-most-once)")
    parser.add_argument("--cair-apos", type=int, default=0, help="processa N mensagens e cai antes do ack da última")
    args = parser.parse_args()

    conexao = conectar()
    canal = conexao.channel()
    declarar_fila(canal, args.fila)
    if args.prefetch:
        canal.basic_qos(prefetch_count=args.prefetch)

    totais = Counter()
    processadas = 0

    def receber(ch, method, properties, corpo):
        nonlocal processadas
        p = json.loads(corpo)
        time.sleep(args.processamento)
        processadas += 1
        totais[method.consumer_tag] += 1
        prioridade = f" prioridade {properties.priority}" if properties.priority is not None else ""
        reentrega = " (REENTREGA)" if method.redelivered else ""
        print(f"[{method.consumer_tag}] pagamento {p['id']:>3} ({p['tipo']:<6}){prioridade}{reentrega}", flush=True)

        if args.cair_apos and processadas == args.cair_apos:
            print(f"[{args.nome}] caindo ANTES do ack do pagamento {p['id']}!", flush=True)
            os._exit(1)

        if not args.auto_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Vários basic_consume no mesmo canal, sem threads: o BlockingConnection do pika não é thread-safe.
    for n in range(1, args.consumers + 1):
        canal.basic_consume(queue=args.fila, on_message_callback=receber, auto_ack=args.auto_ack,
                            consumer_tag=f"{args.nome}#{n}")

    ack = "auto-ack" if args.auto_ack else "ack manual"
    print(f"[{args.nome}] {args.consumers} consumer(s) na fila {args.fila}, {ack}, "
          f"prefetch {args.prefetch or 'sem limite'}. Aguardando...", flush=True)
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        pass
    print(f"\n[{args.nome}] total: " + ", ".join(f"{tag}={n}" for tag, n in sorted(totais.items())), flush=True)
    try:
        conexao.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
