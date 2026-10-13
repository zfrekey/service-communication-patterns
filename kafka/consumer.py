"""Serviço B: consome o tópico `pagamentos` dentro de um consumer group.

O offset só é confirmado (commit) depois de processar a mensagem. Se o consumer
cair antes do commit, a mensagem é entregue de novo (at-least-once).

Uso: python consumer.py [--nome B] [--grupo grupo-b] [--offset earliest|latest]
                        [--processamento 0] [--cair-apos N]
"""
import argparse
import json
import os
import time

from confluent_kafka import Consumer

TOPICO = "pagamentos"


def main():
    parser = argparse.ArgumentParser(description="Serviço B: consumer Kafka")
    parser.add_argument("--nome", default="B")
    parser.add_argument("--grupo", default="grupo-b")
    parser.add_argument("--offset", choices=["earliest", "latest"], default="earliest",
                        help="onde começar quando o grupo ainda não tem offset salvo")
    parser.add_argument("--processamento", type=float, default=0, help="segundos simulando processamento")
    parser.add_argument("--cair-apos", type=int, default=0,
                        help="processa N mensagens e cai antes de fazer o commit da última")
    args = parser.parse_args()
    nome = f"{args.nome}|{args.grupo}"

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": args.grupo,
        "auto.offset.reset": args.offset,
        "enable.auto.commit": False,
        # Detecta consumer morto em 6 s (mínimo do broker) para o rebalanceamento aparecer rápido.
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 2000,
    })

    def atribuidas(_, particoes):
        print(f"[{nome}] partições atribuídas: {sorted(p.partition for p in particoes)}", flush=True)

    def revogadas(_, particoes):
        print(f"[{nome}] partições revogadas: {sorted(p.partition for p in particoes)}", flush=True)

    consumer.subscribe([TOPICO], on_assign=atribuidas, on_revoke=revogadas)
    print(f"[{nome}] aguardando mensagens...", flush=True)

    processadas = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{nome}] erro: {msg.error()}", flush=True)
                continue

            p = json.loads(msg.value())
            time.sleep(args.processamento)
            processadas += 1
            print(f"[{nome}] partição {msg.partition()} offset {msg.offset():>3} -> "
                  f"pagamento {p['id']:>3} ({p['tipo']:<6}) R$ {p['valor']:.2f}", flush=True)

            if args.cair_apos and processadas == args.cair_apos:
                print(f"[{nome}] caindo ANTES do commit do offset {msg.offset()}!", flush=True)
                os._exit(1)

            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        # close() avisa o grupo que este consumer saiu: o rebalanceamento é imediato.
        consumer.close()


if __name__ == "__main__":
    main()
