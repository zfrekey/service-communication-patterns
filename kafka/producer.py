"""Serviço A: publica pagamentos no tópico `pagamentos`.

A chave da mensagem é o tipo do pagamento. Mesma chave -> mesma partição,
então os pagamentos de um mesmo tipo mantêm a ordem entre si.

Uso: python producer.py [--quantidade 12] [--intervalo 0.5] [--inicio 1]
"""
import argparse
import json
import os
import random
import time

from confluent_kafka import Producer

TOPICO = "pagamentos"
# Padrão: broker local. Em duas máquinas: KAFKA_HOST=<ip da VPS>.
BROKER = f"{os.environ.get('KAFKA_HOST', 'localhost')}:9092"
TIPOS = ["pix", "cartao", "boleto"]


def entregue(err, msg):
    if err:
        print(f"[A] ERRO ao enviar: {err}", flush=True)
        return
    p = json.loads(msg.value())
    print(f"[A] pagamento {p['id']:>3} ({p['tipo']:<6}) -> partição {msg.partition()}  offset {msg.offset()}",
          flush=True)


def main():
    parser = argparse.ArgumentParser(description="Serviço A: producer Kafka")
    parser.add_argument("--quantidade", type=int, default=12)
    parser.add_argument("--intervalo", type=float, default=0.5, help="segundos entre envios")
    parser.add_argument("--inicio", type=int, default=1, help="id do primeiro pagamento")
    args = parser.parse_args()

    # acks=all: o broker só confirma depois que todas as réplicas em sincronia (ISR) gravaram.
    producer = Producer({"bootstrap.servers": BROKER, "acks": "all"})

    for i in range(args.inicio, args.inicio + args.quantidade):
        pagamento = {"id": i, "valor": round(random.uniform(10, 500), 2), "tipo": TIPOS[i % len(TIPOS)]}
        producer.produce(TOPICO, key=pagamento["tipo"], value=json.dumps(pagamento), callback=entregue)
        producer.poll(0)
        time.sleep(args.intervalo)

    producer.flush()


if __name__ == "__main__":
    main()
