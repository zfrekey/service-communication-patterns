"""Consumer Python. Uso: consumer.py <group> <nome> [earliest]
Commit manual APOS processar => at-least-once."""
import json, sys
from confluent_kafka import Consumer

group, nome = sys.argv[1], sys.argv[2]
c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": group,
              "auto.offset.reset": sys.argv[3] if len(sys.argv) > 3 else "earliest",
              "enable.auto.commit": False})
c.subscribe(["pagamentos"],
            on_assign=lambda _, ps: print(f"[{nome}] particoes atribuidas: {[p.partition for p in ps]}", flush=True))
try:
    while True:
        m = c.poll(1.0)
        if m is None or m.error(): continue
        e = json.loads(m.value())
        print(f"[{nome}|{group}] p{m.partition()} off={m.offset()} {e['conta']} R${e['valor']} ({e['origem']})", flush=True)
        c.commit(message=m)
except KeyboardInterrupt:
    pass
finally:
    c.close()
