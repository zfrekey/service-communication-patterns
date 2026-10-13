"""Producer Python: envia pagamentos ao topico. A CHAVE (conta) define a particao,
entao eventos da mesma conta mantem ordem."""
import json, sys, time
from confluent_kafka import Producer

TOPIC = "pagamentos"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 12
p = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})

def ok(err, msg):
    print(f"[producer] {'ERRO '+str(err) if err else 'ack'} chave={msg.key().decode()} "
          f"particao={msg.partition()} offset={msg.offset()}")

for i in range(N):
    conta = f"conta-{i % 4}"
    evento = {"id": i, "conta": conta, "valor": round(10 + i * 7.5, 2), "origem": "python"}
    p.produce(TOPIC, key=conta, value=json.dumps(evento), callback=ok)
    p.poll(0); time.sleep(0.2)
p.flush()
