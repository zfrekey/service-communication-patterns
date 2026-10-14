# RabbitMQ

Demonstrações na ordem do roteiro do vídeo. Plano e teoria em [../RABBITMQ.md](../RABBITMQ.md).

## Preparação
```bash
cd rabbitmq
docker compose up -d                 # painel: http://localhost:15672 (guest/guest)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
npm install
```

Cada demo de balanceamento usa dois ou mais terminais. Pare os consumers com Ctrl+C: eles imprimem o total por consumer.

## 1. Roteamento (exchanges)
Cada script declara a exchange e as filas, esvazia as filas, publica um conjunto fixo e imprime quem recebeu o quê.

```bash
python -m roteamento.direct
python -m roteamento.fanout
python -m roteamento.topic
python -m roteamento.headers
python -m roteamento.default
```

Saída esperada:
```
Chave    B    C    D          Chave                       B    C    D
pix      ✔         ✔          pagamento.pix.aprovado      ✔         ✔
cartao        ✔               pagamento.pix.negado        ✔    ✔    ✔
boleto             ✔          pagamento.cartao.aprovado             ✔
                              pagamento.boleto.negado          ✔    ✔

Mensagem      B    C    D     Headers                 B    C
pagamento 1   ✔    ✔    ✔     tipo=pix, alto=sim      ✔    ✔
pagamento 2   ✔    ✔    ✔     tipo=pix, alto=nao           ✔
                              tipo=cartao, alto=sim        ✔
                              tipo=boleto, alto=nao

fila default.B recebeu 1 mensagem(ns): ['default.B']
a mensagem com chave nao-existe sumiu sem erro (ver producer.py --confirm)
```

## 2. Balanceamento
**Round-robin:** dois consumers na fila `trabalho`.
```bash
python consumer.py --nome B          # terminal 1
python consumer.py --nome C          # terminal 2
python producer.py --quantidade 20   # terminal 3
```
B e C recebem 10 cada, alternados.

**Ponderado:** o peso é o número de consumers, não de processos.
```bash
python consumer.py --nome B --consumers 3
python consumer.py --nome C
python producer.py --quantidade 20
```
Totais: `B#1=5, B#2=5, B#3=5` e `C#1=5`, ou seja, B recebe 15 e C recebe 5.

**Bônus (fair dispatch):** com `prefetch 1`, o broker só entrega para quem já deu ack.
```bash
python consumer.py --nome B --prefetch 1 --processamento 1
python consumer.py --nome C --prefetch 1 --processamento 0.2
python producer.py --quantidade 20
```
C recebe bem mais (no ensaio: B=4, C=16).

## 3. Persistente x transiente
```bash
python producer.py --fila duravel --quantidade 3                         # ids 1-3, persistentes
python producer.py --fila duravel --quantidade 3 --inicio 4 --transiente # ids 4-6
# painel: duravel com 6 mensagens
docker compose restart rabbitmq                                          # nunca "down" aqui
# painel: duravel com 3 mensagens
python consumer.py --fila duravel                                        # só chegam os ids 1-3
```

## 4. Com e sem confirmação
**Sem confirmação (`auto_ack`):** o broker empurra tudo para B e apaga.
```bash
python producer.py --quantidade 10
python consumer.py --nome B --auto-ack --processamento 1 --cair-apos 2
# painel: trabalho com 0 mensagens -> 8 pagamentos perdidos
```

**Com confirmação (ack manual):** a mensagem sem ack volta para a fila.
```bash
python producer.py --quantidade 10
python consumer.py --nome B --prefetch 1 --cair-apos 2   # cai antes do ack do pagamento 2
python consumer.py --nome C
```
C recebe `[C#1] pagamento   2 (boleto) (REENTREGA)` e depois os pagamentos 3 a 10.

**Publisher confirm:**
```bash
python producer.py --fila nao-existe --quantidade 1            # "-> fila nao-existe", e a mensagem some
python producer.py --fila nao-existe --quantidade 1 --confirm  # ERRO ... (UnroutableError)
```

## 5. Prioridade
Publique com nenhum consumer ligado, para as mensagens acumularem.
```bash
python producer.py --fila prioridade --quantidade 10 --prioridade-aleatoria
python consumer.py --fila prioridade --prefetch 1
```
A chegada segue a prioridade em ordem decrescente (no ensaio: 9 9 8 7 7 7 2 1 0 0), não a ordem de envio.

## 6. Interoperabilidade (Python + Node)
```bash
python consumer.py --nome B --prefetch 1
node consumer.js
python producer.py --quantidade 10 --intervalo 0.2
```
As mensagens se alternam entre `[B#1]` e `[C/node]` (5 e 5). Sem `--intervalo`, quem der o ack primeiro leva a próxima, e a divisão fica desigual.

## Problemas comuns
- **`PRECONDITION_FAILED`:** a fila existe com outros argumentos. Apague-a no painel.
- **Filas sumiram depois do restart:** use `docker compose restart`. O `hostname` fixo no compose mantém o nome do nó.
- **Sobrou mensagem de outro ensaio:** `docker exec rabbitmq rabbitmqctl purge_queue trabalho`.
