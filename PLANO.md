# Plano do trabalho: Apache Kafka

Trabalho de comunicação distribuída (tema: Apache Kafka). Vídeo de até 5 min + código em `.zip`.
Nota: 50% domínio da teoria, 50% aplicação demonstrando as características do protocolo.

## Objetivo pessoal
Aprender o que um estagiário/júnior realmente precisa saber de Kafka num cenário realista
(stack de referência: Java + Spring Boot).

## Cenário
Eventos de pagamento. A conta é a chave da mensagem.
- Tópico `pagamentos` com 3 partições.
- Producer em Python.
- Consumers em grupos diferentes (`antifraude`, `notificacao`); consumer Java/Spring Boot como bônus.

## O que um júnior precisa saber

### Essencial
1. Tópico, partição, offset: ler uma mensagem não a apaga.
2. Chave e ordem: mesma chave vai para a mesma partição; a ordem só vale dentro da partição.
3. Consumer group: cada partição é lida por um consumer do grupo; mais consumers que partições = ociosos.
4. Commit de offset e entrega: at-least-once se o consumer cai antes do commit.
5. Idempotência no consumer: a mensagem pode chegar duas vezes (crítico em pagamentos).
6. Lag do consumer: primeira métrica olhada quando "está atrasado".

### Cenário real
7. Mensagem que falha: retry e Dead Letter Topic (DLT).
8. Serialização e contrato: JSON vs Avro, schema evolution.
9. Rebalanceamento quando um pod sobe ou cai.

### Pode esperar
Administração de cluster, tuning, Kafka Streams, Connect, exactly-once com transações.

## Pub/sub no Kafka
Não há modo separado; o consumer group define o comportamento.
- Grupos diferentes no mesmo tópico: cada grupo recebe tudo (pub/sub).
- Mesmo grupo: mensagens divididas entre consumers (balanceamento / fila de trabalho).

## Roteiro do vídeo (até 5 min)
| Tempo | Cena | Conteúdo |
|---|---|---|
| 0:00-0:30 | Intro | Problema, por que Kafka, diagrama |
| 0:30-1:15 | Partições e chave | Producer; mesma conta sempre na mesma partição; offsets |
| 1:15-2:00 | Consumer group | Dois consumers dividem partições; rebalanceamento |
| 2:00-2:45 | Pub/sub | Segundo grupo recebe tudo; replay com `earliest` |
| 2:45-3:45 | Falhas | Idempotência e mensagem inválida indo para o DLT |
| 3:45-4:30 | Lag | Consumer parado: lag cresce; volta: zera |
| 4:30-5:00 | Fechamento | Kafka vs RabbitMQ, quando usar cada um |

## Teoria (50% da nota)
1. Log distribuído append-only e retenção.
2. Broker, tópico, partição, offset.
3. Chave, partição e ordem.
4. Consumer groups e rebalanceamento.
5. Replicação, líder e ISR (conceito).
6. Semânticas de entrega: at-most-once, at-least-once, exactly-once.
7. Kafka vs RabbitMQ (log vs fila) e vs REST/RPC (acoplamento temporal).

## Etapas
1. Estudar a teoria.
2. Revisar o código existente.
3. Implementar o cenário (idempotência, DLT, lag; consumer Spring Boot opcional).
4. Testar o roteiro completo sem gravar.
5. Gravar o vídeo (narração própria).
6. Gerar o `.zip` com código e README.

## Entregáveis
- Vídeo separado, sem compactar, até 5 min.
- `.zip` com o código e README.
- Upload dos arquivos no Classroom (sem links).

## Estado atual
Existe apenas a base: `docker-compose.yml` (broker KRaft), `producer.py`, `consumer.py`
(commit manual) e versões Node.js (`producer.js`, `consumer.js`) que provavelmente saem.
Nada foi testado ainda.

## Decisões em aberto
- Individual ou dupla?
- Incluir o consumer Spring Boot (depende de Maven/Gradle disponível)?
