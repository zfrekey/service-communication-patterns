# Plano do trabalho: Apache Kafka

Trabalho de comunicação distribuída sobre Apache Kafka, com vídeo de até 5 min.
Avaliação: 50% domínio da teoria, 50% aplicação demonstrando as características do protocolo.

## Cenário
Eventos de pagamento, com a conta como chave da mensagem.
- Tópico `pagamentos` com 3 partições.
- Producer em Python.
- Consumers em Python e em Java (Spring Boot), em grupos diferentes, lendo o mesmo tópico.

## Vídeo (5 min, 4 cenas)
| Tempo | Cena | Mostra |
|---|---|---|
| 0:00-0:30 | Intro | Problema e diagrama do cenário |
| 0:30-1:45 | Tópico, partição e chave | Partição e offset de cada mensagem; mesma conta na mesma partição |
| 1:45-3:00 | Consumer group | Dois consumers dividindo as partições; rebalanceamento quando um cai |
| 3:00-4:15 | Pub/sub, replay e lag | Grupo `antifraude` (Python) e grupo `notificacao` (Spring Boot) recebendo tudo; `earliest`; lag com consumer parado |
| 4:15-5:00 | Fechamento | Idempotência em demo curta e uma frase comparando com RabbitMQ |

## Implementação (em ordem; só avança quando a etapa anterior funciona)
1. **Básico:** broker, tópico, producer e consumer Python rodando.
2. **Consumer groups e pub/sub:** o mesmo consumer com outros `group.id`.
3. **Lag:** `kafka-consumer-groups.sh --describe`.
4. **Consumer Java:** projeto Spring Boot com `spring-kafka` e `@KafkaListener`, em grupo próprio.
5. **Idempotência:** o consumer guarda os ids já processados e ignora duplicatas, com teste que reenvia a mesma mensagem.

## Conceitos-chave
- **Tópico, partição, offset:** ler uma mensagem não a apaga.
- **Chave e ordem:** mesma chave vai para a mesma partição; a ordem só vale dentro da partição.
- **Consumer group:** cada partição é lida por um consumer do grupo; consumers além do número de partições ficam ociosos.
- **Pub/sub:** não há modo separado; grupos diferentes no mesmo tópico recebem tudo, o mesmo grupo divide as mensagens.
- **Interoperabilidade:** producer e consumers em linguagens diferentes, com JSON como contrato.
- **Commit de offset e entrega:** se o consumer cai antes do commit, a mensagem é reprocessada (at-least-once).
- **Idempotência:** processar a mesma mensagem duas vezes tem o mesmo efeito que uma (crítico em pagamentos).
- **Lag:** diferença entre a última mensagem produzida e a última lida pelo consumer.

## Teoria
1. Log distribuído append-only e retenção.
2. Broker, tópico, partição, offset.
3. Chave, partição e ordem.
4. Consumer groups e rebalanceamento.
5. Replicação, líder e ISR (conceito).
6. Semânticas de entrega: at-most-once, at-least-once, exactly-once.
7. Kafka vs RabbitMQ: log vs fila.

## Fora do escopo
Dead Letter Topic, administração de cluster, tuning, Kafka Streams, Kafka Connect, exactly-once com transações.

## Estado atual
Base pronta, ainda sem testes: `docker-compose.yml` (broker KRaft), `producer.py` e `consumer.py` (commit manual).
Pendente: verificar Maven/Gradle e compatibilidade do Java 25 com Spring Boot antes de criar o consumer Java.
