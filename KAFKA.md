# Kafka

Estudo do Kafka como log distribuído de eventos e da sua diferença em relação ao RabbitMQ, com demonstração de até 5 min.

## Cenário
- **Serviço A** (producer, Python) publica pagamentos `{ id, valor, tipo }` no tópico `pagamentos` (3 partições), usando o `tipo` como chave.
- **Serviço B** (consumer Python, grupo `grupo-b`) e **Serviço C** (consumer em segunda linguagem, grupo `grupo-c`) leem o mesmo tópico.
- Broker: Kafka em modo KRaft (sem ZooKeeper), via `docker-compose.yml`.

## Características a demonstrar
| Característica | Demo | O que aparece na tela |
|---|---|---|
| Tópico, partição, offset | A publica e loga partição/offset de cada mensagem | Mesma chave sempre na mesma partição |
| Ordem por partição | Vários pagamentos `pix` em sequência | Ordem mantida dentro da partição |
| Consumer group | Duas instâncias de B no `grupo-b` | Partições divididas entre elas |
| Rebalanceamento | Derrubar uma instância de B | A outra assume as partições |
| Consumer ocioso | 4 instâncias com 3 partições | Uma fica sem partição |
| Pub/sub | `grupo-b` e `grupo-c` no mesmo tópico | Ambos recebem tudo |
| Replay | Novo grupo com `auto.offset.reset=earliest` | Lê o histórico desde o início; ler não apaga |
| Lag | Parar B e continuar publicando; `kafka-consumer-groups.sh --describe` | Coluna `LAG` crescendo e zerando quando B volta |
| Commit e reentrega | B cai depois de processar e antes do commit | Mensagem processada de novo (at-least-once) |
| Interoperabilidade | C em outra linguagem | JSON como contrato entre linguagens |

## Roteiro do vídeo (duração alvo: 5 min completos)
| Tempo | Cena | Mostra | Fala |
|---|---|---|---|
| 0:00-0:35 | Intro | Diagrama A → tópico (3 partições) → grupos B e C; broker KRaft subindo | Kafka é um log distribuído append-only; broker, tópico, retenção |
| 0:35-1:30 | Partição, chave e offset | A loga partição/offset; vários `pix` na mesma partição, em ordem | Chave → hash → partição; ordem só dentro da partição; offset é a posição no log |
| 1:30-2:25 | Consumer group | Duas instâncias de B dividindo partições; derrubar uma e ver o rebalanceamento; 4ª instância ociosa | Paralelismo limitado ao número de partições; rebalanceamento automático |
| 2:25-3:10 | Pub/sub e replay | `grupo-b` e `grupo-c` (outra linguagem) recebendo tudo; novo grupo com `earliest` lendo o histórico | Não há modo pub/sub separado: grupos diferentes recebem tudo; ler não apaga; JSON como contrato entre linguagens |
| 3:10-3:55 | Lag e commit | B parado, `LAG` subindo e zerando quando volta; B cai antes do commit e reprocessa | Commit de offset; at-least-once; replicação, líder e ISR como conceito |
| 3:55-5:00 | Kafka x RabbitMQ | Tabela abaixo na tela | Log x fila, retenção x remoção, replay, quem controla a leitura, roteamento, prioridade, quando usar cada um |

## Implementação (em ordem; só avança quando a etapa anterior funciona)
1. **Básico:** broker, tópico, A e B rodando.
2. **Consumer groups e pub/sub:** o mesmo consumer com outros `group.id`.
3. **Lag:** `kafka-consumer-groups.sh --describe`.
4. **Serviço C em segunda linguagem**, em grupo próprio.
5. **Commit manual** para mostrar reentrega.

## Conceitos-chave
- **Tópico, partição, offset:** ler uma mensagem não a apaga.
- **Chave e ordem:** mesma chave vai para a mesma partição; a ordem só vale dentro da partição.
- **Consumer group:** cada partição é lida por um consumer do grupo; consumers além do número de partições ficam ociosos.
- **Pub/sub:** grupos diferentes recebem tudo; o mesmo grupo divide.
- **Commit de offset:** o consumer controla onde parou.
- **Lag:** distância entre a última mensagem produzida e a última lida.

## Teoria (pontos de fala)
1. Log distribuído append-only e retenção.
2. Broker, tópico, partição, offset.
3. Chave, partição e ordem.
4. Consumer groups e rebalanceamento.
5. Replicação, líder e ISR (conceito).
6. Semânticas de entrega: at-most-once, at-least-once, exactly-once.

## Kafka vs RabbitMQ
| Aspecto | Kafka | RabbitMQ |
|---|---|---|
| Modelo | Log distribuído append-only | Fila com broker que roteia |
| Após o consumo | Mensagem permanece (retenção por tempo/tamanho) | Mensagem removida após ack |
| Replay | Sim, de qualquer offset | Não (já saiu da fila) |
| Roteamento | Simples: tópico + chave | Rico: direct, fanout, topic, headers |
| Quem controla a leitura | Consumer (offset) | Broker (entrega e ack) |
| Paralelismo | Limitado ao número de partições | Quantos consumers quiser na fila |
| Prioridade | Não tem | Tem (`x-max-priority`) |
| Ordem | Por partição | Por fila |
| Uso típico | Streaming de eventos, alto volume, reprocessamento | Distribuição de tarefas, roteamento flexível |

## Fora do escopo
Dead Letter Topic, administração de cluster, tuning, Kafka Streams, Kafka Connect, transações.

## Estrutura da pasta
```
kafka/
  README.md
  docker-compose.yml   # broker KRaft
  requirements.txt     # confluent-kafka
  package.json         # kafkajs
  producer.py          # Serviço A: loga partição e offset de cada mensagem
  consumer.py          # Serviço B: --grupo, --offset earliest|latest, commit manual
  consumer.js          # Serviço C em Node
  comandos.sh          # criar tópico, descrever grupos (lag), listar partições
```

## Como rodar
```bash
docker compose up -d
./comandos.sh criar-topico                 # pagamentos, 3 partições
python consumer.py --grupo grupo-b         # rodar em 2 terminais para dividir partições
node consumer.js grupo-c
python producer.py --quantidade 20
./comandos.sh lag grupo-b
```

## Estado atual
Arquivos em `kafka/`:
- `docker-compose.yml`: broker KRaft e `kafka-ui` (http://localhost:8080).
- `comandos.sh`: criar tópico, ver offsets, lag e grupos.
- `producer.py`: Serviço A com chave = tipo. Testado: cartao → partição 0, pix → 1, boleto → 2.
- `consumer.py`: Serviço B com commit manual, `--cair-apos` e rebalanceamento rápido. Ainda não testado.

Pendente: `consumer.js` (Serviço C em Node; o cliente `@confluentinc/kafka-javascript` já está instalado) e testes do consumer.
