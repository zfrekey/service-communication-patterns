# Estudo de protocolos de comunicação distribuída

Estudo prático de quatro formas de comunicação entre sistemas distribuídos: **REST, gRPC, RabbitMQ e Apache Kafka**.
Cada protocolo tem sua pasta, com código executável e um documento detalhado:
- [REST.md](REST.md)
- [GRPC.md](GRPC.md)
- [RABBITMQ.md](RABBITMQ.md)
- [KAFKA.md](KAFKA.md)

## Objetivo
Para cada protocolo, entender e demonstrar na prática:
- **Como funciona:** modelo de comunicação, conceitos centrais e garantias.
- **Vantagens:** distribuição, desacoplamento, desempenho, conforme o caso.
- **Interoperabilidade:** pelo menos duas linguagens se comunicando. Python é a base em todos. A segunda linguagem é Node no RabbitMQ e no Kafka; no REST e no gRPC ela é escolhida na hora da implementação (opções em cada plano).
- **Paradigma de comunicação:** síncrono x assíncrono, request/response x streaming x pub/sub.

Cada estudo termina com uma demonstração gravada (máximo de 5 min) mostrando o sistema executando e a comunicação ocorrendo:
- **RabbitMQ e Kafka:** usam os 5 min completos, porque têm mais características para mostrar.
- **REST e gRPC:** em torno de 4 min, completos em conteúdo, sem enchimento para chegar a 5.

## Cenário comum
O mesmo contexto em todos os protocolos, propositalmente simples:
- **Serviço A** envia pagamentos.
- **Serviços B, C, ...** recebem e processam (só imprimem o que chegou e quem processou).

Mensagem padrão, em JSON (ou o equivalente em Protobuf no gRPC):
```json
{ "id": 1, "valor": 100.0, "tipo": "pix" }
```
`tipo` pode ser `pix`, `cartao` ou `boleto`; serve para roteamento e filtros quando necessário.

## Comparação dos paradigmas
| Protocolo | Paradigma | Acoplamento | Formato | Transporte |
|---|---|---|---|---|
| REST | Síncrono, request/response | A espera B responder | JSON | HTTP |
| gRPC | Síncrono + streaming (4 tipos) | Contrato forte (`.proto`) | Protobuf (binário) | HTTP/2 |
| RabbitMQ | Assíncrono, filas, roteamento no broker | A e B desacoplados pelo broker | JSON | AMQP |
| Kafka | Assíncrono, log distribuído, pub/sub | Desacoplado, com replay | JSON | Protocolo próprio sobre TCP |

## Estrutura
```
rest/
grpc/
rabbitmq/
kafka/
```
Cada pasta é autocontida: `README` com como rodar, `docker-compose.yml` quando houver broker, e código nas duas linguagens.
Por enquanto só existe `kafka/`; as outras pastas ainda não foram criadas.

## Ordem de estudo
1. **Kafka:** já tem base.
2. **RabbitMQ:** contraste direto com Kafka (fila x log).
3. **REST:** comunicação síncrona mais simples.
4. **gRPC:** contrato forte e streaming.

## Progresso
- [ ] Kafka
- [ ] RabbitMQ
- [ ] REST
- [ ] gRPC
