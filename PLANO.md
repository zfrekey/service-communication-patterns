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

## Material da aula 6 (Comunicação): base comum dos vídeos
Fonte: slides `Aula06-sd-comunicacao.pdf` (bibliografia do professor: Tanenbaum, *Sistemas Distribuídos: Princípios e Paradigmas*, cap. 4). Os trechos entre aspas estão **exatamente como o professor escreveu** e devem ser citados assim no vídeo. Cada `.md` de protocolo tem a sua seção "Material da aula 6" com as citações específicas; aqui ficam as que valem para todos.

**Fundamentos:**
> "A comunicação entre processos está no coração de qualquer SD"
> "A comunicação em SD é sempre baseada em troca de mensagens"
> "Desejável obter modelos onde a complexidade da comunicação seja transparente para o desenvolvedor"

**Tipos de comunicação:** todo vídeo classifica o seu protocolo nestes quatro eixos, usando as definições do professor:
| Eixo | Definição do professor (literal) |
|---|---|
| Persistente | "Uma mensagem é armazenada pelo middleware de comunicação durante o tempo que for necessário para entregá-la ao receptor" |
| Transiente | "Uma mensagem é armazenada pelo middleware de comunicação somente durante o tempo em que a aplicação remetente e a aplicação receptora estiverem executando"; "Caso o middleware não possa entregar a mensagem, ela será descartada" |
| Síncrona | "Remetente usa chamada bloqueante para envio de mensagem"; "Remetente e receptor deve estar ativos ao mesmo tempo e sincronizados" |
| Assíncrona | "Remetente continua sua execução imediatamente após ter apresentado sua mensagem para transmissão"; "Oculta latência de comunicação intrínseca de sistemas distribuídos" |
| Confiável | "Há garantia de entrega da mensagem"; "Dados não chegam corrompidos" |
| Não-confiável | "Não há garantia de entrega"; "Dados podem chegar corrompidos" |
| Discreta | "Cada mensagem forma uma unidade de informação completa" |
| Fluxo | "As mensagens estão relacionadas uma com as outras pela ordem ou pela relação temporal" |

**Classificação dos quatro protocolos nos eixos da aula** (é a nossa leitura, para cada vídeo dizer em uma frase onde o protocolo se encaixa):
| Protocolo | Persistente x transiente | Síncrona x assíncrona | Confiável x não-confiável | Discreta x fluxo |
|---|---|---|---|---|
| REST | Transiente (se B estiver fora, a requisição falha) | Síncrona (request/response bloqueante) | Confiável no transporte (TCP); retry exige idempotência | Discreta |
| gRPC | Transiente | Síncrona no unary (RPC síncrona da aula); streaming aproxima de fluxo | Confiável no transporte (TCP/HTTP2) | Discreta (unary) e fluxo (streaming) |
| RabbitMQ | Persistente com fila durável + mensagem persistente; transiente sem isso | Assíncrona | Confiável com ack e publisher confirms; sem ack, não | Discreta |
| Kafka | Persistente (log retido em disco, mesmo após a leitura) | Assíncrona | Confiável com commit de offset e replicação | Discreta por mensagem; o tópico é um fluxo ordenado por partição |

**Onde cada protocolo aparece no material:**
- **gRPC:** seção "Chamada de Procedimento Remoto"; a aula cita o gRPC pelo nome.
- **REST:** citado uma vez ("Em aplicações Web, RPC perdeu espaço para comunicação REST."); usa a base de sockets TCP e request-response.
- **RabbitMQ:** seção MOM, AMQP e RabbitMQ; a aula o cita pelo nome.
- **Kafka:** não aparece pelo nome. Entra pelos conceitos de MOM/comunicação persistente assíncrona e de publica/subscreve. Tudo o que é específico do Kafka é "além do material".

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
