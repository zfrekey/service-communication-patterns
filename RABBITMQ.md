# RabbitMQ

Estudo do RabbitMQ como middleware de mensagens: roteamento, balanceamento, persistência, confirmação e prioridade. Cada característica aparece **executando** na demonstração (até 5 min).

## Cenário
- **Serviço A** (producer, Python com `pika`) publica pagamentos `{ id, valor, tipo }`.
- **Serviços B, C, D** (consumers) recebem e imprimem `"[B] recebeu pagamento 3 (pix)"`.
- Um dos consumers em segunda linguagem (Node com `amqplib` ou Java) para mostrar interoperabilidade.
- Broker: `rabbitmq:4-management` (AMQP na 5672, painel em http://localhost:15672).

## Ambiente da gravação: notebook + VPS
Decidido: o broker roda na **VPS** (Hostinger KVM 2, 4 GB de RAM, onde também ficam o portfólio e o xadrez de outro trabalho), e o notebook se conecta por **túnel SSH**. Na tela ficam dois terminais lado a lado (`usuario@vps` e `usuario@notebook`), mostrando que A e B estão em máquinas diferentes e só se conhecem pelo broker.

Por que a VPS e não outro PC: ela já fica ligada, já tem acesso por SSH e não depende de as duas máquinas estarem na mesma rede (Wi-Fi da faculdade costuma isolar os aparelhos).

Por que túnel e não porta aberta: a VPS hospeda coisas no ar, e com o túnel nenhuma porta nova fica exposta. Os scripts continuam usando `localhost`, que no notebook é o túnel. Vale dizer isso no vídeo.

Passo a passo, alternativa com porta aberta e firewall: [DUAS-MAQUINAS.md](DUAS-MAQUINAS.md).

**Antes de ensaiar (na VPS):** `docker --version`; RAM livre com `free -h` (o RabbitMQ usa ~200 MB).

**Túnel (no notebook):** `ssh -N -L 5672:localhost:5672 -L 15672:localhost:15672 usuario@vps`, com o RabbitMQ local parado (mesma porta).

**O que roda onde:**
| Cena | VPS | Notebook |
|---|---|---|
| Roteamento | `roteamento/*.py` | painel em :15672 |
| Balanceamento | producer e consumer C | consumer B (`--consumers 3` no ponderado) |
| Persistente x transiente | producer e `docker compose restart rabbitmq` | consumer lendo o que sobrou |
| Com e sem confirmação | producer e consumer C | consumer B com `--cair-apos`: cai no notebook, a mensagem reaparece em C na VPS |
| Prioridade | producer | consumer com `--prefetch 1` |
| Interoperabilidade | `consumer.py` | `consumer.js` |

Depois da gravação: `docker compose down` para devolver a RAM ao portfólio e ao xadrez.

## Requisitos e como provar cada um
| Requisito | Demo | O que aparece na tela |
|---|---|---|
| Direct | A publica com chave `pix` ou `cartao`; B ligado a `pix`, C a `cartao` | Cada consumer só recebe a sua chave |
| Fanout | A publica uma vez; B, C e D ligados à exchange | Os três recebem a mesma mensagem |
| Topic | Chave `pagamento.<tipo>.<status>`; B em `pagamento.pix.*`, C em `*.*.negado`, D em `#` | Tabela de quem recebeu cada mensagem |
| Headers | B com `x-match: all` (`tipo=pix`, `alto=sim`), C com `x-match: any` | Mesma mensagem chega em um e não no outro |
| Default exchange | `exchange=""` e `routing_key=<nome da fila>` | "Mandar direto para a fila" também passa por uma exchange |
| Round-robin | B e C na mesma fila | 1→B, 2→C, 3→B... |
| Round-robin ponderado | B com **3 consumers** e C com **1** na mesma fila | B recebe ~3 de cada 4 |
| Persistente | Fila durável + `delivery_mode=2`; reiniciar o broker | Mensagem continua na fila |
| Transiente | `delivery_mode=1` | Mensagem some após o restart |
| Sem confirmação | `auto_ack=True`; matar B no meio | Mensagens perdidas |
| Com confirmação (consumer) | Ack manual; matar B antes do ack | Mensagem volta para a fila e vai para C (`redelivered=True`) |
| Com confirmação (producer) | `confirm_delivery()` + `mandatory=True` | Publicar em destino inexistente dá erro; sem confirms, falha silenciosa |
| Prioridade | Fila com `x-max-priority=10`; A publica com B parado; B consome com `prefetch_count=1` | Ordem de chegada segue a prioridade, não a ordem de envio |

### Detalhes que fazem a demo funcionar
- **Ponderado:** o RabbitMQ faz round-robin **entre consumers**, não entre processos. Um processo com 3 consumers na fila tem peso 3. `prefetch_count` diferente não pondera se o consumer processa uma mensagem por vez.
- **Bônus (fair dispatch):** `prefetch_count=1` e B mais lento que C (`sleep`): C recebe mais, porque o broker só entrega quando há ack.
- **Persistência exige as duas coisas:** fila durável **e** mensagem persistente.
- **Restart sem perder dados:** fixar `hostname:` no `docker-compose.yml` (o nome do nó depende dele), usar volume em `/var/lib/rabbitmq` e `docker compose restart` em vez de `down`.
- **RabbitMQ 4.x:** fila clássica não durável e não exclusiva está depreciada; para fila transiente, usar `exclusive`/`auto_delete`.
- **Prioridade:** usar fila **clássica** (quorum queue só tem 2 níveis). Só aparece com mensagens acumuladas, por isso o consumer começa parado.

## Roteiro do vídeo (duração alvo: 5 min completos)
| Tempo | Cena | Mostra | Fala |
|---|---|---|---|
| 0:00-0:35 | Intro | Diagrama A → exchange → binding → fila → B/C/D; painel em :15672 | MOM: "comunicação assíncrona persistente", A não conhece B; AMQP: conexão, canal, exchange, fila; "Uma exchange nunca armazena mensagens" |
| 0:35-1:00 | Direct | A publica `pix` e `cartao`; B e C recebem só a sua | Roteamento por chave exata |
| 1:00-1:20 | Fanout | Uma publicação, B, C e D recebem | Broadcast; a chave é ignorada |
| 1:20-1:50 | Topic | Tabela de quem recebeu cada chave | Curingas `*` (uma palavra) e `#` (zero ou mais) |
| 1:50-2:10 | Headers e default | `x-match all` x `any`; publicação com `exchange=""` | Roteamento por atributos; toda fila já está ligada à default exchange |
| 2:10-2:55 | Balanceamento | Round-robin 1→B, 2→C...; depois B com 3 consumers e C com 1 | Competing consumers e escala horizontal; o peso é o número de consumers |
| 2:55-3:25 | Persistente x transiente | Restart do broker; a persistente continua, a transiente some | Fila durável **e** mensagem persistente; custo de disco x vazão |
| 3:25-4:00 | Com e sem confirmação | `auto_ack`: matar B perde mensagens; ack manual: a mensagem volta e vai para C; publisher confirm com erro | At-most-once x at-least-once; confirmação nos dois lados (consumer e producer) |
| 4:00-4:30 | Prioridade | Fila acumulada sendo consumida fora da ordem de envio | `x-max-priority`; só tem efeito com mensagens acumuladas |
| 4:30-5:00 | Interoperabilidade e fechamento | Consumer Node/Java na mesma fila | Vantagens do RabbitMQ; uma frase sobre Kafka: fila entrega e apaga, log retém |

Se faltar tempo, a default exchange vira uma frase e o fair dispatch fica de fora.

## Material da aula 6 (citar no vídeo)
Trechos da aula `Aula06-sd-comunicacao.pdf`, **exatamente como o professor escreveu**. A coluna "Cena" diz onde cada um entra no roteiro. O RabbitMQ fecha a parte da aula sobre MOM e AMQP; a aula o cita pelo nome.

| Cena | Como está na aula |
|---|---|
| Intro | MOM: "Também conhecido como sistema de enfileiramento de mensagens"; "Proporciona comunicação assíncrona persistente" |
| Intro | "Oferecem capacidade de armazenamento de médio prazo para mensagens, sem exigir que o remetente ou receptor estejam ativos durante a transmissão da mensagem" |
| Intro | "Não há garantias de QUANDO ou SE a mensagem será lida pelo receptor"; "Comunicação fracamente acoplada em relação ao tempo"; "Receptor não precisa estar em execução quando uma mensagem for enviada para sua fila" |
| Intro | "Tão logo uma mensagem tenha sido depositada em uma fila, ali permanecerá até ser removida, independentemente de seu remetente ou receptor estar em execução" |
| Intro | Message broker: "atua como intermediário entre os produtores e consumidores de mensagens"; "Gerencia o roteamento, armazenamento temporário e entrega de mensagens" |
| Intro | "Em 2006, um grupo de trabalho foi formado para alterar essa situação, o que resultou na especificação do Protocolo Avançado de Fila de Mensagens, ou simplesmente AMQP." (motivação: "As soluções de fila de mensagens são, em grande parte, soluções proprietárias.") |
| Intro | "O AMQP permite que um aplicativo estabeleça uma conexão com um gerenciador de filas; uma conexão é um contêiner para vários canais unidirecionais."; "O AMQP pressupõe que o TCP seja utilizado para estabelecer conexões." |
| Intro | "O servidor RabbitMQ é uma das implementações mais populares do AMQP."; "Baseado na versão 0.9 do AMQP, mas oferece suporte total à versão 1.0." |
| Intro / Default | "Em vez de manipular diretamente filas, os produtores entram em contato com uma exchange, que, por sua vez, coloca mensagens em uma ou várias filas." |
| Intro | "Uma exchange nunca armazena mensagens; ela apenas garante que uma mensagem seja colocada em uma fila apropriada." |
| Fanout | Fanout Exchange (Troca de Emissão): "Roteia mensagens para todas as filas vinculadas a ela."; "Ignora completamente a chave de roteamento na mensagem." |
| Direct | Direct Exchange (Troca Direta): "Roteia mensagens para filas com base em uma chave de roteamento."; "A chave de roteamento deve corresponder exatamente à chave especificada na mensagem."; "Se várias filas se associarem à mesma chave, funciona como fanout." → é o caso de B e D ligados a `pix` |
| Topic | Topic Exchange (Troca de Tópico): "Roteia mensagens para filas com base em padrões de chave de roteamento."; "As filas são vinculadas à troca com uma chave de roteamento que inclui padrões (*, #, ...)." |
| Persistente x transiente | "Um aspecto importante do AMQP é o suporte para mensagens persistentes."; "Uma mensagem pode ser marcada como durável, indicando que a fonte espera que qualquer nó intermediário, como uma fila, seja capaz de se recuperar em caso de falha." |
| Persistente x transiente | Persistente: "Uma mensagem é armazenada pelo middleware de comunicação durante o tempo que for necessário para entregá-la ao receptor". Transiente: "Caso o middleware não possa entregar a mensagem, ela será descartada" |
| Com e sem confirmação | "O receptor pode indicar ao remetente se sua mensagem foi aceita (significando que foi processada com sucesso) ou rejeitada." |
| Com e sem confirmação | Passos da transferência: a mensagem fica "em um estado não resolvido" até o receptor informar que concluiu; "como o aplicativo receptor pode indicar à camada de comunicação subjacente do AMQP que concluiu o processamento de uma mensagem, o AMQP possibilita uma confiabilidade real na comunicação de ponta a ponta." |
| Prioridade / Balanceamento | "O controle de fluxo é estabelecido por meio de créditos: um receptor pode informar ao remetente quantas mensagens ele tem permissão para enviar." → é a ideia do `prefetch_count` |
| Balanceamento | Interface básica do MOM: "get: bloqueie até que a fila especificada esteja não vazia e retire a primeira mensagem"; "poll: verifique uma fila especificada em busca de mensagens e retire a primeira, se houver. Nunca bloqueie."; "notify: instale um manipulador (callback) a ser chamado quando uma mensagem for colocada em uma fila específica." → `basic_get` é o poll; `basic_consume` com callback é o notify |

**Classificação nos eixos da aula**: assíncrona; persistente ou transiente conforme a fila e a mensagem (as duas demos mostram os dois lados); confiável com ack e publisher confirms, não-confiável com `auto_ack`; discreta.

**Além do material** (o que o vídeo acrescenta): headers exchange e default exchange (a aula só traz fanout, direct e topic), round-robin e round-robin ponderado, `auto_ack` x ack manual com reentrega, publisher confirms, prioridade com `x-max-priority`, prefetch/fair dispatch, cuidados de durabilidade no RabbitMQ 4.

## Conceitos-chave
- **Exchange, binding, fila:** o producer não publica direto na fila; o broker roteia.
- **Competing consumers:** consumers na mesma fila dividem as mensagens.
- **Ack e redelivery:** sem ack, a mensagem volta → at-least-once.
- **Publisher confirms:** o broker confirma ao producer que assumiu a mensagem.
- **Durabilidade:** fila durável + mensagem persistente.
- **Prefetch (QoS):** limite de mensagens não confirmadas por consumer.
- **Prioridade:** reordena mensagens acumuladas.

## Teoria (pontos de fala)
1. Middleware orientado a mensagens: comunicação assíncrona, A e B desacoplados no tempo e no espaço.
2. AMQP 0-9-1: conexão, canal, exchange, fila, binding.
3. Tipos de exchange e quando usar cada um.
4. Garantias de entrega: at-most-once (`auto_ack`) x at-least-once (ack manual + confirms).
5. Persistência e seu custo (disco x vazão).
6. Escala horizontal com competing consumers.
7. Diferença para Kafka: fila que entrega e apaga x log que retém (detalhe no [KAFKA.md](KAFKA.md)).

## Fora do escopo
Cluster, quorum queues em detalhe, federation/shovel, streams do RabbitMQ.

## Plano de implementação

### Estrutura da pasta
```
rabbitmq/
  README.md
  docker-compose.yml       # rabbitmq:4-management, hostname fixo, volume nomeado
  requirements.txt         # pika
  package.json             # "type": "module"; amqplib
  comum.py                 # conexão, declaração central das filas, mensagem padrão
  roteamento/direct.py     # cada script: declara exchange, filas e bindings,
  roteamento/fanout.py     # publica um conjunto fixo e imprime a matriz
  roteamento/topic.py      # "mensagem x fila que recebeu"
  roteamento/headers.py
  roteamento/default.py
  producer.py              # Serviço A
  consumer.py              # Serviços B, C, D
  consumer.js              # consumer Node
```

### Etapa 1: broker (`docker-compose.yml`)
```yaml
services:
  rabbitmq:
    image: rabbitmq:4-management
    container_name: rabbitmq
    hostname: rabbit            # o nome do nó (rabbit@rabbit) depende dele; sem isso o restart "esquece" as filas
    ports: ["5672:5672", "15672:15672"]
    volumes: [rabbitmq-dados:/var/lib/rabbitmq]
volumes:
  rabbitmq-dados:
```
- **Pronto quando:** o painel abre em http://localhost:15672 (guest/guest).

### Etapa 2: `comum.py`
- `conectar()`: `pika.BlockingConnection(pika.ConnectionParameters("localhost"))`.
- `FILAS`: dicionário com os argumentos de cada fila usada fora do roteamento, e `declarar_fila(canal, nome)` usando esse dicionário. Producer e consumer declaram a fila do mesmo jeito, o que evita o `PRECONDITION_FAILED` (redeclarar com argumentos diferentes):
  - `trabalho`: durável (balanceamento e ack).
  - `duravel`: durável (persistente x transiente).
  - `prioridade`: durável, `x-max-priority: 10`.
- `pagamento(i)`: devolve `{ id, valor, tipo }`, com tipo em ciclo pix/cartao/boleto.

### Etapa 3: roteamento (`roteamento/*.py`)
Cada script é autocontido e determinístico, para a matriz ser a mesma em todo ensaio:
1. Declara a exchange e as filas `<tipo>.B`, `<tipo>.C`, `<tipo>.D`, e **esvazia** (`queue_purge`) cada uma.
2. Cria os bindings.
3. Publica um conjunto fixo de mensagens, logando a chave ou os headers.
4. Lê cada fila com `basic_get` até esvaziar e imprime a matriz.

Resultado esperado (critério de pronto):

**direct** (`pagamentos.direct`): bindings B←`pix`, C←`cartao`, D←`pix` e `boleto` (uma fila pode ter vários bindings, e a mesma chave pode ir para várias filas).
| Mensagem (chave) | B | C | D |
|---|---|---|---|
| pix | ✔ | | ✔ |
| cartao | | ✔ | |
| boleto | | | ✔ |

**fanout** (`pagamentos.fanout`): B, C e D ligados sem chave. Publicar com chave `qualquer-coisa` e as 3 filas recebem as 2 mensagens.

**topic** (`pagamentos.topic`): B←`pagamento.pix.*`, C←`*.*.negado`, D←`#`.
| Chave | B | C | D |
|---|---|---|---|
| `pagamento.pix.aprovado` | ✔ | | ✔ |
| `pagamento.pix.negado` | ✔ | ✔ | ✔ |
| `pagamento.cartao.aprovado` | | | ✔ |
| `pagamento.boleto.negado` | | ✔ | ✔ |

**headers** (`pagamentos.headers`): B com `x-match: all` (`tipo=pix`, `alto=sim`), C com `x-match: any` (mesmos pares).
| Headers | B (all) | C (any) |
|---|---|---|
| `tipo=pix, alto=sim` | ✔ | ✔ |
| `tipo=pix, alto=nao` | | ✔ |
| `tipo=cartao, alto=sim` | | ✔ |
| `tipo=boleto, alto=nao` | | |

**default** (`""`): publicar com `routing_key="default.B"` chega na fila `default.B`; com `routing_key="nao-existe"`, a mensagem é descartada em silêncio (gancho para os publisher confirms da etapa 6).

### Etapa 4: `producer.py` (Serviço A)
Publica pela default exchange em uma fila de `FILAS`. Opções:
- `--fila`, `--quantidade`, `--inicio` (primeiro id), `--intervalo`.
- `--transiente`: `delivery_mode=1`. O padrão é persistente (`delivery_mode=2`).
- `--confirm`: `channel.confirm_delivery()` + `mandatory=True`. Captura `pika.exceptions.UnroutableError` e `NackError`.
- `--prioridade-aleatoria`: `properties.priority` aleatório de 0 a 9.

Cada publicação imprime `[A] pagamento 5 (pix) prioridade 7 persistente -> fila trabalho`.

### Etapa 5: `consumer.py` (Serviços B, C, D) e balanceamento
Opções:
- `--nome`, `--fila`.
- `--consumers N`: N `basic_consume` **no mesmo canal**, com tags `B#1..B#N`. É o que dá o peso no round-robin ponderado, sem threads (o `BlockingConnection` do pika não é thread-safe).
- `--prefetch P`: `basic_qos(prefetch_count=P)`. Sem a opção, fica sem limite.
- `--processamento S`: `sleep` simulando trabalho. Mantenha bem abaixo de 60 s: o callback bloqueia os heartbeats.
- `--auto-ack`: `auto_ack=True`. O padrão é ack manual depois de processar.
- `--cair-apos N`: processa N mensagens e chama `os._exit(1)` **antes** do ack da última.

Cada entrega imprime `[B#2] pagamento 7 (pix)`, com `(REENTREGA)` quando `method.redelivered` for verdadeiro. No Ctrl+C, imprime o total por consumer tag.

Critérios de pronto, com 20 mensagens na fila `trabalho`:
- **Round-robin:** B (1 consumer) e C (1 consumer) recebem exatamente 10 cada, alternados.
- **Ponderado:** B (`--consumers 3`) e C (`--consumers 1`) recebem 15 e 5.
- **Fair dispatch (bônus):** `--prefetch 1`, B com `--processamento 1` e C com `0.2`. C recebe bem mais.

### Etapa 6: persistência e confirmação (roteiro de testes)
- **Persistente x transiente:** publicar 3 persistentes (ids 1-3) e 3 transientes (`--transiente --inicio 4`) em `duravel`. O painel mostra 6. Depois de `docker compose restart rabbitmq`, mostra 3, e consumir confirma que só restaram os ids 1-3. Confirmar no ensaio que o RabbitMQ 4 descarta as transientes no restart.
- **Sem confirmação:** 10 mensagens em `trabalho`, B com `--auto-ack --processamento 1`. Matar B depois de 2. O broker já tinha empurrado tudo para B, então a fila fica com 0 e 8 mensagens se perdem.
- **Com confirmação:** a mesma coisa sem `--auto-ack`, com `--prefetch 1` e `--cair-apos 2`. A mensagem não confirmada volta para a fila e C a recebe com `(REENTREGA)`. No painel, ela passa de "Unacked" para "Ready".
- **Publisher confirm:** `producer.py --fila nao-existe` imprime "publicado" e a mensagem some. Com `--confirm`, o mesmo comando dá `UnroutableError`.

### Etapa 7: prioridade
- Com nenhum consumer ligado, `producer.py --fila prioridade --quantidade 10 --prioridade-aleatoria` loga a ordem de envio.
- Depois, `consumer.py --fila prioridade --prefetch 1`.
- **Pronto quando:** a ordem de chegada é decrescente em prioridade, e não a ordem de envio.

### Etapa 8: `consumer.js` (interoperabilidade)
- `amqplib`: `connect`, `createChannel`, `assertQueue("trabalho", { durable: true })` com os mesmos argumentos do `comum.py`, `prefetch(1)`, `consume` com `ack` manual.
- Log com prefixo `[C/node]`.
- **Pronto quando:** rodando junto com um `consumer.py` na fila `trabalho`, as mensagens se alternam entre Python e Node.

### Etapa 9: README da pasta
Comandos de cada demo na ordem do roteiro do vídeo, com a saída esperada.

### Armadilhas conhecidas
- **`PRECONDITION_FAILED`:** declarar uma fila que já existe com argumentos diferentes (ex.: sem `x-max-priority`) fecha o canal. Toda declaração passa por `comum.declarar_fila`. Se mudar argumentos, apague a fila no painel.
- **`docker compose down` sem `-v`** mantém o volume, mas sem `hostname` fixo o nó muda de nome e as filas "somem". Para a demo de restart, use `docker compose restart`.
- **Filas de roteamento acumulam** entre ensaios. Os scripts fazem `queue_purge` no início.
- **guest/guest:** o RabbitMQ só aceita o guest via localhost, mas a imagem Docker desliga essa trava (`loopback_users.guest = false`). Numa VPS, crie outro usuário (`RABBITMQ_USER`/`RABBITMQ_PASS`) antes de expor a porta.

## Estado
Implementado em [rabbitmq/](rabbitmq/README.md). Todos os critérios de pronto foram verificados (RabbitMQ 4, pika 1.3, amqplib 0.10).
- Interoperabilidade: a alternância Python/Node exige `--intervalo` no producer. Sem ele, com `prefetch 1`, quem der o ack primeiro leva a próxima mensagem.
