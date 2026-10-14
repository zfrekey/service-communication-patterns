# Rodando em duas máquinas (notebook + VPS)

O broker fica na **VPS**, que tem IP público fixo. O notebook está atrás do roteador (NAT) e não recebe conexões, mas consegue abrir conexões para a VPS. Na gravação, um terminal na VPS e outro no notebook, lado a lado:

```
VPS:      broker + producer (Serviço A)
notebook: consumer (Serviço B)  --internet-->  broker na VPS
```

Escolha para a gravação: VPS com túnel SSH (opção 1). Os motivos e o que roda onde em cada cena estão no [RABBITMQ.md](RABBITMQ.md) e no [KAFKA.md](KAFKA.md), na seção "Ambiente da gravação".

Só o broker precisa ficar na VPS. Producer e consumer podem rodar em qualquer uma das duas.

Todos os clientes leem o endereço de variáveis de ambiente. Sem elas, tudo continua em `localhost`, como nas demos locais.

| Variável | Padrão | Usada por |
|---|---|---|
| `RABBITMQ_HOST`, `RABBITMQ_USER`, `RABBITMQ_PASS` | `localhost`, `guest`, `guest` | `rabbitmq/*.py`, `rabbitmq/consumer.js`, compose (usuário na primeira subida) |
| `KAFKA_HOST` | `localhost` | `kafka/*.py`, compose (endereço anunciado aos clientes) |

## Preparação na VPS
```bash
git clone <repo> && cd service-communication-patterns
# o mesmo da preparação local: venv, pip install, npm install em rabbitmq/ e kafka/
```
Memória: o Kafka sozinho usa ~1 GB de RAM e o RabbitMQ ~200 MB. Numa VPS de 1 GB, suba um broker de cada vez.

## Opção 1 (recomendada): túnel SSH
Nenhuma porta fica aberta na internet. O SSH leva a porta da VPS até o `localhost` do notebook, e os scripts rodam sem nenhuma variável.

**RabbitMQ**
```bash
# VPS
cd rabbitmq && docker compose up -d
# notebook (deixe aberto num terminal; pare o RabbitMQ local antes, porque a porta é a mesma)
ssh -N -L 5672:localhost:5672 -L 15672:localhost:15672 usuario@ip-da-vps
# notebook, outro terminal
python consumer.py --nome B
# VPS
python producer.py --quantidade 10 --intervalo 0.5
```
O painel da VPS abre no notebook em http://localhost:15672.

**Kafka**
```bash
# VPS
cd kafka && docker compose up -d && ./comandos.sh criar-topico
# notebook
ssh -N -L 9092:localhost:9092 -L 8080:localhost:8080 usuario@ip-da-vps
python consumer.py
# VPS
python producer.py
```
Funciona porque o broker anuncia `localhost:9092`, e no notebook esse endereço é o túnel.

No vídeo, vale explicar: "o `localhost` do notebook é um túnel até a VPS; o broker está do outro lado da internet". O prompt de cada terminal (`usuario@notebook` x `usuario@vps`) mostra que são duas máquinas.

## Opção 2: porta aberta, conexão direta pelo IP
Fica mais visual, porque o notebook conecta em `RABBITMQ_HOST=203.0.113.10`, mas a porta fica exposta na internet.

**RabbitMQ**
```bash
# VPS (o usuário só é criado com o volume vazio; se o broker já subiu antes: docker compose down -v)
cd rabbitmq && RABBITMQ_USER=demo RABBITMQ_PASS='<senha forte>' docker compose up -d
# notebook
export RABBITMQ_HOST=<ip-da-vps> RABBITMQ_USER=demo RABBITMQ_PASS='<senha forte>'
python consumer.py --nome B
# VPS (com as mesmas variáveis de usuário e senha; o host pode ficar localhost)
python producer.py
```

**Kafka**
```bash
# VPS
cd kafka && KAFKA_HOST=<ip-da-vps> docker compose up -d && ./comandos.sh criar-topico
# notebook e VPS: os clientes dos dois lados usam o IP público,
# porque é esse o endereço que o broker devolve para todo mundo
KAFKA_HOST=<ip-da-vps> python consumer.py
KAFKA_HOST=<ip-da-vps> python producer.py
```
O Kafka daqui não tem autenticação: quem alcançar a porta 9092 lê e escreve no tópico. O mesmo vale para o `kafka-ui` na 8080.

**Firewall (obrigatório na opção 2):** libere as portas só para o IP do notebook (veja em https://ifconfig.me).
- O jeito mais fácil é o firewall do painel do provedor da VPS (security group), com regra de entrada de 5672, 15672, 9092 e 8080 só a partir do IP do notebook.
- **O `ufw` não resolve:** portas publicadas pelo Docker passam por fora dele. Pelo `iptables`, use a cadeia `DOCKER-USER` (troque `eth0` pela interface que aparece em `ip route | grep default`):
  ```bash
  for porta in 5672 15672 9092 8080; do
    sudo iptables -I DOCKER-USER -i eth0 -p tcp -m conntrack --ctorigdstport $porta ! -s <ip-do-notebook> -j DROP
  done
  ```

Ao terminar, derrube tudo: `docker compose down` em cada pasta.

## O que muda nas demos
- **Persistência (RabbitMQ):** o `docker compose restart rabbitmq` roda na VPS.
- **Queda de consumer:** o `--cair-apos` no notebook fica bem visível, porque o consumer cai numa máquina e a mensagem reaparece na outra.
- **Roteamento:** os scripts `roteamento/*.py` fazem tudo num processo só. Não ganham nada com duas máquinas, então rode na VPS ou no notebook.

## O que foi testado
Conexão pelo IP de rede (não `localhost`) na mesma máquina, nos dois brokers: RabbitMQ com usuário próprio via Python e Node, e Kafka com `KAFKA_HOST` anunciado, produzindo, consumindo e usando o `comandos.sh`. Ainda falta ensaiar com a VPS de verdade.
