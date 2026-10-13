# gRPC

Estudo do gRPC como RPC moderno com contrato forte e streaming, com demonstração de até 5 min.

## Cenário
- **Serviço B** (servidor, Python com `grpcio`) recebe e processa pagamentos.
- **Serviço A** (cliente) chama B em Python e em uma **segunda linguagem escolhida na hora da implementação** (opções na etapa 4), ambos usando **o mesmo `.proto`**.
- `grpcurl` com server reflection para chamar B pelo terminal.

## Contrato
```proto
syntax = "proto3";
package pagamentos;

service ServicoB {
  rpc Pagar (Pagamento) returns (Recibo);                         // unary
  rpc ListarPagamentos (Filtro) returns (stream Pagamento);        // server streaming
  rpc PagarLote (stream Pagamento) returns (Resumo);              // client streaming
  rpc PagarAoVivo (stream Pagamento) returns (stream Recibo);     // bidirecional
}

message Pagamento { int32 id = 1; double valor = 2; string tipo = 3; }
message Recibo    { int32 id = 1; string status = 2; }
message Filtro    { string tipo = 1; }
message Resumo    { int32 quantidade = 1; double total = 2; }
```

## Características a demonstrar
| Característica | Demo | O que aparece na tela |
|---|---|---|
| Contrato primeiro | `.proto` gerando código em Python e na segunda linguagem | Stubs gerados nas duas linguagens |
| Unary | A chama `Pagar` | Request/response, parece chamada de função local |
| Server streaming | `ListarPagamentos` com `sleep` entre itens | Itens chegando um a um em A |
| Client streaming | A envia 5 pagamentos em `PagarLote` | B responde uma vez, no final, com o resumo |
| Bidirecional | `PagarAoVivo` | A envia e recebe recibos intercalados |
| Interoperabilidade | Cliente na segunda linguagem chamando B (Python) | Mesmo resultado em outra linguagem |
| Deadline | A lista todos os pagamentos com `timeout=1.2s`; B envia um a cada 0.5 s | Alguns itens chegam e então `DEADLINE_EXCEEDED` |
| Status codes | Pagamento com valor negativo | `INVALID_ARGUMENT` com mensagem |
| Metadata | Token no header da chamada | B rejeita sem token (`UNAUTHENTICATED`) |

## Roteiro do vídeo (duração alvo: ~4 a 4:30 min; não precisa preencher os 5)
| Tempo | Cena | Mostra | Fala |
|---|---|---|---|
| 0:00-0:40 | Intro | Diagrama A → stub → HTTP/2 → stub → B | RPC: chamar uma função remota como se fosse local; stubs e marshalling; a rede introduz falhas parciais |
| 0:40-1:20 | Contrato | O `.proto` e a geração de código em Python e na segunda linguagem | Contrato forte (IDL) x contrato informal do REST; Protobuf binário; campos numerados e evolução compatível |
| 1:20-1:40 | Unary | A chama `Pagar` e recebe o recibo | Request/response clássico |
| 1:40-2:05 | Server streaming | `ListarPagamentos` chegando item a item | B envia vários, A consome conforme chegam |
| 2:05-2:30 | Client streaming | A envia 5 pagamentos; B responde só no final | Útil para upload e lotes |
| 2:30-3:00 | Bidirecional | Envios e recibos intercalados | Dois fluxos independentes sobre o mesmo stream HTTP/2 |
| 3:00-3:25 | Interoperabilidade | Cliente na segunda linguagem chamando B em Python | O `.proto` é o único acordo entre as linguagens |
| 3:25-4:00 | Robustez | Deadline estourando, `INVALID_ARGUMENT`, `UNAUTHENTICATED` sem token | Deadlines, status codes e metadata padronizados |
| 4:00-4:30 | Fechamento | Comparação curta com REST | Vantagens (HTTP/2, binário, tipagem, streaming) e limites (navegador precisa de gRPC-Web, payload não é legível, acoplamento temporal) |

## Conceitos-chave
- **RPC e transparência:** o stub esconde rede, serialização e transporte.
- **Protocol Buffers:** binário, compacto, tipado; campos identificados por número.
- **HTTP/2:** vários streams em uma conexão; base do streaming.
- **4 padrões de comunicação:** unary e 3 de streaming.
- **Evolução do contrato:** campo novo com número novo é compatível; reutilizar número quebra.
- **Deadlines** propagados entre serviços.

## Teoria (pontos de fala)
1. RPC: stubs, marshalling, e falhas parciais que a rede introduz.
2. Contrato forte (IDL) x contrato informal do REST.
3. Protobuf x JSON: tamanho, velocidade, legibilidade.
4. HTTP/2 x HTTP/1.1.
5. Comunicação síncrona: A e B precisam estar no ar ao mesmo tempo.
6. Onde gRPC brilha (comunicação entre serviços) e onde não (APIs públicas, navegador).

## Plano de implementação

### Estrutura da pasta
```
grpc/
  README.md
  pagamentos.proto        # contrato único das duas linguagens (comentado: numeração de campos, 4 tipos de RPC)
  gerar.sh                # protoc para Python: gera pagamentos_pb2.py, .pyi e pagamentos_pb2_grpc.py
  requirements.txt        # grpcio, grpcio-tools, grpcio-reflection
  servico_b/servidor.py   # servidor com os 4 métodos
  servico_a/cliente.py    # um subcomando por demo
  servico_a/<linguagem>/  # cliente na segunda linguagem, com o próprio build/dependências
```
O código gerado fica em `grpc/` e não vai para o git (`*_pb2*.py*` no `.gitignore`); `gerar.sh` recria.

### Etapa 1: contrato e geração
- `pagamentos.proto` com o contrato acima e comentários explicando cada tipo de RPC e a regra de nunca reutilizar o número de um campo.
- `gerar.sh`: `python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. pagamentos.proto` (o `.pyi` dá autocomplete no editor).
- **Pronto quando:** os 3 arquivos são gerados e `python -c "import pagamentos_pb2"` funciona de dentro de `grpc/`.

### Etapa 2: servidor (`servico_b/servidor.py`)
- `grpc.server(ThreadPoolExecutor(10))`, porta configurável (`--porta`, padrão 50051).
- **Estado:** lista em memória, protegida por `Lock` (há várias threads), já com 6 pagamentos de exemplo para as listagens terem conteúdo.
- **Regra de negócio simples:** recibo `aprovado` se valor ≤ 1000, senão `recusado`, para o bidirecional ter respostas variadas.
- **Autenticação por metadata:** um `_autenticar(context)` no começo de cada método compara `authorization` com `token-demo` e chama `context.abort(UNAUTHENTICATED, ...)`. Um interceptor seria mais elegante, mas exige um handler do tipo certo para cada método de streaming; para o estudo, a checagem por método é mais clara.
- **Validação:** `valor <= 0` ou tipo fora de `pix/cartao/boleto` → `context.abort(INVALID_ARGUMENT, mensagem)`.
- **Os 4 métodos:**
  - `Pagar`: valida, guarda, devolve o recibo.
  - `ListarPagamentos`: filtra por tipo (vazio = todos), faz `yield` de um item a cada 0.5 s e para se `context.is_active()` ficar falso (cliente cancelou ou o deadline estourou).
  - `PagarLote`: itera o `request_iterator`, loga cada item recebido e devolve **um** `Resumo` no final.
  - `PagarAoVivo`: para cada pagamento recebido, faz `yield` do recibo na hora.
- **Logs** com prefixo `[B]` em cada evento, para o vídeo mostrar os dois lados lado a lado.
- **Reflection:** `reflection.enable_server_reflection(...)`.
- **Pronto quando:** sobe e loga "ouvindo na porta 50051".

### Etapa 3: cliente Python (`servico_a/cliente.py`)
- Subcomandos: `unary`, `server`, `client`, `bidi`, `deadline`, `erro`, `sem-token` e `tudo`.
- `METADATA = (("authorization", "token-demo"),)` passado em toda chamada, exceto em `sem-token`.
- `client` e `bidi` usam geradores com `sleep` entre envios (0.4 s e 0.7 s), para o intercalamento aparecer na tela.
- Erros capturados com `grpc.RpcError`, imprimindo `e.code().name` e `e.details()`.
- **Pronto quando:** `python -m servico_a.cliente tudo` roda as 7 demos: unary aprovado; server stream só com os `pix`; client stream com resumo único; bidi intercalado com aprovado/recusado; deadline com alguns itens e depois `DEADLINE_EXCEEDED`; `INVALID_ARGUMENT`; `UNAUTHENTICATED`.

### Etapa 4: cliente na segunda linguagem (`servico_a/<linguagem>/`)
A linguagem é escolhida na hora. O plano fixa só o que o cliente precisa fazer; o resto depende da escolha.

**Requisitos, em qualquer linguagem:**
- Usar o `pagamentos.proto` da pasta `grpc/` sem copiar nem alterar. Se o contrato mudar, as duas linguagens mudam juntas.
- Enviar o metadata `authorization: token-demo`.
- Implementar no mínimo `unary`, `server` e `bidi`, com os mesmos nomes de subcomando do cliente Python. `client`, `deadline` e os erros são bônus.
- Logs com prefixo `[A/<linguagem>]`, no mesmo formato do Python.
- **Pronto quando:** o subcomando `tudo` roda contra o servidor Python com as mesmas saídas do cliente Python.

**Opções:**
| Linguagem | Como usa o `.proto` | Esforço | O que rende no vídeo |
|---|---|---|---|
| Node | `@grpc/grpc-js` + `@grpc/proto-loader` carregam o `.proto` em tempo de execução, sem geração | Baixo | "Sem build": o contrato é lido na hora |
| Go | `protoc` com os plugins `protoc-gen-go` e `protoc-gen-go-grpc` | Médio (instalar os plugins) | Stubs tipados; linguagem muito usada em microsserviços |
| Java | `protobuf-maven-plugin` ou `protobuf-gradle-plugin` | Alto (build mais pesado) | Stubs gerados no build; streaming com `StreamObserver` |

**Variação possível:** inverter os papéis, com o servidor na segunda linguagem e o cliente em Python. O `.proto` garante que qualquer combinação funciona; vale só para esse vídeo, não precisa refazer o plano.

### Etapa 5: `grpcurl` (opcional) e README
- `grpcurl -plaintext localhost:50051 list` e `describe pagamentos.ServicoB` mostram a reflection. Não vem instalado; pegar o binário da página de releases do projeto.
- README com comandos e a tabela "característica → demo".

### Armadilhas conhecidas
- **Rodar sempre de dentro de `grpc/`** e como módulo: `python -m servico_b.servidor`, `python -m servico_a.cliente tudo`. Assim os arquivos gerados são encontrados. Se rodar a partir da raiz do repo, a pasta `grpc/` pode **sombrear a biblioteca `grpc`** no `import grpc`.
- O código gerado depende da versão do `grpcio-tools`; depois de atualizar as dependências, rode `gerar.sh` de novo.

## Estado
Plano pronto; código não iniciado.
