# REST

Estudo do REST como estilo de comunicação síncrona sobre HTTP, com demonstração de até 5 min.

## Cenário
- **Serviço B** (API, Python com FastAPI) guarda pagamentos em SQLite (arquivo), para que duas instâncias compartilhem o estado. FastAPI gera o Swagger em `/docs`.
- **Serviço A** (cliente) chama B com `curl` (HTTP cru), Python (`httpx`) e uma **segunda linguagem escolhida na hora da implementação** (opções na etapa 4).

## Endpoints
| Método | URI | Ação | Sucesso | Erros |
|---|---|---|---|---|
| GET | `/pagamentos?tipo=` | Listar, com filtro | 200 | 422 (tipo inválido) |
| GET | `/pagamentos/{id}` | Consultar | 200 / 304 | 404 |
| POST | `/pagamentos` | Criar | 201 + `Location` | 422 |
| PUT | `/pagamentos/{id}` | Substituir | 200 | 404, 422 |
| DELETE | `/pagamentos/{id}` | Remover | 204 | 404 |

## Características a demonstrar
| Característica | Demo | O que aparece na tela |
|---|---|---|
| Recursos e URIs | `/pagamentos` e `/pagamentos/1` | URI é substantivo; a ação vem do verbo |
| Verbos e status codes | CRUD com `curl -i` | Linha de status e cabeçalhos |
| Stateless | Duas instâncias de B (portas diferentes) atendendo A | Qualquer instância responde; não há sessão |
| Idempotência | `PUT`/`DELETE` repetidos dão o mesmo estado; `POST` repetido duplica | Diferença visível na listagem |
| Cache condicional | `GET` devolve `ETag`; repetir com `If-None-Match` | `304 Not Modified` sem corpo |
| Erros | Valor inválido, id inexistente | 422/404 com corpo JSON |
| Contrato documentado | `/docs` | Swagger gerado e testável |
| Interoperabilidade | Cliente na segunda linguagem | Mesma API, outra linguagem, só HTTP + JSON |

## Roteiro do vídeo (duração alvo: ~4 min; não precisa preencher os 5)
| Tempo | Cena | Mostra | Fala |
|---|---|---|---|
| 0:00-0:35 | Intro | Diagrama A → HTTP → B | REST é estilo arquitetural (Fielding), não protocolo; restrições: cliente-servidor, stateless, cache, interface uniforme, camadas |
| 0:35-1:40 | CRUD | `curl -i` com `POST` (201 + `Location`), `GET`, `PUT`, `DELETE` (204), 404 e 422 | Recursos são substantivos, a ação vem do verbo; status codes por família |
| 1:40-2:05 | Stateless | Duas instâncias de B atendendo A alternadamente | Sem sessão no servidor → escala horizontal atrás de um load balancer |
| 2:05-2:30 | Idempotência | `PUT`/`DELETE` repetidos x `POST` repetido duplicando | Métodos seguros e idempotentes; importância para retries em rede não confiável |
| 2:30-2:55 | Cache | `ETag` e `If-None-Match` → 304 | Cache HTTP nativo, uma vantagem que gRPC e mensageria não têm de graça |
| 2:55-3:30 | Interoperabilidade | Cliente Python e cliente na segunda linguagem; Swagger em `/docs` | HTTP + JSON é o denominador comum; OpenAPI como contrato |
| 3:30-4:00 | Fechamento | Mapa dos paradigmas do [PLANO.md](PLANO.md) | Vantagens (simples, universal, navegador) e limites (síncrono, texto); REST x gRPC x mensageria |

## Conceitos-chave
- **Restrições REST:** cliente-servidor, stateless, cache, interface uniforme, sistema em camadas.
- **Métodos seguros x idempotentes:** `GET` é seguro e idempotente; `PUT`/`DELETE` são idempotentes; `POST` não é nenhum dos dois.
- **Status codes:** 2xx sucesso, 3xx redirecionamento/cache, 4xx erro do cliente, 5xx erro do servidor.
- **Modelo de maturidade de Richardson:** a API fica no nível 2.

## Teoria (pontos de fala)
1. REST é estilo, não protocolo; usa HTTP como protocolo de aplicação.
2. Stateless facilita escalar horizontalmente.
3. Idempotência e retries em rede não confiável.
4. Síncrono request/response: A fica esperando B.
5. REST x gRPC (texto x binário, contrato informal x `.proto`) e REST x mensageria (síncrono x assíncrono).

## Plano de implementação

### Estrutura da pasta
```
rest/
  README.md             # como rodar e o que cada parte mostra
  requirements.txt      # fastapi, uvicorn, httpx
  servico_b/api.py      # API FastAPI
  servico_a/cliente.py  # cliente Python: percorre todas as características
  servico_a/<linguagem>/ # cliente na segunda linguagem: subconjunto, para provar interoperabilidade
  demo.sh               # curl -i passo a passo (enter avança), usado no vídeo
```
Python compartilhado na raiz do repo (`.venv`). Tudo roda de dentro de `rest/`.

### Etapa 1: API (`servico_b/api.py`)
- **Modelos Pydantic:** `PagamentoEntrada { valor: float > 0, tipo: Literal["pix","cartao","boleto"] }` e `Pagamento { id, valor, tipo }`, declarando `id` primeiro para todas as respostas terem a mesma ordem de campos.
- **Persistência:** SQLite em `rest/pagamentos.db`, com uma conexão por operação (`closing` + `with con` para commit). A tabela é criada no import. O arquivo entra no `.gitignore`.
- **Instância:** variável de ambiente `INSTANCIA` (B1, B2) e um middleware que devolve o cabeçalho `X-Instancia` em toda resposta, como evidência do stateless.
- **Endpoints:** os da tabela acima. `POST` com `status_code=201` e cabeçalho `Location: /pagamentos/{id}`. `PUT` e `DELETE` chamam `buscar()` antes, que lança 404.
- **ETag:** `sha1` do JSON do pagamento (`sort_keys=True`), entre aspas. Se `If-None-Match` for igual, responde `304` sem corpo. Na resposta normal, `ETag` + `Cache-Control: no-cache` (o cliente sempre revalida, o que deixa o 304 visível).
- **Erros padronizados:** handler para `starlette.exceptions.HTTPException` (pega também rota inexistente e 405) e para `RequestValidationError`, ambos devolvendo `{ "erro": <status>, "detalhe": ... }`. No 422, `detalhe` é uma lista `"campo: mensagem"`.
- **Pronto quando:** `uvicorn servico_b.api:app --port 8000` sobe e `/docs` responde 200.

### Etapa 2: `demo.sh`
- `curl -si` em cada passo: POST → GET lista → GET com ETag → GET com `If-None-Match` (304) → PUT → DELETE (204) → GET (404) → POST inválido (422).
- `read -rp` entre os passos, para controlar o ritmo durante a gravação.
- O id do recurso criado vem da listagem e a ETag do cabeçalho (`grep -i '^etag:'` + `tr -d '\r'`, porque o HTTP usa CRLF).
- **Pronto quando:** `yes "" | ./demo.sh` percorre tudo com os status esperados.

### Etapa 3: `cliente.py` (Serviço A)
- `httpx` com `--instancias` (padrão `:8000` e `:8001`) e `itertools.cycle` alternando a instância a cada requisição.
- Cada chamada imprime método, caminho, status, quem atendeu (`X-Instancia`) e corpo.
- Blocos na ordem do roteiro: criar, listar/filtrar, ETag/304, PUT repetido, POST repetido (duplica), DELETE repetido (204 depois 404), erros.
- **Pronto quando:** com B1 e B2 no ar, a saída mostra as requisições se alternando entre as duas instâncias, com os dados sempre consistentes.

### Etapa 4: cliente na segunda linguagem (`servico_a/<linguagem>/`)
A linguagem é escolhida na hora. O plano fixa só o que o cliente precisa fazer.

**Requisitos, em qualquer linguagem:**
- Fluxo: POST → GET pela `Location` devolvida → PUT → GET filtrado (`?tipo=`) → DELETE.
- Receber a URL base por argumento, para apontar para B1 ou B2.
- Imprimir método, caminho, status, `X-Instancia` e corpo, no mesmo formato do cliente Python, com prefixo `[A/<linguagem>]`.
- Só HTTP + JSON, sem nenhum código compartilhado com a API: é isso que prova a interoperabilidade.
- **Pronto quando:** roda sem erro contra a API Python, apontando para qualquer uma das instâncias.

**Opções:**
| Linguagem | Cliente HTTP / JSON | Esforço | O que rende no vídeo |
|---|---|---|---|
| Node | `fetch` nativo (Node 18+) e `JSON` | Baixo, sem dependências | Script curto, fácil de ler na tela |
| Go | `net/http` e `encoding/json` da biblioteca padrão | Baixo a médio | Structs tipadas mapeando o JSON |
| Java | `java.net.http.HttpClient` (Java 11+) com Jackson ou Gson | Médio (precisa de build para a lib de JSON) | Contraste com uma linguagem fortemente tipada e verbosa |

### Etapa 5: README da pasta
Comandos de preparo e execução e uma tabela "característica → onde aparece".

### Armadilhas conhecidas
- Dados em memória quebram a demo stateless: cada processo teria a sua lista. Por isso o SQLite.
- `pkill -f uvicorn...` num script pode matar o próprio shell, se o padrão aparecer no comando. Prefira guardar o PID (`$!`) e usar `kill`.
- Rode o `uvicorn` de dentro de `rest/` para o módulo `servico_b.api` ser encontrado.

## Estado
Plano pronto; código não iniciado.
