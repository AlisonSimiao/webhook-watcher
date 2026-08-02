# Webhook Watcher

Watcher de binlog do MariaDB em Go que transforma alterações de linhas em eventos de webhook.

O programa se conecta como um **replicador** ao MariaDB, lê o binlog em tempo real e, para cada `UPDATE` detectado, extrai o recurso afetado e gera um evento estruturado. Hoje o evento é apenas impresso no stdout; um consumer com fila e envio HTTP está no roadmap (ver [Próximos passos](#próximos-passos)).

## Como funciona

1. `config.InitDB()` abre/cria o SQLite (`servers.db` por padrão, configurável via `SQLITE_PATH`) e garante o esquema `binlog_servers`. Se não houver servidores cadastrados, um servidor padrão é semeado a partir do `.env` (ou valores de desenvolvimento).
2. `config.LoadServersFromDB()` carrega os servidores ativos (`is_active = 1`).
3. Uma **goroutine por servidor** roda `BinlogWatcher.Start()`, que conecta ao MariaDB e executa `SHOW MASTER STATUS` para obter a posição atual do binlog — o stream começa a partir daí (não há resume após restart).
4. Em um loop, cada evento do binlog é roteado pelo `Producer` para a **estratégia** correspondente ao tipo de evento.
5. Para `UPDATE`, as linhas chegam em pares old/new; a linha nova é usada para montar o evento (`resource_id`, tenant, tabela, timestamp) com um ID único.
6. O evento é emitido como uma linha de log estruturada em JSON (campo `evento`).

## Logs estruturados (CloudWatch)

Todos os logs são **JSON estruturado** via `slog`, uma linha por registro, com `time`, `level`, `msg` e atributos pesquisáveis — ideal para CloudWatch Logs Insights:

```json
{"time":"2026-08-02T01:37:19.029Z","level":"INFO","msg":"Evento processado","server_id":"DB01","evento":{"id":"evt_abc","resource_id":123,"tenant":"meu_tenant","action":"UPDATE","table":"clientes","timestamp":1780000000}}
```

Exemplos de consultas:

```sql
-- eventos de um servidor específico
filter server_id = 'DB01'
-- erros
filter level = 'ERROR'
-- posições de binlog
filter msg like /binlog/
```

O log `create BinlogSyncer` da go-mysql (config não serializável) é descartado pelo `dropMessageHandler`.

## Arquitetura

```
main.go → config.InitDB() + LoadServersFromDB() → goroutine por servidor
  → BinlogWatcher.Start() (binlog.go) → Producer.HandleEvent() (producer/)
    → EventStrategy (Strategy + Registry) → UpdateRowsStrategy → RowsStrategy
```

- **`main.go`** — entrypoint e wiring: inicializa o SQLite, carrega os servidores, dispara uma goroutine por servidor e aguarda com `sync.WaitGroup`. Erros de um servidor são logados sem derrubar os demais.
- **`binlog.go`** — conexão de replicação com o go-mysql, `SHOW MASTER STATUS`, loop de eventos e tratamento de `RotateEvent`. Todas as mensagens levam o prefixo `[server_id]`.
- **`config/`** — `ServerConfig` (credenciais + `ReplicaID` do binlog), `InitDB`, `LoadServersFromDB` e `DefaultServerConfig` (servidor de seed). Driver SQLite CGO-free (`modernc.org/sqlite`).
- **`producer/`** — `Producer` com um registro `map[EventType]EventStrategy`. Adicionar novo tipo de evento = nova estratégia + entrada no mapa, sem tocar no dispatcher. `UpdateRowsStrategy` (update.go) trata UPDATE v1/v2 + compactado MariaDB e reutiliza a base `RowsStrategy` (rows.go), que contém a iteração de linhas (`eachRow`) e a montagem do evento (`buildEvent`).

**Evento gerado** (impresso como JSON):

```json
{
  "id": "evt_ab12cd34...",
  "resource_id": 123,
  "tenant": "meu_tenant",
  "action": "UPDATE",
  "table": "clientes",
  "timestamp": 1780000000
}
```

O ID é `evt_` + SHA-256 (16 bytes hex) sobre `binlogFile:logPos:rowIndex:tenant:table:resourceID`.

## Pré-requisitos

- Go 1.26.
- MariaDB acessível a partir da máquina onde o watcher roda, com **binlog habilitado**:

```ini
# my.cnf
[mysqld]
log-bin=mysql-bin
binlog_row_image=FULL   # necessário se dados antigos importarem
server-id=1
```

- Um usuário com privilégios de replicação e acesso a `SHOW MASTER STATUS`:

```sql
CREATE USER 'watcher'@'%' IDENTIFIED BY 'senha';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'watcher'@'%';
```

O binlog começa a ser lido da posição atual em `SHOW MASTER STATUS`; se o binlog estiver desabilitado, o watcher falha com erro explícito.

## Configuração

Os servidores a escutar ficam cadastrados na tabela `binlog_servers` do SQLite (arquivo `servers.db` na raiz, configurável via env `SQLITE_PATH`). Na primeira execução, se a tabela estiver vazia, é criado um servidor padrão cujas credenciais vêm de variáveis de ambiente — carregadas do arquivo `.env` na raiz (veja `.env.example`) quando existir, com os valores de desenvolvimento como fallback:

```ini
# .env
SERVER_ID=DB01
REPLICA_ID=100
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=kodejifr
DB_FLAVOR=mariadb
```

`replica_id` é `UNIQUE` — o ID de réplica do binlog precisa ser único por servidor escutado (dois watchers com o mesmo ID contra o mesmo MariaDB são rejeitados pelo source). Para cadastrar outro servidor, insira na tabela e reinicie:

```sql
INSERT INTO binlog_servers (server_id, replica_id, host, port, user, password, flavor)
VALUES ('DB02', 101, '192.168.1.10', 3306, 'watcher', 'senha', 'mariadb');
```

Para desativar sem apagar: `UPDATE binlog_servers SET is_active = 0 WHERE server_id = 'DB02';`

## Comandos de servidor

Em vez de editar o SQLite na mão, use o subcomando `server` (não inicia o watcher):

```bash
# adicionar um servidor
go run . server add -id DB02 -replica-id 101 -host 192.168.1.10 -user watcher -password senha -port 3306

# listar servidores cadastrados (inclui inativos)
go run . server list

# atualizar campos informados (só os passados são alterados)
go run . server update -id DB02 -host 192.168.1.11 -port 3307

# desativar um servidor (is_active = 0)
go run . server remove -id DB02
```

O `replica-id` é único: adicionar um servidor com `replica_id` ou `server_id` já existente falha com erro de constraint.

## Como rodar

```bash
go build ./...
go vet ./...
go run .
```

> Use `go build .` ou `go build ./...` para compilar. `go build -o webhook-watcher main.go` compila apenas `main.go` e falha com `undefined: newBinlogWatcher`, pois `binlog.go` e `producer.go` também são `package main`.

### Docker

O `DockerFile` usa o estágio builder com `go build -o webhook-watcher .` e produz uma imagem `alpine` final enxuta:

```bash
docker build -f DockerFile -t webhook-watcher .
```

## Estrutura do projeto

```
.
├── .env.example            # variáveis do servidor padrão (copie para .env)
├── binlog.go               # watcher de binlog (package main)
├── logging.go              # dropMessageHandler (filtra logs não serializáveis)
├── main.go                 # entrypoint (package main)
├── server_cmd.go           # subcomandos server add/list/update/remove
├── config/config.go        # ServerConfig, InitDB, LoadServersFromDB (SQLite)
├── producer/producer.go    # Producer, EventStrategy, Event, registry
├── producer/rows.go        # RowsStrategy (base de eventos ROWS)
├── producer/update.go      # UpdateRowsStrategy
├── servers.db              # SQLite com binlog_servers (criado na 1ª execução)
└── go.mod                  # go-mysql-org/go-mysql, modernc.org/sqlite
```

## Próximos passos

- **Fila + consumer**: hoje o produtor só imprime eventos. O plano é enfileirar os eventos e ter um consumer que:
  1. lê o evento da fila;
  2. enriquece com queries no banco;
  3. monta o payload do webhook;
  4. dispara o request HTTP com retry/backoff.
- **Fila recomendada**: Redis + [asynq](https://github.com/hibiken/asynq) (retry, backoff, dead-letter embutidos). Alternativa sem infra nova: tabela-fila no próprio MariaDB.
- **Novos tipos de evento**: INSERT/DELETE (reutilizando `RowsStrategy` com stride/offset próprios).
