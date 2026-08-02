# Webhook Watcher

Watcher de binlog do MariaDB em Go que transforma alterações de linhas em eventos de webhook.

O programa se conecta como um **replicador** ao MariaDB, lê o binlog em tempo real e, para cada `UPDATE` detectado, extrai o recurso afetado e gera um evento estruturado. Hoje o evento é apenas impresso no stdout; um consumer com fila e envio HTTP está no roadmap (ver [Próximos passos](#próximos-passos)).

## Como funciona

1. `config.LoadConfig()` carrega as credenciais (hardcoded) do banco.
2. `BinlogWatcher.Start()` conecta ao MariaDB e executa `SHOW MASTER STATUS` para obter a posição atual do binlog — o stream começa a partir daí (não há resume após restart).
3. Em um loop, cada evento do binlog é roteado pelo `Producer` para a **estratégia** correspondente ao tipo de evento.
4. Para `UPDATE`, as linhas chegam em pares old/new; a linha nova é usada para montar o evento (`resource_id`, tenant, tabela, timestamp) com um ID único.
5. O evento é impresso no stdout.

## Arquitetura

```
main.go → config.LoadConfig() → BinlogWatcher.Start() (binlog.go)
  → Producer.HandleEvent() (producer/) → EventStrategy (Strategy + Registry)
    → UpdateRowsStrategy → RowsStrategy (base compartilhada)
```

- **`main.go`** — entrypoint e wiring.
- **`binlog.go`** — conexão de replicação com o go-mysql, `SHOW MASTER STATUS`, loop de eventos e tratamento de `RotateEvent`.
- **`config/`** — `Config` com as credenciais do banco (hardcoded) e o flavor MariaDB.
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

As credenciais estão **hardcoded** em `config.LoadConfig()` (sem variáveis de ambiente):

```go
Host: "localhost", Port: 3306, User: "root", Password: "kodejifr", Flavor: MariaDB
```

Para apontar para outro servidor, edite `config/config.go` diretamente.

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
├── binlog.go               # watcher de binlog (package main)
├── main.go                 # entrypoint (package main)
├── config/config.go        # configuração/credenciais
├── producer/producer.go    # Producer, EventStrategy, Event, registry
├── producer/rows.go        # RowsStrategy (base de eventos ROWS)
├── producer/update.go      # UpdateRowsStrategy
└── go.mod                  # go-mysql-org/go-mysql v1.16.0
```

## Próximos passos

- **Fila + consumer**: hoje o produtor só imprime eventos. O plano é enfileirar os eventos e ter um consumer que:
  1. lê o evento da fila;
  2. enriquece com queries no banco;
  3. monta o payload do webhook;
  4. dispara o request HTTP com retry/backoff.
- **Fila recomendada**: Redis + [asynq](https://github.com/hibiken/asynq) (retry, backoff, dead-letter embutidos). Alternativa sem infra nova: tabela-fila no próprio MariaDB.
- **Novos tipos de evento**: INSERT/DELETE (reutilizando `RowsStrategy` com stride/offset próprios).
