# AGENTS.md

Go 1.26 binary (`module webhook-watcher`) that tails a MariaDB binlog and turns row changes into webhook events. Three packages: `main` (`.go` files at repo root), `config`, and `producer`.

## Commands

- Build/vet: `go build ./...` and `go vet ./...` — both pass.
- **Do not** build with `go build -o webhook-watcher main.go`. That compiles only `main.go` and fails with `undefined: newBinlogWatcher`, because `binlog.go` and `producer.go` are also `package main`. Build with `.` instead (the Dockerfile does this; it also needs `COPY . .` before building — it already has it).
- No tests exist; there is no test/lint CI.
- Run `gofmt -w` before committing edits to `config/config.go`.

## Runtime requirements

- DB credentials are stored in a local **SQLite** file (`servers.db`, configurable via `SQLITE_PATH`), table `binlog_servers`, loaded via `config.LoadServersFromDB()`. On first run, if the table is empty, `main.go` seeds a default server from `config.DefaultServerConfig()`. `main.go` calls `godotenv.Load()` first, so the seed (and only the seed) reads env vars from `.env` (`SERVER_ID`, `REPLICA_ID`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_FLAVOR`) with development defaults (localhost:3306, root/kodejifr, 'DB01'). `.env` and `servers.db` are gitignored; `.env.example` documents the vars. Only way to run: point at a MariaDB reachable from this host.
- Needs a user with replication privileges and `SHOW MASTER STATUS` access; server must have binlog enabled with `binlog_row_image=FULL` if old-row data matters.
- `binlog_servers.replica_id` is `UNIQUE` — it is the replication replica ID; two watchers with the same replica_id against the same source get rejected.
- The startup connection runs `SHOW MASTER STATUS`, takes the current file/pos, and streams from there (no resume-on-restart persistence).
- Comments and log output are in Portuguese — keep that style.

## Architecture

- Flow: `main.go` → `config.InitDB()` + `config.LoadServersFromDB()` → **one goroutine per server** → `BinlogWatcher.Start()` (`binlog.go`) → per-event `Producer.HandleEvent()` (`producer/`). `main.go` waits on a `sync.WaitGroup`; a failing server logs its error without killing the others.
- Logs are structured JSON via `slog` (JSON handler on stdout) so they're searchable in CloudWatch. `BinlogWatcher` carries a `server_id` attr (no `[DB01]` prefix anymore); binlog events carry `binlog_file`/`binlog_pos`; the producer logs processed events under an `evento` attr. `main.go` installs `dropMessageHandler` (logging.go) to filter the `create BinlogSyncer` line that go-mysql emits with a non-serializable config. Comments and log output are in Portuguese — keep that style.
- `binlog.go` listens in a loop with a 2s context timeout; `context.DeadlineExceeded` is expected and just `continue`s. `ev.Header.LogPos` is used to advance the position. It also handles `RotateEvent` (updates `pos.Name`/`pos.Pos` on file rollover) and guards `pos.Pos = ev.Header.LogPos` behind `ev != nil`.
- `producer/` (package `producer`) dispatches via **Strategy + Registry**: `Producer` (producer.go) holds `map[replication.EventType]EventStrategy`, `HandleEvent` routes each event type to its strategy; `EventStrategy` (producer.go) is the interface every strategy implements. `UpdateRowsStrategy` (update.go) handles UPDATE v1/v2 + MariaDB compressed; it embeds `RowsStrategy` (rows.go), which provides the shared ROWS logic (`eachRow` iterates rows with stride/offset per action; `buildEvent` extracts the resource ID and builds the event). Registering a new event type = add a map entry + new strategy file.
- UPDATE rows arrive as old/new pairs — stride 2, new row at `Rows[i+1]`; INSERT/DELETE would use stride 1, offset 0. `newRow[0]` is assumed to be an `int32` resource ID (non-`INT` columns are logged and skipped); `Table.Schema` = tenant, `Table.Table` = table.
- Event ID = `evt_` + SHA-256 (first 16 bytes hex) over `binlogFile:logPos:rowIndex:tenant:table:resourceID`.
- Known sharp edge: `generateEventID` relies on `rowIndex` being unique per event to guarantee distinct IDs for same-resource rows.

## Current state / next steps

- The producer **only prints events** to stdout today — no queue, no consumer, no HTTP. A consumer (enrich via DB queries + assemble webhook payload + HTTP request) is planned; recommended queue is Redis + asynq, with a MariaDB table-fila as the zero-infra alternative. Not yet implemented.
