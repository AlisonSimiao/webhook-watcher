package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"webhook-watcher/config"
)

func sqlitePath() string {
	if dbPath := os.Getenv("SQLITE_PATH"); dbPath != "" {
		return dbPath
	}
	return "servers.db"
}

func openDB() *sql.DB {
	db, err := config.InitDB(sqlitePath())
	if err != nil {
		slog.Error("Erro ao abrir banco SQLite", "error", err)
		os.Exit(1)
	}
	return db
}

// httpShardCount lê HTTP_CONSUMER_SHARDS. Precisa ser o mesmo valor no
// processo produtor (initQueue, main.go) e no consumer (runHTTPConsumer,
// consumer_cmd.go) — um valor divergente faz eventos roteados para um shard
// "extra" ficarem parados sem erro visível.
func httpShardCount() int {
	raw := config.GetEnv("HTTP_CONSUMER_SHARDS", "16")
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		slog.Warn("HTTP_CONSUMER_SHARDS inválido; usando default 16", "valor", raw)
		return 16
	}
	return n
}

// runServerCommand executa um subcomando de gerenciamento de servidores e
// retorna o código de saída.
func runServerCommand(args []string) int {
	if len(args) == 0 {
		serverUsage()
		return 1
	}

	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return serverAdd(rest)
	case "list":
		return serverList()
	case "update":
		return serverUpdate(rest)
	case "remove":
		return serverRemove(rest)
	default:
		serverUsage()
		return 1
	}
}

func serverUsage() {
	fmt.Println(`Uso: webhook-watcher server <comando> [flags]

Comandos:
  add     Adiciona um novo servidor
  list    Lista os servidores cadastrados
  update  Atualiza um servidor existente
  remove  Desativa um servidor (is_active = 0)`)
}

func serverAdd(args []string) int {
	fs := flag.NewFlagSet("add", flag.ExitOnError)
	serverID := fs.String("id", "", "identificador lógico do servidor (ex: DB02)")
	replicaID := fs.Int("replica-id", 0, "ID de réplica do binlog (único)")
	host := fs.String("host", "", "endereço do MariaDB")
	port := fs.Int("port", 3306, "porta (padrão 3306)")
	user := fs.String("user", "", "usuário")
	password := fs.String("password", "", "senha")
	flavor := fs.String("flavor", "mariadb", "flavor (mariadb/mysql)")
	fs.Parse(args)

	if *serverID == "" || *replicaID == 0 || *host == "" || *user == "" || *password == "" {
		fs.Usage()
		return 1
	}

	db := openDB()
	defer db.Close()

	res, err := db.Exec(`INSERT INTO binlog_servers (server_id, replica_id, host, port, user, password, flavor) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		*serverID, *replicaID, *host, *port, *user, *password, *flavor)
	if err != nil {
		slog.Error("Erro ao inserir servidor", "error", err)
		return 1
	}
	id, _ := res.LastInsertId()
	slog.Info("Servidor inserido", "server_id", *serverID, "id", id)
	return 0
}

func serverList() int {
	db := openDB()
	defer db.Close()

	const query = `
SELECT id, server_id, replica_id, host, port, user, flavor, is_active, binlog_file, binlog_pos
FROM binlog_servers
ORDER BY id`

	rows, err := db.Query(query)
	if err != nil {
		slog.Error("Erro ao consultar servidores", "error", err)
		return 1
	}
	defer rows.Close()

	fmt.Printf("%-3s %-10s %-10s %-16s %-5s %-12s %-9s %-6s %-20s %s\n", "id", "server_id", "replica_id", "host", "port", "user", "flavor", "ativo", "binlog_file", "binlog_pos")
	for rows.Next() {
		var id, port, replicaID, binlogPos int
		var serverID, host, user, flavor, binlogFile string
		var active bool
		if err := rows.Scan(&id, &serverID, &replicaID, &host, &port, &user, &flavor, &active, &binlogFile, &binlogPos); err != nil {
			slog.Error("Erro ao ler linha de servidor", "error", err)
			return 1
		}
		fmt.Printf("%-3d %-10s %-10d %-16s %-5d %-12s %-9s %-6t %-20s %d\n", id, serverID, replicaID, host, port, user, flavor, active, binlogFile, binlogPos)
	}
	if err := rows.Err(); err != nil {
		slog.Error("Erro ao iterar servidores", "error", err)
		return 1
	}
	return 0
}

func serverUpdate(args []string) int {
	fs := flag.NewFlagSet("update", flag.ExitOnError)
	serverID := fs.String("id", "", "identificador lógico do servidor a atualizar")
	replicaID := fs.Int("replica-id", 0, "novo ID de réplica")
	host := fs.String("host", "", "novo host")
	port := fs.Int("port", 0, "nova porta")
	user := fs.String("user", "", "novo usuário")
	password := fs.String("password", "", "nova senha")
	flavor := fs.String("flavor", "", "novo flavor")
	active := fs.Int("is-active", -1, "ativar (1) ou desativar (0)")
	fs.Parse(args)

	if *serverID == "" {
		fs.Usage()
		return 1
	}

	var sets []string
	var vals []interface{}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "replica-id":
			sets = append(sets, "replica_id = ?")
			vals = append(vals, *replicaID)
		case "host":
			sets = append(sets, "host = ?")
			vals = append(vals, *host)
		case "port":
			sets = append(sets, "port = ?")
			vals = append(vals, *port)
		case "user":
			sets = append(sets, "user = ?")
			vals = append(vals, *user)
		case "password":
			sets = append(sets, "password = ?")
			vals = append(vals, *password)
		case "flavor":
			sets = append(sets, "flavor = ?")
			vals = append(vals, *flavor)
		case "is-active":
			sets = append(sets, "is_active = ?")
			vals = append(vals, *active)
		}
	})
	if len(sets) == 0 {
		slog.Warn("Nenhum campo informado para atualizar")
		return 1
	}

	query := fmt.Sprintf("UPDATE binlog_servers SET %s, updated_at = CURRENT_TIMESTAMP WHERE server_id = ?", strings.Join(sets, ", "))
	vals = append(vals, *serverID)

	db := openDB()
	defer db.Close()

	res, err := db.Exec(query, vals...)
	if err != nil {
		slog.Error("Erro ao atualizar servidor", "server_id", *serverID, "error", err)
		return 1
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		slog.Warn("Nenhum servidor encontrado", "server_id", *serverID)
		return 1
	}
	slog.Info("Servidor atualizado", "server_id", *serverID)
	return 0
}

func serverRemove(args []string) int {
	fs := flag.NewFlagSet("remove", flag.ExitOnError)
	serverID := fs.String("id", "", "identificador lógico do servidor a desativar")
	fs.Parse(args)

	if *serverID == "" {
		fs.Usage()
		return 1
	}

	db := openDB()
	defer db.Close()

	res, err := db.Exec(`UPDATE binlog_servers SET is_active = 0, updated_at = CURRENT_TIMESTAMP WHERE server_id = ?`, *serverID)
	if err != nil {
		slog.Error("Erro ao desativar servidor", "server_id", *serverID, "error", err)
		return 1
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		slog.Warn("Nenhum servidor encontrado", "server_id", *serverID)
		return 1
	}
	slog.Info("Servidor desativado", "server_id", *serverID)
	return 0
}
