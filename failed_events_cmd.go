package main

import (
	"flag"
	"fmt"
	"log/slog"
)

// runFailedEventsCommand executa um subcomando de consulta a eventos que
// falharam ao enfileirar (tabela failed_events) e retorna o código de saída.
func runFailedEventsCommand(args []string) int {
	if len(args) == 0 {
		failedEventsUsage()
		return 1
	}

	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return failedEventsList(rest)
	case "remove":
		return failedEventsRemove(rest)
	default:
		failedEventsUsage()
		return 1
	}
}

func failedEventsUsage() {
	fmt.Println(`Uso: webhook-watcher failed-events <comando> [flags]

Comandos:
  list    Lista eventos que falharam ao enfileirar
  remove  Remove um evento da tabela (após investigação manual)`)
}

func failedEventsList(args []string) int {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	serverID := fs.String("server-id", "", "filtra por servidor (opcional)")
	limit := fs.Int("limit", 50, "máximo de linhas")
	fs.Parse(args)

	db := openDB()
	defer db.Close()

	query := `SELECT id, event_id, server_id, tenant, table_name, action, error, created_at FROM failed_events`
	var queryArgs []interface{}
	if *serverID != "" {
		query += ` WHERE server_id = ?`
		queryArgs = append(queryArgs, *serverID)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	queryArgs = append(queryArgs, *limit)

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		slog.Error("Erro ao consultar failed_events", "error", err)
		return 1
	}
	defer rows.Close()

	fmt.Printf("%-5s %-40s %-10s %-14s %-10s %-8s %-30s %s\n", "id", "event_id", "server_id", "tenant", "table", "action", "error", "created_at")
	for rows.Next() {
		var id int
		var eventID, srvID, tenant, table, action, errMsg, createdAt string
		if err := rows.Scan(&id, &eventID, &srvID, &tenant, &table, &action, &errMsg, &createdAt); err != nil {
			slog.Error("Erro ao ler linha", "error", err)
			return 1
		}
		fmt.Printf("%-5d %-40s %-10s %-14s %-10s %-8s %-30s %s\n", id, eventID, srvID, tenant, table, action, errMsg, createdAt)
	}
	if err := rows.Err(); err != nil {
		slog.Error("Erro ao iterar failed_events", "error", err)
		return 1
	}
	return 0
}

func failedEventsRemove(args []string) int {
	fs := flag.NewFlagSet("remove", flag.ExitOnError)
	id := fs.Int("id", 0, "id da linha em failed_events a remover")
	fs.Parse(args)
	if *id == 0 {
		fs.Usage()
		return 1
	}

	db := openDB()
	defer db.Close()

	res, err := db.Exec(`DELETE FROM failed_events WHERE id = ?`, *id)
	if err != nil {
		slog.Error("Erro ao remover evento", "id", *id, "error", err)
		return 1
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		slog.Warn("Nenhum evento encontrado", "id", *id)
		return 1
	}
	slog.Info("Evento removido de failed_events", "id", *id)
	return 0
}
