package main

import (
	"flag"
	"fmt"
	"log/slog"
)

// runFailedDeliveriesCommand executa um subcomando de consulta a entregas de
// webhook que falharam definitivamente (tabela failed_deliveries) e retorna
// o código de saída.
func runFailedDeliveriesCommand(args []string) int {
	if len(args) == 0 {
		failedDeliveriesUsage()
		return 1
	}

	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return failedDeliveriesList(rest)
	case "remove":
		return failedDeliveriesRemove(rest)
	default:
		failedDeliveriesUsage()
		return 1
	}
}

func failedDeliveriesUsage() {
	fmt.Println(`Uso: webhook-watcher failed-deliveries <comando> [flags]

Comandos:
  list    Lista entregas de webhook que falharam definitivamente
  remove  Remove uma linha da tabela (após investigação manual)`)
}

func failedDeliveriesList(args []string) int {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	tenant := fs.String("tenant", "", "filtra por tenant (opcional)")
	limit := fs.Int("limit", 50, "máximo de linhas")
	fs.Parse(args)

	db := openDB()
	defer db.Close()

	query := `SELECT id, event_id, tenant, table_name, action, url, error, created_at FROM failed_deliveries`
	var queryArgs []interface{}
	if *tenant != "" {
		query += ` WHERE tenant = ?`
		queryArgs = append(queryArgs, *tenant)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	queryArgs = append(queryArgs, *limit)

	rows, err := db.Query(query, queryArgs...)
	if err != nil {
		slog.Error("Erro ao consultar failed_deliveries", "error", err)
		return 1
	}
	defer rows.Close()

	fmt.Printf("%-5s %-40s %-14s %-10s %-8s %-30s %-30s %s\n", "id", "event_id", "tenant", "table", "action", "url", "error", "created_at")
	for rows.Next() {
		var id int
		var eventID, tenantVal, table, action, url, errMsg, createdAt string
		if err := rows.Scan(&id, &eventID, &tenantVal, &table, &action, &url, &errMsg, &createdAt); err != nil {
			slog.Error("Erro ao ler linha", "error", err)
			return 1
		}
		fmt.Printf("%-5d %-40s %-14s %-10s %-8s %-30s %-30s %s\n", id, eventID, tenantVal, table, action, url, errMsg, createdAt)
	}
	if err := rows.Err(); err != nil {
		slog.Error("Erro ao iterar failed_deliveries", "error", err)
		return 1
	}
	return 0
}

func failedDeliveriesRemove(args []string) int {
	fs := flag.NewFlagSet("remove", flag.ExitOnError)
	id := fs.Int("id", 0, "id da linha em failed_deliveries a remover")
	fs.Parse(args)
	if *id == 0 {
		fs.Usage()
		return 1
	}

	db := openDB()
	defer db.Close()

	res, err := db.Exec(`DELETE FROM failed_deliveries WHERE id = ?`, *id)
	if err != nil {
		slog.Error("Erro ao remover entrega", "id", *id, "error", err)
		return 1
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		slog.Warn("Nenhuma entrega encontrada", "id", *id)
		return 1
	}
	slog.Info("Entrega removida de failed_deliveries", "id", *id)
	return 0
}
