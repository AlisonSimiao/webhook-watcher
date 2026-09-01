package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"webhook-watcher/config"
)

// runHubCommand executa um subcomando de configuração/diagnóstico da conexão
// com o hub (schema hub_<ambiente>, de onde vêm os destinos de webhook) e
// retorna o código de saída.
func runHubCommand(args []string) int {
	if len(args) == 0 {
		hubUsage()
		return 1
	}

	sub, rest := args[0], args[1:]
	switch sub {
	case "set":
		return hubSet(rest)
	case "show":
		return hubShow()
	case "query":
		return hubQuery(rest)
	default:
		hubUsage()
		return 1
	}
}

func hubUsage() {
	fmt.Println(`Uso: webhook-watcher hub <comando> [flags]

Comandos:
  set    Configura a conexão e a query de resolução de destino de webhook
  show   Mostra a configuração atual (nunca a senha)
  query  Roda a query configurada de verdade e valida a forma da resposta`)
}

func hubSet(args []string) int {
	fs := flag.NewFlagSet("set", flag.ExitOnError)
	host := fs.String("host", "", "endereço do MariaDB do hub")
	port := fs.Int("port", 3306, "porta (padrão 3306)")
	user := fs.String("user", "", "usuário")
	password := fs.String("password", "", "senha")
	schema := fs.String("schema", "", "schema do hub (ex: hub_producao)")
	query := fs.String("query", "", "query de resolução de destino (opcional; padrão: config.DefaultHookQuery)")
	fs.Parse(args)

	if *host == "" || *user == "" || *password == "" || *schema == "" {
		fs.Usage()
		return 1
	}

	db := openDB()
	defer db.Close()

	if err := config.SaveHubConfig(db, *host, *port, *user, *password, *schema, *query); err != nil {
		slog.Error("Erro ao salvar configuração do hub", "error", err)
		return 1
	}
	slog.Info("Configuração do hub salva", "host", *host, "schema", *schema)
	return 0
}

func hubShow() int {
	db := openDB()
	defer db.Close()

	cfg, err := config.LoadHubConfig(db)
	if errors.Is(err, config.ErrHubConfigNotSet) {
		fmt.Println("hub_config ainda não configurado. Rode: go run . hub set -host ... -user ... -password ... -schema ...")
		return 1
	}
	if err != nil {
		slog.Error("Erro ao carregar configuração do hub", "error", err)
		return 1
	}

	fmt.Printf("host:   %s\n", cfg.Host)
	fmt.Printf("port:   %d\n", cfg.Port)
	fmt.Printf("user:   %s\n", cfg.User)
	fmt.Printf("schema: %s\n", cfg.SchemaName)
	fmt.Printf("query:\n%s\n", cfg.HookQuery)
	return 0
}

func hubQuery(args []string) int {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	limit := fs.Int("limit", 20, "máximo de linhas exibidas")
	fs.Parse(args)

	sqliteDB := openDB()
	defer sqliteDB.Close()

	cfg, err := config.LoadHubConfig(sqliteDB)
	if errors.Is(err, config.ErrHubConfigNotSet) {
		fmt.Println("hub_config ainda não configurado. Rode: go run . hub set -host ... -user ... -password ... -schema ...")
		return 1
	}
	if err != nil {
		slog.Error("Erro ao carregar configuração do hub", "error", err)
		return 1
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.SchemaName)
	hubDB, err := sql.Open("mysql", dsn)
	if err != nil {
		slog.Error("Erro ao abrir conexão com o hub", "error", err)
		return 1
	}
	defer hubDB.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hubDB.PingContext(pingCtx); err != nil {
		slog.Error("Erro ao conectar no hub", "host", cfg.Host, "schema", cfg.SchemaName, "error", err)
		return 1
	}

	rows, err := hubDB.QueryContext(context.Background(), cfg.HookQuery)
	if err != nil {
		slog.Error("Erro ao rodar a query configurada", "error", err)
		return 1
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		slog.Error("Erro ao ler colunas da query", "error", err)
		return 1
	}
	ok, msg := checkHookColumns(cols)
	fmt.Println(msg)
	if !ok {
		// Ainda tenta mostrar as linhas — útil pra diagnosticar mesmo com a
		// forma errada — mas o Scan abaixo só funciona se vierem 3 colunas.
		if len(cols) != 3 {
			return 1
		}
	}

	fmt.Printf("%-30s %-15s %s\n", "tenant", "tipo", "url")
	n := 0
	for rows.Next() && n < *limit {
		var tenant, tipo, url string
		if err := rows.Scan(&tenant, &tipo, &url); err != nil {
			slog.Error("Erro ao ler linha", "error", err)
			return 1
		}
		fmt.Printf("%-30s %-15s %s\n", tenant, tipo, url)
		n++
	}
	if err := rows.Err(); err != nil {
		slog.Error("Erro ao iterar resultado", "error", err)
		return 1
	}
	if n == 0 {
		fmt.Println("(nenhuma linha)")
	}
	return 0
}

// checkHookColumns confere se as colunas devolvidas pela query configurada
// batem com o esperado (tenant, tipo, url, nessa contagem — nomes conferidos
// case-insensitive, mas contagem errada já é motivo de aviso mesmo se os
// nomes não puderem ser comparados).
func checkHookColumns(cols []string) (ok bool, msg string) {
	want := []string{"tenant", "tipo", "url"}
	if len(cols) != len(want) {
		return false, fmt.Sprintf("⚠ esperava %d colunas (%s), veio %d: %v", len(want), strings.Join(want, ", "), len(cols), cols)
	}
	for i, w := range want {
		if !strings.EqualFold(cols[i], w) {
			return false, fmt.Sprintf("⚠ esperava colunas (%s) nessa ordem, veio %v", strings.Join(want, ", "), cols)
		}
	}
	return true, fmt.Sprintf("✓ colunas OK: %s", strings.Join(want, ", "))
}
