package main

import (
	"log/slog"
	"os"
	"sync"

	"webhook-watcher/config"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	// Logs em JSON estruturado para facilitar busca no CloudWatch; descarta
	// o log "create BinlogSyncer" da go-mysql (config contém funções e não é
	// serializável para JSON).
	base := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(slog.New(&dropMessageHandler{
		next:    base.Handler(),
		dropped: map[string]bool{"create BinlogSyncer": true},
	}))

	if len(os.Args) > 1 && os.Args[1] == "server" {
		os.Exit(runServerCommand(os.Args[2:]))
	}

	slog.Info("Iniciando Webhook Watcher")

	db, err := config.InitDB(sqlitePath())
	if err != nil {
		slog.Error("Erro ao inicializar banco SQLite", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	servers, err := config.LoadServersFromDB(db)
	if err != nil {
		slog.Error("Erro ao carregar servidores", "error", err)
		os.Exit(1)
	}

	if len(servers) == 0 {
		seed := config.DefaultServerConfig()
		res, err := db.Exec(`INSERT INTO binlog_servers (server_id, replica_id, host, port, user, password, flavor) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			seed.ServerID, seed.ReplicaID, seed.Host, seed.Port, seed.User, seed.Password, seed.Flavor)
		if err != nil {
			slog.Error("Erro ao inserir servidor padrão", "error", err)
			os.Exit(1)
		}
		if id, err := res.LastInsertId(); err == nil {
			seed.ID = uint64(id)
		}
		slog.Info("Nenhum servidor cadastrado; servidor padrão criado", "server_id", seed.ServerID)
		servers = []config.ServerConfig{seed}
	}

	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Add(1)
		go func(cfg config.ServerConfig) {
			defer wg.Done()
			if err := newBinlogWatcher(cfg, slog.Default()).Start(); err != nil {
				slog.Error("Servidor encerrado com erro", "server_id", cfg.ServerID, "error", err)
			}
		}(s)
	}
	wg.Wait()
}
