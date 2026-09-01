package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"webhook-watcher/config"
	"webhook-watcher/consumer/webhook"
	"webhook-watcher/pkg/queue"
)

const hookCacheRefreshInterval = 5 * time.Minute

// runConsumerCommand despacha os subcomandos "consumer <tipo>". Hoje só
// "http" existe; um novo tipo de consumer (sse, notify) entraria aqui como
// um novo case, cada um com seu próprio *RedisWorker apontando para sua
// própria fila (ver queue.QueueNameHTTP e vizinhos).
func runConsumerCommand(args []string) int {
	if len(args) == 0 {
		consumerUsage()
		return 1
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "http":
		return runHTTPConsumer(rest)
	default:
		consumerUsage()
		return 1
	}
}

func consumerUsage() {
	fmt.Println(`Uso: webhook-watcher consumer <tipo>

Tipos:
  http    Consome webhook-events.http.* e entrega via HTTP POST`)
}

func runHTTPConsumer(args []string) int {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		slog.Error("REDIS_ADDR não definida; suba o Redis com 'docker compose up -d'")
		return 1
	}

	shardCount := httpShardCount()

	sqliteDB := openDB()
	defer sqliteDB.Close()

	hubDB, err := openHubDB(sqliteDB)
	if err != nil {
		slog.Error("Erro ao conectar no hub", "error", err)
		return 1
	}
	defer hubDB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Sinal de encerramento recebido, finalizando consumer HTTP...")
		cancel()
	}()

	cache := webhook.NewHookCache(hubDB, sqliteDB, slog.Default())
	go cache.StartRefreshLoop(ctx, hookCacheRefreshInterval)

	delivery := webhook.NewDelivery(cache, sqliteDB, slog.Default())

	var wg sync.WaitGroup
	for i := 0; i < shardCount; i++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			queueName := queue.QueueNameHTTPShard(shard)
			worker := queue.NewRedisWorker(redisAddr, queueName, queue.WorkerConfig{Concurrency: 1})
			worker.Handle(delivery.Handle)
			if err := worker.Start(); err != nil {
				slog.Error("Erro ao iniciar worker de shard", "fila", queueName, "error", err)
				return
			}
			slog.Info("Consumer HTTP: shard iniciado", "fila", queueName)
			<-ctx.Done()
			worker.Shutdown()
		}(i)
	}

	slog.Info("Consumer HTTP iniciado", "shards", shardCount)
	wg.Wait()
	return 0
}

// openHubDB conecta na schema fixa hub_<ambiente> configurada via
// "go run . hub set" — credenciais próprias, distintas do registro de
// replicação em binlog_servers (SQLite), pois é uma conexão de leitura de
// dados de negócio, não de binlog.
func openHubDB(sqliteDB *sql.DB) (*sql.DB, error) {
	cfg, err := config.LoadHubConfig(sqliteDB)
	if errors.Is(err, config.ErrHubConfigNotSet) {
		return nil, fmt.Errorf("%w (rode 'go run . hub set')", err)
	}
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.SchemaName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir conexão com o hub: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao conectar no hub (%s@%s:%d/%s): %w", cfg.User, cfg.Host, cfg.Port, cfg.SchemaName, err)
	}
	return db, nil
}
