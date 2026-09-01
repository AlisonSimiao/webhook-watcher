package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"webhook-watcher/consumer/webhook"
	"webhook-watcher/pkg/queue"
)

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
  http    Consome webhook-events.http.* e repassa para o srp-hub-api
          (/interno/hooks/<tipo>), que faz a entrega ao cliente final`)
}

// runHTTPConsumer repassa cada evento para o srp-hub-api
// (POST <SRP_HUB_API_BASE_URL>/interno/hooks/<tipo>), que já resolve os
// destinos de webhook por cliente (clientes_hooks/cliente) e faz o fan-out —
// este consumer não lê mais aquelas tabelas nem precisa de conexão própria
// com o MariaDB do hub.
func runHTTPConsumer(args []string) int {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		slog.Error("REDIS_ADDR não definida; suba o Redis com 'docker compose up -d'")
		return 1
	}

	baseURL := os.Getenv("SRP_HUB_API_BASE_URL")
	token := os.Getenv("SRP_HUB_API_TOKEN")
	if baseURL == "" || token == "" {
		slog.Error("SRP_HUB_API_BASE_URL e/ou SRP_HUB_API_TOKEN não definidas")
		return 1
	}

	shardCount := httpShardCount()

	sqliteDB := openDB()
	defer sqliteDB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Sinal de encerramento recebido, finalizando consumer HTTP...")
		cancel()
	}()

	delivery := webhook.NewDelivery(baseURL, token, sqliteDB, slog.Default())

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

	slog.Info("Consumer HTTP iniciado", "shards", shardCount, "srp_hub_api_base_url", baseURL)
	wg.Wait()
	return 0
}
