package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
)

// RedisWorker consome os eventos da fila via o servidor do asynq. O app
// registra um Handler tipado; retry, backoff exponencial e arquivamento
// (dead-letter) são tratados pelo asynq.
type RedisWorker struct {
	server *asynq.Server
	mux    *asynq.ServeMux
}

// WorkerConfig configura o consumo da fila.
type WorkerConfig struct {
	// Concurrency é o número de handlers executados em paralelo (padrão: 10).
	Concurrency int
}

// NewRedisWorker cria o worker conectado ao mesmo Redis/addr da produção.
func NewRedisWorker(addr string, cfg WorkerConfig) *RedisWorker {
	concurrency := cfg.Concurrency
	if concurrency == 0 {
		concurrency = 10
	}
	server := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{
		Concurrency: concurrency,
		Queues:      map[string]int{QueueName: 6},
	})
	return &RedisWorker{server: server, mux: asynq.NewServeMux()}
}

// Handle registra o handler de eventos da fila (deve ser chamado antes de Start).
func (w *RedisWorker) Handle(handler Handler) {
	w.mux.HandleFunc(taskTypeEvent, func(ctx context.Context, t *asynq.Task) error {
		var event Event
		if err := json.Unmarshal(t.Payload(), &event); err != nil {
			// Payload corrompido: falha definitiva, o asynq arquiva.
			return fmt.Errorf("erro ao decodificar evento: %w", err)
		}
		return handler(ctx, &event)
	})
}

// Start inicia o consumo de forma assíncrona (não bloqueia).
func (w *RedisWorker) Start() error {
	return w.server.Start(w.mux)
}

// Shutdown para o consumo de forma graciosa, aguardando tasks em andamento.
func (w *RedisWorker) Shutdown() {
	w.server.Shutdown()
}

// Stop para o consumo imediatamente.
func (w *RedisWorker) Stop() {
	w.server.Stop()
}
