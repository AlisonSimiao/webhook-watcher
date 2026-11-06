package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// Portas padrão do /healthz: diferentes entre producer e consumer para os dois
// poderem rodar na mesma máquina (go run . / go run . consumer http). Em
// container cada um tem sua própria rede, então HEALTH_ADDR pode ser igual.
const (
	defaultProducerHealthAddr = ":8090"
	defaultConsumerHealthAddr = ":8091"
)

// watcherStaleAfter é quanto tempo um watcher pode ficar sem completar uma
// volta do loop antes de ser considerado travado. O loop gira no máximo a
// cada ~2s (timeout do GetEvent), então 30s cobre folga e eventos lentos de
// enriquecimento sem mascarar um travamento real.
const watcherStaleAfter = 30 * time.Second

// healthCheck devolve se o processo está saudável e os detalhes exibidos no
// corpo do /healthz.
type healthCheck func() (healthy bool, details map[string]any)

// startHealthServer sobe o /healthz em background: 200 quando check reporta
// saudável, 503 caso contrário. Falha ao escutar na porta é só logada — o
// health check não deve derrubar o watcher/consumer.
func startHealthServer(addr string, check healthCheck) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(check))
	go func() {
		slog.Info("Health check escutando", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("Erro no servidor de health check", "addr", addr, "error", err)
		}
	}()
}

func healthHandler(check healthCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		healthy, details := check()
		status := "ok"
		code := http.StatusOK
		if !healthy {
			status = "fail"
			code = http.StatusServiceUnavailable
		}
		body := map[string]any{"status": status}
		for k, v := range details {
			body[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	}
}

func healthAddr(fallback string) string {
	if addr := os.Getenv("HEALTH_ADDR"); addr != "" {
		return addr
	}
	return fallback
}

// redisStatus formata o resultado de um ping no Redis para o corpo do
// /healthz. Só informativo: Redis fora não marca o processo como doente,
// porque reiniciá-lo não resolve (o producer já cai em failed_events e o
// asynq do consumer reconecta sozinho).
func redisStatus(ping func() error) string {
	if err := ping(); err != nil {
		return err.Error()
	}
	return "ok"
}

// watcherLiveness acompanha se cada BinlogWatcher continua girando seu loop.
// Saudável = todo watcher bateu nos últimos watcherStaleAfter e nenhum
// encerrou (erro ou panic). Com mais de um servidor, um watcher morto não
// derruba o processo, então é o 503 aqui que faz o orquestrador reiniciá-lo.
type watcherLiveness struct {
	mu      sync.Mutex
	now     func() time.Time
	beats   map[string]time.Time
	stopped map[string]bool
}

func newWatcherLiveness() *watcherLiveness {
	return &watcherLiveness{now: time.Now, beats: map[string]time.Time{}, stopped: map[string]bool{}}
}

func (l *watcherLiveness) beat(serverID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.beats[serverID] = l.now()
}

func (l *watcherLiveness) stop(serverID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped[serverID] = true
}

func (l *watcherLiveness) check() (bool, map[string]string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	healthy := len(l.beats) > 0
	watchers := make(map[string]string, len(l.beats))
	for id, last := range l.beats {
		age := l.now().Sub(last)
		switch {
		case l.stopped[id]:
			watchers[id] = "encerrado"
			healthy = false
		case age > watcherStaleAfter:
			watchers[id] = "travado: sem atividade há " + age.Round(time.Second).String()
			healthy = false
		default:
			watchers[id] = "ok"
		}
	}
	return healthy, watchers
}
