package webhook

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hibiken/asynq"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
)

const defaultHTTPTimeout = 10 * time.Second

// Delivery entrega eventos via HTTP POST às URLs cadastradas em HookCache.
type Delivery struct {
	cache    *HookCache
	sqliteDB *sql.DB
	client   *http.Client
	log      *slog.Logger
}

func NewDelivery(cache *HookCache, sqliteDB *sql.DB, log *slog.Logger) *Delivery {
	return &Delivery{cache: cache, sqliteDB: sqliteDB, client: &http.Client{Timeout: defaultHTTPTimeout}, log: log}
}

// Handle é o queue.Handler registrado no RedisWorker. Zero destinos
// cadastrados para tenant/table é sucesso silencioso (no-op), não erro —
// nem todo evento tem um webhook configurado. Falha de entrega (transporte ou
// status não-2xx) em QUALQUER destino faz Handle retornar erro, acionando o
// retry/backoff do asynq (asynq.MaxRetry/asynq.Timeout já configurados em
// pkg/queue/redis.go) até esgotar as tentativas e ir para o dead-letter do
// asynq (arquivamento). Na última tentativa, cada destino que falhou também é
// gravado em failed_deliveries (SQLite) — paridade de visibilidade com o lado
// producer (failed_events), consultável via "go run . failed-deliveries".
//
// Limitação conhecida e aceita: um retry reenvia a TODOS os destinos do
// evento, inclusive os que já tinham recebido com sucesso na tentativa
// anterior — não há estado de retry por destino nesta rodada.
func (d *Delivery) Handle(ctx context.Context, event *queue.Event) error {
	urls := d.cache.Lookup(event.Tenant, event.Table)
	if len(urls) == 0 {
		d.log.Debug("Nenhum destino de webhook cadastrado; ignorando", "tenant", event.Tenant, "table", event.Table)
		return nil
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("erro ao serializar evento para webhook: %w", err)
	}

	lastAttempt := isLastAttempt(ctx)

	var errs []error
	for _, url := range urls {
		if err := d.deliverOne(ctx, url, body); err != nil {
			d.log.Error("Falha ao entregar webhook", "url", url, "event_id", event.ID, "error", err)
			errs = append(errs, err)
			if lastAttempt {
				d.saveFailedDelivery(event, url, body, err)
			}
			continue
		}
		d.log.Info("Webhook entregue", "url", url, "event_id", event.ID, "tenant", event.Tenant, "table", event.Table)
	}
	if len(errs) > 0 {
		return fmt.Errorf("falha ao entregar %d/%d destinos: %w", len(errs), len(urls), errors.Join(errs...))
	}
	return nil
}

func (d *Delivery) deliverOne(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("erro ao montar requisição: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// NOTA: cliente_hub.acesso provavelmente é um token/segredo de autenticação
	// do destino, mas seu formato/uso (header? assinatura HMAC?) NÃO foi
	// confirmado, e essa tabela não entra no join de resolução de destino
	// desta versão. Não inventar um esquema de auth aqui — pendência aberta.

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("erro de transporte: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("resposta não-2xx: %d", resp.StatusCode)
	}
	return nil
}

func (d *Delivery) saveFailedDelivery(event *queue.Event, url string, payload []byte, deliverErr error) {
	if d.sqliteDB == nil {
		return
	}
	if err := config.SaveFailedDelivery(d.sqliteDB, event.ID, event.Tenant, event.Table, string(event.Action), url, payload, deliverErr.Error()); err != nil {
		d.log.Error("Erro ao salvar entrega descartada em failed_deliveries", "id", event.ID, "url", url, "error", err)
	}
}

// isLastAttempt confere se esta é a última tentativa do asynq para a task
// (via os helpers de contexto do próprio asynq) antes de ser arquivada.
// Fora de um worker asynq real (ex: testes chamando Handle diretamente), os
// helpers retornam ok=false — tratado como "não é a última tentativa", para
// não gravar failed_deliveries fora do fluxo real de esgotamento de retries.
func isLastAttempt(ctx context.Context) bool {
	retried, ok := asynq.GetRetryCount(ctx)
	if !ok {
		return false
	}
	maxRetry, ok := asynq.GetMaxRetry(ctx)
	if !ok {
		return false
	}
	return retried >= maxRetry
}
