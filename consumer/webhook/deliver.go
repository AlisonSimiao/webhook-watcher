package webhook

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
)

const defaultHTTPTimeout = 10 * time.Second

// rotaPorTipo traduz o tipo de Recurso do Contrato para o segmento <tipo> da
// rota /interno/hooks/<tipo> do srp-hub-api. Confirmado contra o código-fonte
// de lá (src/config/constants.js: WEBHOOK.TIPO = { PEDIDO: 'pedidos',
// CLIENTE: 'clientes' }) — hoje os nomes coincidem. Mantido como mapa
// explícito (não identidade implícita) pra servir de allowlist: um tipo sem
// entrada aqui não gera chamada, mesmo com TableProcessor registrado.
var rotaPorTipo = map[string]string{
	"pedidos": "pedidos",
}

// Delivery entrega eventos repassando para o srp-hub-api (POST
// <baseURL>/interno/hooks/<tipo>), que já resolve os destinos de webhook por
// cliente (clientes_hooks/cliente) e faz o fan-out — este consumer não lê
// mais aquelas tabelas diretamente. O corpo é o Contrato canônico inteiro,
// como está em Event.Payload (docs/adr/0003): a rota só valida
// tipoModificacao e recurso e ignora os demais campos, que ficam disponíveis
// para quem quiser usá-los.
type Delivery struct {
	baseURL  string
	token    string
	sqliteDB *sql.DB
	client   *http.Client
	log      *slog.Logger
}

// NewDelivery recebe baseURL (ex: "https://hub-api.multiplier.com.br/v2",
// sem barra final) e token (o valor cru enviado no header Authorization —
// SEM prefixo "Bearer", é comparado literalmente contra
// cliente_hub_autenticacao.token no srp-hub-api).
func NewDelivery(baseURL, token string, sqliteDB *sql.DB, log *slog.Logger) *Delivery {
	return &Delivery{
		baseURL:  strings.TrimSuffix(baseURL, "/"),
		token:    token,
		sqliteDB: sqliteDB,
		client:   &http.Client{Timeout: defaultHTTPTimeout},
		log:      log,
	}
}

// Handle é o queue.Handler registrado no RedisWorker. Tipo de Recurso sem
// tradução conhecida em rotaPorTipo é sucesso silencioso (no-op), não erro — nem todo
// evento tem um tipo suportado pela rota de hooks. Falha de entrega
// (transporte ou status não-2xx) faz Handle retornar erro, acionando o
// retry/backoff do asynq (asynq.MaxRetry/asynq.Timeout já configurados em
// pkg/queue/redis.go) até esgotar as tentativas e ir para o dead-letter do
// asynq (arquivamento). Na última tentativa, a falha também é gravada em
// failed_deliveries (SQLite) — paridade de visibilidade com o lado producer
// (failed_events), consultável via "go run . failed-deliveries".
//
// Nota importante: a partir daqui, quem entrega de fato para o cliente final
// é o srp-hub-api (fetch() próprio, fan-out para clientes_hooks) — a ordem
// de entrega ponta a ponta não é mais garantida por este consumer além desta
// chamada, já que aquela rota responde antes de terminar seu próprio
// fan-out. O sharding aqui também é só melhor esforço para a ordem das
// NOSSAS chamadas: uma task em retry deixa o shard seguir para a próxima
// (docs/adr/0001).
func (d *Delivery) Handle(ctx context.Context, event *queue.Event) error {
	var contrato queue.Contrato
	if err := json.Unmarshal(event.Payload, &contrato); err != nil {
		return fmt.Errorf("payload do evento %s não é um Contrato: %w", event.ID, err)
	}

	if contrato.Tipo == "" {
		// Evento enfileirado antes do Contrato existir ({tipoModificacao,
		// recurso}, sem tipo) — producer e consumer em versões diferentes
		// durante um deploy. Roteia pela tabela do envelope em vez de
		// descartar; o corpo sai como foi enfileirado.
		d.log.Warn("Evento sem tipo no Contrato (formato antigo); roteando por Event.Table", "event_id", event.ID, "table", event.Table)
		contrato.Tipo = event.Table
	}

	tipo, ok := rotaPorTipo[contrato.Tipo]
	if !ok {
		d.log.Debug("Tipo de Recurso sem rota de hook conhecida; ignorando", "tipo", contrato.Tipo)
		return nil
	}

	url := fmt.Sprintf("%s/interno/hooks/%s", d.baseURL, tipo)
	if err := d.deliverOne(ctx, url, event.Tenant, event.Payload); err != nil {
		d.log.Error("Falha ao repassar evento para srp-hub-api", "url", url, "event_id", event.ID, "tenant", event.Tenant, "error", err)
		if isLastAttempt(ctx) {
			d.saveFailedDelivery(event, url, err)
		}
		return fmt.Errorf("falha ao repassar evento para %s: %w", url, err)
	}
	d.log.Info("Evento repassado para srp-hub-api", "url", url, "event_id", event.ID, "tenant", event.Tenant, "table", event.Table)
	return nil
}

func (d *Delivery) deliverOne(ctx context.Context, url, tenant string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("erro ao montar requisição: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", d.token) // valor cru, sem "Bearer " — ver hook.validator.js/auth.middleware.js do srp-hub-api
	req.Header.Set("tenant", tenant)

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

func (d *Delivery) saveFailedDelivery(event *queue.Event, url string, deliverErr error) {
	if d.sqliteDB == nil {
		return
	}
	if err := config.SaveFailedDelivery(d.sqliteDB, event.ID, event.Tenant, event.Table, string(event.Action), url, event.Payload, deliverErr.Error()); err != nil {
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
