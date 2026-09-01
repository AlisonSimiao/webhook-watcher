package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"webhook-watcher/config"
)

// tableToTipo traduz Event.Table para clientes_hooks.tipo. Confirmado contra
// o código-fonte de srp-hub-api (o sistema que escreve/lê clientes_hooks
// hoje, src/config/constants.js: WEBHOOK.TIPO = { PEDIDO: 'pedidos', CLIENTE:
// 'clientes' }) — tipo é IGUAL ao nome da tabela (plural), sem tradução
// nenhuma. Mantido como mapa explícito, e não como identidade implícita, para
// servir de allowlist: uma tabela sem entrada aqui não gera lookup, mesmo que
// tenha TableProcessor registrado no producer.
var tableToTipo = map[string]string{
	"pedidos": "pedidos",
}

// destKey é a chave de lookup do cache: tenant (cliente.tenant) + tipo
// (clientes_hooks.tipo, já traduzido de Event.Table).
type destKey struct {
	tenant string
	tipo   string
}

// HookCache mantém em memória o resultado da query de resolução de destino,
// atualizado periodicamente (ver StartRefreshLoop). Um tenant/tipo pode ter
// mais de uma URL de destino — todas recebem o evento.
type HookCache struct {
	hubDB    *sql.DB // MariaDB do hub (hub_<ambiente>)
	sqliteDB *sql.DB // servers.db — de onde vem a query configurada (hub_config.hook_query)
	log      *slog.Logger

	mu   sync.RWMutex
	dest map[destKey][]string
}

// NewHookCache recebe os dois *sql.DB porque Refresh recarrega a query
// configurada em hub_config.hook_query (via sqliteDB) a cada ciclo antes de
// rodá-la em hubDB — a query fica editável em runtime (go run . hub set
// -query "...") sem precisar reiniciar o consumer.
func NewHookCache(hubDB, sqliteDB *sql.DB, log *slog.Logger) *HookCache {
	return &HookCache{hubDB: hubDB, sqliteDB: sqliteDB, log: log, dest: map[destKey][]string{}}
}

// Lookup retorna as URLs de destino cadastradas para o tenant/tabela do
// evento. table é o Event.Table (plural); é traduzido para "tipo" via
// tableToTipo. Tabela sem tradução conhecida ou tenant/tipo sem hook
// cadastrado devolve slice vazia (nunca erro) — ausência de destino é um
// no-op válido, não uma falha.
func (c *HookCache) Lookup(tenant, table string) []string {
	tipo, ok := tableToTipo[table]
	if !ok {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string(nil), c.dest[destKey{tenant: tenant, tipo: tipo}]...)
}

// Refresh recarrega o cache: relê a query configurada em hub_config
// (sqliteDB), roda contra o hub (hubDB) e substitui o snapshot anterior
// atomicamente (constrói o novo map antes de trocar sob lock — sem janela de
// leitura parcial).
func (c *HookCache) Refresh(ctx context.Context) error {
	hubCfg, err := config.LoadHubConfig(c.sqliteDB)
	if err != nil {
		return fmt.Errorf("erro ao carregar configuração do hub: %w", err)
	}

	rows, err := queryHookDestinations(ctx, c.hubDB, hubCfg.HookQuery)
	if err != nil {
		return fmt.Errorf("erro ao consultar destinos de webhook: %w", err)
	}

	next := map[destKey][]string{}
	for _, r := range rows {
		k := destKey{tenant: r.Tenant, tipo: r.Tipo}
		next[k] = append(next[k], r.URL)
	}

	c.mu.Lock()
	c.dest = next
	c.mu.Unlock()

	c.log.Info("Cache de destinos de webhook atualizado", "total_destinos", len(rows))
	return nil
}

// StartRefreshLoop dispara Refresh imediatamente e depois a cada interval,
// até ctx ser cancelado. Erros de refresh são logados, não fatais — o cache
// anterior continua servindo Lookup (melhor um cache velho do que zero
// entregas).
func (c *HookCache) StartRefreshLoop(ctx context.Context, interval time.Duration) {
	if err := c.Refresh(ctx); err != nil {
		c.log.Error("Erro no refresh inicial do cache de destinos", "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil {
				c.log.Error("Erro ao atualizar cache de destinos", "error", err)
			}
		}
	}
}
