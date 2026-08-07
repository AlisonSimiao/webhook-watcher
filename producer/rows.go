package producer

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
	"webhook-watcher/tables"
	"webhook-watcher/tables/pedido"
)

// RowsStrategy contém a lógica comum a eventos ROWS (insert/update/delete).
type RowsStrategy struct {
	log        *slog.Logger
	db         *sql.DB // MariaDB, para enrich
	sqliteDB   *sql.DB // servers.db, para dead-letter de eventos
	serverID   string
	enqueuer   queue.Enqueuer
	processors []tables.TableProcessor
}

func defaultProcessors() []tables.TableProcessor {
	return []tables.TableProcessor{
		pedido.NewPedidoProcessor(),
	}
}

// eachRow itera sobre as linhas de um RowsEvent. stride e newOffset dependem do
// formato da ação: UPDATE chega em pares old/new (stride 2, nova linha em i+1);
// INSERT e DELETE chegam em linhas únicas (stride 1, offset 0).
func (r *RowsStrategy) eachRow(ctx *EventContext, stride, newOffset int, visit func(rowIndex int, newRow []interface{}) error) error {
	rowsEvent, ok := ctx.Event.Event.(*replication.RowsEvent)
	if !ok {
		return nil
	}
	rowIndex := 0
	for i := 0; i < len(rowsEvent.Rows); i += stride {
		idx := i + newOffset
		if idx >= len(rowsEvent.Rows) {
			r.log.Warn("RowsEvent com número de linhas inesperado (não múltiplo do stride); linha final ignorada",
				"total_rows", len(rowsEvent.Rows), "stride", stride, "offset", newOffset)
			break
		}
		rowIndex++
		if err := visit(rowIndex, rowsEvent.Rows[idx]); err != nil {
			return err
		}
	}
	return nil
}

// rowResourceID extrai o ID do recurso da coluna 0 da linha (int32 ou uint32,
// pois ids são int unsigned no MariaDB).
func rowResourceID(newRow []interface{}) (int, bool) {
	if len(newRow) == 0 {
		return 0, false
	}
	switch id := newRow[0].(type) {
	case int32:
		return int(id), true
	case uint32:
		return int(id), true
	default:
		return 0, false
	}
}

// dispatchTable verifica se há um TableProcessor registrado para a tabela.
func (r *RowsStrategy) dispatchTable(schema, tableName, action string, newRow, oldRow []interface{}) (interface{}, bool, error) {
	tCtx := &tables.TableContext{
		TableName: tableName,
		Schema:    schema,
		Action:    action,
		NewRow:    newRow,
		OldRow:    oldRow,
		DB:        r.db,
		Log:       r.log,
	}

	for _, proc := range r.processors {
		if proc.Supports(tableName) {
			res, err := proc.Process(tCtx)
			return res, true, err
		}
	}

	return nil, false, nil
}

// emit loga o evento e o enfileira. Eventos duplicados (mesmo TaskID) são
// esperados e tratados como no-op idempotente. Se o enqueue falhar (ex: Redis
// indisponível), o evento é salvo em failed_events (servers.db) para consulta
// e intervenção manual — não há retry nem reprocessamento automático.
func (r *RowsStrategy) emit(ctx context.Context, event *queue.Event) {
	r.log.Info("Evento processado", "evento", event)

	err := r.enqueuer.Enqueue(ctx, event)
	if err == nil {
		return
	}
	if queue.IsDuplicate(err) {
		r.log.Debug("Evento já estava na fila", "id", event.ID)
		return
	}

	r.log.Error("Erro ao enfileirar evento; salvando em failed_events", "id", event.ID, "error", err)
	if r.sqliteDB == nil {
		return
	}
	if saveErr := config.SaveFailedEvent(r.sqliteDB, r.serverID, event.ID, event.Tenant, event.Table, string(event.Action), event.Payload, err.Error()); saveErr != nil {
		r.log.Error("Erro ao salvar evento descartado em failed_events", "id", event.ID, "error", saveErr)
	}
}
