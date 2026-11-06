package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
	now        func() time.Time // relógio da dataEvento; injetável nos testes
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

// emitRows monta e enfileira um Evento por linha do RowsEvent, com o
// tipoModificacao derivado da ação. Na DELETE a linha visitada é a deletada e
// vai ao TableProcessor como OldRow; nas demais, é a linha nova (NewRow).
func (r *RowsStrategy) emitRows(ctx *EventContext, action queue.Action, stride, newOffset int) error {
	return r.eachRow(ctx, stride, newOffset, func(rowIndex int, row []interface{}) error {
		rowsEvent := ctx.Event.Event.(*replication.RowsEvent)
		schema := string(rowsEvent.Table.Schema)
		tableName := strings.ToLower(string(rowsEvent.Table.Table))

		resourceID, ok := rowResourceID(row)
		if !ok {
			tipo := "linha vazia"
			if len(row) > 0 {
				tipo = fmt.Sprintf("%T", row[0])
			}
			r.log.Warn("Coluna 0 não é INT assinado; evento ignorado",
				"tenant", schema,
				"table", tableName,
				"tipo", tipo)
			return nil
		}

		newRow, oldRow := row, []interface{}(nil)
		if action == queue.ActionDelete {
			newRow, oldRow = nil, row
		}

		// A dataEvento é lida antes do Process, que é quem lê o estado do
		// Recurso no MariaDB (enriquecimento): o snapshot é no máximo tão novo
		// quanto ela, nunca mais antigo.
		dataEvento := r.dataEvento()

		// Apenas tabelas com TableProcessor registrado geram eventos.
		tipo, recurso, handled, err := r.dispatchTable(schema, tableName, string(action), newRow, oldRow)
		if err != nil {
			r.log.Error("Erro ao processar tabela com TableProcessor", "tabela", tableName, "error", err)
			return err
		}
		if !handled {
			r.log.Debug("Tabela sem TableProcessor; nenhum evento emitido", "tabela", tableName)
			return nil
		}

		recursoJSON, err := json.Marshal(recurso)
		if err != nil {
			r.log.Error("Erro ao serializar recurso do evento", "tabela", tableName, "error", err)
			return err
		}

		id := generateEventID(ctx.BinlogFile, ctx.BinlogPos, rowIndex, schema, tableName, resourceID)
		payload, err := json.Marshal(queue.Contrato{
			IDEvento:        id,
			Tipo:            tipo,
			TipoModificacao: queue.TipoModificacaoDe(action),
			DataEvento:      dataEvento,
			Tenant:          schema,
			Recurso:         recursoJSON,
		})
		if err != nil {
			r.log.Error("Erro ao serializar payload do evento", "tabela", tableName, "error", err)
			return err
		}

		r.emit(ctx.Ctx, &queue.Event{
			ID:         id,
			Tenant:     schema,
			Table:      tableName,
			Action:     action,
			ResourceID: resourceID,
			Payload:    payload,
		})
		return nil
	})
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

// dataEvento formata o instante da leitura do estado do Recurso como no
// Contrato: ISO-8601 em UTC com milissegundos.
func (r *RowsStrategy) dataEvento() string {
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	return now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// dispatchTable verifica se há um TableProcessor registrado para a tabela e
// devolve o tipo de Recurso e o Recurso montado por ele.
func (r *RowsStrategy) dispatchTable(schema, tableName, action string, newRow, oldRow []interface{}) (string, interface{}, bool, error) {
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
			return proc.Tipo(), res, true, err
		}
	}

	return "", nil, false, nil
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
