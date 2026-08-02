package producer

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/tables"
	"webhook-watcher/tables/pedido"
)

// RowsStrategy contém a lógica comum a eventos ROWS (insert/update/delete).
type RowsStrategy struct {
	log        *slog.Logger
	db         *sql.DB
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
		rowIndex++
		if err := visit(rowIndex, rowsEvent.Rows[i+newOffset]); err != nil {
			return err
		}
	}
	return nil
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

// buildEvent monta o Event a partir da nova linha, incluindo o ID único.
func (r *RowsStrategy) buildEvent(ctx *EventContext, rowsEvent *replication.RowsEvent, action Action, rowIndex int, newRow []interface{}) (Event, bool) {
	resourceID, ok := newRow[0].(int32)
	if !ok {
		r.log.Warn("Coluna 0 não é INT assinado; evento ignorado",
			"tenant", rowsEvent.Table.Schema,
			"table", rowsEvent.Table.Table,
			"tipo", fmt.Sprintf("%T", newRow[0]))
		return Event{}, false
	}
	event := Event{
		ResourceID: int(resourceID),
		Tenant:     string(rowsEvent.Table.Schema),
		Action:     action,
		Table:      string(rowsEvent.Table.Table),
		Timestamp:  int64(ctx.Event.Header.Timestamp),
	}
	event.ID = generateEventID(ctx.BinlogFile, ctx.BinlogPos, rowIndex, event)
	return event, true
}
