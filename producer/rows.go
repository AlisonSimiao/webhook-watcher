package producer

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

// RowsStrategy contém a lógica comum a eventos ROWS (insert/update/delete).
type RowsStrategy struct{}

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

// buildEvent monta o Event a partir da nova linha, incluindo o ID único.
func (r *RowsStrategy) buildEvent(ctx *EventContext, rowsEvent *replication.RowsEvent, action Action, rowIndex int, newRow []interface{}) (Event, bool) {
	resourceID, ok := newRow[0].(int32)
	if !ok {
		fmt.Printf("Coluna 0 de %s.%s não é INT assinado (tipo %T); evento ignorado\n",
			rowsEvent.Table.Schema, rowsEvent.Table.Table, newRow[0])
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
