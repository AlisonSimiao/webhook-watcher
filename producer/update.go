package producer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/pkg/queue"
)

// UpdateRowsStrategy trata eventos de UPDATE (pares old/new).
type UpdateRowsStrategy struct {
	RowsStrategy
}

func (s *UpdateRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.eachRow(ctx, 2, 1, func(rowIndex int, newRow []interface{}) error {
		rowsEvent := ctx.Event.Event.(*replication.RowsEvent)
		schema := string(rowsEvent.Table.Schema)
		tableName := strings.ToLower(string(rowsEvent.Table.Table))

		resourceID, ok := rowResourceID(newRow)
		if !ok {
			tipo := "linha vazia"
			if len(newRow) > 0 {
				tipo = fmt.Sprintf("%T", newRow[0])
			}
			s.log.Warn("Coluna 0 não é INT assinado; evento ignorado",
				"tenant", schema,
				"table", tableName,
				"tipo", tipo)
			return nil
		}

		// Apenas tabelas com TableProcessor registrado geram eventos.
		res, handled, err := s.dispatchTable(schema, tableName, string(queue.ActionUpdate), newRow, nil)
		if err != nil {
			s.log.Error("Erro ao processar tabela com TableProcessor", "tabela", tableName, "error", err)
			return err
		}
		if !handled {
			s.log.Debug("Tabela sem TableProcessor; nenhum evento emitido", "tabela", tableName)
			return nil
		}

		payload, err := json.Marshal(res)
		if err != nil {
			s.log.Error("Erro ao serializar payload do evento", "tabela", tableName, "error", err)
			return err
		}

		event := &queue.Event{
			ID:        generateEventID(ctx.BinlogFile, ctx.BinlogPos, rowIndex, schema, tableName, resourceID),
			Tenant:    schema,
			Table:     tableName,
			Action:    queue.ActionUpdate,
			Timestamp: int64(ctx.Event.Header.Timestamp),
			Payload:   payload,
		}
		s.emit(ctx.Ctx, event)
		return nil
	})
}
