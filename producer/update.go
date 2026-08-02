package producer

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

// UpdateRowsStrategy trata eventos de UPDATE (pares old/new).
type UpdateRowsStrategy struct {
	RowsStrategy
}

func (s *UpdateRowsStrategy) Handle(ctx *EventContext) error {
	s.log.Debug("Evento recebido", "tipo", ctx.Event.Header.EventType.String())
	return s.eachRow(ctx, 2, 1, func(rowIndex int, newRow []interface{}) error {
		rowsEvent := ctx.Event.Event.(*replication.RowsEvent)
		tableName := strings.ToLower(string(rowsEvent.Table.Table))

		// Tenta despachar para um TableProcessor registrado
		res, handled, err := s.dispatchTable(string(rowsEvent.Table.Schema), tableName, "UPDATE", newRow, nil)
		if err != nil {
			s.log.Error("Erro ao processar tabela com TableProcessor", "tabela", tableName, "error", err)
			return err
		}
		if handled {
			s.log.Info("Evento customizado de tabela processado", "tabela", tableName, "evento", res)
			return nil
		}

		// Fallback genérico para eventos não customizados
		event, ok := s.buildEvent(ctx, rowsEvent, ActionUpdate, rowIndex, newRow)
		if !ok {
			return nil
		}
		s.log.Info("Evento processado", "evento", event)
		return nil
	})
}
