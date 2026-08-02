package producer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

type Action string

const (
	ActionInsert Action = "INSERT"
	ActionUpdate Action = "UPDATE"
	ActionDelete Action = "DELETE"
)

type Event struct {
	ID         string `json:"id"`
	ResourceID int    `json:"resource_id"`
	Tenant     string `json:"tenant"`
	Action     Action `json:"action"`
	Table      string `json:"table"`
	Timestamp  int64  `json:"timestamp"`
}

// EventContext agrupa os dados comuns a toda estratégia de evento.
type EventContext struct {
	BinlogFile string
	BinlogPos  uint32
	Event      *replication.BinlogEvent
}

// EventStrategy é o contrato que toda estratégia de tratamento de eventos deve
// implementar.
type EventStrategy interface {
	Handle(ctx *EventContext) error
}

// Producer despacha cada tipo de evento para a estratégia registrada.
type Producer struct {
	strategies map[replication.EventType]EventStrategy
}

func NewProducer() *Producer {
	update := &UpdateRowsStrategy{}
	return &Producer{
		strategies: map[replication.EventType]EventStrategy{
			replication.UPDATE_ROWS_EVENTv1:                     update,
			replication.UPDATE_ROWS_EVENTv2:                     update,
			replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1: update,
		},
	}
}

func (p *Producer) HandleEvent(binlogFile string, binlogPos uint32, ev *replication.BinlogEvent) error {
	strategy, ok := p.strategies[ev.Header.EventType]
	if !ok {
		fmt.Printf("Evento sem estratégia registrada: %s\n", ev.Header.EventType.String())
		return nil
	}
	return strategy.Handle(&EventContext{
		BinlogFile: binlogFile,
		BinlogPos:  binlogPos,
		Event:      ev,
	})
}

func generateEventID(binlogFile string, logPos uint32, rowIndex int, event Event) string {
	raw := fmt.Sprintf("%s:%d:%d:%s:%s:%d", binlogFile, logPos, rowIndex, event.Tenant, event.Table, event.ResourceID)
	hash := sha256.Sum256([]byte(raw))

	return "evt_" + hex.EncodeToString(hash[:16])
}
