package producer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"

	"webhook-watcher/pkg/queue"
)

// EventContext agrupa os dados comuns a toda estratégia de evento.
type EventContext struct {
	Ctx        context.Context
	BinlogFile string
	BinlogPos  uint32
	Event      *replication.BinlogEvent
}

// EventStrategy é o contrato que toda estratégia de tratamento de eventos deve
// implementar.
type EventStrategy interface {
	Handle(ctx *EventContext) error
}

// Producer despacha cada tipo de evento para a estratégia registrada e enfileira
// os eventos gerados na fila via Enqueuer.
type Producer struct {
	strategies map[replication.EventType]EventStrategy
	log        *slog.Logger
	db         *sql.DB
	enqueuer   queue.Enqueuer
}

func NewProducer(logger *slog.Logger, db *sql.DB, enqueuer queue.Enqueuer) *Producer {
	update := &UpdateRowsStrategy{
		RowsStrategy: RowsStrategy{
			log:        logger,
			db:         db,
			enqueuer:   enqueuer,
			processors: defaultProcessors(),
		},
	}
	return &Producer{
		strategies: map[replication.EventType]EventStrategy{
			replication.UPDATE_ROWS_EVENTv1:                     update,
			replication.UPDATE_ROWS_EVENTv2:                     update,
			replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1: update,
		},
		log:      logger,
		db:       db,
		enqueuer: enqueuer,
	}
}

func (p *Producer) HandleEvent(binlogFile string, binlogPos uint32, ev *replication.BinlogEvent) error {
	strategy, ok := p.strategies[ev.Header.EventType]
	if !ok {
		p.log.Warn("Evento sem estratégia registrada", "tipo", ev.Header.EventType.String())
		return nil
	}
	return strategy.Handle(&EventContext{
		Ctx:        context.Background(),
		BinlogFile: binlogFile,
		BinlogPos:  binlogPos,
		Event:      ev,
	})
}

func generateEventID(binlogFile string, logPos uint32, rowIndex int, tenant, table string, resourceID int) string {
	raw := fmt.Sprintf("%s:%d:%d:%s:%s:%d", binlogFile, logPos, rowIndex, tenant, table, resourceID)
	hash := sha256.Sum256([]byte(raw))

	return "evt_" + hex.EncodeToString(hash[:16])
}
