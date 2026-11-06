package main

import (
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// positionTracker acompanha a posição do binlog e decide quando ela pode ser
// persistida. Só posições em fronteira de transação são salvas: retomar entre
// um TableMapEvent e o RowsEvent dele faz o go-mysql falhar com
// "no corresponding table map event" a cada restart, travando o watcher.
// Retomar do início da transação reenvia os eventos dela (ver ADR 0001).
type positionTracker struct {
	current   mysql.Position // posição do próximo evento a ler
	safe      mysql.Position // última fronteira de transação vista
	inTx      bool
	lastFlush time.Time
}

const positionFlushInterval = 5 * time.Second

func newPositionTracker(start mysql.Position, now time.Time) *positionTracker {
	return &positionTracker{current: start, safe: start, lastFlush: now}
}

// Current é a posição do próximo evento a ler (a que o HandleEvent recebe).
func (t *positionTracker) Current() mysql.Position { return t.current }

// Safe é a última fronteira de transação vista: a posição a salvar no
// encerramento ou em erro do stream.
func (t *positionTracker) Safe() mysql.Position { return t.safe }

// Observe registra um evento já processado e devolve a posição a persistir,
// se houver: fronteiras de transação a cada positionFlushInterval, rotação na hora.
func (t *positionTracker) Observe(ev *replication.BinlogEvent, now time.Time) (mysql.Position, bool) {
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		t.current = mysql.Position{Name: string(e.NextLogName), Pos: uint32(e.Position)}
		// Rotate no meio da transação é o rotate falso de uma reconexão do syncer
		if t.inTx {
			return mysql.Position{}, false
		}
		t.safe = t.current
		t.lastFlush = now
		return t.safe, true
	case *replication.XIDEvent:
		t.current.Pos = ev.Header.LogPos
		t.closeTx()
	case *replication.QueryEvent:
		t.current.Pos = ev.Header.LogPos
		// BEGIN abre a transação; COMMIT (tabelas não transacionais, sem XID) e DDL a fecham
		if strings.EqualFold(strings.TrimSpace(string(e.Query)), "BEGIN") {
			t.inTx = true
			return mysql.Position{}, false
		}
		t.closeTx()
	case *replication.MariadbGTIDEvent, *replication.GTIDEvent, *replication.TableMapEvent, *replication.RowsEvent:
		t.current.Pos = ev.Header.LogPos
		t.inTx = true
		return mysql.Position{}, false
	default:
		t.current.Pos = ev.Header.LogPos
		return mysql.Position{}, false
	}

	if now.Sub(t.lastFlush) < positionFlushInterval {
		return mysql.Position{}, false
	}
	t.lastFlush = now
	return t.safe, true
}

func (t *positionTracker) closeTx() {
	t.inTx = false
	t.safe = t.current
}
