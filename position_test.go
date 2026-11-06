package main

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const binlogFile = "mariadb-bin.000020"

var t0 = time.Date(2026, 11, 6, 15, 0, 0, 0, time.UTC)

func binlogEvent(logPos uint32, e replication.Event) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{LogPos: logPos}, Event: e}
}

// Offsets reais de um UPDATE no MariaDB local (mysqlbinlog), usados na reprodução
// do watcher travado: retomar em 40729421 (fim do TableMap) falha com
// "no corresponding table map event".
func TestPositionTracker_NuncaSalvaNoMeioDaTransacao(t *testing.T) {
	tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 40729269}, t0)
	depois := t0.Add(time.Minute) // flush de 5s já vencido em todos os eventos

	meio := []*replication.BinlogEvent{
		binlogEvent(40729311, &replication.MariadbGTIDEvent{}),
		binlogEvent(40729372, &replication.TableMapEvent{}),
		binlogEvent(40729421, &replication.RowsEvent{}),
	}
	for _, ev := range meio {
		if pos, ok := tr.Observe(ev, depois); ok {
			t.Fatalf("salvou %+v no meio da transação (evento %T)", pos, ev.Event)
		}
	}

	pos, ok := tr.Observe(binlogEvent(40729504, &replication.XIDEvent{}), depois)
	want := mysql.Position{Name: binlogFile, Pos: 40729504}
	if !ok || pos != want {
		t.Fatalf("após XID: got (%+v, %v), want (%+v, true)", pos, ok, want)
	}
}

func TestPositionTracker_SalvaFronteiraNoMaximoACada5s(t *testing.T) {
	tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 100}, t0)

	if pos, ok := tr.Observe(binlogEvent(200, &replication.XIDEvent{}), t0.Add(time.Second)); ok {
		t.Fatalf("salvou %+v antes de vencer o intervalo de flush", pos)
	}

	pos, ok := tr.Observe(binlogEvent(300, &replication.XIDEvent{}), t0.Add(6*time.Second))
	want := mysql.Position{Name: binlogFile, Pos: 300}
	if !ok || pos != want {
		t.Fatalf("got (%+v, %v), want (%+v, true)", pos, ok, want)
	}

	if pos, ok := tr.Observe(binlogEvent(400, &replication.XIDEvent{}), t0.Add(7*time.Second)); ok {
		t.Fatalf("salvou %+v 1s depois do último flush", pos)
	}
}

func TestPositionTracker_QueryEvent(t *testing.T) {
	depois := t0.Add(time.Minute)
	query := func(logPos uint32, sql string) *replication.BinlogEvent {
		return binlogEvent(logPos, &replication.QueryEvent{Query: []byte(sql)})
	}

	t.Run("COMMIT de tabela não transacional fecha a transação aberta por BEGIN", func(t *testing.T) {
		tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 100}, t0)
		for _, ev := range []*replication.BinlogEvent{
			query(200, "BEGIN"),
			binlogEvent(300, &replication.TableMapEvent{}),
			binlogEvent(400, &replication.RowsEvent{}),
		} {
			if pos, ok := tr.Observe(ev, depois); ok {
				t.Fatalf("salvou %+v no meio da transação (evento %T)", pos, ev.Event)
			}
		}
		pos, ok := tr.Observe(query(500, "COMMIT"), depois)
		if want := (mysql.Position{Name: binlogFile, Pos: 500}); !ok || pos != want {
			t.Fatalf("após COMMIT: got (%+v, %v), want (%+v, true)", pos, ok, want)
		}
	})

	t.Run("DDL é fronteira", func(t *testing.T) {
		tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 100}, t0)
		tr.Observe(binlogEvent(200, &replication.MariadbGTIDEvent{}), depois)
		pos, ok := tr.Observe(query(300, "ALTER TABLE pedidos ADD COLUMN x INT"), depois)
		if want := (mysql.Position{Name: binlogFile, Pos: 300}); !ok || pos != want {
			t.Fatalf("após DDL: got (%+v, %v), want (%+v, true)", pos, ok, want)
		}
	})
}

func TestPositionTracker_RotateEvent(t *testing.T) {
	rotate := func(next string, pos uint64) *replication.BinlogEvent {
		return binlogEvent(0, &replication.RotateEvent{NextLogName: []byte(next), Position: pos})
	}

	t.Run("rotação entre transações salva o novo arquivo na hora", func(t *testing.T) {
		tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 100}, t0)
		tr.Observe(binlogEvent(200, &replication.XIDEvent{}), t0.Add(time.Second))

		pos, ok := tr.Observe(rotate("mariadb-bin.000021", 4), t0.Add(2*time.Second))
		want := mysql.Position{Name: "mariadb-bin.000021", Pos: 4}
		if !ok || pos != want {
			t.Fatalf("got (%+v, %v), want (%+v, true)", pos, ok, want)
		}
		if tr.Current() != want {
			t.Fatalf("Current() = %+v, want %+v", tr.Current(), want)
		}
	})

	// Ao reconectar sozinho, o syncer do go-mysql recomeça da posição do próximo
	// evento (que pode estar no meio da transação) e o servidor manda um rotate
	// falso apontando para ela.
	t.Run("rotate falso no meio da transação não é fronteira", func(t *testing.T) {
		tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 100}, t0)
		depois := t0.Add(time.Minute)
		tr.Observe(binlogEvent(200, &replication.MariadbGTIDEvent{}), depois)
		tr.Observe(binlogEvent(300, &replication.TableMapEvent{}), depois)

		if pos, ok := tr.Observe(rotate(binlogFile, 300), depois); ok {
			t.Fatalf("salvou %+v a partir de rotate no meio da transação", pos)
		}
		if want := (mysql.Position{Name: binlogFile, Pos: 300}); tr.Current() != want {
			t.Fatalf("Current() = %+v, want %+v", tr.Current(), want)
		}

		tr.Observe(binlogEvent(400, &replication.RowsEvent{}), depois)
		pos, ok := tr.Observe(binlogEvent(500, &replication.XIDEvent{}), depois)
		if want := (mysql.Position{Name: binlogFile, Pos: 500}); !ok || pos != want {
			t.Fatalf("após XID: got (%+v, %v), want (%+v, true)", pos, ok, want)
		}
	})
}

// No SIGTERM ou em erro do stream o watcher salva Safe(): a transação em curso
// é relida inteira na volta.
func TestPositionTracker_SafeNoMeioDaTransacaoEhAUltimaFronteira(t *testing.T) {
	tr := newPositionTracker(mysql.Position{Name: binlogFile, Pos: 100}, t0)
	depois := t0.Add(time.Minute)
	tr.Observe(binlogEvent(200, &replication.XIDEvent{}), depois)
	for i, ev := range []*replication.BinlogEvent{
		binlogEvent(300, &replication.MariadbGTIDEvent{}),
		binlogEvent(400, &replication.TableMapEvent{}),
		binlogEvent(500, &replication.RowsEvent{}), // UPDATE grande: vários RowsEvents
		binlogEvent(600, &replication.RowsEvent{}), // para o mesmo table id
	} {
		tr.Observe(ev, depois)
		if want := (mysql.Position{Name: binlogFile, Pos: 200}); tr.Safe() != want {
			t.Fatalf("evento %d: Safe() = %+v, want %+v", i, tr.Safe(), want)
		}
	}
}
