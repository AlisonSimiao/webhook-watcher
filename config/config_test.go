package config

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

func TestInitDB_MigratesOldSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "servers.db")

	old, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("erro ao abrir sqlite: %v", err)
	}
	const oldSchema = `
CREATE TABLE binlog_servers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    server_id  TEXT    NOT NULL UNIQUE,
    replica_id INTEGER NOT NULL UNIQUE,
    host       TEXT    NOT NULL,
    port       INTEGER NOT NULL DEFAULT 3306,
    user       TEXT    NOT NULL,
    password   TEXT    NOT NULL,
    flavor     TEXT    NOT NULL DEFAULT 'mariadb',
    is_active  INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	if _, err := old.Exec(oldSchema); err != nil {
		t.Fatalf("erro ao criar schema antigo: %v", err)
	}
	if _, err := old.Exec(`INSERT INTO binlog_servers (server_id, replica_id, host, user, password) VALUES ('DB01', 100, 'localhost', 'root', 'senha')`); err != nil {
		t.Fatalf("erro ao inserir servidor: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatalf("erro ao fechar sqlite: %v", err)
	}

	db, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB falhou ao migrar schema antigo: %v", err)
	}
	defer db.Close()

	servers, err := LoadServersFromDB(db)
	if err != nil {
		t.Fatalf("LoadServersFromDB falhou: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("esperava 1 servidor, obteve %d", len(servers))
	}
	s := servers[0]
	if s.ServerID != "DB01" || s.Host != "localhost" {
		t.Fatalf("dados do servidor pré-existente foram alterados: %+v", s)
	}
	if s.BinlogFile != "" || s.BinlogPos != 0 {
		t.Fatalf("esperava binlog_file/binlog_pos default vazios após migração, obteve %q/%d", s.BinlogFile, s.BinlogPos)
	}
}

func TestInitDB_Idempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "servers.db")

	db1, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("primeira chamada a InitDB falhou: %v", err)
	}
	db1.Close()

	db2, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("segunda chamada a InitDB falhou: %v", err)
	}
	db2.Close()
}

func TestLoadServersFromDB_DefaultsWhenNeverSet(t *testing.T) {
	db, err := InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO binlog_servers (server_id, replica_id, host, user, password) VALUES ('DB01', 100, 'localhost', 'root', 'senha')`); err != nil {
		t.Fatalf("erro ao inserir servidor: %v", err)
	}

	servers, err := LoadServersFromDB(db)
	if err != nil {
		t.Fatalf("LoadServersFromDB falhou: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("esperava 1 servidor, obteve %d", len(servers))
	}
	if servers[0].BinlogFile != "" || servers[0].BinlogPos != 0 {
		t.Fatalf("esperava binlog_file/binlog_pos default vazios, obteve %q/%d", servers[0].BinlogFile, servers[0].BinlogPos)
	}
}

func TestUpdateBinlogPosition_PersistsAndReloads(t *testing.T) {
	db, err := InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`INSERT INTO binlog_servers (server_id, replica_id, host, user, password) VALUES ('DB01', 100, 'localhost', 'root', 'senha')`); err != nil {
		t.Fatalf("erro ao inserir servidor: %v", err)
	}

	if err := UpdateBinlogPosition(db, "DB01", "mysql-bin.000007", 4521); err != nil {
		t.Fatalf("UpdateBinlogPosition falhou: %v", err)
	}

	servers, err := LoadServersFromDB(db)
	if err != nil {
		t.Fatalf("LoadServersFromDB falhou: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("esperava 1 servidor, obteve %d", len(servers))
	}
	if servers[0].BinlogFile != "mysql-bin.000007" || servers[0].BinlogPos != 4521 {
		t.Fatalf("posição não persistiu corretamente: got %q/%d", servers[0].BinlogFile, servers[0].BinlogPos)
	}
}

func TestSaveFailedEvent_PersistsAndIsQueryable(t *testing.T) {
	db, err := InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	if err := SaveFailedEvent(db, "DB01", "evt_abc123", "meu_tenant", "pedidos", "UPDATE", []byte(`{"id":42}`), "erro ao enfileirar: conexão recusada"); err != nil {
		t.Fatalf("SaveFailedEvent falhou: %v", err)
	}

	row := db.QueryRow(`SELECT event_id, server_id, tenant, table_name, action, payload, error FROM failed_events WHERE event_id = ?`, "evt_abc123")
	var eventID, serverID, tenant, table, action, payload, errMsg string
	if err := row.Scan(&eventID, &serverID, &tenant, &table, &action, &payload, &errMsg); err != nil {
		t.Fatalf("erro ao ler evento salvo: %v", err)
	}
	if eventID != "evt_abc123" || serverID != "DB01" || tenant != "meu_tenant" || table != "pedidos" || action != "UPDATE" {
		t.Fatalf("dados persistidos incorretos: %s %s %s %s %s", eventID, serverID, tenant, table, action)
	}
	if payload != `{"id":42}` {
		t.Fatalf("payload persistido incorreto: %s", payload)
	}
	if errMsg != "erro ao enfileirar: conexão recusada" {
		t.Fatalf("mensagem de erro persistida incorreta: %s", errMsg)
	}
}

func TestSaveFailedDelivery_PersistsAndIsQueryable(t *testing.T) {
	db, err := InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	if err := SaveFailedDelivery(db, "evt_abc123", "meu_tenant", "pedidos", "UPDATE", "https://cliente.example.com/hook", []byte(`{"id":42}`), "resposta não-2xx: 500"); err != nil {
		t.Fatalf("SaveFailedDelivery falhou: %v", err)
	}

	row := db.QueryRow(`SELECT event_id, tenant, table_name, action, url, payload, error FROM failed_deliveries WHERE event_id = ?`, "evt_abc123")
	var eventID, tenant, table, action, url, payload, errMsg string
	if err := row.Scan(&eventID, &tenant, &table, &action, &url, &payload, &errMsg); err != nil {
		t.Fatalf("erro ao ler entrega salva: %v", err)
	}
	if eventID != "evt_abc123" || tenant != "meu_tenant" || table != "pedidos" || action != "UPDATE" || url != "https://cliente.example.com/hook" {
		t.Fatalf("dados persistidos incorretos: %s %s %s %s %s", eventID, tenant, table, action, url)
	}
	if payload != `{"id":42}` {
		t.Fatalf("payload persistido incorreto: %s", payload)
	}
	if errMsg != "resposta não-2xx: 500" {
		t.Fatalf("mensagem de erro persistida incorreta: %s", errMsg)
	}
}

// Producer e consumer rodam em processos separados apontando para o mesmo
// servers.db; cada InitDB abaixo simula um deles. Sem busy_timeout, escritas
// simultâneas falham com SQLITE_BUSY ("database is locked").
func TestInitDB_ConcurrentWritersOnSameFile(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "servers.db")

	producerDB, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB (producer) falhou: %v", err)
	}
	defer producerDB.Close()
	consumerDB, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB (consumer) falhou: %v", err)
	}
	defer consumerDB.Close()

	if _, err := producerDB.Exec(`INSERT INTO binlog_servers (server_id, replica_id, host, user, password) VALUES ('DB01', 100, 'localhost', 'root', 'senha')`); err != nil {
		t.Fatalf("erro ao inserir servidor: %v", err)
	}

	const writes = 50
	errs := make(chan error, 3*writes)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < writes; i++ {
			errs <- UpdateBinlogPosition(producerDB, "DB01", "mariadb-bin.000001", uint32(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writes; i++ {
			errs <- SaveFailedEvent(producerDB, "DB01", fmt.Sprintf("evt_p%d", i), "t", "pedidos", "UPDATE", []byte(`{}`), "erro")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writes; i++ {
			errs <- SaveFailedDelivery(consumerDB, fmt.Sprintf("evt_c%d", i), "t", "pedidos", "UPDATE", "http://x", []byte(`{}`), "erro")
		}
	}()
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("escrita concorrente falhou: %v", err)
		}
	}

	var events, deliveries int
	if err := consumerDB.QueryRow(`SELECT COUNT(*) FROM failed_events`).Scan(&events); err != nil {
		t.Fatalf("erro ao contar failed_events: %v", err)
	}
	if err := producerDB.QueryRow(`SELECT COUNT(*) FROM failed_deliveries`).Scan(&deliveries); err != nil {
		t.Fatalf("erro ao contar failed_deliveries: %v", err)
	}
	if events != writes || deliveries != writes {
		t.Fatalf("esperava %d linhas em cada tabela, obteve failed_events=%d failed_deliveries=%d", writes, events, deliveries)
	}

	var mode string
	if err := producerDB.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("erro ao ler journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, esperava wal", mode)
	}
}
