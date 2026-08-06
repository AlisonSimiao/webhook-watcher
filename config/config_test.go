package config

import (
	"database/sql"
	"path/filepath"
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
