package config

import (
	"database/sql"
	"path/filepath"
	"strings"
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

func TestHubConfig_SaveAndLoad(t *testing.T) {
	db, err := InitDB(filepath.Join(t.TempDir(), "servers.db"))
	if err != nil {
		t.Fatalf("InitDB falhou: %v", err)
	}
	defer db.Close()

	if _, err := LoadHubConfig(db); err != ErrHubConfigNotSet {
		t.Fatalf("esperava ErrHubConfigNotSet antes de qualquer hub set, obteve %v", err)
	}

	if err := SaveHubConfig(db, "db.multiplier.local", 3306, "root", "kodejifr", "hub_development", ""); err != nil {
		t.Fatalf("SaveHubConfig falhou: %v", err)
	}

	cfg, err := LoadHubConfig(db)
	if err != nil {
		t.Fatalf("LoadHubConfig falhou: %v", err)
	}
	if cfg.Host != "db.multiplier.local" || cfg.Port != 3306 || cfg.User != "root" || cfg.Password != "kodejifr" || cfg.SchemaName != "hub_development" {
		t.Fatalf("configuração carregada incorreta: %+v", cfg)
	}
	if cfg.HookQuery != DefaultHookQuery {
		t.Fatalf("esperava DefaultHookQuery quando query vazia foi passada, obteve: %s", cfg.HookQuery)
	}

	if err := SaveHubConfig(db, "outro.host", 3307, "user2", "pass2", "hub_producao", "SELECT 1"); err != nil {
		t.Fatalf("segundo SaveHubConfig (update) falhou: %v", err)
	}

	cfg2, err := LoadHubConfig(db)
	if err != nil {
		t.Fatalf("LoadHubConfig após update falhou: %v", err)
	}
	if cfg2.ID != cfg.ID {
		t.Fatalf("esperava upsert (mesma linha, id %d), obteve nova linha id %d", cfg.ID, cfg2.ID)
	}
	if cfg2.Host != "outro.host" || cfg2.SchemaName != "hub_producao" || cfg2.HookQuery != "SELECT 1" {
		t.Fatalf("configuração não atualizou corretamente: %+v", cfg2)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM hub_config`).Scan(&count); err != nil {
		t.Fatalf("erro ao contar linhas de hub_config: %v", err)
	}
	if count != 1 {
		t.Fatalf("esperava exatamente 1 linha em hub_config (upsert), obteve %d", count)
	}
}

func TestDefaultHookQuery_HasExpectedFilters(t *testing.T) {
	for _, want := range []string{"deleted_at IS NULL", "status = 1", "id_cliente_hub", "AS tenant", "AS tipo", "AS url"} {
		if !strings.Contains(DefaultHookQuery, want) {
			t.Fatalf("DefaultHookQuery não contém %q:\n%s", want, DefaultHookQuery)
		}
	}
}
