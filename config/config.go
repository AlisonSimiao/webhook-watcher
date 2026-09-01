package config

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "modernc.org/sqlite"
)

// ServerConfig descreve um servidor MariaDB a ser escutado.
type ServerConfig struct {
	ID         uint64
	ServerID   string // identificador lógico, ex: 'DB01'
	ReplicaID  uint32 // ID de réplica do binlog
	Host       string
	Port       uint16
	User       string
	Password   string
	Flavor     string
	BinlogFile string // última posição persistida em binlog_servers (resume on restart)
	BinlogPos  uint32 // última posição persistida em binlog_servers (resume on restart)
}

// DefaultServerConfig devolve a configuração padrão de desenvolvimento,
// sobrescrevendo valores do arquivo .env quando presentes.
func DefaultServerConfig() ServerConfig {
	replicaID, err := strconv.Atoi(GetEnv("REPLICA_ID", "100"))
	if err != nil {
		replicaID = 100
	}
	port, err := strconv.Atoi(GetEnv("DB_PORT", "3306"))
	if err != nil {
		port = 3306
	}
	return ServerConfig{
		ServerID:  GetEnv("SERVER_ID", "DB01"),
		ReplicaID: uint32(replicaID),
		Host:      GetEnv("DB_HOST", "localhost"),
		Port:      uint16(port),
		User:      GetEnv("DB_USER", "root"),
		Password:  GetEnv("DB_PASSWORD", "kodejifr"),
		Flavor:    GetEnv("DB_FLAVOR", mysql.MariaDBFlavor),
	}
}

// GetEnv devolve a variável de ambiente key, ou fallback se não estiver
// definida (ou vazia).
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// InitDB abre/cria o banco SQLite e garante o esquema de binlog_servers.
func InitDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir banco SQLite %s: %w", dbPath, err)
	}

	const schema = `
CREATE TABLE IF NOT EXISTS binlog_servers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    server_id  TEXT    NOT NULL UNIQUE,
    replica_id INTEGER NOT NULL UNIQUE,
    host       TEXT    NOT NULL,
    port       INTEGER NOT NULL DEFAULT 3306,
    user       TEXT    NOT NULL,
    password   TEXT    NOT NULL,
    flavor     TEXT    NOT NULL DEFAULT 'mariadb',
    is_active  INTEGER NOT NULL DEFAULT 1,
    binlog_file TEXT   NOT NULL DEFAULT '',
    binlog_pos  INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao criar tabela binlog_servers: %w", err)
	}

	if err := migrateBinlogPositionColumns(db); err != nil {
		db.Close()
		return nil, err
	}

	const failedEventsSchema = `
CREATE TABLE IF NOT EXISTS failed_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT     NOT NULL,
    server_id  TEXT     NOT NULL,
    tenant     TEXT     NOT NULL,
    table_name TEXT     NOT NULL,
    action     TEXT     NOT NULL,
    payload    TEXT     NOT NULL,
    error      TEXT     NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	if _, err := db.Exec(failedEventsSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao criar tabela failed_events: %w", err)
	}

	const failedDeliveriesSchema = `
CREATE TABLE IF NOT EXISTS failed_deliveries (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT     NOT NULL,
    tenant     TEXT     NOT NULL,
    table_name TEXT     NOT NULL,
    action     TEXT     NOT NULL,
    url        TEXT     NOT NULL,
    payload    TEXT     NOT NULL,
    error      TEXT     NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	if _, err := db.Exec(failedDeliveriesSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao criar tabela failed_deliveries: %w", err)
	}

	const hubConfigSchema = `
CREATE TABLE IF NOT EXISTS hub_config (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    host        TEXT    NOT NULL,
    port        INTEGER NOT NULL DEFAULT 3306,
    user        TEXT    NOT NULL,
    password    TEXT    NOT NULL,
    schema_name TEXT    NOT NULL,
    hook_query  TEXT    NOT NULL,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	if _, err := db.Exec(hubConfigSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao criar tabela hub_config: %w", err)
	}

	return db, nil
}

// migrateBinlogPositionColumns garante que bancos servers.db criados antes do
// suporte a resume on restart ganhem as colunas binlog_file/binlog_pos sem
// perder dados (ALTER TABLE aditivo, nunca recria a tabela).
func migrateBinlogPositionColumns(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(binlog_servers)`)
	if err != nil {
		return fmt.Errorf("erro ao consultar table_info de binlog_servers: %w", err)
	}
	existing := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return fmt.Errorf("erro ao ler table_info de binlog_servers: %w", err)
		}
		existing[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("erro ao iterar table_info de binlog_servers: %w", err)
	}
	rows.Close()

	if !existing["binlog_file"] {
		if _, err := db.Exec(`ALTER TABLE binlog_servers ADD COLUMN binlog_file TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("erro ao adicionar coluna binlog_file: %w", err)
		}
	}
	if !existing["binlog_pos"] {
		if _, err := db.Exec(`ALTER TABLE binlog_servers ADD COLUMN binlog_pos INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("erro ao adicionar coluna binlog_pos: %w", err)
		}
	}
	return nil
}

// LoadServersFromDB retorna todos os servidores ativos cadastrados.
func LoadServersFromDB(db *sql.DB) ([]ServerConfig, error) {
	const query = `
SELECT id, server_id, replica_id, host, port, user, password, flavor, binlog_file, binlog_pos
FROM binlog_servers
WHERE is_active = 1
ORDER BY id`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar servidores: %w", err)
	}
	defer rows.Close()

	var servers []ServerConfig
	for rows.Next() {
		var s ServerConfig
		if err := rows.Scan(&s.ID, &s.ServerID, &s.ReplicaID, &s.Host, &s.Port, &s.User, &s.Password, &s.Flavor, &s.BinlogFile, &s.BinlogPos); err != nil {
			return nil, fmt.Errorf("erro ao ler linha de servidor: %w", err)
		}
		servers = append(servers, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("erro ao iterar servidores: %w", err)
	}

	return servers, nil
}

func (c *ServerConfig) SetBinlogData(binlogFile string, binlogPos uint32) {
	c.BinlogFile = binlogFile
	c.BinlogPos = binlogPos
}

// UpdateBinlogPosition persiste a posição de leitura do binlog de um servidor,
// permitindo retomar a partir dela em um restart futuro.
func UpdateBinlogPosition(db *sql.DB, serverID string, binlogFile string, binlogPos uint32) error {
	_, err := db.Exec(
		`UPDATE binlog_servers SET binlog_file = ?, binlog_pos = ?, updated_at = CURRENT_TIMESTAMP WHERE server_id = ?`,
		binlogFile, binlogPos, serverID,
	)
	if err != nil {
		return fmt.Errorf("erro ao persistir posição do binlog: %w", err)
	}
	return nil
}

// SaveFailedEvent persiste um evento que não conseguiu ser enfileirado, para
// consulta e intervenção manual posterior (sem reprocessamento automático).
func SaveFailedEvent(db *sql.DB, serverID, eventID, tenant, table, action string, payload []byte, errMsg string) error {
	_, err := db.Exec(
		`INSERT INTO failed_events (event_id, server_id, tenant, table_name, action, payload, error) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		eventID, serverID, tenant, table, action, string(payload), errMsg,
	)
	if err != nil {
		return fmt.Errorf("erro ao salvar evento descartado: %w", err)
	}
	return nil
}

// SaveFailedDelivery persiste uma falha de entrega HTTP (após o consumer
// esgotar as tentativas do asynq para uma URL de destino), para consulta e
// intervenção manual posterior — mesmo padrão de SaveFailedEvent, mas do
// lado consumer: sem server_id (o consumer não sabe de qual servidor de
// binlog o evento veio) e com url (identifica qual destino falhou, já que um
// evento pode ter mais de um).
func SaveFailedDelivery(db *sql.DB, eventID, tenant, table, action, url string, payload []byte, errMsg string) error {
	_, err := db.Exec(
		`INSERT INTO failed_deliveries (event_id, tenant, table_name, action, url, payload, error) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		eventID, tenant, table, action, url, string(payload), errMsg,
	)
	if err != nil {
		return fmt.Errorf("erro ao salvar entrega descartada: %w", err)
	}
	return nil
}

// DefaultHookQuery é a query padrão de resolução de destino de webhook,
// gravada por "hub set" quando nenhuma query é informada via -query. Faz o
// join clientes_hooks -> cliente (a FK real de id_cliente_hub aponta para
// cliente.id, não para uma tabela cliente_hub) e devolve tenant/tipo/url com
// aliases explícitos — importante para o comando de diagnóstico "hub query"
// conseguir checar os nomes de coluna independente de como a query interna
// nomeia as tabelas.
const DefaultHookQuery = `SELECT c.tenant AS tenant, h.tipo AS tipo, h.url AS url
FROM clientes_hooks h
JOIN cliente c ON c.id = h.id_cliente_hub
WHERE h.deleted_at IS NULL
  AND c.status = 1`

// HubConfig descreve a conexão com o MariaDB do hub (schema fixo
// hub_<ambiente>, usado para resolver destinos de webhook) e a query usada
// para buscar esses destinos.
type HubConfig struct {
	ID         uint64
	Host       string
	Port       uint16
	User       string
	Password   string
	SchemaName string
	HookQuery  string
}

// ErrHubConfigNotSet indica que "hub set" ainda não foi rodado.
var ErrHubConfigNotSet = errors.New("hub_config não configurado; rode 'go run . hub set' primeiro")

// SaveHubConfig grava a configuração do hub (upsert: atualiza a única linha
// existente, ou insere a primeira). Se query for vazio, grava
// DefaultHookQuery.
func SaveHubConfig(db *sql.DB, host string, port int, user, password, schemaName, query string) error {
	if query == "" {
		query = DefaultHookQuery
	}

	var existingID uint64
	err := db.QueryRow(`SELECT id FROM hub_config ORDER BY id LIMIT 1`).Scan(&existingID)
	switch {
	case err == sql.ErrNoRows:
		_, err = db.Exec(
			`INSERT INTO hub_config (host, port, user, password, schema_name, hook_query) VALUES (?, ?, ?, ?, ?, ?)`,
			host, port, user, password, schemaName, query,
		)
	case err == nil:
		_, err = db.Exec(
			`UPDATE hub_config SET host = ?, port = ?, user = ?, password = ?, schema_name = ?, hook_query = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
			host, port, user, password, schemaName, query, existingID,
		)
	}
	if err != nil {
		return fmt.Errorf("erro ao salvar configuração do hub: %w", err)
	}
	return nil
}

// LoadHubConfig lê a configuração única do hub. Devolve ErrHubConfigNotSet se
// "hub set" ainda não foi rodado.
func LoadHubConfig(db *sql.DB) (HubConfig, error) {
	var c HubConfig
	var port int
	row := db.QueryRow(`SELECT id, host, port, user, password, schema_name, hook_query FROM hub_config ORDER BY id LIMIT 1`)
	err := row.Scan(&c.ID, &c.Host, &port, &c.User, &c.Password, &c.SchemaName, &c.HookQuery)
	if err == sql.ErrNoRows {
		return HubConfig{}, ErrHubConfigNotSet
	}
	if err != nil {
		return HubConfig{}, fmt.Errorf("erro ao carregar configuração do hub: %w", err)
	}
	c.Port = uint16(port)
	return c, nil
}
