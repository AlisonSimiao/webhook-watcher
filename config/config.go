package config

import (
	"database/sql"
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
	replicaID, err := strconv.Atoi(getEnv("REPLICA_ID", "100"))
	if err != nil {
		replicaID = 100
	}
	port, err := strconv.Atoi(getEnv("DB_PORT", "3306"))
	if err != nil {
		port = 3306
	}
	return ServerConfig{
		ServerID:  getEnv("SERVER_ID", "DB01"),
		ReplicaID: uint32(replicaID),
		Host:      getEnv("DB_HOST", "localhost"),
		Port:      uint16(port),
		User:      getEnv("DB_USER", "root"),
		Password:  getEnv("DB_PASSWORD", "kodejifr"),
		Flavor:    getEnv("DB_FLAVOR", mysql.MariaDBFlavor),
	}
}

func getEnv(key, fallback string) string {
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
