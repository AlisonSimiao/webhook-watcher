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
	BinlogFile string // apenas em memória (sem resume on restart)
	BinlogPos  uint32 // apenas em memória (sem resume on restart)
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
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao criar tabela binlog_servers: %w", err)
	}

	return db, nil
}

// LoadServersFromDB retorna todos os servidores ativos cadastrados.
func LoadServersFromDB(db *sql.DB) ([]ServerConfig, error) {
	const query = `
SELECT id, server_id, replica_id, host, port, user, password, flavor
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
		if err := rows.Scan(&s.ID, &s.ServerID, &s.ReplicaID, &s.Host, &s.Port, &s.User, &s.Password, &s.Flavor); err != nil {
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
