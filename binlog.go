package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"webhook-watcher/config"
	"webhook-watcher/pkg/queue"
	"webhook-watcher/producer"

	"github.com/go-mysql-org/go-mysql/client"
	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type BinlogWatcher struct {
	cfg      config.ServerConfig
	log      *slog.Logger
	enqueuer queue.Enqueuer
	db       *sql.DB
}

func newBinlogWatcher(cfg config.ServerConfig, logger *slog.Logger, enqueuer queue.Enqueuer, db *sql.DB) *BinlogWatcher {
	return &BinlogWatcher{cfg: cfg, log: logger.With("server_id", cfg.ServerID), enqueuer: enqueuer, db: db}
}

// decideStartPosition decide, sem I/O, se o watcher deve retomar de uma
// posição já persistida (resume on restart) ou se precisa consultar
// SHOW MASTER STATUS por ser a primeira vez que o servidor é escutado.
func decideStartPosition(cfg config.ServerConfig) (pos mysql.Position, needsQuery bool) {
	if cfg.BinlogFile != "" {
		return mysql.Position{Name: cfg.BinlogFile, Pos: cfg.BinlogPos}, false
	}
	return mysql.Position{}, true
}

func (b *BinlogWatcher) persistPosition(pos mysql.Position) {
	b.cfg.SetBinlogData(pos.Name, pos.Pos)
	if b.db == nil {
		return
	}
	if err := config.UpdateBinlogPosition(b.db, b.cfg.ServerID, pos.Name, pos.Pos); err != nil {
		b.log.Warn("Erro ao persistir posição do binlog", "error", err)
	}
}

func (b *BinlogWatcher) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", b.cfg.Host, b.cfg.Port)

	pos, needsQuery := decideStartPosition(b.cfg)
	if needsQuery {
		conn, err := client.Connect(addr, b.cfg.User, b.cfg.Password, "")
		if err != nil {
			return fmt.Errorf("erro ao conectar no banco: %w", err)
		}

		res, err := conn.Execute("SHOW MASTER STATUS")
		conn.Close()
		if err != nil {
			return fmt.Errorf("erro ao consultar SHOW MASTER STATUS: %w", err)
		}

		if res.RowNumber() == 0 {
			return fmt.Errorf("SHOW MASTER STATUS retornou nenhuma linha: binlog provavelmente desabilitado")
		}
		binlogFile, err := res.GetString(0, 0)
		if err != nil {
			return fmt.Errorf("erro ao ler arquivo do binlog: %w", err)
		}
		binlogPos, err := res.GetUint(0, 1)
		if err != nil {
			return fmt.Errorf("erro ao ler posição do binlog: %w", err)
		}
		pos = mysql.Position{Name: binlogFile, Pos: uint32(binlogPos)}
		b.log.Info("Nenhuma posição salva; iniciando a partir do SHOW MASTER STATUS atual", "binlog_file", pos.Name, "binlog_pos", pos.Pos)
	} else {
		b.log.Info("Retomando a partir da última posição persistida", "binlog_file", pos.Name, "binlog_pos", pos.Pos)
	}
	b.persistPosition(pos)

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: b.cfg.ReplicaID,
		Flavor:   b.cfg.Flavor,
		Host:     b.cfg.Host,
		Port:     b.cfg.Port,
		User:     b.cfg.User,
		Password: b.cfg.Password,
	}

	syncer := replication.NewBinlogSyncer(syncerCfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("erro ao iniciar streamer sync: %w", err)
	}

	b.log.Info("Conectado e escutando eventos com sucesso")

	// Abre conexão sql.DB com o banco MariaDB para consultas de enriquecimento
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", b.cfg.User, b.cfg.Password, b.cfg.Host, b.cfg.Port)
	mariadb, errDB := sql.Open("mysql", dsn)
	if errDB != nil {
		b.log.Warn("Não foi possível abrir conexão sql.DB para enriquecimento", "error", errDB)
	} else {
		defer mariadb.Close()
	}

	prod := producer.NewProducer(b.log, mariadb, b.db, b.cfg.ServerID, b.enqueuer)

	const flushInterval = 5 * time.Second
	lastFlush := time.Now()

	for {
		if ctx.Err() != nil {
			b.persistPosition(pos)
			b.log.Info("Encerrando watcher, posição final persistida")
			return nil
		}

		evCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ev, err := streamer.GetEvent(evCtx)
		cancel()

		if err == context.DeadlineExceeded {
			continue
		}
		if ctx.Err() != nil {
			b.persistPosition(pos)
			b.log.Info("Encerrando watcher, posição final persistida")
			return nil
		}
		if err != nil {
			b.persistPosition(pos)
			return fmt.Errorf("erro no evento: %w", err)
		}

		if ev != nil {
			// Evento de rotação: atualiza nome e posição do binlog para o próximo arquivo
			if rotate, ok := ev.Event.(*replication.RotateEvent); ok {
				pos.Name = string(rotate.NextLogName)
				pos.Pos = uint32(rotate.Position)
				b.log.Info("Binlog rotacionado", "binlog_file", pos.Name, "binlog_pos", pos.Pos)
				b.persistPosition(pos)
				lastFlush = time.Now()
				continue
			}

			// NÃO reordenar: HandleEvent recebe a posição PRÉ-mutação (usada em generateEventID)
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.log.Error("Panic ao processar evento; evento descartado, watcher continua",
							"recover", r, "binlog_file", pos.Name, "binlog_pos", pos.Pos)
					}
				}()
				if err := prod.HandleEvent(pos.Name, pos.Pos, ev); err != nil {
					b.log.Error("Erro ao processar evento; evento pode ter sido perdido",
						"error", err, "binlog_file", pos.Name, "binlog_pos", pos.Pos)
				}
			}()
			pos.Pos = ev.Header.LogPos

			if time.Since(lastFlush) >= flushInterval {
				b.persistPosition(pos)
				lastFlush = time.Now()
			}
		}
	}
}
