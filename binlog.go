package main

import (
	"context"
	"fmt"
	"time"

	"webhook-watcher/config"
	"webhook-watcher/producer"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type BinlogWatcher struct {
	cfg config.Config
}

func newBinlogWatcher(cfg config.Config) *BinlogWatcher {
	return &BinlogWatcher{cfg: cfg}
}

func (b *BinlogWatcher) Start() error {
	addr := fmt.Sprintf("%s:%d", b.cfg.Host, b.cfg.Port)

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
	b.cfg.SetBinlogData(binlogFile, uint32(binlogPos))

	pos := mysql.Position{
		Name: binlogFile,
		Pos:  uint32(binlogPos),
	}

	fmt.Printf("Posição atual do binlog: %s:%d\n", pos.Name, pos.Pos)

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: b.cfg.ServerID,
		Flavor:   b.cfg.Flavor,
		Host:     b.cfg.Host,
		Port:     b.cfg.Port,
		User:     b.cfg.User,
		Password: b.cfg.Password,
	}

	syncer := replication.NewBinlogSyncer(syncerCfg)
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("erro ao iniciar streamer sync: %w", err)
	}

	fmt.Println("Conectado e escutando eventos com sucesso!")

	prod := producer.NewProducer()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			continue
		}
		if err != nil {
			return fmt.Errorf("erro no evento: %w", err)
		}

		if ev != nil {
			// Evento de rotação: atualiza nome e posição do binlog para o próximo arquivo
			if rotate, ok := ev.Event.(*replication.RotateEvent); ok {
				pos.Name = string(rotate.NextLogName)
				pos.Pos = uint32(rotate.Position)
				fmt.Printf("Binlog rotacionado para %s:%d\n", pos.Name, pos.Pos)
				continue
			}

			prod.HandleEvent(pos.Name, pos.Pos, ev)
			pos.Pos = ev.Header.LogPos
		}
	}
}
