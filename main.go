package main

import (
	"fmt"
	"log"
	"webhook-watcher/config"
)

func main() {
	fmt.Println("Iniciando Webhook Watcher...")

	cfg := config.LoadConfig()

	watcher := newBinlogWatcher(cfg)
	if err := watcher.Start(); err != nil {
		log.Fatalf("Erro ao rodar watcher de binlog: %v", err)
	}
}
