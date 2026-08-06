package main

import (
	"testing"

	"webhook-watcher/config"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestDecideStartPosition_FreshServer(t *testing.T) {
	cfg := config.ServerConfig{}

	pos, needsQuery := decideStartPosition(cfg)

	if !needsQuery {
		t.Fatalf("esperava needsQuery=true para servidor sem posição salva")
	}
	if pos != (mysql.Position{}) {
		t.Fatalf("esperava posição vazia, obteve %+v", pos)
	}
}

func TestDecideStartPosition_ResumesFromPersisted(t *testing.T) {
	cfg := config.ServerConfig{BinlogFile: "mysql-bin.000005", BinlogPos: 999}

	pos, needsQuery := decideStartPosition(cfg)

	if needsQuery {
		t.Fatalf("esperava needsQuery=false quando já há posição salva")
	}
	want := mysql.Position{Name: "mysql-bin.000005", Pos: 999}
	if pos != want {
		t.Fatalf("posição incorreta: got %+v, want %+v", pos, want)
	}
}
