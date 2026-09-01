package main

import "testing"

func TestCheckHookColumns_ExpectedShape(t *testing.T) {
	ok, msg := checkHookColumns([]string{"tenant", "tipo", "url"})
	if !ok {
		t.Fatalf("esperava ok=true, obteve msg=%q", msg)
	}
}

func TestCheckHookColumns_CaseInsensitive(t *testing.T) {
	ok, _ := checkHookColumns([]string{"Tenant", "TIPO", "Url"})
	if !ok {
		t.Fatal("esperava ok=true (comparação case-insensitive)")
	}
}

func TestCheckHookColumns_WrongCount(t *testing.T) {
	ok, msg := checkHookColumns([]string{"tenant", "url"})
	if ok {
		t.Fatal("esperava ok=false para contagem errada de colunas")
	}
	if msg == "" {
		t.Fatal("esperava mensagem citando o que veio")
	}
}

func TestCheckHookColumns_WrongNames(t *testing.T) {
	ok, msg := checkHookColumns([]string{"tenant", "url", "tipo"})
	if ok {
		t.Fatal("esperava ok=false para ordem/nomes errados")
	}
	if msg == "" {
		t.Fatal("esperava mensagem citando o que veio")
	}
}
