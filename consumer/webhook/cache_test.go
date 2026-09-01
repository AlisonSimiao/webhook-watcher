package webhook

import (
	"io"
	"log/slog"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHookCache_Lookup_TableMatchesTipo(t *testing.T) {
	c := NewHookCache(nil, nil, testLogger())
	c.dest[destKey{tenant: "tenantA", tipo: "pedidos"}] = []string{"https://a.example.com/hook"}

	urls := c.Lookup("tenantA", "pedidos")
	if len(urls) != 1 || urls[0] != "https://a.example.com/hook" {
		t.Fatalf("esperava 1 URL, obteve %v", urls)
	}
}

func TestHookCache_Lookup_UnknownTableReturnsNil(t *testing.T) {
	c := NewHookCache(nil, nil, testLogger())
	c.dest[destKey{tenant: "tenantA", tipo: "pedidos"}] = []string{"https://a.example.com/hook"}

	urls := c.Lookup("tenantA", "clientes")
	if len(urls) != 0 {
		t.Fatalf("esperava slice vazia para tabela sem tradução conhecida, obteve %v", urls)
	}
}

func TestHookCache_Lookup_TenantSemHookRetornaVazio(t *testing.T) {
	c := NewHookCache(nil, nil, testLogger())
	urls := c.Lookup("tenantSemHook", "pedidos")
	if len(urls) != 0 {
		t.Fatalf("esperava slice vazia, obteve %v", urls)
	}
}

func TestHookCache_Lookup_MultipleURLsForSameTenantTipo(t *testing.T) {
	c := NewHookCache(nil, nil, testLogger())
	c.dest[destKey{tenant: "tenantA", tipo: "pedidos"}] = []string{
		"https://a.example.com/hook1",
		"https://a.example.com/hook2",
	}

	urls := c.Lookup("tenantA", "pedidos")
	if len(urls) != 2 {
		t.Fatalf("esperava 2 URLs, obteve %v", urls)
	}
}

func TestHookCache_Lookup_ReturnsCopyNotInternalSlice(t *testing.T) {
	c := NewHookCache(nil, nil, testLogger())
	c.dest[destKey{tenant: "tenantA", tipo: "pedidos"}] = []string{"https://a.example.com/hook"}

	urls := c.Lookup("tenantA", "pedidos")
	urls = append(urls, "injetado")

	again := c.Lookup("tenantA", "pedidos")
	if len(again) != 1 {
		t.Fatalf("Lookup deve retornar uma cópia da slice interna; cache foi alterado: %v", again)
	}
}
