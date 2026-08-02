package main

import (
	"context"
	"log/slog"
)

// dropMessageHandler descarta registros cujas mensagens estão na lista e
// repassa o restante ao handler seguinte. Usado para filtrar logs da
// go-mysql que não são serializáveis para JSON (ex: config com funções).
type dropMessageHandler struct {
	next    slog.Handler
	dropped map[string]bool
}

func (h *dropMessageHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *dropMessageHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.dropped[r.Message] {
		return nil
	}
	return h.next.Handle(ctx, r)
}

func (h *dropMessageHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &dropMessageHandler{next: h.next.WithAttrs(attrs), dropped: h.dropped}
}

func (h *dropMessageHandler) WithGroup(name string) slog.Handler {
	return &dropMessageHandler{next: h.next.WithGroup(name), dropped: h.dropped}
}
