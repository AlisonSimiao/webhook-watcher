package queue

import (
	"context"
	"errors"
	"testing"
)

type fakeTarget struct {
	err   error // nil = sucesso
	calls int
}

func (f *fakeTarget) Enqueue(_ context.Context, _ *Event) error {
	f.calls++
	return f.err
}

func (f *fakeTarget) Close() error { return f.err }

func TestFanoutQueue_EnqueueAllSucceed(t *testing.T) {
	a := &fakeTarget{}
	b := &fakeTarget{}
	f := NewFanoutQueue(a, b)

	if err := f.Enqueue(context.Background(), &Event{ID: "evt_1"}); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}
	if a.calls != 1 || b.calls != 1 {
		t.Fatalf("esperava 1 chamada em cada alvo, obteve a=%d b=%d", a.calls, b.calls)
	}
}

func TestFanoutQueue_AllDuplicate(t *testing.T) {
	a := &fakeTarget{err: ErrDuplicate}
	b := &fakeTarget{err: ErrDuplicate}
	f := NewFanoutQueue(a, b)

	err := f.Enqueue(context.Background(), &Event{ID: "evt_1"})
	if !IsDuplicate(err) {
		t.Fatalf("esperava ErrDuplicate, obteve %v", err)
	}
}

func TestFanoutQueue_PartialDuplicatePartialSuccess(t *testing.T) {
	a := &fakeTarget{err: ErrDuplicate}
	b := &fakeTarget{}
	f := NewFanoutQueue(a, b)

	if err := f.Enqueue(context.Background(), &Event{ID: "evt_1"}); err != nil {
		t.Fatalf("esperava nil (duplicidade parcial não é falha), obteve %v", err)
	}
}

func TestFanoutQueue_OneRealFailureAmongDuplicates(t *testing.T) {
	a := &fakeTarget{err: ErrDuplicate}
	b := &fakeTarget{err: errors.New("erro real de enqueue")}
	f := NewFanoutQueue(a, b)

	err := f.Enqueue(context.Background(), &Event{ID: "evt_1"})
	if err == nil {
		t.Fatal("esperava erro, obteve nil")
	}
	if IsDuplicate(err) {
		t.Fatalf("uma falha real não deveria ser mascarada como duplicidade: %v", err)
	}
}

func TestFanoutQueue_AllTargetsCalledEvenIfEarlyOneFails(t *testing.T) {
	a := &fakeTarget{err: errors.New("falha")}
	b := &fakeTarget{}
	c := &fakeTarget{}
	f := NewFanoutQueue(a, b, c)

	_ = f.Enqueue(context.Background(), &Event{ID: "evt_1"})

	if a.calls != 1 || b.calls != 1 || c.calls != 1 {
		t.Fatalf("esperava todos os alvos chamados, obteve a=%d b=%d c=%d", a.calls, b.calls, c.calls)
	}
}

func TestFanoutQueue_Close(t *testing.T) {
	a := &fakeTarget{}
	b := &fakeTarget{err: errors.New("falha ao fechar")}
	f := NewFanoutQueue(a, b)

	if err := f.Close(); err == nil {
		t.Fatal("esperava erro agregado de Close, obteve nil")
	}
}

func TestFanoutQueue_NoTargets(t *testing.T) {
	f := NewFanoutQueue()

	if err := f.Enqueue(context.Background(), &Event{ID: "evt_1"}); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("esperava nil, obteve %v", err)
	}
}
