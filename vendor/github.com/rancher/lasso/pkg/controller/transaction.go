package controller

import (
	"context"
	"sync"
)

type hTransactionKey struct{}

type HandlerTransaction struct {
	context.Context

	lock   sync.Mutex
	parent context.Context
	todo   []func()
}

func (h *HandlerTransaction) do(f func()) {
	if h == nil {
		f()
		return
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	h.todo = append(h.todo, f)
}

func (h *HandlerTransaction) Commit() {
	h.lock.Lock()
	fs := h.todo
	h.todo = nil
	h.lock.Unlock()

	for _, f := range fs {
		f()
	}
}

func (h *HandlerTransaction) Rollback() {
	h.lock.Lock()
	h.todo = nil
	h.lock.Unlock()
}

func NewHandlerTransaction(ctx context.Context) *HandlerTransaction {
	ht := &HandlerTransaction{
		parent: ctx,
	}
	ctx = context.WithValue(ctx, hTransactionKey{}, ht)
	ht.Context = ctx
	return ht
}

func getHandlerTransaction(ctx context.Context) *HandlerTransaction {
	v, _ := ctx.Value(hTransactionKey{}).(*HandlerTransaction)
	return v
}
