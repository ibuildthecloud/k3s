package cache

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/client"
)

const (
	defaultTimeout = 15 * time.Second
)

type healthcheck struct {
	lock     sync.Mutex
	cf       client.SharedClientFactory
	callback func(bool)
}

func (h *healthcheck) ping(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	return h.cf.IsHealthy(ctx)
}

func (h *healthcheck) start(ctx context.Context, cf client.SharedClientFactory) error {
	first, err := h.initialize(cf)
	if err != nil {
		return err
	}
	if first {
		h.ensureHealthy(ctx)
	}
	return nil
}

func (h *healthcheck) initialize(cf client.SharedClientFactory) (bool, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.cf != nil {
		return false, nil
	}

	h.cf = cf
	return true, nil
}

func (h *healthcheck) ensureHealthy(ctx context.Context) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.pingUntilGood(ctx)
}

func (h *healthcheck) report(good bool) {
	if h.callback != nil {
		h.callback(good)
	}
}

func (h *healthcheck) pingUntilGood(ctx context.Context) {
	for {
		if h.ping(ctx) {
			h.report(true)
			return
		}

		h.report(false)
		time.Sleep(defaultTimeout)
	}
}
