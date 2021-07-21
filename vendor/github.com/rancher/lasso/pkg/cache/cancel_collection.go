package cache

import (
	"context"
	"sync"
	"sync/atomic"
)

type CancelCollection struct {
	id    int64
	items sync.Map
}

func (c *CancelCollection) Add(ctx context.Context, obj interface{}) {
	key := atomic.AddInt64(&c.id, 1)
	c.items.Store(key, obj)
	go func() {
		<-ctx.Done()
		c.items.Delete(key)
	}()
}

func (c *CancelCollection) List() (result []interface{}) {
	c.items.Range(func(key, value interface{}) bool {
		result = append(result, value)
		return true
	})
	return
}
