/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// Package lrucache provides reference-count-aware lru cache.
package lrucache

import (
	"sync"

	"github.com/golang/groupcache/lru"
)

// Cache is "groupcache/lru"-like cache. The difference is that "groupcache/lru" immediately
// finalizes theevicted contents using OnEvicted callback but our version strictly tracks the
// reference counts of contents and calls OnEvicted when nobody refers to the evicted contents.
type Cache struct {
	cache *lru.Cache
	mu    sync.Mutex

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key string, value interface{})
}

// New creates new cache.
func New(maxEntries int) *Cache {
	inner := lru.New(maxEntries)
	inner.OnEvicted = func(key lru.Key, value interface{}) {
		// Decrease the ref count incremented in Add().
		// When nobody refers to this value, this value will be finalized via refCounter.
		value.(*refCounter).finalize()
	}
	return &Cache{
		cache: inner,
	}
}

// Get retrieves the specified object from the cache and increments the reference counter of the
// target content. Client must call `done` callback to decrease the reference count when the value
// will no longer be used.
func (c *Cache) Get(key string) (value interface{}, done func(), ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, ok := c.cache.Get(key)
	if !ok {
		return nil, nil, false
	}
	rc := o.(*refCounter)
	rc.inc()
	return rc.v, c.decreaseOnceFunc(rc), true
}

// Add adds object to the cache and returns the cached contents with incrementing the reference count.
// If the specified content already exists in the cache, this sets `added` to false and returns
// "already cached" content (i.e. doesn't replace the content with the new one). Client must call
// `done` callback to decrease the counter when the value will no longer be used.
func (c *Cache) Add(key string, value interface{}) (cachedValue interface{}, done func(), added bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o, ok := c.cache.Get(key); ok {
		rc := o.(*refCounter)
		rc.inc()
		return rc.v, c.decreaseOnceFunc(rc), false
	}
	rc := &refCounter{
		key:       key,
		v:         value,
		onEvicted: c.OnEvicted,
	}
	rc.initialize() // Keep this object having at least 1 ref count (will be decreased in OnEviction)
	rc.inc()        // The client references this object (will be decreased on "done")
	c.cache.Add(key, rc)
	return rc.v, c.decreaseOnceFunc(rc), true
}

// Remove removes the specified contents from the cache. OnEvicted callback will be called when
// nobody refers to the removed content.
func (c *Cache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Remove(key)
}

func (c *Cache) decreaseOnceFunc(rc *refCounter) func() {
	var once sync.Once
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		once.Do(func() { rc.dec() })
	}
}

type refCounter struct {
	onEvicted func(key string, value interface{})

	key       string
	v         interface{}
	refCounts int64

	mu sync.Mutex

	initializeOnce sync.Once
	finalizeOnce   sync.Once
}

func (r *refCounter) inc() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refCounts++
}

func (r *refCounter) dec() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refCounts--
	if r.refCounts <= 0 && r.onEvicted != nil {
		// nobody will refer this object
		r.onEvicted(r.key, r.v)
	}
}

func (r *refCounter) initialize() {
	r.initializeOnce.Do(func() { r.inc() })
}

func (r *refCounter) finalize() {
	r.finalizeOnce.Do(func() { r.dec() })
}
