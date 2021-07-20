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

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package layer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/stargz-snapshotter/cache"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/fs/config"
	"github.com/containerd/stargz-snapshotter/fs/reader"
	"github.com/containerd/stargz-snapshotter/fs/remote"
	"github.com/containerd/stargz-snapshotter/fs/source"
	"github.com/containerd/stargz-snapshotter/task"
	"github.com/containerd/stargz-snapshotter/util/lrucache"
	"github.com/containerd/stargz-snapshotter/util/namedmutex"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	defaultResolveResultEntry = 30
	defaultMaxLRUCacheEntry   = 10
	defaultMaxCacheFds        = 10
	defaultPrefetchTimeoutSec = 10
	memoryCacheType           = "memory"
)

// Layer represents a layer.
type Layer interface {
	// Info returns the information of this layer.
	Info() Info

	// RootNode returns the root node of this layer.
	RootNode() (fusefs.InodeEmbedder, error)

	// Check checks if the layer is still connectable.
	Check() error

	// Refresh refreshes the layer connection.
	Refresh(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error

	// Verify verifies this layer using the passed TOC Digest.
	Verify(tocDigest digest.Digest) (err error)

	// SkipVerify skips verification for this layer.
	SkipVerify()

	// Prefetch prefetches the specified size. If the layer is eStargz and contains landmark files,
	// the range indicated by these files is respected.
	// Calling this function before calling Verify or SkipVerify will fail.
	Prefetch(prefetchSize int64) error

	// ReadAt reads this layer.
	ReadAt([]byte, int64, ...remote.Option) (int, error)

	// WaitForPrefetchCompletion waits untils Prefetch completes.
	WaitForPrefetchCompletion() error

	// BackgroundFetch fetches the entire layer contents to the cache.
	// Fetching contents is done as a background task.
	// Calling this function before calling Verify or SkipVerify will fail.
	BackgroundFetch() error

	// Done releases the reference to this layer. The resources related to this layer will be
	// discarded sooner or later. Queries after calling this function won't be serviced.
	Done()
}

// Info is the current status of a layer.
type Info struct {
	Digest      digest.Digest
	Size        int64
	FetchedSize int64
}

// Resolver resolves the layer location and provieds the handler of that layer.
type Resolver struct {
	rootDir               string
	resolver              *remote.Resolver
	prefetchTimeout       time.Duration
	layerCache            *lrucache.Cache
	layerCacheMu          sync.Mutex
	blobCache             *lrucache.Cache
	blobCacheMu           sync.Mutex
	backgroundTaskManager *task.BackgroundTaskManager
	resolveLock           *namedmutex.NamedMutex
	config                config.Config
}

// NewResolver returns a new layer resolver.
func NewResolver(root string, backgroundTaskManager *task.BackgroundTaskManager, cfg config.Config) (*Resolver, error) {
	resolveResultEntry := cfg.ResolveResultEntry
	if resolveResultEntry == 0 {
		resolveResultEntry = defaultResolveResultEntry
	}
	prefetchTimeout := time.Duration(cfg.PrefetchTimeoutSec) * time.Second
	if prefetchTimeout == 0 {
		prefetchTimeout = defaultPrefetchTimeoutSec * time.Second
	}

	// layerCache caches resolved layers for future use. This is useful in a use-case where
	// the filesystem resolves and caches all layers in an image (not only queried one) in parallel,
	// before they are actually queried.
	layerCache := lrucache.New(resolveResultEntry)
	layerCache.OnEvicted = func(key string, value interface{}) {
		if err := value.(*layer).close(); err != nil {
			logrus.WithField("key", key).WithError(err).Warnf("failed to clean up layer")
			return
		}
		logrus.WithField("key", key).Debugf("cleaned up layer")
	}

	// blobCache caches resolved blobs for futural use. This is especially useful when a layer
	// isn't eStargz/stargz (the *layer object won't be created/cached in this case).
	blobCache := lrucache.New(resolveResultEntry)
	blobCache.OnEvicted = func(key string, value interface{}) {
		if err := value.(remote.Blob).Close(); err != nil {
			logrus.WithField("key", key).WithError(err).Warnf("failed to clean up blob")
			return
		}
		logrus.WithField("key", key).Debugf("cleaned up blob")
	}

	return &Resolver{
		rootDir:               root,
		resolver:              remote.NewResolver(cfg.BlobConfig),
		layerCache:            layerCache,
		blobCache:             blobCache,
		prefetchTimeout:       prefetchTimeout,
		backgroundTaskManager: backgroundTaskManager,
		config:                cfg,
		resolveLock:           new(namedmutex.NamedMutex),
	}, nil
}

func newCache(root string, cacheType string, cfg config.Config) (cache.BlobCache, error) {
	if cacheType == memoryCacheType {
		return cache.NewMemoryCache(), nil
	}

	dcc := cfg.DirectoryCacheConfig
	maxDataEntry := dcc.MaxLRUCacheEntry
	if maxDataEntry == 0 {
		maxDataEntry = defaultMaxLRUCacheEntry
	}
	maxFdEntry := dcc.MaxCacheFds
	if maxFdEntry == 0 {
		maxFdEntry = defaultMaxCacheFds
	}

	bufPool := &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	dCache, fCache := lrucache.New(maxDataEntry), lrucache.New(maxFdEntry)
	dCache.OnEvicted = func(key string, value interface{}) {
		bufPool.Put(value)
	}
	fCache.OnEvicted = func(key string, value interface{}) {
		value.(*os.File).Close()
	}
	// create a cache on an unique directory
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	cachePath, err := ioutil.TempDir(root, "")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize directory cache")
	}
	return cache.NewDirectoryCache(
		cachePath,
		cache.DirectoryCacheConfig{
			SyncAdd:   dcc.SyncAdd,
			DataCache: dCache,
			FdCache:   fCache,
			BufPool:   bufPool,
		},
	)
}

// Resolve resolves a layer based on the passed layer blob information.
func (r *Resolver) Resolve(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) (_ Layer, retErr error) {
	name := refspec.String() + "/" + desc.Digest.String()

	// Wait if resolving this layer is already running. The result
	// can hopefully get from the LRU cache.
	r.resolveLock.Lock(name)
	defer r.resolveLock.Unlock(name)

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("src", name))

	// First, try to retrieve this layer from the underlying LRU cache.
	r.layerCacheMu.Lock()
	c, done, ok := r.layerCache.Get(name)
	r.layerCacheMu.Unlock()
	if ok {
		if l := c.(*layer); l.Check() == nil {
			log.G(ctx).Debugf("hit layer cache %q", name)
			return &layerRef{l, done}, nil
		}
		// Cached layer is invalid
		done()
		r.layerCacheMu.Lock()
		r.layerCache.Remove(name)
		r.layerCacheMu.Unlock()
	}

	log.G(ctx).Debugf("resolving")

	// Resolve the blob.
	blobR, err := r.resolveBlob(ctx, hosts, refspec, desc)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to resolve the blob")
	}
	defer func() {
		if retErr != nil {
			blobR.done()
		}
	}()

	fsCache, err := newCache(filepath.Join(r.rootDir, "fscache"), r.config.FSCacheType, r.config)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create fs cache")
	}
	defer func() {
		if retErr != nil {
			fsCache.Close()
		}
	}()

	// Get a reader for stargz archive.
	// Each file's read operation is a prioritized task and all background tasks
	// will be stopped during the execution so this can avoid being disturbed for
	// NW traffic by background tasks.
	sr := io.NewSectionReader(readerAtFunc(func(p []byte, offset int64) (n int, err error) {
		r.backgroundTaskManager.DoPrioritizedTask()
		defer r.backgroundTaskManager.DonePrioritizedTask()
		return blobR.ReadAt(p, offset)
	}), 0, blobR.Size())
	vr, err := reader.NewReader(sr, fsCache)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read layer")
	}

	// Combine layer information together and cache it.
	l := newLayer(r, desc, blobR, vr)
	r.layerCacheMu.Lock()
	cachedL, done2, added := r.layerCache.Add(name, l)
	r.layerCacheMu.Unlock()
	if !added {
		l.close() // layer already exists in the cache. discrad this.
	}

	log.G(ctx).Debugf("resolved")
	return &layerRef{cachedL.(*layer), done2}, nil
}

// resolveBlob resolves a blob based on the passed layer blob information.
func (r *Resolver) resolveBlob(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) (_ *blobRef, retErr error) {
	name := refspec.String() + "/" + desc.Digest.String()

	// Try to retrieve the blob from the underlying LRU cache.
	r.blobCacheMu.Lock()
	c, done, ok := r.blobCache.Get(name)
	r.blobCacheMu.Unlock()
	if ok {
		if blob := c.(remote.Blob); blob.Check() == nil {
			return &blobRef{blob, done}, nil
		}
		// invalid blob. discard this.
		done()
		r.blobCacheMu.Lock()
		r.blobCache.Remove(name)
		r.blobCacheMu.Unlock()
	}

	httpCache, err := newCache(filepath.Join(r.rootDir, "httpcache"), r.config.HTTPCacheType, r.config)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create http cache")
	}
	defer func() {
		if retErr != nil {
			httpCache.Close()
		}
	}()

	// Resolve the blob and cache the result.
	b, err := r.resolver.Resolve(ctx, hosts, refspec, desc, httpCache)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve the source")
	}
	r.blobCacheMu.Lock()
	cachedB, done, added := r.blobCache.Add(name, b)
	r.blobCacheMu.Unlock()
	if !added {
		b.Close() // blob already exists in the cache. discard this.
	}
	return &blobRef{cachedB.(remote.Blob), done}, nil
}

// Cache is similar to Resolve but the result isn't returned. Instead, it'll be stored in the cache.
func (r *Resolver) Cache(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error {
	l, err := r.Resolve(ctx, hosts, refspec, desc)
	if err != nil {
		return err
	}
	// Release this layer. However, this will remain on the cache until eviction.
	// Until then, the client can reuse this (already pre-resolved) layer.
	l.Done()
	return nil
}

func newLayer(
	resolver *Resolver,
	desc ocispec.Descriptor,
	blob *blobRef,
	vr *reader.VerifiableReader,
) *layer {
	return &layer{
		resolver:         resolver,
		desc:             desc,
		blob:             blob,
		verifiableReader: vr,
		prefetchWaiter:   newWaiter(),
	}
}

type layer struct {
	resolver         *Resolver
	desc             ocispec.Descriptor
	blob             *blobRef
	verifiableReader *reader.VerifiableReader
	prefetchWaiter   *waiter

	r reader.Reader

	closed   bool
	closedMu sync.Mutex
}

func (l *layer) Info() Info {
	return Info{
		Digest:      l.desc.Digest,
		Size:        l.blob.Size(),
		FetchedSize: l.blob.FetchedSize(),
	}
}

func (l *layer) Check() error {
	if l.isClosed() {
		return fmt.Errorf("layer is already closed")
	}
	return l.blob.Check()
}

func (l *layer) Refresh(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error {
	if l.isClosed() {
		return fmt.Errorf("layer is already closed")
	}
	return l.blob.Refresh(ctx, hosts, refspec, desc)
}

func (l *layer) Verify(tocDigest digest.Digest) (err error) {
	if l.isClosed() {
		return fmt.Errorf("layer is already closed")
	}
	l.r, err = l.verifiableReader.VerifyTOC(tocDigest)
	return
}

func (l *layer) SkipVerify() {
	l.r = l.verifiableReader.SkipVerify()
}

func (l *layer) Prefetch(prefetchSize int64) error {
	defer l.prefetchWaiter.done() // Notify the completion

	if l.isClosed() {
		return fmt.Errorf("layer is already closed")
	}
	if l.r == nil {
		return fmt.Errorf("layer hasn't been verified yet")
	}
	lr := l.r
	if _, ok := lr.Lookup(estargz.NoPrefetchLandmark); ok {
		// do not prefetch this layer
		return nil
	} else if e, ok := lr.Lookup(estargz.PrefetchLandmark); ok {
		// override the prefetch size with optimized value
		prefetchSize = e.Offset
	} else if prefetchSize > l.blob.Size() {
		// adjust prefetch size not to exceed the whole layer size
		prefetchSize = l.blob.Size()
	}

	// Fetch the target range
	if err := l.blob.Cache(0, prefetchSize); err != nil {
		return errors.Wrap(err, "failed to prefetch layer")
	}

	// Cache uncompressed contents of the prefetched range
	if err := lr.Cache(reader.WithFilter(func(e *estargz.TOCEntry) bool {
		return e.Offset < prefetchSize // Cache only prefetch target
	})); err != nil {
		return errors.Wrap(err, "failed to cache prefetched layer")
	}

	return nil
}

func (l *layer) WaitForPrefetchCompletion() error {
	if l.isClosed() {
		return fmt.Errorf("layer is already closed")
	}
	return l.prefetchWaiter.wait(l.resolver.prefetchTimeout)
}

func (l *layer) BackgroundFetch() error {
	if l.isClosed() {
		return fmt.Errorf("layer is already closed")
	}
	if l.r == nil {
		return fmt.Errorf("layer hasn't been verified yet")
	}
	lr := l.r
	br := io.NewSectionReader(readerAtFunc(func(p []byte, offset int64) (retN int, retErr error) {
		l.resolver.backgroundTaskManager.InvokeBackgroundTask(func(ctx context.Context) {
			retN, retErr = l.blob.ReadAt(
				p,
				offset,
				remote.WithContext(ctx),              // Make cancellable
				remote.WithCacheOpts(cache.Direct()), // Do not pollute mem cache
			)
		}, 120*time.Second)
		return
	}), 0, l.blob.Size())
	return lr.Cache(
		reader.WithReader(br),                // Read contents in background
		reader.WithCacheOpts(cache.Direct()), // Do not pollute mem cache
	)
}

func (l *layerRef) Done() {
	l.done()
}

func (l *layer) RootNode() (fusefs.InodeEmbedder, error) {
	if l.isClosed() {
		return nil, fmt.Errorf("layer is already closed")
	}
	if l.r == nil {
		return nil, fmt.Errorf("layer hasn't been verified yet")
	}
	return newNode(l.desc.Digest, l.r, l.blob)
}

func (l *layer) ReadAt(p []byte, offset int64, opts ...remote.Option) (int, error) {
	return l.blob.ReadAt(p, offset, opts...)
}

func (l *layer) close() error {
	l.closedMu.Lock()
	defer l.closedMu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	defer l.blob.done() // Close reader first, then close the blob
	l.verifiableReader.Close()
	if l.r != nil {
		return l.r.Close()
	}
	return nil
}

func (l *layer) isClosed() bool {
	l.closedMu.Lock()
	closed := l.closed
	l.closedMu.Unlock()
	return closed
}

// blobRef is a reference to the blob in the cache. Calling `done` decreases the reference counter
// of this blob in the underlying cache. When nobody refers to the blob in the cache, resources bound
// to this blob will be discarded.
type blobRef struct {
	remote.Blob
	done func()
}

// layerRef is a reference to the layer in the cache. Calling `Done` or `done` decreases the
// reference counter of this blob in the underlying cache. When nobody refers to the layer in the
// cache, resources bound to this layer will be discarded.
type layerRef struct {
	*layer
	done func()
}

func newWaiter() *waiter {
	return &waiter{
		completionCond: sync.NewCond(&sync.Mutex{}),
	}
}

type waiter struct {
	isDone         bool
	isDoneMu       sync.Mutex
	completionCond *sync.Cond
}

func (w *waiter) done() {
	w.isDoneMu.Lock()
	w.isDone = true
	w.isDoneMu.Unlock()
	w.completionCond.Broadcast()
}

func (w *waiter) wait(timeout time.Duration) error {
	wait := func() <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			w.isDoneMu.Lock()
			isDone := w.isDone
			w.isDoneMu.Unlock()

			w.completionCond.L.Lock()
			if !isDone {
				w.completionCond.Wait()
			}
			w.completionCond.L.Unlock()
			ch <- struct{}{}
		}()
		return ch
	}
	select {
	case <-time.After(timeout):
		w.isDoneMu.Lock()
		w.isDone = true
		w.isDoneMu.Unlock()
		w.completionCond.Broadcast()
		return fmt.Errorf("timeout(%v)", timeout)
	case <-wait():
		return nil
	}
}

type readerAtFunc func([]byte, int64) (int, error)

func (f readerAtFunc) ReadAt(p []byte, offset int64) (int, error) { return f(p, offset) }
