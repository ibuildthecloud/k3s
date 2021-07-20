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

package cache

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/stargz-snapshotter/util/lrucache"
	"github.com/containerd/stargz-snapshotter/util/namedmutex"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

const (
	defaultMaxLRUCacheEntry = 10
	defaultMaxCacheFds      = 10
)

type DirectoryCacheConfig struct {

	// Number of entries of LRU cache (default: 10).
	// This won't be used when DataCache is specified.
	MaxLRUCacheEntry int

	// Number of file descriptors to cache (default: 10).
	// This won't be used when FdCache is specified.
	MaxCacheFds int

	// On Add, wait until the data is fully written to the cache directory.
	SyncAdd bool

	// DataCache is an on-memory cache of the data.
	// OnEvicted will be overridden and replaced for internal use.
	DataCache *lrucache.Cache

	// FdCache is a cache for opened file descriptors.
	// OnEvicted will be overridden and replaced for internal use.
	FdCache *lrucache.Cache

	// BufPool will be used for pooling bytes.Buffer.
	BufPool *sync.Pool
}

// TODO: contents validation.

// BlobCache represents a cache for bytes data
type BlobCache interface {
	// Add returns a writer to add contents to cache
	Add(key string, opts ...Option) (Writer, error)

	// Get returns a reader to read the specified contents
	// from cache
	Get(key string, opts ...Option) (Reader, error)

	// Close closes the cache
	Close() error
}

// Reader provides the data cached.
type Reader interface {
	io.ReaderAt
	Close() error
}

// Writer enables the client to cache byte data. Commit() must be
// called after data is fully written to Write(). To abort the written
// data, Abort() must be called.
type Writer interface {
	io.WriteCloser
	Commit() error
	Abort() error
}

type cacheOpt struct {
	direct bool
}

type Option func(o *cacheOpt) *cacheOpt

// When Direct option is specified for FetchAt and Add methods, these operation
// won't use on-memory caches. When you know that the targeting value won't be
// used immediately, you can prevent the limited space of on-memory caches from
// being polluted by these unimportant values.
func Direct() Option {
	return func(o *cacheOpt) *cacheOpt {
		o.direct = true
		return o
	}
}

func NewDirectoryCache(directory string, config DirectoryCacheConfig) (BlobCache, error) {
	if !filepath.IsAbs(directory) {
		return nil, fmt.Errorf("dir cache path must be an absolute path; got %q", directory)
	}
	bufPool := config.BufPool
	if bufPool == nil {
		bufPool = &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		}
	}
	dataCache := config.DataCache
	if dataCache == nil {
		maxEntry := config.MaxLRUCacheEntry
		if maxEntry == 0 {
			maxEntry = defaultMaxLRUCacheEntry
		}
		dataCache = lrucache.New(maxEntry)
		dataCache.OnEvicted = func(key string, value interface{}) {
			bufPool.Put(value)
		}
	}
	fdCache := config.FdCache
	if fdCache == nil {
		maxEntry := config.MaxCacheFds
		if maxEntry == 0 {
			maxEntry = defaultMaxCacheFds
		}
		fdCache = lrucache.New(maxEntry)
		fdCache.OnEvicted = func(key string, value interface{}) {
			value.(*os.File).Close()
		}
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	wipdir := filepath.Join(directory, "wip")
	if err := os.MkdirAll(wipdir, 0700); err != nil {
		return nil, err
	}
	dc := &directoryCache{
		cache:        dataCache,
		fileCache:    fdCache,
		wipLock:      new(namedmutex.NamedMutex),
		directory:    directory,
		wipDirectory: wipdir,
		bufPool:      bufPool,
	}
	dc.syncAdd = config.SyncAdd
	return dc, nil
}

// directoryCache is a cache implementation which backend is a directory.
type directoryCache struct {
	cache        *lrucache.Cache
	fileCache    *lrucache.Cache
	wipDirectory string
	directory    string
	wipLock      *namedmutex.NamedMutex

	bufPool *sync.Pool

	syncAdd bool

	closed   bool
	closedMu sync.Mutex
}

func (dc *directoryCache) Get(key string, opts ...Option) (Reader, error) {
	if dc.isClosed() {
		return nil, fmt.Errorf("cache is already closed")
	}

	opt := &cacheOpt{}
	for _, o := range opts {
		opt = o(opt)
	}

	if !opt.direct {
		// Get data from memory
		if b, done, ok := dc.cache.Get(key); ok {
			return &reader{
				ReaderAt: bytes.NewReader(b.(*bytes.Buffer).Bytes()),
				closeFunc: func() error {
					done()
					return nil
				},
			}, nil
		}

		// Get data from disk. If the file is already opened, use it.
		if f, done, ok := dc.fileCache.Get(key); ok {
			return &reader{
				ReaderAt: f.(*os.File),
				closeFunc: func() error {
					done() // file will be closed when it's evicted from the cache
					return nil
				},
			}, nil
		}
	}

	// Open the cache file and read the target region
	// TODO: If the target cache is write-in-progress, should we wait for the completion
	//       or simply report the cache miss?
	file, err := os.Open(dc.cachePath(key))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open blob file for %q", key)
	}

	// If "direct" option is specified, do not cache the file on memory.
	// This option is useful for preventing memory cache from being polluted by data
	// that won't be accessed immediately.
	if opt.direct {
		return &reader{
			ReaderAt:  file,
			closeFunc: func() error { return file.Close() },
		}, nil
	}

	// TODO: should we cache the entire file data on memory?
	//       but making I/O (possibly huge) on every fetching
	//       might be costly.
	return &reader{
		ReaderAt: file,
		closeFunc: func() error {
			_, done, added := dc.fileCache.Add(key, file)
			defer done() // Release it immediately. Cleaned up on eviction.
			if !added {
				return file.Close() // file already exists in the cache. close it.
			}
			return nil
		},
	}, nil
}

func (dc *directoryCache) Add(key string, opts ...Option) (Writer, error) {
	if dc.isClosed() {
		return nil, fmt.Errorf("cache is already closed")
	}

	opt := &cacheOpt{}
	for _, o := range opts {
		opt = o(opt)
	}

	wip, err := dc.wipFile(key)
	if err != nil {
		return nil, err
	}
	w := &writer{
		WriteCloser: wip,
		commitFunc: func() error {
			if dc.isClosed() {
				return fmt.Errorf("cache is already closed")
			}
			// Commit the cache contents
			c := dc.cachePath(key)
			if err := os.MkdirAll(filepath.Dir(c), os.ModePerm); err != nil {
				var allErr error
				if err := os.Remove(wip.Name()); err != nil {
					allErr = multierror.Append(allErr, err)
				}
				return multierror.Append(allErr,
					errors.Wrapf(err, "failed to create cache directory %q", c))
			}
			return os.Rename(wip.Name(), c)
		},
		abortFunc: func() error {
			return os.Remove(wip.Name())
		},
	}

	// If "direct" option is specified, do not cache the passed data on memory.
	// This option is useful for preventing memory cache from being polluted by data
	// that won't be accessed immediately.
	if opt.direct {
		return w, nil
	}

	b := dc.bufPool.Get().(*bytes.Buffer)
	b.Reset()
	memW := &writer{
		WriteCloser: nopWriteCloser(io.Writer(b)),
		commitFunc: func() error {
			if dc.isClosed() {
				w.Close()
				return fmt.Errorf("cache is already closed")
			}
			cached, done, added := dc.cache.Add(key, b)
			if !added {
				dc.bufPool.Put(b) // already exists in the cache. abort it.
			}
			commit := func() error {
				defer done()
				defer w.Close()
				n, err := w.Write(cached.(*bytes.Buffer).Bytes())
				if err != nil || n != cached.(*bytes.Buffer).Len() {
					w.Abort()
					return err
				}
				return w.Commit()
			}
			if dc.syncAdd {
				return commit()
			}
			go func() {
				if err := commit(); err != nil {
					fmt.Println("failed to commit to file:", err)
				}
			}()
			return nil
		},
		abortFunc: func() error {
			defer w.Close()
			defer w.Abort()
			dc.bufPool.Put(b) // abort it.
			return nil
		},
	}

	return memW, nil
}

func (dc *directoryCache) Close() error {
	dc.closedMu.Lock()
	defer dc.closedMu.Unlock()
	if dc.closed {
		return nil
	}
	dc.closed = true
	if err := os.RemoveAll(dc.directory); err != nil {
		return err
	}
	return nil
}

func (dc *directoryCache) isClosed() bool {
	dc.closedMu.Lock()
	closed := dc.closed
	dc.closedMu.Unlock()
	return closed
}

func (dc *directoryCache) cachePath(key string) string {
	return filepath.Join(dc.directory, key[:2], key)
}

func (dc *directoryCache) wipFile(key string) (*os.File, error) {
	return ioutil.TempFile(dc.wipDirectory, key+"-*")
}

func NewMemoryCache() BlobCache {
	return &MemoryCache{
		Membuf: map[string]*bytes.Buffer{},
	}
}

// MemoryCache is a cache implementation which backend is a memory.
type MemoryCache struct {
	Membuf map[string]*bytes.Buffer
	mu     sync.Mutex
}

func (mc *MemoryCache) Get(key string, opts ...Option) (Reader, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	b, ok := mc.Membuf[key]
	if !ok {
		return nil, fmt.Errorf("Missed cache: %q", key)
	}
	return &reader{bytes.NewReader(b.Bytes()), func() error { return nil }}, nil
}

func (mc *MemoryCache) Add(key string, opts ...Option) (Writer, error) {
	b := new(bytes.Buffer)
	return &writer{
		WriteCloser: nopWriteCloser(io.Writer(b)),
		commitFunc: func() error {
			mc.mu.Lock()
			defer mc.mu.Unlock()
			mc.Membuf[key] = b
			return nil
		},
		abortFunc: func() error { return nil },
	}, nil
}

func (mc *MemoryCache) Close() error {
	return nil
}

type reader struct {
	io.ReaderAt
	closeFunc func() error
}

func (r *reader) Close() error { return r.closeFunc() }

type writer struct {
	io.WriteCloser
	commitFunc func() error
	abortFunc  func() error
}

func (w *writer) Commit() error {
	return w.commitFunc()
}

func (w *writer) Abort() error {
	return w.abortFunc()
}

type writeCloser struct {
	io.Writer
	closeFunc func() error
}

func (w *writeCloser) Close() error { return w.closeFunc() }

func nopWriteCloser(w io.Writer) io.WriteCloser {
	return &writeCloser{w, func() error { return nil }}
}
