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

package reader

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/containerd/stargz-snapshotter/cache"
	"github.com/containerd/stargz-snapshotter/estargz"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const maxWalkDepth = 10000

type Reader interface {
	OpenFile(name string) (io.ReaderAt, error)
	Lookup(name string) (*estargz.TOCEntry, bool)
	Cache(opts ...CacheOption) error
	Close() error
}

// VerifiableReader produces a Reader with a given verifier.
type VerifiableReader struct {
	r *reader
}

func (vr *VerifiableReader) SkipVerify() Reader {
	vr.r.verifier = nopTOCEntryVerifier{}
	return vr.r
}

func (vr *VerifiableReader) VerifyTOC(tocDigest digest.Digest) (Reader, error) {
	v, err := vr.r.r.VerifyTOC(tocDigest)
	if err != nil {
		return nil, err
	}
	vr.r.verifier = v
	return vr.r, nil
}

func (vr *VerifiableReader) Close() error {
	return vr.r.Close()
}

type nopTOCEntryVerifier struct{}

func (nev nopTOCEntryVerifier) Verifier(ce *estargz.TOCEntry) (digest.Verifier, error) {
	return nopVerifier{}, nil
}

type nopVerifier struct{}

func (nv nopVerifier) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (nv nopVerifier) Verified() bool {
	return true
}

// NewReader creates a Reader based on the given stargz blob and cache implementation.
// It returns VerifiableReader so the caller must provide a estargz.TOCEntryVerifier
// to use for verifying file or chunk contained in this stargz blob.
func NewReader(sr *io.SectionReader, cache cache.BlobCache) (*VerifiableReader, error) {
	r, err := estargz.Open(sr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse stargz")
	}

	vr := &reader{
		r:     r,
		sr:    sr,
		cache: cache,
		bufPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}

	return &VerifiableReader{vr}, nil
}

type reader struct {
	r        *estargz.Reader
	sr       *io.SectionReader
	cache    cache.BlobCache
	bufPool  sync.Pool
	verifier estargz.TOCEntryVerifier

	closed   bool
	closedMu sync.Mutex
}

func (gr *reader) OpenFile(name string) (io.ReaderAt, error) {
	if gr.isClosed() {
		return nil, fmt.Errorf("reader is already closed")
	}

	sr, err := gr.r.OpenFile(name)
	if err != nil {
		return nil, err
	}
	e, ok := gr.r.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("failed to get TOCEntry %q", name)
	}
	return &file{
		name:   name,
		digest: e.Digest,
		r:      gr.r,
		cache:  gr.cache,
		ra:     sr,
		gr:     gr,
	}, nil
}

func (gr *reader) Lookup(name string) (*estargz.TOCEntry, bool) {
	return gr.r.Lookup(name)
}

func (gr *reader) Cache(opts ...CacheOption) (err error) {
	if gr.isClosed() {
		return fmt.Errorf("reader is already closed")
	}

	var cacheOpts cacheOptions
	for _, o := range opts {
		o(&cacheOpts)
	}

	r := gr.r
	if cacheOpts.reader != nil {
		if r, err = estargz.Open(cacheOpts.reader); err != nil {
			return errors.Wrap(err, "failed to parse stargz")
		}
	}
	root, ok := r.Lookup("")
	if !ok {
		return fmt.Errorf("failed to get a TOCEntry of the root")
	}

	filter := func(*estargz.TOCEntry) bool {
		return true
	}
	if cacheOpts.filter != nil {
		filter = cacheOpts.filter
	}

	eg, egCtx := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		return gr.cacheWithReader(egCtx,
			0, eg, semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0))),
			root, r, filter, cacheOpts.cacheOpts...)
	})
	return eg.Wait()
}

func (gr *reader) Close() error {
	gr.closedMu.Lock()
	defer gr.closedMu.Unlock()
	if gr.closed {
		return nil
	}
	gr.closed = true
	return gr.cache.Close()
}

func (gr *reader) isClosed() bool {
	gr.closedMu.Lock()
	closed := gr.closed
	gr.closedMu.Unlock()
	return closed
}

func (gr *reader) cacheWithReader(ctx context.Context, currentDepth int, eg *errgroup.Group, sem *semaphore.Weighted, dir *estargz.TOCEntry, r *estargz.Reader, filter func(*estargz.TOCEntry) bool, opts ...cache.Option) (rErr error) {
	if currentDepth > maxWalkDepth {
		return fmt.Errorf("TOCEntry tree is too deep (depth:%d)", currentDepth)
	}
	dir.ForeachChild(func(_ string, e *estargz.TOCEntry) bool {
		if e.Type == "dir" {
			// Walk through all files on this stargz file.

			// Ignore a TOCEntry of "./" (formated as "" by stargz lib) on root directory
			// because this points to the root directory itself.
			if e.Name == "" && dir.Name == "" {
				return true
			}

			// Make sure the entry is the immediate child for avoiding loop.
			if filepath.Dir(filepath.Clean(e.Name)) != filepath.Clean(dir.Name) {
				rErr = fmt.Errorf("invalid child path %q; must be child of %q",
					e.Name, dir.Name)
				return false
			}
			if err := gr.cacheWithReader(ctx, currentDepth+1, eg, sem, e, r, filter, opts...); err != nil {
				rErr = err
				return false
			}
			return true
		} else if e.Type != "reg" {
			// Only cache regular files
			return true
		} else if !filter(e) {
			// This entry need to be filtered out
			return true
		} else if e.Name == estargz.TOCTarName {
			// We don't need to cache TOC json file
			return true
		}

		sr, err := r.OpenFile(e.Name)
		if err != nil {
			rErr = err
			return false
		}

		var nr int64
		for nr < e.Size {
			ce, ok := r.ChunkEntryForOffset(e.Name, nr)
			if !ok {
				break
			}
			nr += ce.ChunkSize

			if err := sem.Acquire(ctx, 1); err != nil {
				rErr = err
				return false
			}

			eg.Go(func() (retErr error) {
				defer sem.Release(1)

				// Check if the target chunks exists in the cache
				id := genID(e.Digest, ce.ChunkOffset, ce.ChunkSize)
				if r, err := gr.cache.Get(id, opts...); err == nil {
					return r.Close()
				}

				// missed cache, needs to fetch and add it to the cache
				cr := io.NewSectionReader(sr, ce.ChunkOffset, ce.ChunkSize)
				v, err := gr.verifier.Verifier(ce)
				if err != nil {
					return errors.Wrapf(err, "verifier not found %q(off:%d,size:%d)",
						e.Name, ce.ChunkOffset, ce.ChunkSize)
				}
				br := bufio.NewReaderSize(io.TeeReader(cr, v), int(ce.ChunkSize))
				if _, err := br.Peek(int(ce.ChunkSize)); err != nil {
					return fmt.Errorf("cacheWithReader.peek: %v", err)
				}
				w, err := gr.cache.Add(id, opts...)
				if err != nil {
					return err
				}
				defer w.Close()
				if _, err := io.CopyN(w, br, ce.ChunkSize); err != nil {
					w.Abort()
					return errors.Wrapf(err,
						"failed to cache file payload of %q (offset:%d,size:%d)",
						e.Name, ce.ChunkOffset, ce.ChunkSize)
				}
				if !v.Verified() {
					w.Abort()
					return fmt.Errorf("invalid chunk %q (offset:%d,size:%d)",
						e.Name, ce.ChunkOffset, ce.ChunkSize)
				}

				return w.Commit()
			})
		}

		return true
	})

	return
}

type file struct {
	name   string
	digest string
	ra     io.ReaderAt
	r      *estargz.Reader
	cache  cache.BlobCache
	gr     *reader
}

// ReadAt reads chunks from the stargz file with trying to fetch as many chunks
// as possible from the cache.
func (sf *file) ReadAt(p []byte, offset int64) (int, error) {
	nr := 0
	for nr < len(p) {
		ce, ok := sf.r.ChunkEntryForOffset(sf.name, offset+int64(nr))
		if !ok {
			break
		}
		var (
			id           = genID(sf.digest, ce.ChunkOffset, ce.ChunkSize)
			lowerDiscard = positive(offset - ce.ChunkOffset)
			upperDiscard = positive(ce.ChunkOffset + ce.ChunkSize - (offset + int64(len(p))))
			expectedSize = ce.ChunkSize - upperDiscard - lowerDiscard
		)

		// Check if the content exists in the cache
		if r, err := sf.cache.Get(id); err == nil {
			n, err := r.ReadAt(p[nr:int64(nr)+expectedSize], lowerDiscard)
			if (err == nil || err == io.EOF) && int64(n) == expectedSize {
				nr += n
				r.Close()
				continue
			}
			r.Close()
		}

		// We missed cache. Take it from underlying reader.
		// We read the whole chunk here and add it to the cache so that following
		// reads against neighboring chunks can take the data without decmpression.
		if lowerDiscard == 0 && upperDiscard == 0 {
			// We can directly store the result to the given buffer
			ip := p[nr : int64(nr)+ce.ChunkSize]
			n, err := sf.ra.ReadAt(ip, ce.ChunkOffset)
			if err != nil && err != io.EOF {
				return 0, errors.Wrap(err, "failed to read data")
			}

			// Verify this chunk
			if err := sf.verify(ip, ce); err != nil {
				return 0, errors.Wrap(err, "invalid chunk")
			}

			// Cache this chunk
			if w, err := sf.cache.Add(id); err == nil {
				if cn, err := w.Write(ip); err != nil || cn != len(ip) {
					w.Abort()
				} else {
					w.Commit()
				}
				w.Close()
			}
			nr += n
			continue
		}

		// Use temporally buffer for aligning this chunk
		b := sf.gr.bufPool.Get().(*bytes.Buffer)
		b.Reset()
		b.Grow(int(ce.ChunkSize))
		ip := b.Bytes()[:ce.ChunkSize]
		if _, err := sf.ra.ReadAt(ip, ce.ChunkOffset); err != nil && err != io.EOF {
			sf.gr.bufPool.Put(b)
			return 0, errors.Wrap(err, "failed to read data")
		}

		// Verify this chunk
		if err := sf.verify(ip, ce); err != nil {
			sf.gr.bufPool.Put(b)
			return 0, errors.Wrap(err, "invalid chunk")
		}

		// Cache this chunk
		if w, err := sf.cache.Add(id); err == nil {
			if cn, err := w.Write(ip); err != nil || cn != len(ip) {
				w.Abort()
			} else {
				w.Commit()
			}
			w.Close()
		}
		n := copy(p[nr:], ip[lowerDiscard:ce.ChunkSize-upperDiscard])
		sf.gr.bufPool.Put(b)
		if int64(n) != expectedSize {
			return 0, fmt.Errorf("unexpected final data size %d; want %d", n, expectedSize)
		}
		nr += n
	}

	return nr, nil
}

func (sf *file) verify(p []byte, ce *estargz.TOCEntry) error {
	v, err := sf.gr.verifier.Verifier(ce)
	if err != nil {
		return errors.Wrapf(err, "verifier not found %q (offset:%d,size:%d)",
			ce.Name, ce.ChunkOffset, ce.ChunkSize)
	}
	if _, err := v.Write(p); err != nil {
		return errors.Wrapf(err, "failed to verify %q (offset:%d,size:%d)",
			ce.Name, ce.ChunkOffset, ce.ChunkSize)
	}
	if !v.Verified() {
		return fmt.Errorf("invalid chunk %q (offset:%d,size:%d)",
			ce.Name, ce.ChunkOffset, ce.ChunkSize)
	}

	return nil
}

func genID(digest string, offset, size int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", digest, offset, size)))
	return fmt.Sprintf("%x", sum)
}

func positive(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

type CacheOption func(*cacheOptions)

type cacheOptions struct {
	cacheOpts []cache.Option
	filter    func(*estargz.TOCEntry) bool
	reader    *io.SectionReader
}

func WithCacheOpts(cacheOpts ...cache.Option) CacheOption {
	return func(opts *cacheOptions) {
		opts.cacheOpts = cacheOpts
	}
}

func WithFilter(filter func(*estargz.TOCEntry) bool) CacheOption {
	return func(opts *cacheOptions) {
		opts.filter = filter
	}
}

func WithReader(sr *io.SectionReader) CacheOption {
	return func(opts *cacheOptions) {
		opts.reader = sr
	}
}
