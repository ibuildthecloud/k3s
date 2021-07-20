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

package remote

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"sync"
	"time"

	"github.com/containerd/containerd/reference"
	"github.com/containerd/stargz-snapshotter/cache"
	"github.com/containerd/stargz-snapshotter/fs/source"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

var contentRangeRegexp = regexp.MustCompile(`bytes ([0-9]+)-([0-9]+)/([0-9]+|\\*)`)

type Blob interface {
	Check() error
	Size() int64
	FetchedSize() int64
	ReadAt(p []byte, offset int64, opts ...Option) (int, error)
	Cache(offset int64, size int64, opts ...Option) error
	Refresh(ctx context.Context, host source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error
	Close() error
}

type blob struct {
	fetcher   *fetcher
	fetcherMu sync.Mutex

	size          int64
	chunkSize     int64
	cache         cache.BlobCache
	lastCheck     time.Time
	lastCheckMu   sync.Mutex
	checkInterval time.Duration
	fetchTimeout  time.Duration

	fetchedRegionSet   regionSet
	fetchedRegionSetMu sync.Mutex

	resolver *Resolver

	closed   bool
	closedMu sync.Mutex
}

func (b *blob) Close() error {
	b.closedMu.Lock()
	defer b.closedMu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	return b.cache.Close()
}

func (b *blob) isClosed() bool {
	b.closedMu.Lock()
	closed := b.closed
	b.closedMu.Unlock()
	return closed
}

func (b *blob) Refresh(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) error {
	if b.isClosed() {
		return fmt.Errorf("blob is already closed")
	}

	// refresh the fetcher
	new, newSize, err := newFetcher(ctx, hosts, refspec, desc)
	if err != nil {
		return err
	} else if newSize != b.size {
		return fmt.Errorf("Invalid size of new blob %d; want %d", newSize, b.size)
	}

	// update the blob's fetcher with new one
	b.fetcherMu.Lock()
	b.fetcher = new
	b.fetcherMu.Unlock()
	b.lastCheckMu.Lock()
	b.lastCheck = time.Now()
	b.lastCheckMu.Unlock()

	return nil
}

func (b *blob) Check() error {
	if b.isClosed() {
		return fmt.Errorf("blob is already closed")
	}

	now := time.Now()
	b.lastCheckMu.Lock()
	lastCheck := b.lastCheck
	b.lastCheckMu.Unlock()
	if now.Sub(lastCheck) < b.checkInterval {
		// do nothing if not expired
		return nil
	}
	b.fetcherMu.Lock()
	fr := b.fetcher
	b.fetcherMu.Unlock()
	err := fr.check()
	if err == nil {
		// update lastCheck only if check succeeded.
		// on failure, we should check this layer next time again.
		b.lastCheckMu.Lock()
		b.lastCheck = now
		b.lastCheckMu.Unlock()
	}

	return err
}

func (b *blob) Size() int64 {
	return b.size
}

func (b *blob) FetchedSize() int64 {
	b.fetchedRegionSetMu.Lock()
	sz := b.fetchedRegionSet.totalSize()
	b.fetchedRegionSetMu.Unlock()
	return sz
}

func (b *blob) Cache(offset int64, size int64, opts ...Option) error {
	if b.isClosed() {
		return fmt.Errorf("blob is already closed")
	}

	var cacheOpts options
	for _, o := range opts {
		o(&cacheOpts)
	}

	b.fetcherMu.Lock()
	fr := b.fetcher
	b.fetcherMu.Unlock()

	fetchReg := region{floor(offset, b.chunkSize), ceil(offset+size-1, b.chunkSize) - 1}
	discard := make(map[region]io.Writer)
	b.walkChunks(fetchReg, func(reg region) error {
		if r, err := b.cache.Get(fr.genID(reg), cacheOpts.cacheOpts...); err == nil {
			return r.Close() // nop if the cache hits
		}
		discard[reg] = ioutil.Discard
		return nil
	})
	if err := b.fetchRange(discard, &cacheOpts); err != nil {
		return err
	}

	return nil
}

// ReadAt reads remote chunks from specified offset for the buffer size.
// It tries to fetch as many chunks as possible from local cache.
// We can configure this function with options.
func (b *blob) ReadAt(p []byte, offset int64, opts ...Option) (int, error) {
	if b.isClosed() {
		return 0, fmt.Errorf("blob is already closed")
	}

	if len(p) == 0 || offset > b.size {
		return 0, nil
	}

	// Make the buffer chunk aligned
	allRegion := region{floor(offset, b.chunkSize), ceil(offset+int64(len(p))-1, b.chunkSize) - 1}
	allData := make(map[region]io.Writer)

	var readAtOpts options
	for _, o := range opts {
		o(&readAtOpts)
	}

	// Fetcher can be suddenly updated so we take and use the snapshot of it for
	// consistency.
	b.fetcherMu.Lock()
	fr := b.fetcher
	b.fetcherMu.Unlock()

	b.walkChunks(allRegion, func(chunk region) error {
		var (
			base         = positive(chunk.b - offset)
			lowerUnread  = positive(offset - chunk.b)
			upperUnread  = positive(chunk.e + 1 - (offset + int64(len(p))))
			expectedSize = chunk.size() - upperUnread - lowerUnread
		)

		// Check if the content exists in the cache
		r, err := b.cache.Get(fr.genID(chunk), readAtOpts.cacheOpts...)
		if err == nil {
			defer r.Close()
			n, err := r.ReadAt(p[base:base+expectedSize], lowerUnread)
			if (err == nil || err == io.EOF) && int64(n) == expectedSize {
				return nil
			}
		}

		// We missed cache. Take it from remote registry.
		// We get the whole chunk here and add it to the cache so that following
		// reads against neighboring chunks can take the data without making HTTP requests.
		allData[chunk] = newBytesWriter(p[base:base+expectedSize], lowerUnread)
		return nil
	})

	// Read required data
	if err := b.fetchRange(allData, &readAtOpts); err != nil {
		return 0, err
	}

	// Adjust the buffer size according to the blob size
	if remain := b.size - offset; int64(len(p)) >= remain {
		if remain < 0 {
			remain = 0
		}
		p = p[:remain]
	}

	return len(p), nil
}

// fetchRange fetches all specified chunks from local cache and remote blob.
func (b *blob) fetchRange(allData map[region]io.Writer, opts *options) error {
	if len(allData) == 0 {
		return nil
	}

	// Fetcher can be suddenly updated so we take and use the snapshot of it for
	// consistency.
	b.fetcherMu.Lock()
	fr := b.fetcher
	b.fetcherMu.Unlock()

	// request missed regions
	var req []region
	fetched := make(map[region]bool)
	for reg := range allData {
		req = append(req, reg)
		fetched[reg] = false
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.fetchTimeout)
	defer cancel()
	mr, err := fr.fetch(ctx, req, true, opts)
	if err != nil {
		return err
	}
	defer mr.Close()

	// Update the check timer because we succeeded to access the blob
	b.lastCheckMu.Lock()
	b.lastCheck = time.Now()
	b.lastCheckMu.Unlock()

	// chunk and cache responsed data. Regions must be aligned by chunk size.
	// TODO: Reorganize remoteData to make it be aligned by chunk size
	for {
		reg, p, err := mr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrapf(err, "failed to read multipart resp")
		}
		if err := b.walkChunks(reg, func(chunk region) (retErr error) {
			id := fr.genID(chunk)
			cw, err := b.cache.Add(id, opts.cacheOpts...)
			if err != nil {
				return err
			}
			defer cw.Close()
			w := io.Writer(cw)

			// If this chunk is one of the targets, write the content to the
			// passed reader too.
			if _, ok := fetched[chunk]; ok {
				w = io.MultiWriter(w, allData[chunk])
			}

			// Copy the target chunk
			if _, err := io.CopyN(w, p, chunk.size()); err != nil {
				cw.Abort()
				return err
			}

			// Add the target chunk to the cache
			if err := cw.Commit(); err != nil {
				return err
			}

			b.fetchedRegionSetMu.Lock()
			b.fetchedRegionSet.add(chunk)
			b.fetchedRegionSetMu.Unlock()
			fetched[chunk] = true
			return nil
		}); err != nil {
			return errors.Wrapf(err, "failed to get chunks")
		}
	}

	// Check all chunks are fetched
	var unfetched []region
	for c, b := range fetched {
		if !b {
			unfetched = append(unfetched, c)
		}
	}
	if unfetched != nil {
		return fmt.Errorf("failed to fetch region %v", unfetched)
	}

	return nil
}

type walkFunc func(reg region) error

// walkChunks walks chunks from begin to end in order in the specified region.
// specified region must be aligned by chunk size.
func (b *blob) walkChunks(allRegion region, walkFn walkFunc) error {
	if allRegion.b%b.chunkSize != 0 {
		return fmt.Errorf("region (%d, %d) must be aligned by chunk size",
			allRegion.b, allRegion.e)
	}
	for i := allRegion.b; i <= allRegion.e && i < b.size; i += b.chunkSize {
		reg := region{i, i + b.chunkSize - 1}
		if reg.e >= b.size {
			reg.e = b.size - 1
		}
		if err := walkFn(reg); err != nil {
			return err
		}
	}
	return nil
}

func newBytesWriter(dest []byte, destOff int64) io.Writer {
	return &bytesWriter{
		dest:    dest,
		destOff: destOff,
		current: 0,
	}
}

type bytesWriter struct {
	dest    []byte
	destOff int64
	current int64
}

func (bw *bytesWriter) Write(p []byte) (int, error) {
	defer func() { bw.current = bw.current + int64(len(p)) }()

	var (
		destBase = positive(bw.current - bw.destOff)
		pBegin   = positive(bw.destOff - bw.current)
		pEnd     = positive(bw.destOff + int64(len(bw.dest)) - bw.current)
	)

	if destBase > int64(len(bw.dest)) {
		return len(p), nil
	}
	if pBegin >= int64(len(p)) {
		return len(p), nil
	}
	if pEnd > int64(len(p)) {
		pEnd = int64(len(p))
	}

	copy(bw.dest[destBase:], p[pBegin:pEnd])

	return len(p), nil
}

func floor(n int64, unit int64) int64 {
	return (n / unit) * unit
}

func ceil(n int64, unit int64) int64 {
	return (n/unit + 1) * unit
}

func positive(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
