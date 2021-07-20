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
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/stargz-snapshotter/cache"
	"github.com/containerd/stargz-snapshotter/fs/config"
	"github.com/containerd/stargz-snapshotter/fs/source"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

const (
	defaultChunkSize        = 50000
	defaultValidIntervalSec = 60
	defaultFetchTimeoutSec  = 300
)

func NewResolver(cfg config.BlobConfig) *Resolver {
	if cfg.ChunkSize == 0 { // zero means "use default chunk size"
		cfg.ChunkSize = defaultChunkSize
	}
	if cfg.ValidInterval == 0 { // zero means "use default interval"
		cfg.ValidInterval = defaultValidIntervalSec
	}
	if cfg.CheckAlways {
		cfg.ValidInterval = 0
	}
	if cfg.FetchTimeoutSec == 0 {
		cfg.FetchTimeoutSec = defaultFetchTimeoutSec
	}

	return &Resolver{
		blobConfig: cfg,
	}
}

type Resolver struct {
	blobConfig config.BlobConfig
}

func (r *Resolver) Resolve(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor, blobCache cache.BlobCache) (Blob, error) {
	fetcher, size, err := newFetcher(ctx, hosts, refspec, desc)
	if err != nil {
		return nil, err
	}
	return &blob{
		fetcher:       fetcher,
		size:          size,
		chunkSize:     r.blobConfig.ChunkSize,
		cache:         blobCache,
		lastCheck:     time.Now(),
		checkInterval: time.Duration(r.blobConfig.ValidInterval) * time.Second,
		resolver:      r,
		fetchTimeout:  time.Duration(r.blobConfig.FetchTimeoutSec) * time.Second,
	}, nil
}

func newFetcher(ctx context.Context, hosts source.RegistryHosts, refspec reference.Spec, desc ocispec.Descriptor) (*fetcher, int64, error) {
	reghosts, err := hosts(refspec)
	if err != nil {
		return nil, 0, err
	}
	if desc.Digest.String() == "" {
		return nil, 0, fmt.Errorf("Digest is mandatory in layer descriptor")
	}
	digest := desc.Digest
	pullScope, err := repositoryScope(refspec, false)
	if err != nil {
		return nil, 0, err
	}

	// Try to create fetcher until succeeded
	rErr := fmt.Errorf("failed to resolve")
	for _, host := range reghosts {
		if host.Host == "" || strings.Contains(host.Host, "/") {
			rErr = errors.Wrapf(rErr, "invalid destination (host %q, ref:%q, digest:%q)",
				host.Host, refspec, digest)
			continue // Try another

		}

		// Prepare transport with authorization functionality
		tr := host.Client.Transport
		timeout := host.Client.Timeout
		if host.Authorizer != nil {
			tr = &transport{
				inner: tr,
				auth:  host.Authorizer,
				scope: pullScope,
			}
		}

		// Resolve redirection and get blob URL
		blobURL := fmt.Sprintf("%s://%s/%s/blobs/%s",
			host.Scheme,
			path.Join(host.Host, host.Path),
			strings.TrimPrefix(refspec.Locator, refspec.Hostname()+"/"),
			digest)
		url, err := redirect(ctx, blobURL, tr, timeout)
		if err != nil {
			rErr = errors.Wrapf(rErr, "failed to redirect (host %q, ref:%q, digest:%q): %v",
				host.Host, refspec, digest, err)
			continue // Try another
		}

		// Get size information
		// TODO: we should try to use the Size field in the descriptor here.
		size, err := getSize(ctx, url, tr, timeout)
		if err != nil {
			rErr = errors.Wrapf(rErr, "failed to get size (host %q, ref:%q, digest:%q): %v",
				host.Host, refspec, digest, err)
			continue // Try another
		}

		// Hit one destination
		return &fetcher{
			url:     url,
			tr:      tr,
			blobURL: blobURL,
			timeout: timeout,
		}, size, nil
	}

	return nil, 0, errors.Wrapf(rErr, "cannot resolve layer")
}

type transport struct {
	inner http.RoundTripper
	auth  docker.Authorizer
	scope string
}

func (tr *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := docker.WithScope(req.Context(), tr.scope)
	roundTrip := func(req *http.Request) (*http.Response, error) {
		// authorize the request using docker.Authorizer
		if err := tr.auth.Authorize(ctx, req); err != nil {
			return nil, err
		}

		// send the request
		return tr.inner.RoundTrip(req)
	}

	resp, err := roundTrip(req)
	if err != nil {
		return nil, err
	}

	// TODO: support more status codes and retries
	if resp.StatusCode == http.StatusUnauthorized {

		// prepare authorization for the target host using docker.Authorizer
		if err := tr.auth.AddResponses(ctx, []*http.Response{resp}); err != nil {
			if errdefs.IsNotImplemented(err) {
				return resp, nil
			}
			return nil, err
		}

		// re-authorize and send the request
		return roundTrip(req.Clone(ctx))
	}

	return resp, nil
}

func redirect(ctx context.Context, blobURL string, tr http.RoundTripper, timeout time.Duration) (url string, err error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// We use GET request for redirect.
	// gcr.io returns 200 on HEAD without Location header (2020).
	// ghcr.io returns 200 on HEAD without Location header (2020).
	req, err := http.NewRequestWithContext(ctx, "GET", blobURL, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to make request to the registry")
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err := tr.RoundTrip(req)
	if err != nil {
		return "", errors.Wrapf(err, "failed to request")
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()

	if res.StatusCode/100 == 2 {
		url = blobURL
	} else if redir := res.Header.Get("Location"); redir != "" && res.StatusCode/100 == 3 {
		// TODO: Support nested redirection
		url = redir
	} else {
		return "", fmt.Errorf("failed to access to the registry with code %v", res.StatusCode)
	}

	return
}

func getSize(ctx context.Context, url string, tr http.RoundTripper, timeout time.Duration) (int64, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return 0, err
	}
	req.Close = false
	res, err := tr.RoundTrip(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	}
	headStatusCode := res.StatusCode

	// Failed to do HEAD request. Fall back to GET.
	// ghcr.io (https://github-production-container-registry.s3.amazonaws.com) doesn't allow
	// HEAD request (2020).
	req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to make request to the registry")
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err = tr.RoundTrip(req)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to request")
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()

	if res.StatusCode == http.StatusOK {
		return strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	} else if res.StatusCode == http.StatusPartialContent {
		_, size, err := parseRange(res.Header.Get("Content-Range"))
		return size, err
	}

	return 0, fmt.Errorf("failed to get size with code (HEAD=%v, GET=%v)",
		headStatusCode, res.StatusCode)
}

type fetcher struct {
	url           string
	urlMu         sync.Mutex
	tr            http.RoundTripper
	blobURL       string
	singleRange   bool
	singleRangeMu sync.Mutex
	timeout       time.Duration
}

type multipartReadCloser interface {
	Next() (region, io.Reader, error)
	Close() error
}

func (f *fetcher) fetch(ctx context.Context, rs []region, retry bool, opts *options) (multipartReadCloser, error) {
	if len(rs) == 0 {
		return nil, fmt.Errorf("no request queried")
	}

	var (
		tr              = f.tr
		singleRangeMode = f.isSingleRangeMode()
	)

	if opts.ctx != nil {
		ctx = opts.ctx
	}
	if opts.tr != nil {
		tr = opts.tr
	}

	// squash requesting chunks for reducing the total size of request header
	// (servers generally have limits for the size of headers)
	// TODO: when our request has too many ranges, we need to divide it into
	//       multiple requests to avoid huge header.
	var s regionSet
	for _, reg := range rs {
		s.add(reg)
	}
	requests := s.rs
	if singleRangeMode {
		// Squash requests if the layer doesn't support multi range.
		requests = []region{superRegion(requests)}
	}

	// Request to the registry
	f.urlMu.Lock()
	url := f.url
	f.urlMu.Unlock()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	var ranges string
	for _, reg := range requests {
		ranges += fmt.Sprintf("%d-%d,", reg.b, reg.e)
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%s", ranges[:len(ranges)-1]))
	req.Header.Add("Accept-Encoding", "identity")
	req.Close = false
	res, err := tr.RoundTrip(req) // NOT DefaultClient; don't want redirects
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusOK {
		// We are getting the whole blob in one part (= status 200)
		size, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse Content-Length")
		}
		return singlePartReader(region{0, size - 1}, res.Body), nil
	} else if res.StatusCode == http.StatusPartialContent {
		mediaType, params, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
		if err != nil {
			return nil, errors.Wrapf(err, "invalid media type %q", mediaType)
		}
		if strings.HasPrefix(mediaType, "multipart/") {
			// We are getting a set of chunks as a multipart body.
			return multiPartReader(res.Body, params["boundary"]), nil
		}

		// We are getting single range
		reg, _, err := parseRange(res.Header.Get("Content-Range"))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse Content-Range")
		}
		return singlePartReader(reg, res.Body), nil
	} else if retry && res.StatusCode == http.StatusForbidden {
		// re-redirect and retry this once.
		if err := f.refreshURL(ctx); err != nil {
			return nil, errors.Wrapf(err, "failed to refresh URL on %v", res.Status)
		}
		return f.fetch(ctx, rs, false, opts)
	} else if retry && res.StatusCode == http.StatusBadRequest && !singleRangeMode {
		// gcr.io (https://storage.googleapis.com) returns 400 on multi-range request (2020 #81)
		f.singleRangeMode()                  // fallbacks to singe range request mode
		return f.fetch(ctx, rs, false, opts) // retries with the single range mode
	}

	return nil, fmt.Errorf("unexpected status code: %v", res.Status)
}

func (f *fetcher) check() error {
	ctx := context.Background()
	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}
	f.urlMu.Lock()
	url := f.url
	f.urlMu.Unlock()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.Wrapf(err, "check failed: failed to make request")
	}
	req.Close = false
	req.Header.Set("Range", "bytes=0-1")
	res, err := f.tr.RoundTrip(req)
	if err != nil {
		return errors.Wrapf(err, "check failed: failed to request to registry")
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()
	if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusPartialContent {
		return nil
	} else if res.StatusCode == http.StatusForbidden {
		// Try to re-redirect this blob
		rCtx := context.Background()
		if f.timeout > 0 {
			var rCancel context.CancelFunc
			rCtx, rCancel = context.WithTimeout(rCtx, f.timeout)
			defer rCancel()
		}
		if err := f.refreshURL(rCtx); err == nil {
			return nil
		}
		return fmt.Errorf("failed to refresh URL on status %v", res.Status)
	}

	return fmt.Errorf("unexpected status code %v", res.StatusCode)
}

func (f *fetcher) refreshURL(ctx context.Context) error {
	newURL, err := redirect(ctx, f.blobURL, f.tr, f.timeout)
	if err != nil {
		return err
	}
	f.urlMu.Lock()
	f.url = newURL
	f.urlMu.Unlock()
	return nil
}

func (f *fetcher) genID(reg region) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", f.blobURL, reg.b, reg.e)))
	return fmt.Sprintf("%x", sum)
}

func (f *fetcher) singleRangeMode() {
	f.singleRangeMu.Lock()
	f.singleRange = true
	f.singleRangeMu.Unlock()
}

func (f *fetcher) isSingleRangeMode() bool {
	f.singleRangeMu.Lock()
	r := f.singleRange
	f.singleRangeMu.Unlock()
	return r
}

func singlePartReader(reg region, rc io.ReadCloser) multipartReadCloser {
	return &singlepartReader{
		r:      rc,
		Closer: rc,
		reg:    reg,
	}
}

type singlepartReader struct {
	io.Closer
	r      io.Reader
	reg    region
	called bool
}

func (sr *singlepartReader) Next() (region, io.Reader, error) {
	if !sr.called {
		sr.called = true
		return sr.reg, sr.r, nil
	}
	return region{}, nil, io.EOF
}

func multiPartReader(rc io.ReadCloser, boundary string) multipartReadCloser {
	return &multipartReader{
		m:      multipart.NewReader(rc, boundary),
		Closer: rc,
	}
}

type multipartReader struct {
	io.Closer
	m *multipart.Reader
}

func (sr *multipartReader) Next() (region, io.Reader, error) {
	p, err := sr.m.NextPart()
	if err != nil {
		return region{}, nil, err
	}
	reg, _, err := parseRange(p.Header.Get("Content-Range"))
	if err != nil {
		return region{}, nil, errors.Wrapf(err, "failed to parse Content-Range")
	}
	return reg, p, nil
}

func parseRange(header string) (region, int64, error) {
	submatches := contentRangeRegexp.FindStringSubmatch(header)
	if len(submatches) < 4 {
		return region{}, 0, fmt.Errorf("Content-Range %q doesn't have enough information", header)
	}
	begin, err := strconv.ParseInt(submatches[1], 10, 64)
	if err != nil {
		return region{}, 0, errors.Wrapf(err, "failed to parse beginning offset %q", submatches[1])
	}
	end, err := strconv.ParseInt(submatches[2], 10, 64)
	if err != nil {
		return region{}, 0, errors.Wrapf(err, "failed to parse end offset %q", submatches[2])
	}
	blobSize, err := strconv.ParseInt(submatches[3], 10, 64)
	if err != nil {
		return region{}, 0, errors.Wrapf(err, "failed to parse blob size %q", submatches[3])
	}

	return region{begin, end}, blobSize, nil
}

type Option func(*options)

type options struct {
	ctx       context.Context
	tr        http.RoundTripper
	cacheOpts []cache.Option
}

func WithContext(ctx context.Context) Option {
	return func(opts *options) {
		opts.ctx = ctx
	}
}

func WithRoundTripper(tr http.RoundTripper) Option {
	return func(opts *options) {
		opts.tr = tr
	}
}

func WithCacheOpts(cacheOpts ...cache.Option) Option {
	return func(opts *options) {
		opts.cacheOpts = cacheOpts
	}
}

// NOTE: ported from https://github.com/containerd/containerd/blob/v1.5.2/remotes/docker/scope.go#L29-L42
// TODO: import this from containerd package once we drop support to continerd v1.4.x
//
// repositoryScope returns a repository scope string such as "repository:foo/bar:pull"
// for "host/foo/bar:baz".
// When push is true, both pull and push are added to the scope.
func repositoryScope(refspec reference.Spec, push bool) (string, error) {
	u, err := url.Parse("dummy://" + refspec.Locator)
	if err != nil {
		return "", err
	}
	s := "repository:" + strings.TrimPrefix(u.Path, "/") + ":pull"
	if push {
		s += ",push"
	}
	return s, nil
}
