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

//
// Example implementation of FileSystem.
//
// This implementation uses stargz by CRFS(https://github.com/google/crfs) as
// image format, which has following feature:
// - We can use docker registry as a backend store (means w/o additional layer
//   stores).
// - The stargz-formatted image is still docker-compatible (means normal
//   runtimes can still use the formatted image).
//
// Currently, we reimplemented CRFS-like filesystem for ease of integration.
// But in the near future, we intend to integrate it with CRFS.
//

package fs

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/fs/config"
	"github.com/containerd/stargz-snapshotter/fs/layer"
	fsmetrics "github.com/containerd/stargz-snapshotter/fs/metrics"
	"github.com/containerd/stargz-snapshotter/fs/source"
	"github.com/containerd/stargz-snapshotter/snapshot"
	"github.com/containerd/stargz-snapshotter/task"
	metrics "github.com/docker/go-metrics"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

const (
	defaultMaxConcurrency = 2
	fusermountBin         = "fusermount"
)

type Option func(*options)

type options struct {
	getSources source.GetSources
}

func WithGetSources(s source.GetSources) Option {
	return func(opts *options) {
		opts.getSources = s
	}
}

func NewFilesystem(root string, cfg config.Config, opts ...Option) (_ snapshot.FileSystem, err error) {
	var fsOpts options
	for _, o := range opts {
		o(&fsOpts)
	}
	maxConcurrency := cfg.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = defaultMaxConcurrency
	}
	getSources := fsOpts.getSources
	if getSources == nil {
		getSources = source.FromDefaultLabels(func(refspec reference.Spec) (hosts []docker.RegistryHost, _ error) {
			return docker.ConfigureDefaultRegistries(docker.WithPlainHTTP(docker.MatchLocalhost))(refspec.Hostname())
		})
	}
	tm := task.NewBackgroundTaskManager(maxConcurrency, 5*time.Second)
	r, err := layer.NewResolver(root, tm, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to setup resolver")
	}

	var ns *metrics.Namespace
	if !cfg.NoPrometheus {
		ns = metrics.NewNamespace("stargz", "fs", nil)
	}
	c := fsmetrics.NewLayerMetrics(ns)
	if ns != nil {
		metrics.Register(ns)
	}

	return &filesystem{
		resolver:              r,
		getSources:            getSources,
		prefetchSize:          cfg.PrefetchSize,
		noprefetch:            cfg.NoPrefetch,
		noBackgroundFetch:     cfg.NoBackgroundFetch,
		debug:                 cfg.Debug,
		layer:                 make(map[string]layer.Layer),
		backgroundTaskManager: tm,
		allowNoVerification:   cfg.AllowNoVerification,
		disableVerification:   cfg.DisableVerification,
		metricsController:     c,
	}, nil
}

type filesystem struct {
	resolver              *layer.Resolver
	prefetchSize          int64
	noprefetch            bool
	noBackgroundFetch     bool
	debug                 bool
	layer                 map[string]layer.Layer
	layerMu               sync.Mutex
	backgroundTaskManager *task.BackgroundTaskManager
	allowNoVerification   bool
	disableVerification   bool
	getSources            source.GetSources
	metricsController     *fsmetrics.Controller
}

func (fs *filesystem) Mount(ctx context.Context, mountpoint string, labels map[string]string) (retErr error) {
	// This is a prioritized task and all background tasks will be stopped
	// execution so this can avoid being disturbed for NW traffic by background
	// tasks.
	fs.backgroundTaskManager.DoPrioritizedTask()
	defer fs.backgroundTaskManager.DonePrioritizedTask()
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", mountpoint))

	// Get source information of this layer.
	src, err := fs.getSources(labels)
	if err != nil {
		return err
	} else if len(src) == 0 {
		return fmt.Errorf("source must be passed")
	}

	// Resolve the target layer
	var (
		resultChan = make(chan layer.Layer)
		errChan    = make(chan error)
	)
	go func() {
		rErr := fmt.Errorf("failed to resolve target")
		for _, s := range src {
			l, err := fs.resolver.Resolve(ctx, s.Hosts, s.Name, s.Target)
			if err == nil {
				resultChan <- l
				return
			}
			rErr = errors.Wrapf(rErr, "failed to resolve layer %q from %q: %v",
				s.Target.Digest, s.Name, err)
		}
		errChan <- rErr
	}()

	// Also resolve and cache other layers in parallel
	preResolve := src[0] // TODO: should we pre-resolve blobs in other sources as well?
	for _, desc := range neighboringLayers(preResolve.Manifest, preResolve.Target) {
		desc := desc
		go func() {
			// Avoids to get canceled by client.
			ctx := log.WithLogger(context.Background(),
				log.G(ctx).WithField("mountpoint", mountpoint))
			err := fs.resolver.Cache(ctx, preResolve.Hosts, preResolve.Name, desc)
			if err != nil {
				log.G(ctx).WithError(err).Debug("failed to pre-resolve")
			}
		}()
	}

	// Wait for resolving completion
	var l layer.Layer
	select {
	case l = <-resultChan:
	case err := <-errChan:
		log.G(ctx).WithError(err).Debug("failed to resolve layer")
		return errors.Wrapf(err, "failed to resolve layer")
	case <-time.After(30 * time.Second):
		log.G(ctx).Debug("failed to resolve layer (timeout)")
		return fmt.Errorf("failed to resolve layer (timeout)")
	}
	defer func() {
		if retErr != nil {
			l.Done() // don't use this layer.
		}
	}()

	// Verify layer's content
	if fs.disableVerification {
		// Skip if verification is disabled completely
		l.SkipVerify()
		log.G(ctx).Debugf("Verification forcefully skipped")
	} else if tocDigest, ok := labels[estargz.TOCJSONDigestAnnotation]; ok {
		// Verify this layer using the TOC JSON digest passed through label.
		dgst, err := digest.Parse(tocDigest)
		if err != nil {
			log.G(ctx).WithError(err).Debugf("failed to parse passed TOC digest %q", dgst)
			return errors.Wrapf(err, "invalid TOC digest: %v", tocDigest)
		}
		if err := l.Verify(dgst); err != nil {
			log.G(ctx).WithError(err).Debugf("invalid layer")
			return errors.Wrapf(err, "invalid stargz layer")
		}
		log.G(ctx).Debugf("verified")
	} else if _, ok := labels[config.TargetSkipVerifyLabel]; ok && fs.allowNoVerification {
		// If unverified layer is allowed, use it with warning.
		// This mode is for legacy stargz archives which don't contain digests
		// necessary for layer verification.
		l.SkipVerify()
		log.G(ctx).Warningf("No verification is held for layer")
	} else {
		// Verification must be done. Don't mount this layer.
		return fmt.Errorf("digest of TOC JSON must be passed")
	}
	node, err := l.RootNode()
	if err != nil {
		log.G(ctx).WithError(err).Warnf("Failed to get root node")
		return errors.Wrapf(err, "failed to get root node")
	}

	// Register the mountpoint layer
	fs.layerMu.Lock()
	fs.layer[mountpoint] = l
	fs.layerMu.Unlock()
	fs.metricsController.Add(mountpoint, l)

	// Prefetch this layer. We prefetch several layers in parallel. The first
	// Check() for this layer waits for the prefetch completion.
	if !fs.noprefetch {
		prefetchSize := fs.prefetchSize
		if psStr, ok := labels[config.TargetPrefetchSizeLabel]; ok {
			if ps, err := strconv.ParseInt(psStr, 10, 64); err == nil {
				prefetchSize = ps
			}
		}
		go func() {
			fs.backgroundTaskManager.DoPrioritizedTask()
			defer fs.backgroundTaskManager.DonePrioritizedTask()
			if err := l.Prefetch(prefetchSize); err != nil {
				log.G(ctx).WithError(err).Debug("failed to prefetched layer")
				return
			}
			log.G(ctx).Debug("completed to prefetch")
		}()
	}

	// Fetch whole layer aggressively in background. We use background
	// reader for this so prioritized tasks(Mount, Check, etc...) can
	// interrupt the reading. This can avoid disturbing prioritized tasks
	// about NW traffic.
	if !fs.noBackgroundFetch {
		go func() {
			if err := l.BackgroundFetch(); err != nil {
				log.G(ctx).WithError(err).Debug("failed to fetch whole layer")
				return
			}
			log.G(ctx).Debug("completed to fetch all layer data in background")
		}()
	}

	// mount the node to the specified mountpoint
	// TODO: bind mount the state directory as a read-only fs on snapshotter's side
	timeSec := time.Second
	rawFS := fusefs.NewNodeFS(node, &fusefs.Options{
		AttrTimeout:     &timeSec,
		EntryTimeout:    &timeSec,
		NullPermissions: true,
	})
	mountOpts := &fuse.MountOptions{
		AllowOther: true,     // allow users other than root&mounter to access fs
		FsName:     "stargz", // name this filesystem as "stargz"
		Debug:      fs.debug,
	}
	if _, err := exec.LookPath(fusermountBin); err == nil {
		mountOpts.Options = []string{"suid"} // option for fusermount; allow setuid inside container
	} else {
		log.G(ctx).WithError(err).Debugf("%s not installed; trying direct mount", fusermountBin)
		mountOpts.DirectMount = true
	}
	server, err := fuse.NewServer(rawFS, mountpoint, mountOpts)
	if err != nil {
		log.G(ctx).WithError(err).Debug("failed to make filesystem server")
		return err
	}

	go server.Serve()
	return server.WaitMount()
}

func (fs *filesystem) Check(ctx context.Context, mountpoint string, labels map[string]string) error {
	// This is a prioritized task and all background tasks will be stopped
	// execution so this can avoid being disturbed for NW traffic by background
	// tasks.
	fs.backgroundTaskManager.DoPrioritizedTask()
	defer fs.backgroundTaskManager.DonePrioritizedTask()

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", mountpoint))

	fs.layerMu.Lock()
	l := fs.layer[mountpoint]
	fs.layerMu.Unlock()
	if l == nil {
		log.G(ctx).Debug("layer not registered")
		return fmt.Errorf("layer not registered")
	}

	// Check the blob connectivity and try to refresh the connection on failure
	if err := fs.check(ctx, l, labels); err != nil {
		log.G(ctx).WithError(err).Warn("check failed")
		return err
	}

	// Wait for prefetch compeletion
	if !fs.noprefetch {
		if err := l.WaitForPrefetchCompletion(); err != nil {
			log.G(ctx).WithError(err).Warn("failed to sync with prefetch completion")
		}
	}

	return nil
}

func (fs *filesystem) check(ctx context.Context, l layer.Layer, labels map[string]string) error {
	err := l.Check()
	if err == nil {
		return nil
	}
	log.G(ctx).WithError(err).Warn("failed to connect to blob")

	// Check failed. Try to refresh the connection with fresh source information
	src, err := fs.getSources(labels)
	if err != nil {
		return err
	}
	var (
		retrynum = 1
		rErr     = fmt.Errorf("failed to refresh connection")
	)
	for retry := 0; retry < retrynum; retry++ {
		log.G(ctx).Warnf("refreshing(%d)...", retry)
		for _, s := range src {
			err := l.Refresh(ctx, s.Hosts, s.Name, s.Target)
			if err == nil {
				log.G(ctx).Debug("Successfully refreshed connection")
				return nil
			}
			log.G(ctx).WithError(err).Warnf("failed to refresh the layer %q from %q",
				s.Target.Digest, s.Name)
			rErr = errors.Wrapf(rErr, "failed(layer:%q, ref:%q): %v",
				s.Target.Digest, s.Name, err)
		}
	}

	return rErr
}

func (fs *filesystem) Unmount(ctx context.Context, mountpoint string) error {
	fs.layerMu.Lock()
	l, ok := fs.layer[mountpoint]
	if !ok {
		fs.layerMu.Unlock()
		return fmt.Errorf("specified path %q isn't a mountpoint", mountpoint)
	}
	delete(fs.layer, mountpoint) // unregisters the corresponding layer
	l.Done()
	fs.layerMu.Unlock()
	fs.metricsController.Remove(mountpoint)
	// The goroutine which serving the mountpoint possibly becomes not responding.
	// In case of such situations, we use MNT_FORCE here and abort the connection.
	// In the future, we might be able to consider to kill that specific hanging
	// goroutine using channel, etc.
	// See also: https://www.kernel.org/doc/html/latest/filesystems/fuse.html#aborting-a-filesystem-connection
	return syscall.Unmount(mountpoint, syscall.MNT_FORCE)
}

// neighboringLayers returns layer descriptors except the `target` layer in the specified manifest.
func neighboringLayers(manifest ocispec.Manifest, target ocispec.Descriptor) (descs []ocispec.Descriptor) {
	for _, desc := range manifest.Layers {
		if desc.Digest.String() != target.Digest.String() {
			descs = append(descs, desc)
		}
	}
	return
}
