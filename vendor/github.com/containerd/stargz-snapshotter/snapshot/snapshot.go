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

package snapshot

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/stargz-snapshotter/snapshot/overlayutils"
	"github.com/moby/sys/mountinfo"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	targetSnapshotLabel = "containerd.io/snapshot.ref"
	remoteLabel         = "containerd.io/snapshot/remote"
	remoteLabelVal      = "remote snapshot"

	// remoteSnapshotLogKey is a key for log line, which indicates whether
	// `Prepare` method successfully prepared targeting remote snapshot or not, as
	// defined in the following:
	// - "true"  : indicates the snapshot has been successfully prepared as a
	//             remote snapshot
	// - "false" : indicates the snapshot failed to be prepared as a remote
	//             snapshot
	// - null    : undetermined
	remoteSnapshotLogKey = "remote-snapshot-prepared"
	prepareSucceeded     = "true"
	prepareFailed        = "false"
)

// FileSystem is a backing filesystem abstraction.
//
// Mount() tries to mount a remote snapshot to the specified mount point
// directory. If succeed, the mountpoint directory will be treated as a layer
// snapshot. If Mount() fails, the mountpoint directory MUST be cleaned up.
// Check() is called to check the connectibity of the existing layer snapshot
// every time the layer is used by containerd.
// Unmount() is called to unmount a remote snapshot from the specified mount point
// directory.
type FileSystem interface {
	Mount(ctx context.Context, mountpoint string, labels map[string]string) error
	Check(ctx context.Context, mountpoint string, labels map[string]string) error
	Unmount(ctx context.Context, mountpoint string) error
}

// SnapshotterConfig is used to configure the remote snapshotter instance
type SnapshotterConfig struct {
	asyncRemove bool
}

// Opt is an option to configure the remote snapshotter
type Opt func(config *SnapshotterConfig) error

// AsynchronousRemove defers removal of filesystem content until
// the Cleanup method is called. Removals will make the snapshot
// referred to by the key unavailable and make the key immediately
// available for re-use.
func AsynchronousRemove(config *SnapshotterConfig) error {
	config.asyncRemove = true
	return nil
}

type snapshotter struct {
	root        string
	ms          *storage.MetaStore
	asyncRemove bool

	// fs is a filesystem that this snapshotter recognizes.
	fs        FileSystem
	userxattr bool // whether to enable "userxattr" mount option
}

// NewSnapshotter returns a Snapshotter which can use unpacked remote layers
// as snapshots. This is implemented based on the overlayfs snapshotter, so
// diffs are stored under the provided root and a metadata file is stored under
// the root as same as overlayfs snapshotter.
func NewSnapshotter(ctx context.Context, root string, targetFs FileSystem, opts ...Opt) (snapshots.Snapshotter, error) {
	if targetFs == nil {
		return nil, fmt.Errorf("Specify filesystem to use")
	}

	var config SnapshotterConfig
	for _, opt := range opts {
		if err := opt(&config); err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	userxattr, err := overlayutils.NeedsUserXAttr(root)
	if err != nil {
		logrus.WithError(err).Warnf("cannot detect whether \"userxattr\" option needs to be used, assuming to be %v", userxattr)
	}

	o := &snapshotter{
		root:        root,
		ms:          ms,
		asyncRemove: config.asyncRemove,
		fs:          targetFs,
		userxattr:   userxattr,
	}

	if err := o.restoreRemoteSnapshot(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to restore remote snapshot")
	}

	return o, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()
	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
// for remote snapshots, no scan will be held and recognise the number of inodes
// and these sizes as "zero".
//
// For committed snapshots, the value is returned from the metadata database.
func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	id, info, usage, err := storage.GetInfo(ctx, key)
	t.Rollback() // transaction no longer needed at this point.

	if err != nil {
		return snapshots.Usage{}, err
	}

	upperPath := o.upperPath(id)

	if info.Kind == snapshots.KindActive {
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}

		usage = snapshots.Usage(du)
	}

	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	s, err := o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
	if err != nil {
		return nil, err
	}

	// Try to prepare the remote snapshot. If succeeded, we commit the snapshot now
	// and return ErrAlreadyExists.
	var base snapshots.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return nil, err
		}
	}
	if target, ok := base.Labels[targetSnapshotLabel]; ok {
		// NOTE: If passed labels include a target of the remote snapshot, `Prepare`
		//       must log whether this method succeeded to prepare that remote snapshot
		//       or not, using the key `remoteSnapshotLogKey` defined in the above. This
		//       log is used by tests in this project.
		lCtx := log.WithLogger(ctx, log.G(ctx).WithField("key", key).WithField("parent", parent))
		if err := o.prepareRemoteSnapshot(ctx, key, base.Labels); err != nil {
			log.G(lCtx).WithField(remoteSnapshotLogKey, prepareFailed).
				WithError(err).Debug("failed to prepare remote snapshot")
		} else {
			base.Labels[remoteLabel] = remoteLabelVal // Mark this snapshot as remote
			err := o.Commit(ctx, target, key, append(opts, snapshots.WithLabels(base.Labels))...)
			if err == nil || errdefs.IsAlreadyExists(err) {
				// count also AlreadyExists as "success"
				log.G(lCtx).WithField(remoteSnapshotLogKey, prepareSucceeded).Debug("prepared remote snapshot")
				return nil, errors.Wrapf(errdefs.ErrAlreadyExists, "target snapshot %q", target)
			}
			log.G(lCtx).WithField(remoteSnapshotLogKey, prepareFailed).
				WithError(err).Debug("failed to internally commit remote snapshot")
			// Don't fallback here (= prohibit to use this key again) because the FileSystem
			// possible has done some work on this "upper" directory.
			return nil, err
		}
	}

	return o.mounts(ctx, s, parent)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	s, err := o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
	if err != nil {
		return nil, err
	}
	return o.mounts(ctx, s, parent)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	s, err := storage.GetSnapshot(ctx, key)
	t.Rollback()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active mount")
	}
	return o.mounts(ctx, s, key)
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	// grab the existing id
	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	usage, err := fs.DiskUsage(ctx, o.upperPath(id))
	if err != nil {
		return err
	}

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}

	return t.Commit()
}

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	_, _, err = storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	if !o.asyncRemove {
		var removals []string
		const cleanupCommitted = false
		removals, err = o.getCleanupDirectories(ctx, t, cleanupCommitted)
		if err != nil {
			return errors.Wrap(err, "unable to get directories for removal")
		}

		// Remove directories after the transaction is closed, failures must not
		// return error since the transaction is committed with the removal
		// key no longer available.
		defer func() {
			if err == nil {
				for _, dir := range removals {
					if err := o.cleanupSnapshotDirectory(ctx, dir); err != nil {
						log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
					}
				}
			}
		}()

	}

	return t.Commit()
}

// Walk the snapshots.
func (o *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return storage.WalkInfo(ctx, fn, fs...)
}

// Cleanup cleans up disk resources from removed or abandoned snapshots
func (o *snapshotter) Cleanup(ctx context.Context) error {
	const cleanupCommitted = false
	return o.cleanup(ctx, cleanupCommitted)
}

func (o *snapshotter) cleanup(ctx context.Context, cleanupCommitted bool) error {
	cleanup, err := o.cleanupDirectories(ctx, cleanupCommitted)
	if err != nil {
		return err
	}

	log.G(ctx).Debugf("cleanup: dirs=%v", cleanup)
	for _, dir := range cleanup {
		if err := o.cleanupSnapshotDirectory(ctx, dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}

	return nil
}

func (o *snapshotter) cleanupDirectories(ctx context.Context, cleanupCommitted bool) ([]string, error) {
	// Get a write transaction to ensure no other write transaction can be entered
	// while the cleanup is scanning.
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}

	defer t.Rollback()
	return o.getCleanupDirectories(ctx, t, cleanupCommitted)
}

func (o *snapshotter) getCleanupDirectories(ctx context.Context, t storage.Transactor, cleanupCommitted bool) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	cleanup := []string{}
	for _, d := range dirs {
		if !cleanupCommitted {
			if _, ok := ids[d]; ok {
				continue
			}
		}

		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

func (o *snapshotter) cleanupSnapshotDirectory(ctx context.Context, dir string) error {

	// On a remote snapshot, the layer is mounted on the "fs" directory.
	// We use Filesystem's Unmount API so that it can do necessary finalization
	// before/after the unmount.
	mp := filepath.Join(dir, "fs")
	if err := o.fs.Unmount(ctx, mp); err != nil {
		log.G(ctx).WithError(err).WithField("dir", mp).Debug("failed to unmount")
	}
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrapf(err, "failed to remove directory %q", dir)
	}
	return nil
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ storage.Snapshot, err error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return storage.Snapshot{}, err
	}

	var td, path string
	defer func() {
		if err != nil {
			if td != "" {
				if err1 := o.cleanupSnapshotDirectory(ctx, td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := o.cleanupSnapshotDirectory(ctx, path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = errors.Wrapf(err, "failed to remove path: %v", err1)
				}
			}
		}
	}()

	snapshotDir := filepath.Join(o.root, "snapshots")
	td, err = o.prepareDirectory(ctx, snapshotDir, kind)
	if err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
		}
		return storage.Snapshot{}, errors.Wrap(err, "failed to create prepare snapshot dir")
	}
	rollback := true
	defer func() {
		if rollback {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	s, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return storage.Snapshot{}, errors.Wrap(err, "failed to create snapshot")
	}

	if len(s.ParentIDs) > 0 {
		st, err := os.Stat(o.upperPath(s.ParentIDs[0]))
		if err != nil {
			return storage.Snapshot{}, errors.Wrap(err, "failed to stat parent")
		}

		stat := st.Sys().(*syscall.Stat_t)

		if err := os.Lchown(filepath.Join(td, "fs"), int(stat.Uid), int(stat.Gid)); err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
			return storage.Snapshot{}, errors.Wrap(err, "failed to chown")
		}
	}

	path = filepath.Join(snapshotDir, s.ID)
	if err = os.Rename(td, path); err != nil {
		return storage.Snapshot{}, errors.Wrap(err, "failed to rename")
	}
	td = ""

	rollback = false
	if err = t.Commit(); err != nil {
		return storage.Snapshot{}, errors.Wrap(err, "commit failed")
	}

	return s, nil
}

func (o *snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := ioutil.TempDir(snapshotDir, "new-")
	if err != nil {
		return "", errors.Wrap(err, "failed to create temp dir")
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, err
	}

	if kind == snapshots.KindActive {
		if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
			return td, err
		}
	}

	return td, nil
}

func (o *snapshotter) mounts(ctx context.Context, s storage.Snapshot, checkKey string) ([]mount.Mount, error) {
	// Make sure that all layers lower than the target layer are available
	if checkKey != "" && !o.checkAvailability(ctx, checkKey) {
		return nil, errors.Wrapf(errdefs.ErrUnavailable, "layer %q unavailable", s.ID)
	}

	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return []mount.Mount{
			{
				Source: o.upperPath(s.ID),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}, nil
	}
	var options []string

	if s.Kind == snapshots.KindActive {
		options = append(options,
			fmt.Sprintf("workdir=%s", o.workPath(s.ID)),
			fmt.Sprintf("upperdir=%s", o.upperPath(s.ID)),
		)
	} else if len(s.ParentIDs) == 1 {
		return []mount.Mount{
			{
				Source: o.upperPath(s.ParentIDs[0]),
				Type:   "bind",
				Options: []string{
					"ro",
					"rbind",
				},
			},
		}, nil
	}

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.upperPath(s.ParentIDs[i])
	}

	options = append(options, fmt.Sprintf("lowerdir=%s", strings.Join(parentPaths, ":")))
	if o.userxattr {
		options = append(options, "userxattr")
	}
	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}, nil

}

func (o *snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "work")
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	// unmount all mounts including Committed
	const cleanupCommitted = true
	ctx := context.Background()
	if err := o.cleanup(ctx, cleanupCommitted); err != nil {
		log.G(ctx).WithError(err).Warn("failed to cleanup")
	}
	return o.ms.Close()
}

// prepareRemoteSnapshot tries to prepare the snapshot as a remote snapshot
// using filesystems registered in this snapshotter.
func (o *snapshotter) prepareRemoteSnapshot(ctx context.Context, key string, labels map[string]string) error {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	if err := o.fs.Mount(ctx, o.upperPath(id), labels); err != nil {
		return err
	}

	return nil
}

// checkAvailability checks avaiability of the specified layer and all lower
// layers using filesystem's checking functionality.
func (o *snapshotter) checkAvailability(ctx context.Context, key string) bool {
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("key", key))
	log.G(ctx).Debug("checking layer availability")

	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		log.G(ctx).WithError(err).Warn("failed to get transaction")
		return false
	}
	defer t.Rollback()

	eg, egCtx := errgroup.WithContext(ctx)
	for cKey := key; cKey != ""; {
		id, info, _, err := storage.GetInfo(ctx, cKey)
		if err != nil {
			log.G(ctx).WithError(err).Warnf("failed to get info of %q", cKey)
			return false
		}
		mp := o.upperPath(id)
		lCtx := log.WithLogger(ctx, log.G(ctx).WithField("mount-point", mp))
		if _, ok := info.Labels[remoteLabel]; ok {
			eg.Go(func() error {
				log.G(lCtx).Debug("checking mount point")
				if err := o.fs.Check(egCtx, mp, info.Labels); err != nil {
					log.G(lCtx).WithError(err).Warn("layer is unavailable")
					return err
				}
				return nil
			})
		} else {
			log.G(lCtx).Debug("layer is normal snapshot(overlayfs)")
		}
		cKey = info.Parent
	}
	if err := eg.Wait(); err != nil {
		return false
	}
	return true
}

func (o *snapshotter) restoreRemoteSnapshot(ctx context.Context) error {
	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		return err
	}
	for _, m := range mounts {
		if strings.HasPrefix(m.Mountpoint, filepath.Join(o.root, "snapshots")) {
			if err := syscall.Unmount(m.Mountpoint, syscall.MNT_FORCE); err != nil {
				return errors.Wrapf(err, "failed to unmount %s", m.Mountpoint)
			}
		}
	}

	var task []snapshots.Info
	if err := o.Walk(ctx, func(ctx context.Context, info snapshots.Info) error {
		if _, ok := info.Labels[remoteLabel]; ok {
			task = append(task, info)
		}
		return nil
	}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	for _, info := range task {
		if err := o.prepareRemoteSnapshot(ctx, info.Name, info.Labels); err != nil {
			return errors.Wrapf(err, "failed to prepare remote snapshot: %s", info.Name)
		}
	}

	return nil
}
