// +build !no_embedded_executor

package executor

import (
	"errors"
	"io/ioutil"
	"path/filepath"

	"github.com/rancher/k3s/pkg/version"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
)

func (e Embedded) CurrentETCDOptions() (InitialOptions, error) {
	return InitialOptions{}, nil
}

func (e Embedded) ETCD(args ETCDConfig) error {
	configFile, err := args.ToConfigFile()
	if err != nil {
		return err
	}
	cfg, err := embed.ConfigFromFile(configFile)
	if err != nil {
		return err
	}
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil
	}

	go func() {
		select {
		case err := <-etcd.Server.ErrNotify():
			if errors.Is(err, rafthttp.ErrMemberRemoved) {
				tombstoneFile := filepath.Join(args.DataDir, "tombstone")
				if err := ioutil.WriteFile(tombstoneFile, []byte{}, 0600); err != nil {
					logrus.Fatalf("failed to write tombstone file to %s", tombstoneFile)
				}
				logrus.Infof("this node has been removed from the cluster please restart %s to rejoin the cluster", version.Program)
				return
			}

		case <-etcd.Server.StopNotify():
			logrus.Fatalf("etcd stopped")
		case err := <-etcd.Err():
			logrus.Fatalf("etcd exited: %v", err)
		}
	}()
	return nil
}
