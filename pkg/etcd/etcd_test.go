package etcd

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/util/tests"
	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func generateTestConfig() *config.Control {
	_, clusterIPNet, _ := net.ParseCIDR("10.42.0.0/16")
	_, serviceIPNet, _ := net.ParseCIDR("10.43.0.0/16")

	return &config.Control{
		HTTPSPort:             6443,
		SupervisorPort:        6443,
		AdvertisePort:         6443,
		ClusterDomain:         "cluster.local",
		ClusterDNS:            net.ParseIP("10.43.0.10"),
		ClusterIPRange:        clusterIPNet,
		DataDir:               "/tmp/k3s/", // Different than the default value
		FlannelBackend:        "vxlan",
		EtcdSnapshotName:      "etcd-snapshot",
		EtcdSnapshotCron:      "0 */12 * * *",
		EtcdSnapshotRetention: 5,
		EtcdS3Endpoint:        "s3.amazonaws.com",
		EtcdS3Region:          "us-east-1",
		SANs:                  []string{"127.0.0.1"},
		ServiceIPRange:        serviceIPNet,
	}
}

func generateTestHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {})
}

func TestETCD_IsInitialized(t *testing.T) {
	type args struct {
		ctx    context.Context
		config *config.Control
	}
	tests := []struct {
		name     string
		args     args
		setup    func(*config.Control) error
		teardown func(*config.Control) error
		want     bool
		wantErr  bool
	}{
		{
			name: "Directory exists",
			args: args{
				ctx:    context.TODO(),
				config: generateTestConfig(),
			},
			setup: func(cnf *config.Control) error {
				if err := tests.GenerateDataDir(cnf); err != nil {
					return err
				}
				return os.MkdirAll(walDir(cnf), 0700)
			},
			teardown: func(cnf *config.Control) error {
				tests.CleanupDataDir(cnf)
				return os.Remove(walDir(cnf))
			},
			wantErr: false,
			want:    true,
		},
		{
			name: "Directory does not exist",
			args: args{
				ctx:    context.TODO(),
				config: generateTestConfig(),
			},
			setup: func(cnf *config.Control) error {
				if err := tests.GenerateDataDir(cnf); err != nil {
					return err
				}
				// We don't care if removal fails to find the dir
				os.Remove(walDir(cnf))
				return nil
			},
			teardown: func(cnf *config.Control) error {
				tests.CleanupDataDir(cnf)
				return nil
			},
			wantErr: false,
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewETCD()
			defer tt.teardown(tt.args.config)
			if err := tt.setup(tt.args.config); err != nil {
				t.Errorf("Prep for ETCD.IsInitialized() failed = %v", err)
				return
			}
			got, err := e.IsInitialized(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ETCD.IsInitialized() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ETCD.IsInitialized() = %+v\nWant = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestETCD_Register(t *testing.T) {
	type args struct {
		ctx     context.Context
		config  *config.Control
		handler http.Handler
	}
	tests := []struct {
		name     string
		args     args
		setup    func(cnf *config.Control) error
		teardown func(cnf *config.Control) error
		wantErr  bool
	}{
		{
			name: "Call Register with standard config",
			args: args{
				ctx:     context.TODO(),
				config:  generateTestConfig(),
				handler: generateTestHandler(),
			},
			setup: func(cnf *config.Control) error {
				return tests.GenerateRuntime(cnf)
			},
			teardown: func(cnf *config.Control) error {
				tests.CleanupDataDir(cnf)
				return nil
			},
		},
		{
			name: "Call Register with a tombstone file created",
			args: args{
				ctx:     context.TODO(),
				config:  generateTestConfig(),
				handler: generateTestHandler(),
			},
			setup: func(cnf *config.Control) error {
				if err := tests.GenerateRuntime(cnf); err != nil {
					return err
				}
				if err := os.MkdirAll(etcdDBDir(cnf), 0700); err != nil {
					return err
				}
				tombstoneFile := filepath.Join(etcdDBDir(cnf), "tombstone")
				if _, err := os.Create(tombstoneFile); err != nil {
					return err
				}
				return nil
			},
			teardown: func(cnf *config.Control) error {
				tombstoneFile := filepath.Join(etcdDBDir(cnf), "tombstone")
				os.Remove(tombstoneFile)
				tests.CleanupDataDir(cnf)
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewETCD()

			defer tt.teardown(tt.args.config)
			if err := tt.setup(tt.args.config); err != nil {
				t.Errorf("Setup for ETCD.Register() failed = %v", err)
				return
			}
			_, err := e.Register(tt.args.ctx, tt.args.config, tt.args.handler)
			if (err != nil) != tt.wantErr {
				t.Errorf("ETCD.Register() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestETCD_Start(t *testing.T) {
	type fields struct {
		client  *clientv3.Client
		config  *config.Control
		name    string
		runtime *config.ControlRuntime
		address string
		cron    *cron.Cron
		s3      *S3
	}
	type args struct {
		ctx              context.Context
		clientAccessInfo *clientaccess.Info
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		setup    func(cnf *config.Control) error
		teardown func(cnf *config.Control) error
		wantErr  bool
	}{
		{
			name: "Start etcd without clientAccessInfo and without snapshots",
			fields: fields{
				config:  generateTestConfig(),
				address: "192.168.1.123", // Local IP address
			},
			args: args{
				ctx:              context.TODO(),
				clientAccessInfo: nil,
			},
			setup: func(cnf *config.Control) error {
				cnf.EtcdDisableSnapshots = true
				return tests.GenerateRuntime(cnf)
			},
			teardown: func(cnf *config.Control) error {
				tests.CleanupDataDir(cnf)
				return nil
			},
		},
		{
			name: "Start etcd without clientAccessInfo on",
			fields: fields{
				config:  generateTestConfig(),
				address: "192.168.1.123", // Local IP address
				cron:    cron.New(),
			},
			args: args{
				ctx:              context.TODO(),
				clientAccessInfo: nil,
			},
			setup: func(cnf *config.Control) error {
				return tests.GenerateRuntime(cnf)
			},
			teardown: func(cnf *config.Control) error {
				tests.CleanupDataDir(cnf)
				return nil
			},
		},
		{
			name: "Start etcd with an existing cluster",
			fields: fields{
				config:  generateTestConfig(),
				address: "192.168.1.123", // Local IP address
				cron:    cron.New(),
			},
			args: args{
				ctx:              context.TODO(),
				clientAccessInfo: nil,
			},
			setup: func(cnf *config.Control) error {
				if err := tests.GenerateRuntime(cnf); err != nil {
					return err
				}
				return os.MkdirAll(walDir(cnf), 0700)
			},
			teardown: func(cnf *config.Control) error {
				tests.CleanupDataDir(cnf)
				os.Remove(walDir(cnf))
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ETCD{
				client:  tt.fields.client,
				config:  tt.fields.config,
				name:    tt.fields.name,
				runtime: tt.fields.runtime,
				address: tt.fields.address,
				cron:    tt.fields.cron,
				s3:      tt.fields.s3,
			}
			defer tt.teardown(e.config)
			if err := tt.setup(e.config); err != nil {
				t.Errorf("Setup for ETCD.Start() failed = %v", err)
				return
			}
			if err := e.Start(tt.args.ctx, tt.args.clientAccessInfo); (err != nil) != tt.wantErr {
				t.Errorf("ETCD.Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
