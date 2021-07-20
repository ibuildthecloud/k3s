package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/k3s-io/kine/pkg/client"
	endpoint2 "github.com/k3s-io/kine/pkg/endpoint"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
	certutil "github.com/rancher/dynamiclistener/cert"
	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/daemons/control/deps"
	"github.com/rancher/k3s/pkg/daemons/executor"
	"github.com/rancher/k3s/pkg/version"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/snapshot"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/util/retry"
)

const (
	endpoint            = "https://127.0.0.1:2379"
	testTimeout         = time.Second * 10
	manageTickerTime    = time.Second * 15
	learnerMaxStallTime = time.Minute * 5

	// defaultDialTimeout is intentionally short so that connections timeout within the testTimeout defined above
	defaultDialTimeout = 2 * time.Second
	// other defaults from k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go
	defaultKeepAliveTime    = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second

	maxBackupRetention = 5
)

var (
	learnerProgressKey = version.Program + "/etcd/learnerProgress"
	// AddressKey will contain the value of api addresses list
	AddressKey = version.Program + "/apiaddresses"

	snapshotConfigMapName = version.Program + "-etcd-snapshots"
)

type ETCD struct {
	client  *etcd.Client
	config  *config.Control
	name    string
	runtime *config.ControlRuntime
	address string
	cron    *cron.Cron
	s3      *S3
}

type learnerProgress struct {
	ID               uint64      `json:"id,omitempty"`
	Name             string      `json:"name,omitempty"`
	RaftAppliedIndex uint64      `json:"raftAppliedIndex,omitempty"`
	LastProgress     metav1.Time `json:"lastProgress,omitempty"`
}

// Members contains a slice that holds all
// members of the cluster.
type Members struct {
	Members []*etcdserverpb.Member `json:"members"`
}

// NewETCD creates a new value of type
// ETCD with an initialized cron value.
func NewETCD() *ETCD {
	return &ETCD{
		cron: cron.New(),
	}
}

// EndpointName returns the name of the endpoint.
func (e *ETCD) EndpointName() string {
	return "etcd"
}

// SetControlConfig sets the given config on the etcd struct.
func (e *ETCD) SetControlConfig(config *config.Control) {
	e.config = config
}

// Test ensures that the local node is a voting member of the target cluster.
// If it is still a learner or not a part of the cluster, an error is raised.
func (e *ETCD) Test(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	status, err := e.client.Status(ctx, endpoint)
	if err != nil {
		return err
	}

	if status.IsLearner {
		return errors.New("this server has not yet been promoted from learner to voting member")
	}

	members, err := e.client.MemberList(ctx)
	if err != nil {
		return err
	}

	var memberNameUrls []string
	for _, member := range members.Members {
		for _, peerURL := range member.PeerURLs {
			if peerURL == e.peerURL() && e.name == member.Name {
				return nil
			}
		}
		if len(member.PeerURLs) > 0 {
			memberNameUrls = append(memberNameUrls, member.Name+"="+member.PeerURLs[0])
		}
	}
	return errors.Errorf("this server is a not a member of the etcd cluster. Found %v, expect: %s=%s", memberNameUrls, e.name, e.address)
}

// etcdDBDir returns the path to dataDir/db/etcd
func etcdDBDir(config *config.Control) string {
	return filepath.Join(config.DataDir, "db", "etcd")
}

// walDir returns the path to etcdDBDir/member/wal
func walDir(config *config.Control) string {
	return filepath.Join(etcdDBDir(config), "member", "wal")
}

func sqliteFile(config *config.Control) string {
	return filepath.Join(config.DataDir, "db", "state.db")
}

// nameFile returns the path to etcdDBDir/name.
func nameFile(config *config.Control) string {
	return filepath.Join(etcdDBDir(config), "name")
}

// ResetFile returns the path to etcdDBDir/reset-flag.
func ResetFile(config *config.Control) string {
	return filepath.Join(config.DataDir, "db", "reset-flag")
}

// IsInitialized checks to see if a WAL directory exists. If so, we assume that etcd
// has already been brought up at least once.
func (e *ETCD) IsInitialized(ctx context.Context, config *config.Control) (bool, error) {
	dir := walDir(config)
	if s, err := os.Stat(dir); err == nil && s.IsDir() {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, errors.Wrapf(err, "invalid state for wal directory %s", dir)
	}
}

// Reset resets an etcd node
func (e *ETCD) Reset(ctx context.Context, rebootstrap func() error) error {
	// Wait for etcd to come up as a new single-node cluster, then exit
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			if err := e.Test(ctx); err == nil {
				members, err := e.client.MemberList(ctx)
				if err != nil {
					continue
				}

				// storageBootstrap() - runtime structure has been written with correct certificate data
				if err := rebootstrap(); err != nil {
					logrus.Fatal(err)
				}

				// call functions to rewrite them from daemons/control/server.go (prepare())
				if err := deps.GenServerDeps(e.config, e.runtime); err != nil {
					logrus.Fatal(err)
				}

				if len(members.Members) == 1 && members.Members[0].Name == e.name {
					logrus.Infof("Etcd is running, restart without --cluster-reset flag now. Backup and delete ${datadir}/server/db on each peer etcd server and rejoin the nodes")
					os.Exit(0)
				}
			}
		}
	}()

	// If asked to restore from a snapshot, do so
	if e.config.ClusterResetRestorePath != "" {
		if e.config.EtcdS3 {
			if err := e.initS3IfNil(ctx); err != nil {
				return err
			}
			logrus.Infof("Retrieving etcd snapshot %s from S3", e.config.ClusterResetRestorePath)
			if err := e.s3.Download(ctx); err != nil {
				return err
			}
			logrus.Infof("S3 download complete for %s", e.config.ClusterResetRestorePath)
		}

		info, err := os.Stat(e.config.ClusterResetRestorePath)
		if os.IsNotExist(err) {
			return fmt.Errorf("etcd: snapshot path does not exist: %s", e.config.ClusterResetRestorePath)
		}
		if info.IsDir() {
			return fmt.Errorf("etcd: snapshot path must be a file, not a directory: %s", e.config.ClusterResetRestorePath)
		}
		if err := e.Restore(ctx); err != nil {
			return err
		}
	}

	if err := e.setName(true); err != nil {
		return err
	}
	// touch a file to avoid multiple resets
	if err := ioutil.WriteFile(ResetFile(e.config), []byte{}, 0600); err != nil {
		return err
	}
	return e.newCluster(ctx, true)
}

// Start starts the datastore
func (e *ETCD) Start(ctx context.Context, clientAccessInfo *clientaccess.Info) error {
	existingCluster, err := e.IsInitialized(ctx, e.config)
	if err != nil {
		return errors.Wrapf(err, "configuration validation failed")
	}

	if !e.config.EtcdDisableSnapshots {
		e.setSnapshotFunction(ctx)
		e.cron.Start()
	}

	go e.manageLearners(ctx)

	if existingCluster {
		//check etcd dir permission
		etcdDir := etcdDBDir(e.config)
		info, err := os.Stat(etcdDir)
		if err != nil {
			return err
		}
		if info.Mode() != 0700 {
			if err := os.Chmod(etcdDir, 0700); err != nil {
				return err
			}
		}
		opt, err := executor.CurrentETCDOptions()
		if err != nil {
			return err
		}
		return e.cluster(ctx, false, opt)
	}

	if clientAccessInfo == nil {
		return e.newCluster(ctx, false)
	}

	err = e.join(ctx, clientAccessInfo)
	return errors.Wrap(err, "joining etcd cluster")
}

// join attempts to add a member to an existing cluster
func (e *ETCD) join(ctx context.Context, clientAccessInfo *clientaccess.Info) error {
	clientURLs, memberList, err := ClientURLs(ctx, clientAccessInfo, e.config.PrivateIP)
	if err != nil {
		return err
	}

	client, err := GetClient(ctx, e.runtime, clientURLs...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var (
		cluster []string
		add     = true
	)

	members, err := client.MemberList(ctx)
	if err != nil {
		logrus.Errorf("Failed to get member list from etcd cluster. Will assume this member is already added")
		members = &etcd.MemberListResponse{
			Members: append(memberList.Members, &etcdserverpb.Member{
				Name:     e.name,
				PeerURLs: []string{e.peerURL()},
			}),
		}
		add = false
	}

	for _, member := range members.Members {
		for _, peer := range member.PeerURLs {
			u, err := url.Parse(peer)
			if err != nil {
				return err
			}
			// An uninitialized member won't have a name
			if u.Hostname() == e.address && (member.Name == e.name || member.Name == "") {
				add = false
			}
			if member.Name == "" && u.Hostname() == e.address {
				member.Name = e.name
			}
			if len(member.PeerURLs) > 0 {
				cluster = append(cluster, fmt.Sprintf("%s=%s", member.Name, member.PeerURLs[0]))
			}
		}
	}

	if add {
		logrus.Infof("Adding %s to etcd cluster %v", e.peerURL(), cluster)
		if _, err = client.MemberAddAsLearner(ctx, []string{e.peerURL()}); err != nil {
			return err
		}
		cluster = append(cluster, fmt.Sprintf("%s=%s", e.name, e.peerURL()))
	}

	logrus.Infof("Starting etcd for cluster %v", cluster)
	return e.cluster(ctx, false, executor.InitialOptions{
		Cluster: strings.Join(cluster, ","),
		State:   "existing",
	})
}

// Register configures a new etcd client and adds db info routes for the http request handler.
func (e *ETCD) Register(ctx context.Context, config *config.Control, handler http.Handler) (http.Handler, error) {
	e.config = config
	e.runtime = config.Runtime

	client, err := GetClient(ctx, e.runtime, endpoint)
	if err != nil {
		return nil, err
	}
	e.client = client

	address, err := GetAdvertiseAddress(config.PrivateIP)
	if err != nil {
		return nil, err
	}
	e.address = address
	e.config.Datastore.Endpoint = endpoint
	e.config.Datastore.Config.CAFile = e.runtime.ETCDServerCA
	e.config.Datastore.Config.CertFile = e.runtime.ClientETCDCert
	e.config.Datastore.Config.KeyFile = e.runtime.ClientETCDKey

	if err := e.setName(false); err != nil {
		return nil, err
	}
	e.config.Runtime.ClusterControllerStart = func(ctx context.Context) error {
		Register(ctx, e, e.config.Runtime.Core.Core().V1().Node())
		return nil
	}

	tombstoneFile := filepath.Join(etcdDBDir(e.config), "tombstone")
	if _, err := os.Stat(tombstoneFile); err == nil {
		logrus.Infof("tombstone file has been detected, removing data dir to rejoin the cluster")
		if _, err := backupDirWithRetention(etcdDBDir(e.config), maxBackupRetention); err != nil {
			return nil, err
		}
	}
	return e.handler(handler), err
}

// setName sets a unique name for this cluster member. The first time this is called,
// or if force is set to true, a new name will be generated and written to disk. The persistent
// name is used on subsequent calls.
func (e *ETCD) setName(force bool) error {
	fileName := nameFile(e.config)
	data, err := ioutil.ReadFile(fileName)
	if os.IsNotExist(err) || force {
		h, err := os.Hostname()
		if err != nil {
			return err
		}
		e.name = strings.SplitN(h, ".", 2)[0] + "-" + uuid.New().String()[:8]
		if err := os.MkdirAll(filepath.Dir(fileName), 0700); err != nil {
			return err
		}
		return ioutil.WriteFile(fileName, []byte(e.name), 0600)
	} else if err != nil {
		return err
	}
	e.name = string(data)
	return nil
}

// handler wraps the handler with routes for database info
func (e *ETCD) handler(next http.Handler) http.Handler {
	mux := mux.NewRouter()
	mux.Handle("/db/info", e.infoHandler())
	mux.NotFoundHandler = next
	return mux
}

// infoHandler returns etcd cluster information. This is used by new members when joining the custer.
func (e *ETCD) infoHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()

		members, err := e.client.MemberList(ctx)
		if err != nil {
			json.NewEncoder(rw).Encode(&Members{
				Members: []*etcdserverpb.Member{
					{
						Name:       e.name,
						PeerURLs:   []string{e.peerURL()},
						ClientURLs: []string{e.clientURL()},
					},
				},
			})
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(&Members{
			Members: members.Members,
		})
	})
}

// getClient returns an etcd client connected to the specified endpoints
func GetClient(ctx context.Context, runtime *config.ControlRuntime, endpoints ...string) (*etcd.Client, error) {
	cfg, err := getClientConfig(ctx, runtime, endpoints...)
	if err != nil {
		return nil, err
	}
	return etcd.New(*cfg)
}

//getClientConfig generates an etcd client config connected to the specified endpoints
func getClientConfig(ctx context.Context, runtime *config.ControlRuntime, endpoints ...string) (*etcd.Config, error) {
	tlsConfig, err := toTLSConfig(runtime)
	if err != nil {
		return nil, err
	}
	cfg := &etcd.Config{
		Endpoints:            endpoints,
		TLS:                  tlsConfig,
		Context:              ctx,
		DialTimeout:          defaultDialTimeout,
		DialKeepAliveTime:    defaultKeepAliveTime,
		DialKeepAliveTimeout: defaultKeepAliveTimeout,
	}
	return cfg, nil
}

// toTLSConfig converts the ControlRuntime configuration to TLS configuration suitable
// for use by etcd.
func toTLSConfig(runtime *config.ControlRuntime) (*tls.Config, error) {
	clientCert, err := tls.LoadX509KeyPair(runtime.ClientETCDCert, runtime.ClientETCDKey)
	if err != nil {
		return nil, err
	}

	pool, err := certutil.NewPool(runtime.ETCDServerCA)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{clientCert},
	}, nil
}

// getAdvertiseAddress returns the IP address best suited for advertising to clients
func GetAdvertiseAddress(advertiseIP string) (string, error) {
	ip := advertiseIP
	if ip == "" {
		ipAddr, err := utilnet.ChooseHostInterface()
		if err != nil {
			return "", err
		}
		ip = ipAddr.String()
	}

	return ip, nil
}

// newCluster returns options to set up etcd for a new cluster
func (e *ETCD) newCluster(ctx context.Context, reset bool) error {
	err := e.cluster(ctx, reset, executor.InitialOptions{
		AdvertisePeerURL: fmt.Sprintf("https://%s:2380", e.address),
		Cluster:          fmt.Sprintf("%s=https://%s:2380", e.name, e.address),
		State:            "new",
	})
	if err != nil {
		return err
	}
	if err := e.migrateFromSQLite(ctx); err != nil {
		return fmt.Errorf("failed to migrate content from sqlite to etcd: %w", err)
	}
	return nil
}

func (e *ETCD) migrateFromSQLite(ctx context.Context) error {
	_, err := os.Stat(sqliteFile(e.config))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	logrus.Infof("Migrating content from sqlite to etcd")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, err = endpoint2.Listen(ctx, endpoint2.Config{
		Endpoint: endpoint2.SQLiteBackend,
	})
	if err != nil {
		return err
	}

	sqliteClient, err := client.New(endpoint2.ETCDConfig{
		Endpoints: []string{"unix://kine.sock"},
	})
	if err != nil {
		return err
	}
	defer sqliteClient.Close()

	etcdClient, err := GetClient(ctx, e.runtime, "https://localhost:2379")
	if err != nil {
		return err
	}
	defer etcdClient.Close()

	values, err := sqliteClient.List(ctx, "/registry/", 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		logrus.Infof("Migrating etcd key %s", value.Key)
		_, err := etcdClient.Put(ctx, string(value.Key), string(value.Data))
		if err != nil {
			return err
		}
	}

	return os.Rename(sqliteFile(e.config), sqliteFile(e.config)+".migrated")
}

// peerURL returns the peer access address for the local node
func (e *ETCD) peerURL() string {
	return fmt.Sprintf("https://%s:2380", e.address)
}

// clientURL returns the client access address for the local node
func (e *ETCD) clientURL() string {
	return fmt.Sprintf("https://%s:2379", e.address)
}

// metricsURL returns the metrics access address
func (e *ETCD) metricsURL(expose bool) string {
	if expose {
		return fmt.Sprintf("http://%s:2381", e.address)
	}
	return "http://127.0.0.1:2381"
}

// cluster returns ETCDConfig for a cluster
func (e *ETCD) cluster(ctx context.Context, forceNew bool, options executor.InitialOptions) error {
	return executor.ETCD(executor.ETCDConfig{
		Name:                e.name,
		InitialOptions:      options,
		ForceNewCluster:     forceNew,
		ListenClientURLs:    fmt.Sprintf(e.clientURL() + ",https://127.0.0.1:2379"),
		ListenMetricsURLs:   e.metricsURL(e.config.EtcdExposeMetrics),
		ListenPeerURLs:      e.peerURL(),
		AdvertiseClientURLs: e.clientURL(),
		DataDir:             etcdDBDir(e.config),
		ServerTrust: executor.ServerTrust{
			CertFile:       e.config.Runtime.ServerETCDCert,
			KeyFile:        e.config.Runtime.ServerETCDKey,
			ClientCertAuth: true,
			TrustedCAFile:  e.config.Runtime.ETCDServerCA,
		},
		PeerTrust: executor.PeerTrust{
			CertFile:       e.config.Runtime.PeerServerClientETCDCert,
			KeyFile:        e.config.Runtime.PeerServerClientETCDKey,
			ClientCertAuth: true,
			TrustedCAFile:  e.config.Runtime.ETCDPeerCA,
		},
		ElectionTimeout:   5000,
		HeartbeatInterval: 500,
		Logger:            "zap",
		LogOutputs:        []string{"stderr"},
	})
}

// removePeer removes a peer from the cluster. The peer ID and IP address must both match.
func (e *ETCD) removePeer(ctx context.Context, id, address string, removeSelf bool) error {
	members, err := e.client.MemberList(ctx)
	if err != nil {
		return err
	}

	for _, member := range members.Members {
		if member.Name != id {
			continue
		}
		for _, peerURL := range member.PeerURLs {
			u, err := url.Parse(peerURL)
			if err != nil {
				return err
			}
			if u.Hostname() == address {
				if e.address == address && !removeSelf {
					return errors.New("node has been deleted from the cluster")
				}
				logrus.Infof("Removing name=%s id=%d address=%s from etcd", member.Name, member.ID, address)
				_, err := e.client.MemberRemove(ctx, member.ID)
				if err == rpctypes.ErrGRPCMemberNotFound {
					return nil
				}
				return err
			}
		}
	}

	return nil
}

// manageLearners monitors the etcd cluster to ensure that learners are making progress towards
// being promoted to full voting member. The checks only run on the cluster member that is
// the etcd leader.
func (e *ETCD) manageLearners(ctx context.Context) error {
	t := time.NewTicker(manageTickerTime)
	defer t.Stop()

	for range t.C {
		ctx, cancel := context.WithTimeout(ctx, testTimeout)
		defer cancel()

		// Check to see if the local node is the leader. Only the leader should do learner management.
		if status, err := e.client.Status(ctx, endpoint); err != nil {
			logrus.Errorf("Failed to check local etcd status for learner management: %v", err)
			continue
		} else if status.Header.MemberId != status.Leader {
			continue
		}

		progress, err := e.getLearnerProgress(ctx)
		if err != nil {
			logrus.Errorf("Failed to get recorded learner progress from etcd: %v", err)
			continue
		}

		members, err := e.client.MemberList(ctx)
		if err != nil {
			logrus.Errorf("Failed to get etcd members for learner management: %v", err)
			continue
		}

		for _, member := range members.Members {
			if member.IsLearner {
				if err := e.trackLearnerProgress(ctx, progress, member); err != nil {
					logrus.Errorf("Failed to track learner progress towards promotion: %v", err)
				}
				break
			}
		}
	}
	return nil
}

// trackLearnerProcess attempts to promote a learner. If it cannot be promoted, progress through the raft index is tracked.
// If the learner does not make any progress in a reasonable amount of time, it is evicted from the cluster.
func (e *ETCD) trackLearnerProgress(ctx context.Context, progress *learnerProgress, member *etcdserverpb.Member) error {
	// Try to promote it. If it can be promoted, no further tracking is necessary
	if _, err := e.client.MemberPromote(ctx, member.ID); err != nil {
		logrus.Debugf("Unable to promote learner %s: %v", member.Name, err)
	} else {
		logrus.Infof("Promoted learner %s", member.Name)
		return nil
	}

	now := time.Now()

	// If this is the first time we've tracked this member's progress, reset stats
	if progress.Name != member.Name || progress.ID != member.ID {
		progress.ID = member.ID
		progress.Name = member.Name
		progress.RaftAppliedIndex = 0
		progress.LastProgress.Time = now
	}

	// Update progress by retrieving status from the member's first reachable client URL
	for _, ep := range member.ClientURLs {
		ctx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
		defer cancel()
		status, err := e.client.Status(ctx, ep)
		if err != nil {
			logrus.Debugf("Failed to get etcd status from learner %s at %s: %v", member.Name, ep, err)
			continue
		}

		if progress.RaftAppliedIndex < status.RaftAppliedIndex {
			logrus.Debugf("Learner %s has progressed from RaftAppliedIndex %d to %d", progress.Name, progress.RaftAppliedIndex, status.RaftAppliedIndex)
			progress.RaftAppliedIndex = status.RaftAppliedIndex
			progress.LastProgress.Time = now
		}
		break
	}

	// Warn if the learner hasn't made any progress
	if !progress.LastProgress.Time.Equal(now) {
		logrus.Warnf("Learner %s stalled at RaftAppliedIndex=%d for %s", progress.Name, progress.RaftAppliedIndex, now.Sub(progress.LastProgress.Time).String())
	}

	// See if it's time to evict yet
	if now.Sub(progress.LastProgress.Time) > learnerMaxStallTime {
		if _, err := e.client.MemberRemove(ctx, member.ID); err != nil {
			return err
		}
		logrus.Warnf("Removed learner %s from etcd cluster", member.Name)
		return nil
	}

	return e.setLearnerProgress(ctx, progress)
}

// getLearnerProgress returns the stored learnerProgress struct as retrieved from etcd
func (e *ETCD) getLearnerProgress(ctx context.Context) (*learnerProgress, error) {
	progress := &learnerProgress{}

	value, err := e.client.Get(ctx, learnerProgressKey)
	if err != nil {
		return nil, err
	}

	if value.Count < 1 {
		return progress, nil
	}

	if err := json.NewDecoder(bytes.NewBuffer(value.Kvs[0].Value)).Decode(progress); err != nil {
		return nil, err
	}
	return progress, nil
}

// setLearnerProgress stores the learnerProgress struct to etcd
func (e *ETCD) setLearnerProgress(ctx context.Context, status *learnerProgress) error {
	w := &bytes.Buffer{}

	if err := json.NewEncoder(w).Encode(status); err != nil {
		return err
	}

	_, err := e.client.Put(ctx, learnerProgressKey, w.String())
	return err
}

// clientURLs returns a list of all non-learner etcd cluster member client access URLs
func ClientURLs(ctx context.Context, clientAccessInfo *clientaccess.Info, selfIP string) ([]string, Members, error) {
	var memberList Members
	resp, err := clientAccessInfo.Get("/db/info")
	if err != nil {
		return nil, memberList, err
	}

	if err := json.Unmarshal(resp, &memberList); err != nil {
		return nil, memberList, err
	}
	ip, err := GetAdvertiseAddress(selfIP)
	if err != nil {
		return nil, memberList, err
	}
	var clientURLs []string
members:
	for _, member := range memberList.Members {
		// excluding learner member from the client list
		if member.IsLearner {
			continue
		}
		for _, url := range member.ClientURLs {
			if strings.Contains(url, ip) {
				continue members
			}
		}
		clientURLs = append(clientURLs, member.ClientURLs...)
	}
	return clientURLs, memberList, nil
}

// snapshotDir ensures that the snapshot directory exists, and then returns its path.
func snapshotDir(config *config.Control) (string, error) {
	if config.EtcdSnapshotDir == "" {
		// we have to create the snapshot dir if we are using
		// the default snapshot dir if it doesn't exist
		defaultSnapshotDir := filepath.Join(config.DataDir, "db", "snapshots")
		s, err := os.Stat(defaultSnapshotDir)
		if err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(defaultSnapshotDir, 0700); err != nil {
					return "", err
				}
				return defaultSnapshotDir, nil
			}
			return "", err
		}
		if s.IsDir() {
			return defaultSnapshotDir, nil
		}
	}
	return config.EtcdSnapshotDir, nil
}

// preSnapshotSetup checks to see if the necessary components are in place
// to perform an Etcd snapshot. This is necessary primarily for on-demand
// snapshots since they're performed before normal Etcd setup is completed.
func (e *ETCD) preSnapshotSetup(ctx context.Context, config *config.Control) error {
	if e.client == nil {
		if e.config == nil {
			e.config = config
		}
		client, err := GetClient(ctx, e.config.Runtime, endpoint)
		if err != nil {
			return err
		}
		e.client = client
	}
	if e.runtime == nil {
		e.runtime = config.Runtime
	}
	return nil
}

// Snapshot attempts to save a new snapshot to the configured directory, and then clean up any old
// snapshots in excess of the retention limits. This method is used in the internal cron snapshot
// system as well as used to do on-demand snapshots.
func (e *ETCD) Snapshot(ctx context.Context, config *config.Control) error {
	if err := e.preSnapshotSetup(ctx, config); err != nil {
		return err
	}

	status, err := e.client.Status(ctx, endpoint)
	if err != nil {
		return errors.Wrap(err, "failed to check etcd status for snapshot")
	}

	if status.IsLearner {
		logrus.Warnf("Skipping snapshot: not supported for learner")
		return nil
	}

	snapshotDir, err := snapshotDir(e.config)
	if err != nil {
		return errors.Wrap(err, "failed to get the snapshot dir")
	}

	cfg, err := getClientConfig(ctx, e.runtime, endpoint)
	if err != nil {
		return errors.Wrap(err, "failed to get config for etcd snapshot")
	}

	nodeName := os.Getenv("NODE_NAME")
	snapshotName := fmt.Sprintf("%s-%s-%d", e.config.EtcdSnapshotName, nodeName, time.Now().Unix())
	snapshotPath := filepath.Join(snapshotDir, snapshotName)

	logrus.Infof("Saving etcd snapshot to %s", snapshotPath)

	if err := snapshot.NewV3(nil).Save(ctx, *cfg, snapshotPath); err != nil {
		return errors.Wrap(err, "failed to save snapshot")
	}

	if e.config.EtcdS3 {
		logrus.Infof("Saving etcd snapshot %s to S3", snapshotName)
		if err := e.initS3IfNil(ctx); err != nil {
			return err
		}
		if err := e.s3.upload(ctx, snapshotPath); err != nil {
			return err
		}
		logrus.Infof("S3 upload complete for %s", snapshotName)

		if e.config.EtcdSnapshotRetention >= 1 {
			if err := e.s3.snapshotRetention(ctx); err != nil {
				return errors.Wrap(err, "failed to apply s3 snapshot retention")
			}
		}
	}

	// check if we need to perform a retention check
	if e.config.EtcdSnapshotRetention >= 1 {
		if err := snapshotRetention(e.config.EtcdSnapshotRetention, e.config.EtcdSnapshotName, snapshotDir); err != nil {
			return errors.Wrap(err, "failed to apply snapshot retention")
		}
	}

	return e.StoreSnapshotData(ctx)
}

type s3Config struct {
	Endpoint      string `json:"endpoint,omitempty"`
	EndpointCA    string `json:"endpointCA,omitempty"`
	SkipSSLVerify bool   `json:"skipSSLVerify,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	Region        string `json:"region,omitempty"`
	Folder        string `json:"folder,omitempty"`
}

// SnapshotFile represents a single snapshot and it's
// metadata.
type SnapshotFile struct {
	Name string `json:"name"`
	// Location contains the full path of the snapshot. For
	// local paths, the location will be prefixed with "file://".
	Location  string       `json:"location,omitempty"`
	NodeName  string       `json:"nodeName,omitempty"`
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`
	Size      int64        `json:"size,omitempty"`
	S3        *s3Config    `json:"s3Config,omitempty"`
}

// listSnapshots provides a list of the currently stored
// snapshots on disk or in S3 along with their relevant
// metadata.
func (e *ETCD) listSnapshots(ctx context.Context, snapshotDir string) ([]SnapshotFile, error) {
	var snapshots []SnapshotFile

	if e.config.EtcdS3 {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		if err := e.initS3IfNil(ctx); err != nil {
			return nil, err
		}

		objects := e.s3.client.ListObjects(ctx, e.config.EtcdS3BucketName, minio.ListObjectsOptions{})

		for obj := range objects {
			if obj.Err != nil {
				return nil, obj.Err
			}
			if obj.Size == 0 {
				continue
			}

			ca, err := time.Parse(time.RFC3339, obj.LastModified.Format(time.RFC3339))
			if err != nil {
				return nil, err
			}

			snapshots = append(snapshots, SnapshotFile{
				Name:     filepath.Base(obj.Key),
				NodeName: "s3",
				CreatedAt: &metav1.Time{
					Time: ca,
				},
				Size: obj.Size,
				S3: &s3Config{
					Endpoint:      e.config.EtcdS3Endpoint,
					EndpointCA:    e.config.EtcdS3EndpointCA,
					SkipSSLVerify: e.config.EtcdS3SkipSSLVerify,
					Bucket:        e.config.EtcdS3BucketName,
					Region:        e.config.EtcdS3Region,
					Folder:        e.config.EtcdS3Folder,
				},
			})
		}

		return snapshots, nil
	}

	files, err := ioutil.ReadDir(snapshotDir)
	if err != nil {
		return nil, err
	}

	nodeName := os.Getenv("NODE_NAME")

	for _, f := range files {
		snapshots = append(snapshots, SnapshotFile{
			Name:     f.Name(),
			Location: "file://" + filepath.Join(snapshotDir, f.Name()),
			NodeName: nodeName,
			CreatedAt: &metav1.Time{
				Time: f.ModTime(),
			},
			Size: f.Size(),
		})
	}

	return snapshots, nil
}

// initS3IfNil initializes the S3 client
// if it hasn't yet been initialized.
func (e *ETCD) initS3IfNil(ctx context.Context) error {
	if e.s3 == nil {
		s3, err := NewS3(ctx, e.config)
		if err != nil {
			return err
		}
		e.s3 = s3
	}

	return nil
}

// PruneSnapshots perfrorms a retention run with the given
// retention duration and removes expired snapshots.
func (e *ETCD) PruneSnapshots(ctx context.Context) error {
	snapshotDir, err := snapshotDir(e.config)
	if err != nil {
		return errors.Wrap(err, "failed to get the snapshot dir")
	}

	if e.config.EtcdS3 {
		if e.initS3IfNil(ctx); err != nil {
			return err
		}

		return e.s3.snapshotRetention(ctx)
	}

	return snapshotRetention(e.config.EtcdSnapshotRetention, e.config.EtcdSnapshotName, snapshotDir)
}

// ListSnapshots is an exported wrapper method that wraps an
// unexported method of the same name.
func (e *ETCD) ListSnapshots(ctx context.Context) ([]SnapshotFile, error) {
	snapshotDir, err := snapshotDir(e.config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get the snapshot dir")
	}

	return e.listSnapshots(ctx, snapshotDir)
}

// deleteSnapshots removes the given snapshots from
// either local storage or S3.
func (e *ETCD) DeleteSnapshots(ctx context.Context, snapshots []string) error {
	snapshotDir, err := snapshotDir(e.config)
	if err != nil {
		return errors.Wrap(err, "failed to get the snapshot dir")
	}

	if e.config.EtcdS3 {
		logrus.Info("Removing the given etcd snapshot(s) from S3")
		logrus.Debugf("Removing the given etcd snapshot(s) from S3: %v", snapshots)

		if e.initS3IfNil(ctx); err != nil {
			return err
		}

		objectsCh := make(chan minio.ObjectInfo)

		ctx, cancel := context.WithTimeout(ctx, defaultS3OpTimeout)
		defer cancel()

		go func() {
			defer close(objectsCh)

			opts := minio.ListObjectsOptions{
				Recursive: true,
			}

			for obj := range e.s3.client.ListObjects(ctx, e.config.EtcdS3BucketName, opts) {
				if obj.Err != nil {
					logrus.Error(obj.Err)
					return
				}

				// iterate through the given snapshots and only
				// add them to the channel for remove if they're
				// actually found from the bucket listing.
				for _, snapshot := range snapshots {
					if snapshot == obj.Key {
						objectsCh <- obj
					}
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				logrus.Errorf("Unable to delete snapshot: %v", ctx.Err())
				return e.StoreSnapshotData(ctx)
			case <-time.After(time.Millisecond * 100):
				continue
			case err, ok := <-e.s3.client.RemoveObjects(ctx, e.config.EtcdS3BucketName, objectsCh, minio.RemoveObjectsOptions{}):
				if err.Err != nil {
					logrus.Errorf("Unable to delete snapshot: %v", err.Err)
				}
				if !ok {
					return e.StoreSnapshotData(ctx)
				}
			}
		}
	}

	logrus.Info("Removing the given locally stored etcd snapshot(s)")
	logrus.Debugf("Removing the given locally stored etcd snapshot(s): %v", snapshots)

	for _, s := range snapshots {
		// check if the given snapshot exists. If it does,
		// remove it, otherwise continue.
		sf := filepath.Join(snapshotDir, s)
		if _, err := os.Stat(sf); os.IsNotExist(err) {
			continue
		}
		if err := os.Remove(sf); err != nil {
			return err
		}
	}

	return e.StoreSnapshotData(ctx)
}

// updateSnapshotData populates the given map with the contents of the given slice.
func updateSnapshotData(data map[string]string, snapshotFiles []SnapshotFile) error {
	for _, v := range snapshotFiles {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		data[v.Name] = string(b)
	}

	return nil
}

// StoreSnapshotData stores the given snapshot data in the "snapshots" ConfigMap.
func (e *ETCD) StoreSnapshotData(ctx context.Context) error {
	logrus.Infof("Saving current etcd snapshot set to %s ConfigMap", snapshotConfigMapName)

	snapshotDir, err := snapshotDir(e.config)
	if err != nil {
		return errors.Wrap(err, "failed to get the snapshot dir")
	}

	return retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
	}, func() error {
		// make sure the core.Factory is initialize. There can
		// be a race between this core code startup.
		for e.config.Runtime.Core == nil {
			runtime.Gosched()
		}

		snapshotConfigMap, getErr := e.config.Runtime.Core.Core().V1().ConfigMap().Get(metav1.NamespaceSystem, snapshotConfigMapName, metav1.GetOptions{})

		snapshotFiles, err := e.listSnapshots(ctx, snapshotDir)
		if err != nil {
			return err
		}

		data := make(map[string]string, len(snapshotFiles))
		if err := updateSnapshotData(data, snapshotFiles); err != nil {
			return err
		}

		if apierrors.IsNotFound(getErr) {
			cm := v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      snapshotConfigMapName,
					Namespace: metav1.NamespaceSystem,
				},
				Data: data,
			}
			_, err := e.config.Runtime.Core.Core().V1().ConfigMap().Create(&cm)
			return err
		}

		if snapshotConfigMap.Data == nil {
			snapshotConfigMap.Data = make(map[string]string)
		}

		nodeName := os.Getenv("NODE_NAME")

		// remove entries for this node only
		for k, v := range snapshotConfigMap.Data {
			var sf SnapshotFile
			if err := json.Unmarshal([]byte(v), &sf); err != nil {
				return err
			}
			if sf.NodeName == nodeName || sf.NodeName == "s3" {
				delete(snapshotConfigMap.Data, k)
			}
		}

		// save this node's entries to the ConfigMap
		for k, v := range data {
			snapshotConfigMap.Data[k] = v
		}

		_, err = e.config.Runtime.Core.Core().V1().ConfigMap().Update(snapshotConfigMap)
		return err
	})
}

// setSnapshotFunction schedules snapshots at the configured interval.
func (e *ETCD) setSnapshotFunction(ctx context.Context) {
	e.cron.AddFunc(e.config.EtcdSnapshotCron, func() {
		if err := e.Snapshot(ctx, e.config); err != nil {
			logrus.Error(err)
		}
	})
}

// Restore performs a restore of the ETCD datastore from
// the given snapshot path. This operation exists upon
// completion.
func (e *ETCD) Restore(ctx context.Context) error {
	// check the old etcd data dir
	oldDataDir := etcdDBDir(e.config) + "-old-" + strconv.Itoa(int(time.Now().Unix()))
	if e.config.ClusterResetRestorePath == "" {
		return errors.New("no etcd restore path was specified")
	}
	// make sure snapshot exists before restoration
	if _, err := os.Stat(e.config.ClusterResetRestorePath); err != nil {
		return err
	}
	// move the data directory to a temp path
	if err := os.Rename(etcdDBDir(e.config), oldDataDir); err != nil {
		return err
	}
	logrus.Infof("Pre-restore etcd database moved to %s", oldDataDir)
	sManager := snapshot.NewV3(nil)
	if err := sManager.Restore(snapshot.RestoreConfig{
		SnapshotPath:   e.config.ClusterResetRestorePath,
		Name:           e.name,
		OutputDataDir:  etcdDBDir(e.config),
		OutputWALDir:   walDir(e.config),
		PeerURLs:       []string{e.peerURL()},
		InitialCluster: e.name + "=" + e.peerURL(),
	}); err != nil {
		return err
	}
	return nil
}

// snapshotRetention iterates through the snapshots and removes the oldest
// leaving the desired number of snapshots.
func snapshotRetention(retention int, snapshotPrefix string, snapshotDir string) error {
	nodeName := os.Getenv("NODE_NAME")

	var snapshotFiles []os.FileInfo
	if err := filepath.Walk(snapshotDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(info.Name(), snapshotPrefix+"-"+nodeName) {
			snapshotFiles = append(snapshotFiles, info)
		}
		return nil
	}); err != nil {
		return err
	}
	if len(snapshotFiles) <= retention {
		return nil
	}
	sort.Slice(snapshotFiles, func(i, j int) bool {
		return snapshotFiles[i].Name() < snapshotFiles[j].Name()
	})

	delCount := len(snapshotFiles) - retention
	for _, df := range snapshotFiles[:delCount] {
		if err := os.Remove(filepath.Join(snapshotDir, df.Name())); err != nil {
			return err
		}
	}

	return nil
}

// backupDirWithRetention will move the dir to a backup dir
// and will keep only maxBackupRetention of dirs.
func backupDirWithRetention(dir string, maxBackupRetention int) (string, error) {
	backupDir := dir + "-backup-" + strconv.Itoa(int(time.Now().Unix()))
	if _, err := os.Stat(dir); err != nil {
		return "", nil
	}
	files, err := ioutil.ReadDir(filepath.Dir(dir))
	if err != nil {
		return "", err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().After(files[j].ModTime())
	})
	count := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), filepath.Base(dir)+"-backup") && f.IsDir() {
			count++
			if count > maxBackupRetention {
				if err := os.RemoveAll(filepath.Join(filepath.Dir(dir), f.Name())); err != nil {
					return "", err
				}
			}
		}
	}
	// move the directory to a temp path
	if err := os.Rename(dir, backupDir); err != nil {
		return "", err
	}
	return backupDir, nil
}

// GetAPIServerURLFromETCD will try to fetch the version.Program/apiaddresses key from etcd
// when it succeed it will parse the first address in the list and return back an address
func GetAPIServerURLFromETCD(ctx context.Context, cfg *config.Control) (string, error) {
	if cfg.Runtime == nil {
		return "", fmt.Errorf("runtime is not ready yet")
	}
	cl, err := GetClient(ctx, cfg.Runtime, endpoint)
	if err != nil {
		return "", err
	}
	etcdResp, err := cl.KV.Get(ctx, AddressKey)
	if err != nil {
		return "", err
	}

	if etcdResp.Count < 1 {
		return "", fmt.Errorf("servers addresses are not yet set")
	}
	var addresses []string
	if err := json.Unmarshal(etcdResp.Kvs[0].Value, &addresses); err != nil {
		return "", fmt.Errorf("failed to unmarshal etcd key: %v", err)
	}

	return addresses[0], nil
}

// GetMembersClientURLs will list through the member lists in etcd and return
// back a combined list of client urls for each member in the cluster
func (e *ETCD) GetMembersClientURLs(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	members, err := e.client.MemberList(ctx)
	if err != nil {
		return nil, err
	}

	var memberUrls []string
	for _, member := range members.Members {
		for _, clientURL := range member.ClientURLs {
			memberUrls = append(memberUrls, string(clientURL))
		}
	}
	return memberUrls, nil
}

// RemoveSelf will remove the member if it exists in the cluster
func (e *ETCD) RemoveSelf(ctx context.Context) error {
	if err := e.removePeer(ctx, e.name, e.address, true); err != nil {
		return err
	}

	// backup the data dir to avoid issues when re-enabling etcd
	oldDataDir := etcdDBDir(e.config) + "-old-" + strconv.Itoa(int(time.Now().Unix()))

	// move the data directory to a temp path
	return os.Rename(etcdDBDir(e.config), oldDataDir)
}
