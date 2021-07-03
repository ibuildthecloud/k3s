package cluster

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/rancher/dynamiclistener"
	"github.com/rancher/dynamiclistener/factory"
	"github.com/rancher/dynamiclistener/storage/file"
	"github.com/rancher/dynamiclistener/storage/kubernetes"
	"github.com/rancher/dynamiclistener/storage/memory"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/etcd"
	"github.com/rancher/k3s/pkg/version"
	"github.com/rancher/wrangler/pkg/generated/controllers/core"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newListener returns a new TCP listener and HTTP request handler using dynamiclistener.
// dynamiclistener will use the cluster's Server CA to sign the dynamically generate certificate,
// and will sync the certs into the Kubernetes datastore, with a local disk cache.
func (c *Cluster) newListener(ctx context.Context) (net.Listener, http.Handler, error) {
	if c.managedDB != nil {
		if _, err := os.Stat(etcd.ResetFile(c.config)); err == nil {
			// delete the dynamic listener file if it exists after restoration to fix restoration
			// on fresh nodes
			os.Remove(filepath.Join(c.config.DataDir, "tls/dynamic-cert.json"))
		}
	}
	tcp, err := dynamiclistener.NewTCPListener(c.config.BindAddress, c.config.SupervisorPort)
	if err != nil {
		return nil, nil, err
	}
	cert, key, err := factory.LoadCerts(c.runtime.ServerCA, c.runtime.ServerCAKey)
	if err != nil {
		return nil, nil, err
	}
	storage := tlsStorage(ctx, c.config.DataDir, c.runtime)
	return dynamiclistener.NewListener(tcp, storage, cert, key, dynamiclistener.Config{
		ExpirationDaysCheck: config.CertificateRenewDays,
		Organization:        []string{version.Program},
		SANs:                append(c.config.SANs, "localhost", "kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc."+c.config.ClusterDomain),
		CN:                  version.Program,
		TLSConfig: &tls.Config{
			ClientAuth:   tls.RequestClientCert,
			MinVersion:   c.config.TLSMinVersion,
			CipherSuites: c.config.TLSCipherSuites,
		},
	})
}

// initClusterAndHTTPS sets up the dynamic tls listener, request router,
// and cluster database. Once the database is up, it starts the supervisor http server.
func (c *Cluster) initClusterAndHTTPS(ctx context.Context) error {
	// Set up dynamiclistener TLS listener and request handler
	listener, handler, err := c.newListener(ctx)
	if err != nil {
		return err
	}

	// Get the base request handler
	handler, err = c.getHandler(handler)
	if err != nil {
		return err
	}

	// Config the cluster database and allow it to add additional request handlers
	handler, err = c.initClusterDB(ctx, handler)
	if err != nil {
		return err
	}

	// Create a HTTP server with the registered request handlers, using logrus for logging
	server := http.Server{
		Handler:  handler,
		ErrorLog: log.New(logrus.StandardLogger().Writer(), "Cluster-Http-Server ", log.LstdFlags),
	}

	// Start the supervisor http server on the tls listener
	go func() {
		err := server.Serve(listener)
		logrus.Fatalf("server stopped: %v", err)
	}()

	// Shutdown the http server when the context is closed
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	return nil
}

// tlsStorage creates an in-memory cache for dynamiclistener's certificate, backed by a file on disk
// and the Kubernetes datastore.
func tlsStorage(ctx context.Context, dataDir string, runtime *config.ControlRuntime) dynamiclistener.TLSStorage {
	fileStorage := file.New(filepath.Join(dataDir, "tls/dynamic-cert.json"))
	cache := memory.NewBacked(fileStorage)
	return kubernetes.New(ctx, func() *core.Factory {
		return runtime.Core
	}, metav1.NamespaceSystem, version.Program+"-serving", cache)
}
