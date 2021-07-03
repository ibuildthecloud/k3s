package server

import (
	"context"
	"crypto"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	certutil "github.com/rancher/dynamiclistener/cert"
	"github.com/rancher/k3s/pkg/bootstrap"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/nodepassword"
	"github.com/rancher/k3s/pkg/version"
	coreclient "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/json"
)

const (
	staticURL = "/static/"
)

func router(ctx context.Context, config *Config) http.Handler {
	serverConfig := &config.ControlConfig
	nodeAuth := passwordBootstrap(ctx, config)

	prefix := "/v1-" + version.Program
	authed := mux.NewRouter()
	authed.Use(authMiddleware(serverConfig, version.Program+":agent"))
	authed.NotFoundHandler = serverConfig.Runtime.Handler
	authed.Path(prefix + "/serving-kubelet.crt").Handler(servingKubeletCert(serverConfig, serverConfig.Runtime.ServingKubeletKey, nodeAuth))
	authed.Path(prefix + "/client-kubelet.crt").Handler(clientKubeletCert(serverConfig, serverConfig.Runtime.ClientKubeletKey, nodeAuth))
	authed.Path(prefix + "/client-kube-proxy.crt").Handler(fileHandler(serverConfig.Runtime.ClientKubeProxyCert, serverConfig.Runtime.ClientKubeProxyKey))
	authed.Path(prefix + "/client-" + version.Program + "-controller.crt").Handler(fileHandler(serverConfig.Runtime.ClientK3sControllerCert, serverConfig.Runtime.ClientK3sControllerKey))
	authed.Path(prefix + "/client-ca.crt").Handler(fileHandler(serverConfig.Runtime.ClientCA))
	authed.Path(prefix + "/server-ca.crt").Handler(fileHandler(serverConfig.Runtime.ServerCA))
	authed.Path(prefix + "/config").Handler(configHandler(serverConfig))

	nodeAuthed := mux.NewRouter()
	nodeAuthed.Use(authMiddleware(serverConfig, "system:nodes"))
	nodeAuthed.Path(prefix + "/connect").Handler(serverConfig.Runtime.Tunnel)
	nodeAuthed.NotFoundHandler = authed

	serverAuthed := mux.NewRouter()
	serverAuthed.Use(authMiddleware(serverConfig, version.Program+":server"))
	serverAuthed.NotFoundHandler = nodeAuthed
	serverAuthed.Path("/db/info").Handler(nodeAuthed)
	if serverConfig.Runtime.HTTPBootstrap {
		serverAuthed.Path(prefix + "/server-bootstrap").Handler(bootstrap.Handler(&serverConfig.Runtime.ControlRuntimeBootstrap))
	}

	staticDir := filepath.Join(serverConfig.DataDir, "static")
	router := mux.NewRouter()
	router.NotFoundHandler = serverAuthed
	router.PathPrefix(staticURL).Handler(serveStatic(staticURL, staticDir))
	router.Path("/cacerts").Handler(cacerts(serverConfig.Runtime.ServerCA))
	router.Path("/ping").Handler(ping())

	return router
}

func cacerts(serverCA string) http.Handler {
	var ca []byte
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if ca == nil {
			var err error
			ca, err = ioutil.ReadFile(serverCA)
			if err != nil {
				sendError(err, resp)
				return
			}
		}
		resp.Header().Set("content-type", "text/plain")
		resp.Write(ca)
	})
}

func getNodeInfo(req *http.Request) (string, string, error) {
	nodeName := req.Header.Get(version.Program + "-Node-Name")
	if nodeName == "" {
		return "", "", errors.New("node name not set")
	}

	nodePassword := req.Header.Get(version.Program + "-Node-Password")
	if nodePassword == "" {
		return "", "", errors.New("node password not set")
	}

	return strings.ToLower(nodeName), nodePassword, nil
}

func getCACertAndKeys(caCertFile, caKeyFile, signingKeyFile string) ([]*x509.Certificate, crypto.Signer, crypto.Signer, error) {
	keyBytes, err := ioutil.ReadFile(signingKeyFile)
	if err != nil {
		return nil, nil, nil, err
	}

	key, err := certutil.ParsePrivateKeyPEM(keyBytes)
	if err != nil {
		return nil, nil, nil, err
	}

	caKeyBytes, err := ioutil.ReadFile(caKeyFile)
	if err != nil {
		return nil, nil, nil, err
	}

	caKey, err := certutil.ParsePrivateKeyPEM(caKeyBytes)
	if err != nil {
		return nil, nil, nil, err
	}

	caBytes, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, nil, nil, err
	}

	caCert, err := certutil.ParseCertsPEM(caBytes)
	if err != nil {
		return nil, nil, nil, err
	}

	return caCert, caKey.(crypto.Signer), key.(crypto.Signer), nil
}

func servingKubeletCert(server *config.Control, keyFile string, auth nodePassBootstrapper) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if req.TLS == nil {
			resp.WriteHeader(http.StatusNotFound)
			return
		}

		nodeName, errCode, err := auth(req)
		if err != nil {
			sendError(err, resp, errCode)
			return
		}

		caCert, caKey, key, err := getCACertAndKeys(server.Runtime.ServerCA, server.Runtime.ServerCAKey, server.Runtime.ServingKubeletKey)
		if err != nil {
			sendError(err, resp)
			return
		}

		ips := []net.IP{net.ParseIP("127.0.0.1")}

		if nodeIP := req.Header.Get(version.Program + "-Node-IP"); nodeIP != "" {
			for _, v := range strings.Split(nodeIP, ",") {
				ip := net.ParseIP(v)
				if ip == nil {
					sendError(fmt.Errorf("invalid IP address %s", ip), resp)
					return
				}
				ips = append(ips, ip)
			}
		}

		cert, err := certutil.NewSignedCert(certutil.Config{
			CommonName: nodeName,
			Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			AltNames: certutil.AltNames{
				DNSNames: []string{nodeName, "localhost"},
				IPs:      ips,
			},
		}, key, caCert[0], caKey)
		if err != nil {
			sendError(err, resp)
			return
		}

		keyBytes, err := ioutil.ReadFile(keyFile)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusInternalServerError)
			return
		}

		resp.Write(append(certutil.EncodeCertPEM(cert), certutil.EncodeCertPEM(caCert[0])...))
		resp.Write(keyBytes)
	})
}

func clientKubeletCert(server *config.Control, keyFile string, auth nodePassBootstrapper) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if req.TLS == nil {
			resp.WriteHeader(http.StatusNotFound)
			return
		}

		nodeName, errCode, err := auth(req)
		if err != nil {
			sendError(err, resp, errCode)
			return
		}

		caCert, caKey, key, err := getCACertAndKeys(server.Runtime.ClientCA, server.Runtime.ClientCAKey, server.Runtime.ClientKubeletKey)
		if err != nil {
			sendError(err, resp)
			return
		}

		cert, err := certutil.NewSignedCert(certutil.Config{
			CommonName:   "system:node:" + nodeName,
			Organization: []string{"system:nodes"},
			Usages:       []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}, key, caCert[0], caKey)
		if err != nil {
			sendError(err, resp)
			return
		}

		keyBytes, err := ioutil.ReadFile(keyFile)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusInternalServerError)
			return
		}

		resp.Write(append(certutil.EncodeCertPEM(cert), certutil.EncodeCertPEM(caCert[0])...))
		resp.Write(keyBytes)
	})
}

func fileHandler(fileName ...string) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if req.TLS == nil {
			resp.WriteHeader(http.StatusNotFound)
			return
		}
		resp.Header().Set("Content-Type", "text/plain")

		if len(fileName) == 1 {
			http.ServeFile(resp, req, fileName[0])
			return
		}

		for _, f := range fileName {
			bytes, err := ioutil.ReadFile(f)
			if err != nil {
				logrus.Errorf("Failed to read %s: %v", f, err)
				resp.WriteHeader(http.StatusInternalServerError)
				return
			}
			resp.Write(bytes)
		}
	})
}

func configHandler(server *config.Control) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if req.TLS == nil {
			resp.WriteHeader(http.StatusNotFound)
			return
		}
		resp.Header().Set("content-type", "application/json")
		json.NewEncoder(resp).Encode(server)
	})
}

func ping() http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		data := []byte("pong")
		resp.Header().Set("Content-Type", "text/plain")
		resp.Header().Set("Content-Length", strconv.Itoa(len(data)))
		resp.Write(data)
	})
}

func serveStatic(urlPrefix, staticDir string) http.Handler {
	return http.StripPrefix(urlPrefix, http.FileServer(http.Dir(staticDir)))
}

func sendError(err error, resp http.ResponseWriter, status ...int) {
	var code int
	if len(status) == 1 {
		code = status[0]
	}
	if code == 0 || code == http.StatusOK {
		code = http.StatusInternalServerError
	}
	logrus.Error(err)
	resp.WriteHeader(code)
	resp.Write([]byte(err.Error()))
}

// nodePassBootstrapper returns a node name, or http error code and error
type nodePassBootstrapper func(req *http.Request) (string, int, error)

func passwordBootstrap(ctx context.Context, config *Config) nodePassBootstrapper {
	runtime := config.ControlConfig.Runtime
	var secretClient coreclient.SecretClient
	var once sync.Once

	return nodePassBootstrapper(func(req *http.Request) (string, int, error) {
		nodeName, nodePassword, err := getNodeInfo(req)
		if err != nil {
			return "", http.StatusBadRequest, err
		}

		if secretClient == nil {
			if runtime.Core != nil {
				// initialize the client if we can
				secretClient = runtime.Core.Core().V1().Secret()
			} else if nodeName == os.Getenv("NODE_NAME") {
				// or verify the password locally and ensure a secret later
				return verifyLocalPassword(ctx, config, &once, nodeName, nodePassword)
			} else {
				// or reject the request until the core is ready
				return "", http.StatusServiceUnavailable, errors.New("runtime core not ready")
			}
		}

		if err := nodepassword.Ensure(secretClient, nodeName, nodePassword); err != nil {
			return "", http.StatusForbidden, err
		}

		return nodeName, http.StatusOK, nil
	})
}

func verifyLocalPassword(ctx context.Context, config *Config, once *sync.Once, nodeName, nodePassword string) (string, int, error) {
	// use same password file location that the agent creates
	nodePasswordRoot := "/"
	if config.Rootless {
		nodePasswordRoot = filepath.Join(config.ControlConfig.DataDir, "agent")
	}
	nodeConfigPath := filepath.Join(nodePasswordRoot, "etc", "rancher", "node")
	nodePasswordFile := filepath.Join(nodeConfigPath, "password")

	passBytes, err := ioutil.ReadFile(nodePasswordFile)
	if err != nil {
		return "", http.StatusInternalServerError, errors.Wrap(err, "unable to read node password file")
	}

	password := strings.TrimSpace(string(passBytes))
	if password != nodePassword {
		return "", http.StatusForbidden, errors.Wrapf(err, "unable to verify local password for node '%s'", nodeName)
	}

	// make sure the secret is created when the api server is ready
	ensureSecret := func() {
		runtime := config.ControlConfig.Runtime
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				if runtime.Core != nil {
					logrus.Debugf("runtime core has become available, ensuring password secret for node '%s'", nodeName)
					secretClient := runtime.Core.Core().V1().Secret()
					if err := nodepassword.Ensure(secretClient, nodeName, nodePassword); err != nil {
						logrus.Warnf("error ensuring node password secret for pre-validated node '%s': %v", nodeName, err)
					}
					return
				}
			}
		}
	}

	go once.Do(ensureSecret)

	logrus.Debugf("password verified locally for node '%s'", nodeName)

	return nodeName, http.StatusOK, nil
}
