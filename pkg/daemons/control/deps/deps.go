package deps

import (
	"crypto"
	cryptorand "crypto/rand"
	"crypto/x509"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	certutil "github.com/rancher/dynamiclistener/cert"
	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/passwd"
	"github.com/rancher/k3s/pkg/token"
	"github.com/rancher/k3s/pkg/version"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	apiserverconfigv1 "k8s.io/apiserver/pkg/apis/config/v1"
	"k8s.io/kubernetes/pkg/controlplane"
)

const (
	ipsecTokenSize = 48
	aescbcKeySize  = 32

	RequestHeaderCN = "system:auth-proxy"
)

var (
	kubeconfigTemplate = template.Must(template.New("kubeconfig").Parse(`apiVersion: v1
clusters:
- cluster:
    server: {{.URL}}
    certificate-authority: {{.CACert}}
  name: local
contexts:
- context:
    cluster: local
    namespace: default
    user: user
  name: Default
current-context: Default
kind: Config
preferences: {}
users:
- name: user
  user:
    client-certificate: {{.ClientCert}}
    client-key: {{.ClientKey}}
`))
)

func migratePassword(p *passwd.Passwd) error {
	server, _ := p.Pass("server")
	node, _ := p.Pass("node")
	if server == "" && node != "" {
		return p.EnsureUser("server", version.Program+":server", node)
	}
	return nil
}

func KubeConfig(dest, url, caCert, clientCert, clientKey string) error {
	data := struct {
		URL        string
		CACert     string
		ClientCert string
		ClientKey  string
	}{
		URL:        url,
		CACert:     caCert,
		ClientCert: clientCert,
		ClientKey:  clientKey,
	}

	output, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer output.Close()

	return kubeconfigTemplate.Execute(output, &data)
}

// FillRuntimeCerts is responsible for filling out all the
// .crt and .key filenames for a ControlRuntime.
func FillRuntimeCerts(config *config.Control, runtime *config.ControlRuntime) {
	runtime.ClientCA = filepath.Join(config.DataDir, "tls", "client-ca.crt")
	runtime.ClientCAKey = filepath.Join(config.DataDir, "tls", "client-ca.key")
	runtime.ServerCA = filepath.Join(config.DataDir, "tls", "server-ca.crt")
	runtime.ServerCAKey = filepath.Join(config.DataDir, "tls", "server-ca.key")
	runtime.RequestHeaderCA = filepath.Join(config.DataDir, "tls", "request-header-ca.crt")
	runtime.RequestHeaderCAKey = filepath.Join(config.DataDir, "tls", "request-header-ca.key")
	runtime.IPSECKey = filepath.Join(config.DataDir, "cred", "ipsec.psk")

	runtime.ServiceKey = filepath.Join(config.DataDir, "tls", "service.key")
	runtime.PasswdFile = filepath.Join(config.DataDir, "cred", "passwd")
	runtime.NodePasswdFile = filepath.Join(config.DataDir, "cred", "node-passwd")

	runtime.KubeConfigAdmin = filepath.Join(config.DataDir, "cred", "admin.kubeconfig")
	runtime.KubeConfigController = filepath.Join(config.DataDir, "cred", "controller.kubeconfig")
	runtime.KubeConfigScheduler = filepath.Join(config.DataDir, "cred", "scheduler.kubeconfig")
	runtime.KubeConfigAPIServer = filepath.Join(config.DataDir, "cred", "api-server.kubeconfig")
	runtime.KubeConfigCloudController = filepath.Join(config.DataDir, "cred", "cloud-controller.kubeconfig")

	runtime.ClientAdminCert = filepath.Join(config.DataDir, "tls", "client-admin.crt")
	runtime.ClientAdminKey = filepath.Join(config.DataDir, "tls", "client-admin.key")
	runtime.ClientControllerCert = filepath.Join(config.DataDir, "tls", "client-controller.crt")
	runtime.ClientControllerKey = filepath.Join(config.DataDir, "tls", "client-controller.key")
	runtime.ClientCloudControllerCert = filepath.Join(config.DataDir, "tls", "client-"+version.Program+"-cloud-controller.crt")
	runtime.ClientCloudControllerKey = filepath.Join(config.DataDir, "tls", "client-"+version.Program+"-cloud-controller.key")
	runtime.ClientSchedulerCert = filepath.Join(config.DataDir, "tls", "client-scheduler.crt")
	runtime.ClientSchedulerKey = filepath.Join(config.DataDir, "tls", "client-scheduler.key")
	runtime.ClientKubeAPICert = filepath.Join(config.DataDir, "tls", "client-kube-apiserver.crt")
	runtime.ClientKubeAPIKey = filepath.Join(config.DataDir, "tls", "client-kube-apiserver.key")
	runtime.ClientKubeProxyCert = filepath.Join(config.DataDir, "tls", "client-kube-proxy.crt")
	runtime.ClientKubeProxyKey = filepath.Join(config.DataDir, "tls", "client-kube-proxy.key")
	runtime.ClientK3sControllerCert = filepath.Join(config.DataDir, "tls", "client-"+version.Program+"-controller.crt")
	runtime.ClientK3sControllerKey = filepath.Join(config.DataDir, "tls", "client-"+version.Program+"-controller.key")

	runtime.ServingKubeAPICert = filepath.Join(config.DataDir, "tls", "serving-kube-apiserver.crt")
	runtime.ServingKubeAPIKey = filepath.Join(config.DataDir, "tls", "serving-kube-apiserver.key")

	runtime.ClientKubeletKey = filepath.Join(config.DataDir, "tls", "client-kubelet.key")
	runtime.ServingKubeletKey = filepath.Join(config.DataDir, "tls", "serving-kubelet.key")

	runtime.ClientAuthProxyCert = filepath.Join(config.DataDir, "tls", "client-auth-proxy.crt")
	runtime.ClientAuthProxyKey = filepath.Join(config.DataDir, "tls", "client-auth-proxy.key")

	runtime.ETCDServerCA = filepath.Join(config.DataDir, "tls", "etcd", "server-ca.crt")
	runtime.ETCDServerCAKey = filepath.Join(config.DataDir, "tls", "etcd", "server-ca.key")
	runtime.ETCDPeerCA = filepath.Join(config.DataDir, "tls", "etcd", "peer-ca.crt")
	runtime.ETCDPeerCAKey = filepath.Join(config.DataDir, "tls", "etcd", "peer-ca.key")
	runtime.ServerETCDCert = filepath.Join(config.DataDir, "tls", "etcd", "server-client.crt")
	runtime.ServerETCDKey = filepath.Join(config.DataDir, "tls", "etcd", "server-client.key")
	runtime.PeerServerClientETCDCert = filepath.Join(config.DataDir, "tls", "etcd", "peer-server-client.crt")
	runtime.PeerServerClientETCDKey = filepath.Join(config.DataDir, "tls", "etcd", "peer-server-client.key")
	runtime.ClientETCDCert = filepath.Join(config.DataDir, "tls", "etcd", "client.crt")
	runtime.ClientETCDKey = filepath.Join(config.DataDir, "tls", "etcd", "client.key")

	if config.EncryptSecrets {
		runtime.EncryptionConfig = filepath.Join(config.DataDir, "cred", "encryption-config.json")
	}
}

// GenServerDeps is responsible for generating the cluster dependencies
// needed to successfully bootstrap a cluster.
func GenServerDeps(config *config.Control, runtime *config.ControlRuntime) error {
	if err := genCerts(config, runtime); err != nil {
		return err
	}

	if err := genServiceAccount(runtime); err != nil {
		return err
	}

	if err := genUsers(config, runtime); err != nil {
		return err
	}

	if err := genEncryptedNetworkInfo(config, runtime); err != nil {
		return err
	}

	if err := genEncryptionConfig(config, runtime); err != nil {
		return err
	}

	return readTokens(runtime)
}

func readTokens(runtime *config.ControlRuntime) error {
	tokens, err := passwd.Read(runtime.PasswdFile)
	if err != nil {
		return err
	}

	if nodeToken, ok := tokens.Pass("node"); ok {
		runtime.AgentToken = "node:" + nodeToken
	}
	if serverToken, ok := tokens.Pass("server"); ok {
		runtime.ServerToken = "server:" + serverToken
	}

	return nil
}

func getNodePass(config *config.Control, serverPass string) string {
	if config.AgentToken == "" {
		if _, passwd, ok := clientaccess.ParseUsernamePassword(serverPass); ok {
			return passwd
		}
		return serverPass
	}
	return config.AgentToken
}

func genUsers(config *config.Control, runtime *config.ControlRuntime) error {
	passwd, err := passwd.Read(runtime.PasswdFile)
	if err != nil {
		return err
	}

	if err := migratePassword(passwd); err != nil {
		return err
	}

	serverPass, err := getServerPass(passwd, config)
	if err != nil {
		return err
	}

	nodePass := getNodePass(config, serverPass)

	if err := passwd.EnsureUser("node", version.Program+":agent", nodePass); err != nil {
		return err
	}

	if err := passwd.EnsureUser("server", version.Program+":server", serverPass); err != nil {
		return err
	}

	return passwd.Write(runtime.PasswdFile)
}

func genEncryptedNetworkInfo(controlConfig *config.Control, runtime *config.ControlRuntime) error {
	if s, err := os.Stat(runtime.IPSECKey); err == nil && s.Size() > 0 {
		psk, err := ioutil.ReadFile(runtime.IPSECKey)
		if err != nil {
			return err
		}
		controlConfig.IPSECPSK = strings.TrimSpace(string(psk))
		return nil
	}

	psk, err := token.Random(ipsecTokenSize)
	if err != nil {
		return err
	}

	controlConfig.IPSECPSK = psk
	return ioutil.WriteFile(runtime.IPSECKey, []byte(psk+"\n"), 0600)
}

func getServerPass(passwd *passwd.Passwd, config *config.Control) (string, error) {
	var (
		err error
	)

	serverPass := config.Token
	if serverPass == "" {
		serverPass, _ = passwd.Pass("server")
	}
	if serverPass == "" {
		serverPass, err = token.Random(16)
		if err != nil {
			return "", err
		}
	}

	return serverPass, nil
}

func genCerts(config *config.Control, runtime *config.ControlRuntime) error {
	if err := genClientCerts(config, runtime); err != nil {
		return err
	}
	if err := genServerCerts(config, runtime); err != nil {
		return err
	}
	if err := genRequestHeaderCerts(config, runtime); err != nil {
		return err
	}
	return genETCDCerts(config, runtime)
}

func getSigningCertFactory(regen bool, altNames *certutil.AltNames, extKeyUsage []x509.ExtKeyUsage, caCertFile, caKeyFile string) signedCertFactory {
	return func(commonName string, organization []string, certFile, keyFile string) (bool, error) {
		return createClientCertKey(regen, commonName, organization, altNames, extKeyUsage, caCertFile, caKeyFile, certFile, keyFile)
	}
}

func genClientCerts(config *config.Control, runtime *config.ControlRuntime) error {
	regen, err := createSigningCertKey(version.Program+"-client", runtime.ClientCA, runtime.ClientCAKey)
	if err != nil {
		return err
	}

	factory := getSigningCertFactory(regen, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, runtime.ClientCA, runtime.ClientCAKey)

	var certGen bool
	apiEndpoint := fmt.Sprintf("https://127.0.0.1:%d", config.APIServerPort)

	certGen, err = factory("system:admin", []string{"system:masters"}, runtime.ClientAdminCert, runtime.ClientAdminKey)
	if err != nil {
		return err
	}
	if certGen {
		if err := KubeConfig(runtime.KubeConfigAdmin, apiEndpoint, runtime.ServerCA, runtime.ClientAdminCert, runtime.ClientAdminKey); err != nil {
			return err
		}
	}

	certGen, err = factory("system:kube-controller-manager", nil, runtime.ClientControllerCert, runtime.ClientControllerKey)
	if err != nil {
		return err
	}
	if certGen {
		if err := KubeConfig(runtime.KubeConfigController, apiEndpoint, runtime.ServerCA, runtime.ClientControllerCert, runtime.ClientControllerKey); err != nil {
			return err
		}
	}

	certGen, err = factory("system:kube-scheduler", nil, runtime.ClientSchedulerCert, runtime.ClientSchedulerKey)
	if err != nil {
		return err
	}
	if certGen {
		if err := KubeConfig(runtime.KubeConfigScheduler, apiEndpoint, runtime.ServerCA, runtime.ClientSchedulerCert, runtime.ClientSchedulerKey); err != nil {
			return err
		}
	}

	certGen, err = factory("kube-apiserver", nil, runtime.ClientKubeAPICert, runtime.ClientKubeAPIKey)
	if err != nil {
		return err
	}
	if certGen {
		if err := KubeConfig(runtime.KubeConfigAPIServer, apiEndpoint, runtime.ServerCA, runtime.ClientKubeAPICert, runtime.ClientKubeAPIKey); err != nil {
			return err
		}
	}

	if _, err = factory("system:kube-proxy", nil, runtime.ClientKubeProxyCert, runtime.ClientKubeProxyKey); err != nil {
		return err
	}
	// This user (system:k3s-controller by default) must be bound to a role in rolebindings.yaml or the downstream equivalent
	if _, err = factory("system:"+version.Program+"-controller", nil, runtime.ClientK3sControllerCert, runtime.ClientK3sControllerKey); err != nil {
		return err
	}

	if _, _, err := certutil.LoadOrGenerateKeyFile(runtime.ClientKubeletKey, regen); err != nil {
		return err
	}

	certGen, err = factory(version.Program+"-cloud-controller-manager", nil, runtime.ClientCloudControllerCert, runtime.ClientCloudControllerKey)
	if err != nil {
		return err
	}
	if certGen {
		if err := KubeConfig(runtime.KubeConfigCloudController, apiEndpoint, runtime.ServerCA, runtime.ClientCloudControllerCert, runtime.ClientCloudControllerKey); err != nil {
			return err
		}
	}

	return nil
}

func genServerCerts(config *config.Control, runtime *config.ControlRuntime) error {
	regen, err := createServerSigningCertKey(config, runtime)
	if err != nil {
		return err
	}

	_, apiServerServiceIP, err := controlplane.ServiceIPRange(*config.ServiceIPRange)
	if err != nil {
		return err
	}

	altNames := &certutil.AltNames{
		DNSNames: []string{"localhost", "kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc." + config.ClusterDomain},
		IPs:      []net.IP{apiServerServiceIP},
	}

	addSANs(altNames, config.SANs)

	if _, err := createClientCertKey(regen, "kube-apiserver", nil,
		altNames, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		runtime.ServerCA, runtime.ServerCAKey,
		runtime.ServingKubeAPICert, runtime.ServingKubeAPIKey); err != nil {
		return err
	}

	if _, _, err := certutil.LoadOrGenerateKeyFile(runtime.ServingKubeletKey, regen); err != nil {
		return err
	}

	return nil
}

func genETCDCerts(config *config.Control, runtime *config.ControlRuntime) error {
	regen, err := createSigningCertKey("etcd-server", runtime.ETCDServerCA, runtime.ETCDServerCAKey)
	if err != nil {
		return err
	}

	altNames := &certutil.AltNames{
		DNSNames: []string{"localhost"},
	}
	addSANs(altNames, config.SANs)

	if _, err := createClientCertKey(regen, "etcd-server", nil,
		altNames, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		runtime.ETCDServerCA, runtime.ETCDServerCAKey,
		runtime.ServerETCDCert, runtime.ServerETCDKey); err != nil {
		return err
	}

	if _, err := createClientCertKey(regen, "etcd-client", nil,
		nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		runtime.ETCDServerCA, runtime.ETCDServerCAKey,
		runtime.ClientETCDCert, runtime.ClientETCDKey); err != nil {
		return err
	}

	regen, err = createSigningCertKey("etcd-peer", runtime.ETCDPeerCA, runtime.ETCDPeerCAKey)
	if err != nil {
		return err
	}

	if _, err := createClientCertKey(regen, "etcd-peer", nil,
		altNames, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		runtime.ETCDPeerCA, runtime.ETCDPeerCAKey,
		runtime.PeerServerClientETCDCert, runtime.PeerServerClientETCDKey); err != nil {
		return err
	}

	return nil
}

func genRequestHeaderCerts(config *config.Control, runtime *config.ControlRuntime) error {
	regen, err := createSigningCertKey(version.Program+"-request-header", runtime.RequestHeaderCA, runtime.RequestHeaderCAKey)
	if err != nil {
		return err
	}

	if _, err := createClientCertKey(regen, RequestHeaderCN, nil,
		nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		runtime.RequestHeaderCA, runtime.RequestHeaderCAKey,
		runtime.ClientAuthProxyCert, runtime.ClientAuthProxyKey); err != nil {
		return err
	}

	return nil
}

type signedCertFactory = func(commonName string, organization []string, certFile, keyFile string) (bool, error)

func createServerSigningCertKey(config *config.Control, runtime *config.ControlRuntime) (bool, error) {
	TokenCA := filepath.Join(config.DataDir, "tls", "token-ca.crt")
	TokenCAKey := filepath.Join(config.DataDir, "tls", "token-ca.key")

	if exists(TokenCA, TokenCAKey) && !exists(runtime.ServerCA) && !exists(runtime.ServerCAKey) {
		logrus.Infof("Upgrading token-ca files to server-ca")
		if err := os.Link(TokenCA, runtime.ServerCA); err != nil {
			return false, err
		}
		if err := os.Link(TokenCAKey, runtime.ServerCAKey); err != nil {
			return false, err
		}
		return true, nil
	}
	return createSigningCertKey(version.Program+"-server", runtime.ServerCA, runtime.ServerCAKey)
}

func addSANs(altNames *certutil.AltNames, sans []string) {
	for _, san := range sans {
		ip := net.ParseIP(san)
		if ip == nil {
			altNames.DNSNames = append(altNames.DNSNames, san)
		} else {
			altNames.IPs = append(altNames.IPs, ip)
		}
	}
}

func sansChanged(certFile string, sans *certutil.AltNames) bool {
	if sans == nil {
		return false
	}

	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		return false
	}

	certificates, err := certutil.ParseCertsPEM(certBytes)
	if err != nil {
		return false
	}

	if len(certificates) == 0 {
		return false
	}

	if !sets.NewString(certificates[0].DNSNames...).HasAll(sans.DNSNames...) {
		return true
	}

	ips := sets.NewString()
	for _, ip := range certificates[0].IPAddresses {
		ips.Insert(ip.String())
	}

	for _, ip := range sans.IPs {
		if !ips.Has(ip.String()) {
			return true
		}
	}

	return false
}

func createClientCertKey(regen bool, commonName string, organization []string, altNames *certutil.AltNames, extKeyUsage []x509.ExtKeyUsage, caCertFile, caKeyFile, certFile, keyFile string) (bool, error) {
	caBytes, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return false, err
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caBytes)

	// check for certificate expiration
	if !regen {
		regen = expired(certFile, pool)
	}

	if !regen {
		regen = sansChanged(certFile, altNames)
	}

	if !regen {
		if exists(certFile, keyFile) {
			return false, nil
		}
	}

	caKeyBytes, err := ioutil.ReadFile(caKeyFile)
	if err != nil {
		return false, err
	}

	caKey, err := certutil.ParsePrivateKeyPEM(caKeyBytes)
	if err != nil {
		return false, err
	}

	caCert, err := certutil.ParseCertsPEM(caBytes)
	if err != nil {
		return false, err
	}

	keyBytes, _, err := certutil.LoadOrGenerateKeyFile(keyFile, regen)
	if err != nil {
		return false, err
	}

	key, err := certutil.ParsePrivateKeyPEM(keyBytes)
	if err != nil {
		return false, err
	}

	cfg := certutil.Config{
		CommonName:   commonName,
		Organization: organization,
		Usages:       extKeyUsage,
	}
	if altNames != nil {
		cfg.AltNames = *altNames
	}
	cert, err := certutil.NewSignedCert(cfg, key.(crypto.Signer), caCert[0], caKey.(crypto.Signer))
	if err != nil {
		return false, err
	}

	return true, certutil.WriteCert(certFile, append(certutil.EncodeCertPEM(cert), certutil.EncodeCertPEM(caCert[0])...))
}

func exists(files ...string) bool {
	for _, file := range files {
		if _, err := os.Stat(file); err != nil {
			return false
		}
	}
	return true
}

func genServiceAccount(runtime *config.ControlRuntime) error {
	_, keyErr := os.Stat(runtime.ServiceKey)
	if keyErr == nil {
		return nil
	}

	key, err := certutil.NewPrivateKey()
	if err != nil {
		return err
	}

	return certutil.WriteKey(runtime.ServiceKey, certutil.EncodePrivateKeyPEM(key))
}

func createSigningCertKey(prefix, certFile, keyFile string) (bool, error) {
	if exists(certFile, keyFile) {
		return false, nil
	}

	caKeyBytes, _, err := certutil.LoadOrGenerateKeyFile(keyFile, false)
	if err != nil {
		return false, err
	}

	caKey, err := certutil.ParsePrivateKeyPEM(caKeyBytes)
	if err != nil {
		return false, err
	}

	cfg := certutil.Config{
		CommonName: fmt.Sprintf("%s-ca@%d", prefix, time.Now().Unix()),
	}

	cert, err := certutil.NewSelfSignedCACert(cfg, caKey.(crypto.Signer))
	if err != nil {
		return false, err
	}

	if err := certutil.WriteCert(certFile, certutil.EncodeCertPEM(cert)); err != nil {
		return false, err
	}
	return true, nil
}

func expired(certFile string, pool *x509.CertPool) bool {
	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		return false
	}
	certificates, err := certutil.ParseCertsPEM(certBytes)
	if err != nil {
		return false
	}
	_, err = certificates[0].Verify(x509.VerifyOptions{
		Roots: pool,
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageAny,
		},
	})
	if err != nil {
		return true
	}
	return certutil.IsCertExpired(certificates[0], config.CertificateRenewDays)
}

func genEncryptionConfig(controlConfig *config.Control, runtime *config.ControlRuntime) error {
	if !controlConfig.EncryptSecrets {
		return nil
	}
	if s, err := os.Stat(runtime.EncryptionConfig); err == nil && s.Size() > 0 {
		return nil
	}

	aescbcKey := make([]byte, aescbcKeySize, aescbcKeySize)
	_, err := cryptorand.Read(aescbcKey)
	if err != nil {
		return err
	}
	encodedKey := b64.StdEncoding.EncodeToString(aescbcKey)

	encConfig := apiserverconfigv1.EncryptionConfiguration{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EncryptionConfiguration",
			APIVersion: "apiserver.config.k8s.io/v1",
		},
		Resources: []apiserverconfigv1.ResourceConfiguration{
			{
				Resources: []string{"secrets"},
				Providers: []apiserverconfigv1.ProviderConfiguration{
					{
						AESCBC: &apiserverconfigv1.AESConfiguration{
							Keys: []apiserverconfigv1.Key{
								{
									Name:   "aescbckey",
									Secret: encodedKey,
								},
							},
						},
					},
					{
						Identity: &apiserverconfigv1.IdentityConfiguration{},
					},
				},
			},
		},
	}
	jsonfile, err := json.Marshal(encConfig)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(runtime.EncryptionConfig, jsonfile, 0600)
}
