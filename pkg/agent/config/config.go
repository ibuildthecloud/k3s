package config

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	sysnet "net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/agent/proxy"
	"github.com/rancher/k3s/pkg/cli/cmds"
	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/rancher/k3s/pkg/containerd"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/daemons/control/deps"
	"github.com/rancher/k3s/pkg/util"
	"github.com/rancher/k3s/pkg/version"
	"github.com/rancher/wrangler/pkg/slice"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/net"
)

const (
	DefaultPodManifestPath = "pod-manifests"
)

func Get(ctx context.Context, agent cmds.Agent, proxy proxy.Proxy) *config.Node {
	for {
		agentConfig, err := get(ctx, &agent, proxy)
		if err != nil {
			logrus.Errorf("Failed to configure agent: %v", err)
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				logrus.Fatalf("Interrupted")
			}
		}
		return agentConfig
	}
}

type HTTPRequester func(u string, client *http.Client, username, password string) ([]byte, error)

func Request(path string, info *clientaccess.Info, requester HTTPRequester) ([]byte, error) {
	u, err := url.Parse(info.BaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = path
	return requester(u.String(), clientaccess.GetHTTPClient(info.CACerts), info.Username, info.Password)
}

func getNodeNamedCrt(nodeName string, nodeIPs []sysnet.IP, nodePasswordFile string) HTTPRequester {
	return func(u string, client *http.Client, username, password string) ([]byte, error) {
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}

		if username != "" {
			req.SetBasicAuth(username, password)
		}

		req.Header.Set(version.Program+"-Node-Name", nodeName)
		nodePassword, err := ensureNodePassword(nodePasswordFile)
		if err != nil {
			return nil, err
		}
		req.Header.Set(version.Program+"-Node-Password", nodePassword)
		req.Header.Set(version.Program+"-Node-IP", util.JoinIPs(nodeIPs))

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("Node password rejected, duplicate hostname or contents of '%s' may not match server node-passwd entry, try enabling a unique node name with the --with-node-id flag", nodePasswordFile)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s: %s", u, resp.Status)
		}

		return ioutil.ReadAll(resp.Body)
	}
}

func ensureNodeID(nodeIDFile string) (string, error) {
	if _, err := os.Stat(nodeIDFile); err == nil {
		id, err := ioutil.ReadFile(nodeIDFile)
		return strings.TrimSpace(string(id)), err
	}
	id := make([]byte, 4, 4)
	_, err := cryptorand.Read(id)
	if err != nil {
		return "", err
	}
	nodeID := hex.EncodeToString(id)
	return nodeID, ioutil.WriteFile(nodeIDFile, []byte(nodeID+"\n"), 0644)
}

func ensureNodePassword(nodePasswordFile string) (string, error) {
	if _, err := os.Stat(nodePasswordFile); err == nil {
		password, err := ioutil.ReadFile(nodePasswordFile)
		return strings.TrimSpace(string(password)), err
	}
	password := make([]byte, 16, 16)
	_, err := cryptorand.Read(password)
	if err != nil {
		return "", err
	}
	nodePassword := hex.EncodeToString(password)
	return nodePassword, ioutil.WriteFile(nodePasswordFile, []byte(nodePassword+"\n"), 0600)
}

func upgradeOldNodePasswordPath(oldNodePasswordFile, newNodePasswordFile string) {
	password, err := ioutil.ReadFile(oldNodePasswordFile)
	if err != nil {
		return
	}
	if err := ioutil.WriteFile(newNodePasswordFile, password, 0600); err != nil {
		logrus.Warnf("Unable to write password file: %v", err)
		return
	}
	if err := os.Remove(oldNodePasswordFile); err != nil {
		logrus.Warnf("Unable to remove old password file: %v", err)
		return
	}
}

func getServingCert(nodeName string, nodeIPs []sysnet.IP, servingCertFile, servingKeyFile, nodePasswordFile string, info *clientaccess.Info) (*tls.Certificate, error) {
	servingCert, err := Request("/v1-"+version.Program+"/serving-kubelet.crt", info, getNodeNamedCrt(nodeName, nodeIPs, nodePasswordFile))
	if err != nil {
		return nil, err
	}

	servingCert, servingKey := splitCertKeyPEM(servingCert)

	if err := ioutil.WriteFile(servingCertFile, servingCert, 0600); err != nil {
		return nil, errors.Wrapf(err, "failed to write node cert")
	}

	if err := ioutil.WriteFile(servingKeyFile, servingKey, 0600); err != nil {
		return nil, errors.Wrapf(err, "failed to write node key")
	}

	cert, err := tls.X509KeyPair(servingCert, servingKey)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}

func getHostFile(filename, keyFile string, info *clientaccess.Info) error {
	basename := filepath.Base(filename)
	fileBytes, err := info.Get("/v1-" + version.Program + "/" + basename)
	if err != nil {
		return err
	}
	if keyFile == "" {
		if err := ioutil.WriteFile(filename, fileBytes, 0600); err != nil {
			return errors.Wrapf(err, "failed to write cert %s", filename)
		}
	} else {
		fileBytes, keyBytes := splitCertKeyPEM(fileBytes)
		if err := ioutil.WriteFile(filename, fileBytes, 0600); err != nil {
			return errors.Wrapf(err, "failed to write cert %s", filename)
		}
		if err := ioutil.WriteFile(keyFile, keyBytes, 0600); err != nil {
			return errors.Wrapf(err, "failed to write key %s", filename)
		}
	}
	return nil
}

func splitCertKeyPEM(bytes []byte) (certPem []byte, keyPem []byte) {
	for {
		b, rest := pem.Decode(bytes)
		if b == nil {
			break
		}
		bytes = rest

		if strings.Contains(b.Type, "PRIVATE KEY") {
			keyPem = append(keyPem, pem.EncodeToMemory(b)...)
		} else {
			certPem = append(certPem, pem.EncodeToMemory(b)...)
		}
	}

	return
}

func getNodeNamedHostFile(filename, keyFile, nodeName string, nodeIPs []sysnet.IP, nodePasswordFile string, info *clientaccess.Info) error {
	basename := filepath.Base(filename)
	fileBytes, err := Request("/v1-"+version.Program+"/"+basename, info, getNodeNamedCrt(nodeName, nodeIPs, nodePasswordFile))
	if err != nil {
		return err
	}
	fileBytes, keyBytes := splitCertKeyPEM(fileBytes)

	if err := ioutil.WriteFile(filename, fileBytes, 0600); err != nil {
		return errors.Wrapf(err, "failed to write cert %s", filename)
	}
	if err := ioutil.WriteFile(keyFile, keyBytes, 0600); err != nil {
		return errors.Wrapf(err, "failed to write key %s", filename)
	}
	return nil
}

func getHostnameAndIPs(info cmds.Agent) (string, []sysnet.IP, error) {
	ips := []sysnet.IP{}
	if len(info.NodeIP) == 0 {
		hostIP, err := net.ChooseHostInterface()
		if err != nil {
			return "", nil, err
		}
		ips = append(ips, hostIP)
	} else {
		for _, hostIP := range info.NodeIP {
			for _, v := range strings.Split(hostIP, ",") {
				ip := sysnet.ParseIP(v)
				if ip == nil {
					return "", nil, fmt.Errorf("invalid node-ip %s", v)
				}
				ips = append(ips, ip)
			}
		}
	}

	name := info.NodeName
	if name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return "", nil, err
		}
		name = hostname
	}

	// Use lower case hostname to comply with kubernetes constraint:
	// https://github.com/kubernetes/kubernetes/issues/71140
	name = strings.ToLower(name)

	return name, ips, nil
}

func isValidResolvConf(resolvConfFile string) bool {
	file, err := os.Open(resolvConfFile)
	if err != nil {
		return false
	}
	defer file.Close()

	nameserver := regexp.MustCompile(`^nameserver\s+([^\s]*)`)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ipMatch := nameserver.FindStringSubmatch(scanner.Text())
		if len(ipMatch) == 2 {
			ip := sysnet.ParseIP(ipMatch[1])
			if ip == nil || !ip.IsGlobalUnicast() {
				return false
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false
	}
	return true
}

func locateOrGenerateResolvConf(envInfo *cmds.Agent) string {
	if envInfo.ResolvConf != "" {
		return envInfo.ResolvConf
	}
	resolvConfs := []string{"/etc/resolv.conf", "/run/systemd/resolve/resolv.conf"}
	for _, conf := range resolvConfs {
		if isValidResolvConf(conf) {
			return conf
		}
	}

	tmpConf := filepath.Join(os.TempDir(), version.Program+"-resolv.conf")
	if err := ioutil.WriteFile(tmpConf, []byte("nameserver 8.8.8.8\n"), 0444); err != nil {
		logrus.Errorf("Failed to write %s: %v", tmpConf, err)
		return ""
	}
	return tmpConf
}

func get(ctx context.Context, envInfo *cmds.Agent, proxy proxy.Proxy) (*config.Node, error) {
	if envInfo.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	info, err := clientaccess.ParseAndValidateToken(proxy.SupervisorURL(), envInfo.Token)
	if err != nil {
		return nil, err
	}

	controlConfig, err := getConfig(info)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve configuration from server")
	}

	// If the supervisor and externally-facing apiserver are not on the same port, tell the proxy where to find the apiserver.
	if controlConfig.SupervisorPort != controlConfig.HTTPSPort {
		if err := proxy.SetAPIServerPort(ctx, controlConfig.HTTPSPort); err != nil {
			return nil, errors.Wrapf(err, "failed to setup access to API Server port %d on at %s", controlConfig.HTTPSPort, proxy.SupervisorURL())
		}
	}

	var flannelIface *sysnet.Interface
	if !envInfo.NoFlannel && len(envInfo.FlannelIface) > 0 {
		flannelIface, err = sysnet.InterfaceByName(envInfo.FlannelIface)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to find interface")
		}
	}

	clientCAFile := filepath.Join(envInfo.DataDir, "agent", "client-ca.crt")
	if err := getHostFile(clientCAFile, "", info); err != nil {
		return nil, err
	}

	serverCAFile := filepath.Join(envInfo.DataDir, "agent", "server-ca.crt")
	if err := getHostFile(serverCAFile, "", info); err != nil {
		return nil, err
	}

	servingKubeletCert := filepath.Join(envInfo.DataDir, "agent", "serving-kubelet.crt")
	servingKubeletKey := filepath.Join(envInfo.DataDir, "agent", "serving-kubelet.key")

	nodePasswordRoot := "/"
	if envInfo.Rootless {
		nodePasswordRoot = filepath.Join(envInfo.DataDir, "agent")
	}
	nodeConfigPath := filepath.Join(nodePasswordRoot, "etc", "rancher", "node")
	if err := os.MkdirAll(nodeConfigPath, 0755); err != nil {
		return nil, err
	}

	oldNodePasswordFile := filepath.Join(envInfo.DataDir, "agent", "node-password.txt")
	newNodePasswordFile := filepath.Join(nodeConfigPath, "password")
	upgradeOldNodePasswordPath(oldNodePasswordFile, newNodePasswordFile)

	nodeName, nodeIPs, err := getHostnameAndIPs(*envInfo)
	if err != nil {
		return nil, err
	}

	if envInfo.WithNodeID {
		nodeID, err := ensureNodeID(filepath.Join(nodeConfigPath, "id"))
		if err != nil {
			return nil, err
		}
		nodeName += "-" + nodeID
	}

	os.Setenv("NODE_NAME", nodeName)

	servingCert, err := getServingCert(nodeName, nodeIPs, servingKubeletCert, servingKubeletKey, newNodePasswordFile, info)
	if err != nil {
		return nil, err
	}

	clientKubeletCert := filepath.Join(envInfo.DataDir, "agent", "client-kubelet.crt")
	clientKubeletKey := filepath.Join(envInfo.DataDir, "agent", "client-kubelet.key")
	if err := getNodeNamedHostFile(clientKubeletCert, clientKubeletKey, nodeName, nodeIPs, newNodePasswordFile, info); err != nil {
		return nil, err
	}

	kubeconfigKubelet := filepath.Join(envInfo.DataDir, "agent", "kubelet.kubeconfig")
	if err := deps.KubeConfig(kubeconfigKubelet, proxy.APIServerURL(), serverCAFile, clientKubeletCert, clientKubeletKey); err != nil {
		return nil, err
	}

	clientKubeProxyCert := filepath.Join(envInfo.DataDir, "agent", "client-kube-proxy.crt")
	clientKubeProxyKey := filepath.Join(envInfo.DataDir, "agent", "client-kube-proxy.key")
	if err := getHostFile(clientKubeProxyCert, clientKubeProxyKey, info); err != nil {
		return nil, err
	}

	kubeconfigKubeproxy := filepath.Join(envInfo.DataDir, "agent", "kubeproxy.kubeconfig")
	if err := deps.KubeConfig(kubeconfigKubeproxy, proxy.APIServerURL(), serverCAFile, clientKubeProxyCert, clientKubeProxyKey); err != nil {
		return nil, err
	}

	clientK3sControllerCert := filepath.Join(envInfo.DataDir, "agent", "client-"+version.Program+"-controller.crt")
	clientK3sControllerKey := filepath.Join(envInfo.DataDir, "agent", "client-"+version.Program+"-controller.key")
	if err := getHostFile(clientK3sControllerCert, clientK3sControllerKey, info); err != nil {
		return nil, err
	}

	kubeconfigK3sController := filepath.Join(envInfo.DataDir, "agent", version.Program+"controller.kubeconfig")
	if err := deps.KubeConfig(kubeconfigK3sController, proxy.APIServerURL(), serverCAFile, clientK3sControllerCert, clientK3sControllerKey); err != nil {
		return nil, err
	}

	nodeConfig := &config.Node{
		Docker:                   envInfo.Docker,
		SELinux:                  envInfo.EnableSELinux,
		ContainerRuntimeEndpoint: envInfo.ContainerRuntimeEndpoint,
		FlannelBackend:           controlConfig.FlannelBackend,
		ServerHTTPSPort:          controlConfig.HTTPSPort,
	}
	nodeConfig.FlannelIface = flannelIface
	nodeConfig.Images = filepath.Join(envInfo.DataDir, "agent", "images")
	nodeConfig.AgentConfig.NodeName = nodeName
	nodeConfig.AgentConfig.NodeConfigPath = nodeConfigPath
	nodeConfig.AgentConfig.ServingKubeletCert = servingKubeletCert
	nodeConfig.AgentConfig.ServingKubeletKey = servingKubeletKey
	nodeConfig.AgentConfig.ClusterDNS = controlConfig.ClusterDNS
	nodeConfig.AgentConfig.ClusterDomain = controlConfig.ClusterDomain
	nodeConfig.AgentConfig.ResolvConf = locateOrGenerateResolvConf(envInfo)
	nodeConfig.AgentConfig.ClientCA = clientCAFile
	nodeConfig.AgentConfig.ListenAddress = "0.0.0.0"
	nodeConfig.AgentConfig.KubeConfigKubelet = kubeconfigKubelet
	nodeConfig.AgentConfig.KubeConfigKubeProxy = kubeconfigKubeproxy
	nodeConfig.AgentConfig.KubeConfigK3sController = kubeconfigK3sController
	if envInfo.Rootless {
		nodeConfig.AgentConfig.RootDir = filepath.Join(envInfo.DataDir, "agent", "kubelet")
	}
	nodeConfig.AgentConfig.Snapshotter = envInfo.Snapshotter
	nodeConfig.AgentConfig.IPSECPSK = controlConfig.IPSECPSK
	nodeConfig.AgentConfig.StrongSwanDir = filepath.Join(envInfo.DataDir, "agent", "strongswan")
	nodeConfig.CACerts = info.CACerts
	nodeConfig.Containerd.Config = filepath.Join(envInfo.DataDir, "agent", "etc", "containerd", "config.toml")
	nodeConfig.Containerd.Root = filepath.Join(envInfo.DataDir, "agent", "containerd")
	if !nodeConfig.Docker && nodeConfig.ContainerRuntimeEndpoint == "" {
		switch nodeConfig.AgentConfig.Snapshotter {
		case "overlayfs":
			if err := containerd.OverlaySupported(nodeConfig.Containerd.Root); err != nil {
				return nil, errors.Wrapf(err, "\"overlayfs\" snapshotter cannot be enabled for %q, try using \"fuse-overlayfs\" or \"native\"",
					nodeConfig.Containerd.Root)
			}
		case "fuse-overlayfs":
			if err := containerd.FuseoverlayfsSupported(nodeConfig.Containerd.Root); err != nil {
				return nil, errors.Wrapf(err, "\"fuse-overlayfs\" snapshotter cannot be enabled for %q, try using \"native\"",
					nodeConfig.Containerd.Root)
			}
		case "stargz":
			if err := containerd.StargzSupported(nodeConfig.Containerd.Root); err != nil {
				return nil, errors.Wrapf(err, "\"stargz\" snapshotter cannot be enabled for %q, try using \"overlayfs\" or \"native\"",
					nodeConfig.Containerd.Root)
			}
			nodeConfig.AgentConfig.ImageServiceSocket = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"
		}
	}
	nodeConfig.Containerd.Opt = filepath.Join(envInfo.DataDir, "agent", "containerd")
	if !envInfo.Debug {
		nodeConfig.Containerd.Log = filepath.Join(envInfo.DataDir, "agent", "containerd", "containerd.log")
	}
	applyContainerdStateAndAddress(nodeConfig)
	nodeConfig.Containerd.Template = filepath.Join(envInfo.DataDir, "agent", "etc", "containerd", "config.toml.tmpl")
	nodeConfig.Certificate = servingCert

	nodeConfig.AgentConfig.NodeIPs = nodeIPs
	nodeIP, err := util.GetFirst4(nodeIPs)
	if err != nil {
		return nil, errors.Wrap(err, "cannot configure IPv4 node-ip")
	}
	nodeConfig.AgentConfig.NodeIP = nodeIP.String()

	for _, externalIP := range envInfo.NodeExternalIP {
		for _, v := range strings.Split(externalIP, ",") {
			ip := sysnet.ParseIP(v)
			if ip == nil {
				return nil, fmt.Errorf("invalid node-external-ip %s", v)
			}
			nodeConfig.AgentConfig.NodeExternalIPs = append(nodeConfig.AgentConfig.NodeExternalIPs, ip)
		}
	}

	// if configured, set NodeExternalIP to the first IPv4 address, for legacy clients
	if len(nodeConfig.AgentConfig.NodeExternalIPs) > 0 {
		nodeExternalIP, err := util.GetFirst4(nodeConfig.AgentConfig.NodeExternalIPs)
		if err != nil {
			return nil, errors.Wrap(err, "cannot configure IPv4 node-external-ip")
		}
		nodeConfig.AgentConfig.NodeExternalIP = nodeExternalIP.String()
	}

	if nodeConfig.FlannelBackend == config.FlannelBackendNone {
		nodeConfig.NoFlannel = true
	} else {
		nodeConfig.NoFlannel = envInfo.NoFlannel
	}

	if !nodeConfig.NoFlannel {
		hostLocal, err := exec.LookPath("host-local")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to find host-local")
		}

		if envInfo.FlannelConf == "" {
			nodeConfig.FlannelConf = filepath.Join(envInfo.DataDir, "agent", "etc", "flannel", "net-conf.json")
		} else {
			nodeConfig.FlannelConf = envInfo.FlannelConf
			nodeConfig.FlannelConfOverride = true
		}
		nodeConfig.AgentConfig.CNIBinDir = filepath.Dir(hostLocal)
		nodeConfig.AgentConfig.CNIConfDir = filepath.Join(envInfo.DataDir, "agent", "etc", "cni", "net.d")
	}

	if !nodeConfig.Docker && nodeConfig.ContainerRuntimeEndpoint == "" {
		nodeConfig.AgentConfig.RuntimeSocket = nodeConfig.Containerd.Address
	} else {
		nodeConfig.AgentConfig.RuntimeSocket = nodeConfig.ContainerRuntimeEndpoint
		nodeConfig.AgentConfig.CNIPlugin = true
	}

	if controlConfig.ClusterIPRange != nil {
		nodeConfig.AgentConfig.ClusterCIDR = controlConfig.ClusterIPRange
		nodeConfig.AgentConfig.ClusterCIDRs = []*sysnet.IPNet{controlConfig.ClusterIPRange}
	}

	if len(controlConfig.ClusterIPRanges) > 0 {
		nodeConfig.AgentConfig.ClusterCIDRs = controlConfig.ClusterIPRanges
	}

	if controlConfig.ServiceIPRange != nil {
		nodeConfig.AgentConfig.ServiceCIDR = controlConfig.ServiceIPRange
		nodeConfig.AgentConfig.ServiceCIDRs = []*sysnet.IPNet{controlConfig.ServiceIPRange}
	}

	if len(controlConfig.ServiceIPRanges) > 0 {
		nodeConfig.AgentConfig.ServiceCIDRs = controlConfig.ServiceIPRanges
	}

	if controlConfig.ServiceNodePortRange != nil {
		nodeConfig.AgentConfig.ServiceNodePortRange = *controlConfig.ServiceNodePortRange
	}

	if len(controlConfig.ClusterDNSs) == 0 {
		nodeConfig.AgentConfig.ClusterDNSs = []sysnet.IP{controlConfig.ClusterDNS}
	} else {
		nodeConfig.AgentConfig.ClusterDNSs = controlConfig.ClusterDNSs
	}

	nodeConfig.AgentConfig.PauseImage = envInfo.PauseImage
	nodeConfig.AgentConfig.AirgapExtraRegistry = envInfo.AirgapExtraRegistry
	nodeConfig.AgentConfig.SystemDefaultRegistry = controlConfig.SystemDefaultRegistry

	// Apply SystemDefaultRegistry to PauseImage and AirgapExtraRegistry
	if controlConfig.SystemDefaultRegistry != "" {
		if nodeConfig.AgentConfig.PauseImage != "" && !strings.HasPrefix(nodeConfig.AgentConfig.PauseImage, controlConfig.SystemDefaultRegistry) {
			nodeConfig.AgentConfig.PauseImage = controlConfig.SystemDefaultRegistry + "/" + nodeConfig.AgentConfig.PauseImage
		}
		if !slice.ContainsString(nodeConfig.AgentConfig.AirgapExtraRegistry, controlConfig.SystemDefaultRegistry) {
			nodeConfig.AgentConfig.AirgapExtraRegistry = append(nodeConfig.AgentConfig.AirgapExtraRegistry, controlConfig.SystemDefaultRegistry)
		}
	}

	nodeConfig.AgentConfig.ExtraKubeletArgs = envInfo.ExtraKubeletArgs
	nodeConfig.AgentConfig.ExtraKubeProxyArgs = envInfo.ExtraKubeProxyArgs
	nodeConfig.AgentConfig.NodeTaints = envInfo.Taints
	nodeConfig.AgentConfig.NodeLabels = envInfo.Labels
	nodeConfig.AgentConfig.ImageCredProvBinDir = envInfo.ImageCredProvBinDir
	nodeConfig.AgentConfig.ImageCredProvConfig = envInfo.ImageCredProvConfig
	nodeConfig.AgentConfig.PrivateRegistry = envInfo.PrivateRegistry
	nodeConfig.AgentConfig.DisableCCM = controlConfig.DisableCCM
	nodeConfig.AgentConfig.DisableNPC = controlConfig.DisableNPC
	nodeConfig.AgentConfig.DisableKubeProxy = controlConfig.DisableKubeProxy
	nodeConfig.AgentConfig.Rootless = envInfo.Rootless
	nodeConfig.AgentConfig.PodManifests = filepath.Join(envInfo.DataDir, "agent", DefaultPodManifestPath)
	nodeConfig.AgentConfig.ProtectKernelDefaults = envInfo.ProtectKernelDefaults

	if err := validateNetworkConfig(nodeConfig); err != nil {
		return nil, err
	}

	return nodeConfig, nil
}

func getConfig(info *clientaccess.Info) (*config.Control, error) {
	data, err := info.Get("/v1-" + version.Program + "/config")
	if err != nil {
		return nil, err
	}

	controlControl := &config.Control{}
	return controlControl, json.Unmarshal(data, controlControl)
}

// validateNetworkConfig ensures that the network configuration values provided by the server make sense.
func validateNetworkConfig(nodeConfig *config.Node) error {
	// Old versions of the server do not send enough information to correctly start the NPC. Users
	// need to upgrade the server to at least the same version as the agent, or disable the NPC
	// cluster-wide.
	if nodeConfig.AgentConfig.DisableNPC == false && (nodeConfig.AgentConfig.ServiceCIDR == nil || nodeConfig.AgentConfig.ServiceNodePortRange.Size == 0) {
		return fmt.Errorf("incompatible down-level server detected; servers must be upgraded to at least %s, or restarted with --disable-network-policy", version.Version)
	}

	return nil
}
