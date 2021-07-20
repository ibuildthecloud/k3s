// +build windows

package agent

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Microsoft/hcsshim"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/util"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
)

var (
	NetworkName string = "vxlan0"
)

func checkRuntimeEndpoint(cfg *config.Agent, argsMap map[string]string) {
	if strings.HasPrefix(cfg.RuntimeSocket, windowsPrefix) {
		argsMap["container-runtime-endpoint"] = cfg.RuntimeSocket
	} else {
		argsMap["container-runtime-endpoint"] = windowsPrefix + cfg.RuntimeSocket
	}
}

func kubeProxyArgs(cfg *config.Agent) map[string]string {
	argsMap := map[string]string{
		"proxy-mode":           "kernelspace",
		"healthz-bind-address": "127.0.0.1",
		"kubeconfig":           cfg.KubeConfigKubeProxy,
		"cluster-cidr":         util.JoinIPNets(cfg.ClusterCIDRs),
	}
	if cfg.NodeName != "" {
		argsMap["hostname-override"] = cfg.NodeName
	}

	if sourceVip := waitForManagementIp(NetworkName); sourceVip != "" {
		argsMap["source-vip"] = sourceVip
	}

	return argsMap
}

func kubeletArgs(cfg *config.Agent) map[string]string {
	argsMap := map[string]string{
		"healthz-bind-address":         "127.0.0.1",
		"read-only-port":               "0",
		"cluster-domain":               cfg.ClusterDomain,
		"kubeconfig":                   cfg.KubeConfigKubelet,
		"eviction-hard":                "imagefs.available<5%,nodefs.available<5%",
		"eviction-minimum-reclaim":     "imagefs.available=10%,nodefs.available=10%",
		"fail-swap-on":                 "false",
		"authentication-token-webhook": "true",
		"anonymous-auth":               "false",
		"authorization-mode":           modes.ModeWebhook,
	}
	if cfg.PodManifests != "" && argsMap["pod-manifest-path"] == "" {
		argsMap["pod-manifest-path"] = cfg.PodManifests
	}
	if err := os.MkdirAll(argsMap["pod-manifest-path"], 0755); err != nil {
		logrus.Errorf("Failed to mkdir %s: %v", argsMap["pod-manifest-path"], err)
	}
	if cfg.RootDir != "" {
		argsMap["root-dir"] = cfg.RootDir
		argsMap["cert-dir"] = filepath.Join(cfg.RootDir, "pki")
	}
	if cfg.CNIConfDir != "" {
		argsMap["cni-conf-dir"] = cfg.CNIConfDir
	}
	if cfg.CNIBinDir != "" {
		argsMap["cni-bin-dir"] = cfg.CNIBinDir
	}
	if cfg.CNIPlugin {
		argsMap["network-plugin"] = "cni"
	}
	if len(cfg.ClusterDNS) > 0 {
		argsMap["cluster-dns"] = util.JoinIPs(cfg.ClusterDNSs)
	}
	if cfg.ResolvConf != "" {
		argsMap["resolv-conf"] = cfg.ResolvConf
	}
	if cfg.RuntimeSocket != "" {
		argsMap["container-runtime"] = "remote"
		argsMap["serialize-image-pulls"] = "false"
		checkRuntimeEndpoint(cfg, argsMap)
	} else if cfg.PauseImage != "" {
		argsMap["pod-infra-container-image"] = cfg.PauseImage
	}
	if cfg.ListenAddress != "" {
		argsMap["address"] = cfg.ListenAddress
	}
	if cfg.ClientCA != "" {
		argsMap["anonymous-auth"] = "false"
		argsMap["client-ca-file"] = cfg.ClientCA
	}
	if cfg.ServingKubeletCert != "" && cfg.ServingKubeletKey != "" {
		argsMap["tls-cert-file"] = cfg.ServingKubeletCert
		argsMap["tls-private-key-file"] = cfg.ServingKubeletKey
	}
	if cfg.NodeName != "" {
		argsMap["hostname-override"] = cfg.NodeName
	}
	defaultIP, err := net.ChooseHostInterface()
	if err != nil || defaultIP.String() != cfg.NodeIP {
		argsMap["node-ip"] = cfg.NodeIP
	}

	argsMap["node-labels"] = strings.Join(cfg.NodeLabels, ",")
	if len(cfg.NodeTaints) > 0 {
		argsMap["register-with-taints"] = strings.Join(cfg.NodeTaints, ",")
	}
	if !cfg.DisableCCM {
		argsMap["cloud-provider"] = "external"
	}

	if ImageCredProvAvailable(cfg) {
		logrus.Infof("Kubelet image credential provider bin dir and configuration file found.")
		argsMap["feature-gates"] = AddFeatureGate(argsMap["feature-gates"], "KubeletCredentialProviders=true")
		argsMap["image-credential-provider-bin-dir"] = cfg.ImageCredProvBinDir
		argsMap["image-credential-provider-config"] = cfg.ImageCredProvConfig
	}

	if cfg.ProtectKernelDefaults {
		argsMap["protect-kernel-defaults"] = "true"
	}
	return argsMap
}

func waitForManagementIp(networkName string) string {
	for range time.Tick(time.Second * 5) {
		network, err := hcsshim.GetHNSEndpointByName(networkName)
		if err != nil {
			logrus.WithError(err).Warning("can't find HNS endpoint for network, retrying", networkName)
			continue
		}
		return network.IPAddress.String()
	}
	return ""
}
