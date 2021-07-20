// +build linux

package agent

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/runc/libcontainer/system"
	"github.com/rancher/k3s/pkg/cgroups"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/util"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
)

func createRootlessConfig(argsMap map[string]string, hasCFS, hasPIDs bool) {
	// "/sys/fs/cgroup" is namespaced
	cgroupfsWritable := unix.Access("/sys/fs/cgroup", unix.W_OK) == nil
	if hasCFS && hasPIDs && cgroupfsWritable {
		logrus.Info("cgroup v2 controllers are delegated for rootless.")
		// cgroupfs v2, delegated for rootless by systemd
		argsMap["cgroup-driver"] = "cgroupfs"
	} else {
		logrus.Warn("cgroup v2 controllers are not delegated for rootless. Setting cgroup driver to \"none\".")
		// flags are from https://github.com/rootless-containers/usernetes/blob/v20190826.0/boot/kubelet.sh
		argsMap["cgroup-driver"] = "none"
		argsMap["feature-gates=SupportNoneCgroupDriver"] = "true"
		argsMap["cgroups-per-qos"] = "false"
		argsMap["enforce-node-allocatable"] = ""
	}
}

func checkRuntimeEndpoint(cfg *config.Agent, argsMap map[string]string) {
	if strings.HasPrefix(argsMap["container-runtime-endpoint"], unixPrefix) {
		argsMap["container-runtime-endpoint"] = cfg.RuntimeSocket
	} else {
		argsMap["container-runtime-endpoint"] = unixPrefix + cfg.RuntimeSocket
	}
}

func kubeProxyArgs(cfg *config.Agent) map[string]string {
	argsMap := map[string]string{
		"proxy-mode":                        "iptables",
		"healthz-bind-address":              "127.0.0.1",
		"kubeconfig":                        cfg.KubeConfigKubeProxy,
		"cluster-cidr":                      util.JoinIPNets(cfg.ClusterCIDRs),
		"conntrack-max-per-core":            "0",
		"conntrack-tcp-timeout-established": "0s",
		"conntrack-tcp-timeout-close-wait":  "0s",
	}
	if cfg.NodeName != "" {
		argsMap["hostname-override"] = cfg.NodeName
	}
	return argsMap
}

func kubeletArgs(cfg *config.Agent) map[string]string {
	argsMap := map[string]string{
		"healthz-bind-address":     "127.0.0.1",
		"read-only-port":           "0",
		"cluster-domain":           cfg.ClusterDomain,
		"kubeconfig":               cfg.KubeConfigKubelet,
		"eviction-hard":            "imagefs.available<5%,nodefs.available<5%",
		"eviction-minimum-reclaim": "imagefs.available=10%,nodefs.available=10%",
		"fail-swap-on":             "false",
		//"cgroup-root": "/k3s",
		"cgroup-driver":                "cgroupfs",
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
		argsMap["seccomp-profile-root"] = filepath.Join(cfg.RootDir, "seccomp")
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
		argsMap["containerd"] = cfg.RuntimeSocket
		argsMap["serialize-image-pulls"] = "false"
		checkRuntimeEndpoint(cfg, argsMap)
	} else if cfg.PauseImage != "" {
		argsMap["pod-infra-container-image"] = cfg.PauseImage
	}
	if cfg.ImageServiceSocket != "" {
		if strings.HasPrefix(cfg.ImageServiceSocket, unixPrefix) {
			argsMap["image-service-endpoint"] = cfg.ImageServiceSocket
		} else {
			argsMap["image-service-endpoint"] = unixPrefix + cfg.ImageServiceSocket
		}
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
	kubeletRoot, runtimeRoot, hasCFS, hasPIDs := cgroups.CheckCgroups()
	if !hasCFS {
		logrus.Warn("Disabling CPU quotas due to missing cpu.cfs_period_us")
		argsMap["cpu-cfs-quota"] = "false"
	}
	if !hasPIDs {
		logrus.Warn("Disabling pod PIDs limit feature due to missing cgroup pids support")
		argsMap["cgroups-per-qos"] = "false"
		argsMap["enforce-node-allocatable"] = ""
		argsMap["feature-gates"] = addFeatureGate(argsMap["feature-gates"], "SupportPodPidsLimit=false")
	}
	if kubeletRoot != "" {
		argsMap["kubelet-cgroups"] = kubeletRoot
	}
	if runtimeRoot != "" {
		argsMap["runtime-cgroups"] = runtimeRoot
	}
	if system.RunningInUserNS() {
		argsMap["feature-gates"] = addFeatureGate(argsMap["feature-gates"], "DevicePlugins=false")
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
		argsMap["feature-gates"] = addFeatureGate(argsMap["feature-gates"], "KubeletCredentialProviders=true")
		argsMap["image-credential-provider-bin-dir"] = cfg.ImageCredProvBinDir
		argsMap["image-credential-provider-config"] = cfg.ImageCredProvConfig
	}

	if cfg.Rootless {
		createRootlessConfig(argsMap, hasCFS, hasCFS)
	}

	if cfg.ProtectKernelDefaults {
		argsMap["protect-kernel-defaults"] = "true"
	}
	return argsMap
}
