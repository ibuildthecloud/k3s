package agent

import (
	"math/rand"
	"os"
	"time"

	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/daemons/executor"
	"github.com/sirupsen/logrus"
	"k8s.io/component-base/logs"
	_ "k8s.io/component-base/metrics/prometheus/restclient" // for client metric registration
	_ "k8s.io/component-base/metrics/prometheus/version"    // for version metric registration
)

const (
	unixPrefix    = "unix://"
	windowsPrefix = "npipe://"
)

func Agent(config *config.Agent) error {
	rand.Seed(time.Now().UTC().UnixNano())

	logs.InitLogs()
	defer logs.FlushLogs()
	if err := startKubelet(config); err != nil {
		return err
	}

	if !config.DisableKubeProxy {
		return startKubeProxy(config)
	}

	return nil
}

func startKubeProxy(cfg *config.Agent) error {
	argsMap := kubeProxyArgs(cfg)
	args := config.GetArgsList(argsMap, cfg.ExtraKubeProxyArgs)
	logrus.Infof("Running kube-proxy %s", config.ArgString(args))
	return executor.KubeProxy(args)
}

func startKubelet(cfg *config.Agent) error {
	argsMap := kubeletArgs(cfg)

	args := config.GetArgsList(argsMap, cfg.ExtraKubeletArgs)
	logrus.Infof("Running kubelet %s", config.ArgString(args))

	return executor.Kubelet(args)
}

// AddFeatureGate correctly appends a feature gate key pair to the feature gates CLI switch.
func AddFeatureGate(current, new string) string {
	if current == "" {
		return new
	}
	return current + "," + new
}

// ImageCredProvAvailable checks to see if the kubelet image credential provider bin dir and config
// files exist and are of the correct types. This is exported so that it may be used by downstream projects.
func ImageCredProvAvailable(cfg *config.Agent) bool {
	if info, err := os.Stat(cfg.ImageCredProvBinDir); err != nil || !info.IsDir() {
		logrus.Debugf("Kubelet image credential provider bin directory check failed: %v", err)
		return false
	}
	if info, err := os.Stat(cfg.ImageCredProvConfig); err != nil || info.IsDir() {
		logrus.Debugf("Kubelet image credential provider config file check failed: %v", err)
		return false
	}
	return true
}
