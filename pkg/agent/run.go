package agent

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	systemd "github.com/coreos/go-systemd/daemon"
	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/agent/config"
	"github.com/rancher/k3s/pkg/agent/containerd"
	"github.com/rancher/k3s/pkg/agent/flannel"
	"github.com/rancher/k3s/pkg/agent/netpol"
	"github.com/rancher/k3s/pkg/agent/proxy"
	"github.com/rancher/k3s/pkg/agent/syssetup"
	"github.com/rancher/k3s/pkg/agent/tunnel"
	"github.com/rancher/k3s/pkg/cgroups"
	"github.com/rancher/k3s/pkg/cli/cmds"
	"github.com/rancher/k3s/pkg/clientaccess"
	cp "github.com/rancher/k3s/pkg/cloudprovider"
	"github.com/rancher/k3s/pkg/daemons/agent"
	daemonconfig "github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/daemons/executor"
	"github.com/rancher/k3s/pkg/nodeconfig"
	"github.com/rancher/k3s/pkg/rootless"
	"github.com/rancher/k3s/pkg/util"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/controller-manager/app"
	app2 "k8s.io/kubernetes/cmd/kube-proxy/app"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	utilsnet "k8s.io/utils/net"
	utilpointer "k8s.io/utils/pointer"
)

func run(ctx context.Context, cfg cmds.Agent, proxy proxy.Proxy) error {
	nodeConfig := config.Get(ctx, cfg, proxy)

	dualCluster, err := utilsnet.IsDualStackCIDRs(nodeConfig.AgentConfig.ClusterCIDRs)
	if err != nil {
		return errors.Wrap(err, "failed to validate cluster-cidr")
	}
	dualService, err := utilsnet.IsDualStackCIDRs(nodeConfig.AgentConfig.ServiceCIDRs)
	if err != nil {
		return errors.Wrap(err, "failed to validate service-cidr")
	}
	dualNode, err := utilsnet.IsDualStackIPs(nodeConfig.AgentConfig.NodeIPs)
	if err != nil {
		return errors.Wrap(err, "failed to validate node-ip")
	}

	enableIPv6 := dualCluster || dualService || dualNode
	conntrackConfig, err := getConntrackConfig(nodeConfig)
	if err != nil {
		return errors.Wrap(err, "failed to validate kube-proxy conntrack configuration")
	}
	syssetup.Configure(enableIPv6, conntrackConfig)

	if err := setupCriCtlConfig(cfg, nodeConfig); err != nil {
		return err
	}

	if err := executor.Bootstrap(ctx, nodeConfig, cfg); err != nil {
		return err
	}

	if !nodeConfig.NoFlannel {
		if err := flannel.Prepare(ctx, nodeConfig); err != nil {
			return err
		}
	}

	if !nodeConfig.Docker && nodeConfig.ContainerRuntimeEndpoint == "" {
		if err := containerd.Run(ctx, nodeConfig); err != nil {
			return err
		}
	}

	notifySocket := os.Getenv("NOTIFY_SOCKET")
	os.Unsetenv("NOTIFY_SOCKET")

	if err := setupTunnelAndRunAgent(ctx, nodeConfig, cfg, proxy); err != nil {
		return err
	}

	coreClient, err := coreClient(nodeConfig.AgentConfig.KubeConfigKubelet)
	if err != nil {
		return err
	}

	app.WaitForAPIServer(coreClient, 30*time.Second)

	if !nodeConfig.NoFlannel {
		if err := flannel.Run(ctx, nodeConfig, coreClient.CoreV1().Nodes()); err != nil {
			return err
		}
	}

	if err := configureNode(ctx, &nodeConfig.AgentConfig, coreClient.CoreV1().Nodes()); err != nil {
		return err
	}

	if !nodeConfig.AgentConfig.DisableNPC {
		if err := netpol.Run(ctx, nodeConfig); err != nil {
			return err
		}
	}

	os.Setenv("NOTIFY_SOCKET", notifySocket)
	systemd.SdNotify(true, "READY=1\n")

	<-ctx.Done()
	return ctx.Err()
}

// getConntrackConfig uses the kube-proxy code to parse the user-provided kube-proxy-arg values, and
// extract the conntrack settings so that K3s can set them itself. This allows us to soft-fail when
// running K3s in Docker, where kube-proxy is no longer allowed to set conntrack sysctls on newer kernels.
// When running rootless, we do not attempt to set conntrack sysctls - this behavior is copied from kubeadm.
func getConntrackConfig(nodeConfig *daemonconfig.Node) (*kubeproxyconfig.KubeProxyConntrackConfiguration, error) {
	ctConfig := &kubeproxyconfig.KubeProxyConntrackConfiguration{
		MaxPerCore:            utilpointer.Int32Ptr(0),
		Min:                   utilpointer.Int32Ptr(0),
		TCPEstablishedTimeout: &metav1.Duration{},
		TCPCloseWaitTimeout:   &metav1.Duration{},
	}

	if nodeConfig.AgentConfig.Rootless {
		return ctConfig, nil
	}

	cmd := app2.NewProxyCommand()
	if err := cmd.ParseFlags(daemonconfig.GetArgs(map[string]string{}, nodeConfig.AgentConfig.ExtraKubeProxyArgs)); err != nil {
		return nil, err
	}
	maxPerCore, err := cmd.Flags().GetInt32("conntrack-max-per-core")
	if err != nil {
		return nil, err
	}
	ctConfig.MaxPerCore = &maxPerCore
	min, err := cmd.Flags().GetInt32("conntrack-min")
	if err != nil {
		return nil, err
	}
	ctConfig.Min = &min
	establishedTimeout, err := cmd.Flags().GetDuration("conntrack-tcp-timeout-established")
	if err != nil {
		return nil, err
	}
	ctConfig.TCPEstablishedTimeout.Duration = establishedTimeout
	closeWaitTimeout, err := cmd.Flags().GetDuration("conntrack-tcp-timeout-close-wait")
	if err != nil {
		return nil, err
	}
	ctConfig.TCPCloseWaitTimeout.Duration = closeWaitTimeout
	return ctConfig, nil
}

func coreClient(cfg string) (kubernetes.Interface, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", cfg)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(restConfig)
}

func Run(ctx context.Context, cfg cmds.Agent) error {
	if err := cgroups.Validate(); err != nil {
		return err
	}

	if cfg.Rootless && !cfg.RootlessAlreadyUnshared {
		if err := rootless.Rootless(cfg.DataDir); err != nil {
			return err
		}
	}

	agentDir := filepath.Join(cfg.DataDir, "agent")
	if err := os.MkdirAll(agentDir, 0700); err != nil {
		return err
	}

	proxy, err := proxy.NewSupervisorProxy(ctx, !cfg.DisableLoadBalancer, agentDir, cfg.ServerURL, cfg.LBServerPort)
	if err != nil {
		return err
	}

	for {
		newToken, err := clientaccess.ParseAndValidateTokenForUser(proxy.SupervisorURL(), cfg.Token, "node")
		if err != nil {
			logrus.Error(err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			continue
		}
		cfg.Token = newToken.String()
		break
	}

	return run(ctx, cfg, proxy)
}

func configureNode(ctx context.Context, agentConfig *daemonconfig.Agent, nodes v1.NodeInterface) error {
	count := 0
	for {
		node, err := nodes.Get(ctx, agentConfig.NodeName, metav1.GetOptions{})
		if err != nil {
			if count%30 == 0 {
				logrus.Infof("Waiting for kubelet to be ready on node %s: %v", agentConfig.NodeName, err)
			}
			count++
			time.Sleep(1 * time.Second)
			continue
		}

		updateNode := false
		if labels, changed := updateMutableLabels(agentConfig, node.Labels); changed {
			node.Labels = labels
			updateNode = true
		}

		if !agentConfig.DisableCCM {
			if annotations, changed := updateAddressAnnotations(agentConfig, node.Annotations); changed {
				node.Annotations = annotations
				updateNode = true
			}
			if labels, changed := updateLegacyAddressLabels(agentConfig, node.Labels); changed {
				node.Labels = labels
				updateNode = true
			}
		}

		// inject node config
		if changed, err := nodeconfig.SetNodeConfigAnnotations(node); err != nil {
			return err
		} else if changed {
			updateNode = true
		}

		if updateNode {
			if _, err := nodes.Update(ctx, node, metav1.UpdateOptions{}); err != nil {
				logrus.Infof("Failed to update node %s: %v", agentConfig.NodeName, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					continue
				}
			}
			logrus.Infof("labels have been set successfully on node: %s", agentConfig.NodeName)
		} else {
			logrus.Infof("labels have already set on node: %s", agentConfig.NodeName)
		}

		break
	}

	return nil
}

func updateMutableLabels(agentConfig *daemonconfig.Agent, nodeLabels map[string]string) (map[string]string, bool) {
	result := map[string]string{}

	for _, m := range agentConfig.NodeLabels {
		var (
			v string
			p = strings.SplitN(m, `=`, 2)
			k = p[0]
		)
		if len(p) > 1 {
			v = p[1]
		}
		result[k] = v
	}
	result = labels.Merge(nodeLabels, result)
	return result, !equality.Semantic.DeepEqual(nodeLabels, result)
}

func updateLegacyAddressLabels(agentConfig *daemonconfig.Agent, nodeLabels map[string]string) (map[string]string, bool) {
	ls := labels.Set(nodeLabels)
	if ls.Has(cp.InternalIPKey) || ls.Has(cp.HostnameKey) {
		result := map[string]string{
			cp.InternalIPKey: agentConfig.NodeIP,
			cp.HostnameKey:   agentConfig.NodeName,
		}

		if agentConfig.NodeExternalIP != "" {
			result[cp.ExternalIPKey] = agentConfig.NodeExternalIP
		}

		result = labels.Merge(nodeLabels, result)
		return result, !equality.Semantic.DeepEqual(nodeLabels, result)
	}
	return nil, false
}

func updateAddressAnnotations(agentConfig *daemonconfig.Agent, nodeAnnotations map[string]string) (map[string]string, bool) {
	result := map[string]string{
		cp.InternalIPKey: util.JoinIPs(agentConfig.NodeIPs),
		cp.HostnameKey:   agentConfig.NodeName,
	}

	if agentConfig.NodeExternalIP != "" {
		result[cp.ExternalIPKey] = util.JoinIPs(agentConfig.NodeExternalIPs)
	}

	result = labels.Merge(nodeAnnotations, result)
	return result, !equality.Semantic.DeepEqual(nodeAnnotations, result)
}

// setupTunnelAndRunAgent should start the setup tunnel before starting kubelet and kubeproxy
// there are special case for etcd agents, it will wait until it can find the apiaddress from
// the address channel and update the proxy with the servers addresses, if in rke2 we need to
// start the agent before the tunnel is setup to allow kubelet to start first and start the pods
func setupTunnelAndRunAgent(ctx context.Context, nodeConfig *daemonconfig.Node, cfg cmds.Agent, proxy proxy.Proxy) error {
	var agentRan bool
	if cfg.ETCDAgent {
		// only in rke2 run the agent before the tunnel setup and check for that later in the function
		if proxy.IsAPIServerLBEnabled() {
			if err := agent.Agent(&nodeConfig.AgentConfig); err != nil {
				return err
			}
			agentRan = true
		}
		select {
		case address := <-cfg.APIAddressCh:
			cfg.ServerURL = address
			u, err := url.Parse(cfg.ServerURL)
			if err != nil {
				logrus.Warn(err)
			}
			proxy.Update([]string{fmt.Sprintf("%s:%d", u.Hostname(), nodeConfig.ServerHTTPSPort)})
		case <-ctx.Done():
			return ctx.Err()
		}
	} else if cfg.ClusterReset && proxy.IsAPIServerLBEnabled() {
		if err := agent.Agent(&nodeConfig.AgentConfig); err != nil {
			return err
		}
		agentRan = true
	}

	if err := tunnel.Setup(ctx, nodeConfig, proxy); err != nil {
		return err
	}
	if !agentRan {
		return agent.Agent(&nodeConfig.AgentConfig)
	}
	return nil
}
