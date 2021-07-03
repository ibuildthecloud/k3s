package server

import (
	"context"
	"fmt"
	"io/ioutil"
	net2 "net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/helm-controller/pkg/helm"
	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/apiaddresses"
	"github.com/rancher/k3s/pkg/cli/cmds"
	"github.com/rancher/k3s/pkg/clientaccess"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/daemons/control"
	"github.com/rancher/k3s/pkg/datadir"
	"github.com/rancher/k3s/pkg/deploy"
	"github.com/rancher/k3s/pkg/node"
	"github.com/rancher/k3s/pkg/nodepassword"
	"github.com/rancher/k3s/pkg/rootlessports"
	"github.com/rancher/k3s/pkg/servicelb"
	"github.com/rancher/k3s/pkg/static"
	"github.com/rancher/k3s/pkg/util"
	"github.com/rancher/k3s/pkg/version"
	v1 "github.com/rancher/wrangler-api/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/pkg/leader"
	"github.com/rancher/wrangler/pkg/resolvehome"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
)

const (
	MasterRoleLabelKey       = "node-role.kubernetes.io/master"
	ControlPlaneRoleLabelKey = "node-role.kubernetes.io/control-plane"
	ETCDRoleLabelKey         = "node-role.kubernetes.io/etcd"
)

func ResolveDataDir(dataDir string) (string, error) {
	dataDir, err := datadir.Resolve(dataDir)
	return filepath.Join(dataDir, "server"), err
}

func StartServer(ctx context.Context, config *Config) error {
	if err := setupDataDirAndChdir(&config.ControlConfig); err != nil {
		return err
	}

	if err := setNoProxyEnv(&config.ControlConfig); err != nil {
		return err
	}

	if err := control.Server(ctx, &config.ControlConfig); err != nil {
		return errors.Wrap(err, "starting kubernetes")
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(config.StartupHooks))

	config.ControlConfig.Runtime.Handler = router(ctx, config)
	shArgs := cmds.StartupHookArgs{
		APIServerReady:  config.ControlConfig.Runtime.APIServerReady,
		KubeConfigAdmin: config.ControlConfig.Runtime.KubeConfigAdmin,
		Skips:           config.ControlConfig.Skips,
		Disables:        config.ControlConfig.Disables,
	}
	for _, hook := range config.StartupHooks {
		if err := hook(ctx, wg, shArgs); err != nil {
			return errors.Wrap(err, "startup hook")
		}
	}

	if config.ControlConfig.DisableAPIServer {
		go setETCDLabelsAndAnnotations(ctx, config)
	} else {
		go startOnAPIServerReady(ctx, wg, config)
	}

	ip := net2.ParseIP(config.ControlConfig.BindAddress)
	if ip == nil {
		hostIP, err := net.ChooseHostInterface()
		if err == nil {
			ip = hostIP
		} else {
			ip = net2.ParseIP("127.0.0.1")
		}
	}

	if err := printTokens(ip.String(), &config.ControlConfig); err != nil {
		return err
	}

	return writeKubeConfig(config.ControlConfig.Runtime.ServerCA, config)
}

func startOnAPIServerReady(ctx context.Context, wg *sync.WaitGroup, config *Config) {
	select {
	case <-ctx.Done():
		return
	case <-config.ControlConfig.Runtime.APIServerReady:
		if err := runControllers(ctx, wg, config); err != nil {
			logrus.Fatalf("failed to start controllers: %v", err)
		}
	}
}

func runControllers(ctx context.Context, wg *sync.WaitGroup, config *Config) error {
	controlConfig := &config.ControlConfig

	sc, err := NewContext(ctx, controlConfig.Runtime.KubeConfigAdmin)
	if err != nil {
		return errors.Wrap(err, "failed to create new server context")
	}

	wg.Wait()
	if err := stageFiles(ctx, sc, controlConfig); err != nil {
		return errors.Wrap(err, "failed to stage files")
	}

	// run migration before we set controlConfig.Runtime.Core
	if err := nodepassword.MigrateFile(
		sc.Core.Core().V1().Secret(),
		sc.Core.Core().V1().Node(),
		controlConfig.Runtime.NodePasswdFile); err != nil {
		logrus.Warn(errors.Wrap(err, "error migrating node-password file"))
	}
	controlConfig.Runtime.Core = sc.Core

	if controlConfig.Runtime.ClusterControllerStart != nil {
		if err := controlConfig.Runtime.ClusterControllerStart(ctx); err != nil {
			return errors.Wrap(err, "failed to start cluster controllers")
		}
	}

	for _, controller := range config.Controllers {
		if err := controller(ctx, sc); err != nil {
			return errors.Wrapf(err, "failed to start custom controller %s", util.GetFunctionName(controller))
		}
	}

	if err := sc.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start wranger controllers")
	}

	start := func(ctx context.Context) {
		if err := coreControllers(ctx, sc, config); err != nil {
			panic(err)
		}
		for _, controller := range config.LeaderControllers {
			if err := controller(ctx, sc); err != nil {
				panic(errors.Wrap(err, "leader controller"))
			}
		}
		if err := sc.Start(ctx); err != nil {
			panic(err)
		}
	}

	go setControlPlaneRoleLabel(ctx, sc.Core.Core().V1().Node(), config)

	go setClusterDNSConfig(ctx, config, sc.Core.Core().V1().ConfigMap())

	if controlConfig.NoLeaderElect {
		go func() {
			start(ctx)
			<-ctx.Done()
			logrus.Fatal("controllers exited")
		}()
	} else {
		go leader.RunOrDie(ctx, "", version.Program, sc.K8s, start)
	}

	return nil
}

func coreControllers(ctx context.Context, sc *Context, config *Config) error {
	if err := node.Register(ctx,
		!config.ControlConfig.Skips["coredns"],
		sc.Core.Core().V1().Secret(),
		sc.Core.Core().V1().ConfigMap(),
		sc.Core.Core().V1().Node()); err != nil {
		return err
	}

	// apply SystemDefaultRegistry setting to Helm and ServiceLB before starting controllers
	if config.ControlConfig.SystemDefaultRegistry != "" {
		helm.DefaultJobImage = config.ControlConfig.SystemDefaultRegistry + "/" + helm.DefaultJobImage
		servicelb.DefaultLBImage = config.ControlConfig.SystemDefaultRegistry + "/" + servicelb.DefaultLBImage
	}

	if !config.ControlConfig.DisableHelmController {
		helm.Register(ctx,
			sc.Apply,
			sc.Helm.Helm().V1().HelmChart(),
			sc.Helm.Helm().V1().HelmChartConfig(),
			sc.Batch.Batch().V1().Job(),
			sc.Auth.Rbac().V1().ClusterRoleBinding(),
			sc.Core.Core().V1().ServiceAccount(),
			sc.Core.Core().V1().ConfigMap())
	}

	if err := servicelb.Register(ctx,
		sc.K8s,
		sc.Apply,
		sc.Apps.Apps().V1().DaemonSet(),
		sc.Apps.Apps().V1().Deployment(),
		sc.Core.Core().V1().Node(),
		sc.Core.Core().V1().Pod(),
		sc.Core.Core().V1().Service(),
		sc.Core.Core().V1().Endpoints(),
		!config.DisableServiceLB,
		config.Rootless); err != nil {
		return err
	}

	if err := apiaddresses.Register(ctx, config.ControlConfig.Runtime, sc.Core.Core().V1().Endpoints()); err != nil {
		return err
	}

	if config.Rootless {
		return rootlessports.Register(ctx,
			sc.Core.Core().V1().Service(),
			!config.DisableServiceLB,
			config.ControlConfig.HTTPSPort)
	}

	return nil
}

func stageFiles(ctx context.Context, sc *Context, controlConfig *config.Control) error {
	dataDir := filepath.Join(controlConfig.DataDir, "static")
	if err := static.Stage(dataDir); err != nil {
		return err
	}
	dataDir = filepath.Join(controlConfig.DataDir, "manifests")
	templateVars := map[string]string{
		"%{CLUSTER_DNS}%":                 controlConfig.ClusterDNS.String(),
		"%{CLUSTER_DOMAIN}%":              controlConfig.ClusterDomain,
		"%{DEFAULT_LOCAL_STORAGE_PATH}%":  controlConfig.DefaultLocalStoragePath,
		"%{SYSTEM_DEFAULT_REGISTRY}%":     registryTemplate(controlConfig.SystemDefaultRegistry),
		"%{SYSTEM_DEFAULT_REGISTRY_RAW}%": controlConfig.SystemDefaultRegistry,
	}

	skip := controlConfig.Skips
	if !skip["traefik"] && isHelmChartTraefikV1(sc) {
		logrus.Warn("Skipping Traefik v2 deployment due to existing Traefik v1 installation")
		skip["traefik"] = true
	}
	if err := deploy.Stage(dataDir, templateVars, skip); err != nil {
		return err
	}

	return deploy.WatchFiles(ctx,
		sc.K8s,
		sc.Apply,
		sc.K3s.K3s().V1().Addon(),
		controlConfig.Disables,
		dataDir)
}

// registryTemplate behaves like the system_default_registry template in Rancher helm charts,
// and returns the registry value with a trailing forward slash if the registry string is not empty.
// If it is empty, it is passed through as a no-op.
func registryTemplate(registry string) string {
	if registry == "" {
		return registry
	}
	return registry + "/"
}

// isHelmChartTraefikV1 checks for an existing HelmChart resource with spec.chart containing traefik-1,
// as deployed by the legacy chart (https://%{KUBERNETES_API}%/static/charts/traefik-1.81.0.tgz)
func isHelmChartTraefikV1(sc *Context) bool {
	prefix := "traefik-1."
	helmChart, err := sc.Helm.Helm().V1().HelmChart().Get(metav1.NamespaceSystem, "traefik", metav1.GetOptions{})
	if err != nil {
		logrus.WithError(err).Info("Failed to get existing traefik HelmChart")
		return false
	}
	chart := path.Base(helmChart.Spec.Chart)
	if strings.HasPrefix(chart, prefix) {
		logrus.WithField("chart", chart).Info("Found existing traefik v1 HelmChart")
		return true
	}
	return false
}

func HomeKubeConfig(write, rootless bool) (string, error) {
	if write {
		if os.Getuid() == 0 && !rootless {
			return datadir.GlobalConfig, nil
		}
		return resolvehome.Resolve(datadir.HomeConfig)
	}

	if _, err := os.Stat(datadir.GlobalConfig); err == nil {
		return datadir.GlobalConfig, nil
	}

	return resolvehome.Resolve(datadir.HomeConfig)
}

func printTokens(advertiseIP string, config *config.Control) error {
	var (
		nodeFile string
	)

	if advertiseIP == "" {
		advertiseIP = "127.0.0.1"
	}

	if len(config.Runtime.ServerToken) > 0 {
		p := filepath.Join(config.DataDir, "token")
		if err := writeToken(config.Runtime.ServerToken, p, config.Runtime.ServerCA); err == nil {
			logrus.Infof("Node token is available at %s", p)
			nodeFile = p
		}

		// backwards compatibility
		np := filepath.Join(config.DataDir, "node-token")
		if !isSymlink(np) {
			if err := os.RemoveAll(np); err != nil {
				return err
			}
			if err := os.Symlink(p, np); err != nil {
				return err
			}
		}
	}

	if len(nodeFile) > 0 {
		printToken(config.SupervisorPort, advertiseIP, "To join node to cluster:", "agent")
	}

	return nil
}

func writeKubeConfig(certs string, config *Config) error {
	ip := config.ControlConfig.BindAddress
	if ip == "" {
		ip = "127.0.0.1"
	}
	url := fmt.Sprintf("https://%s:%d", ip, config.ControlConfig.HTTPSPort)
	kubeConfig, err := HomeKubeConfig(true, config.Rootless)
	def := true
	if err != nil {
		kubeConfig = filepath.Join(config.ControlConfig.DataDir, "kubeconfig-"+version.Program+".yaml")
		def = false
	}
	kubeConfigSymlink := kubeConfig
	if config.ControlConfig.KubeConfigOutput != "" {
		kubeConfig = config.ControlConfig.KubeConfigOutput
	}

	if isSymlink(kubeConfigSymlink) {
		if err := os.Remove(kubeConfigSymlink); err != nil {
			logrus.Errorf("Failed to remove kubeconfig symlink")
		}
	}

	if err = clientaccess.WriteClientKubeConfig(kubeConfig, url, config.ControlConfig.Runtime.ServerCA, config.ControlConfig.Runtime.ClientAdminCert,
		config.ControlConfig.Runtime.ClientAdminKey); err == nil {
		logrus.Infof("Wrote kubeconfig %s", kubeConfig)
	} else {
		logrus.Errorf("Failed to generate kubeconfig: %v", err)
		return err
	}

	if config.ControlConfig.KubeConfigMode != "" {
		mode, err := strconv.ParseInt(config.ControlConfig.KubeConfigMode, 8, 0)
		if err == nil {
			util.SetFileModeForPath(kubeConfig, os.FileMode(mode))
		} else {
			logrus.Errorf("Failed to set %s to mode %s: %v", kubeConfig, os.FileMode(mode), err)
		}
	} else {
		util.SetFileModeForPath(kubeConfig, os.FileMode(0600))
	}

	if kubeConfigSymlink != kubeConfig {
		if err := writeConfigSymlink(kubeConfig, kubeConfigSymlink); err != nil {
			logrus.Errorf("Failed to write kubeconfig symlink: %v", err)
		}
	}

	if def {
		logrus.Infof("Run: %s kubectl", filepath.Base(os.Args[0]))
	}

	return nil
}

func setupDataDirAndChdir(config *config.Control) error {
	var (
		err error
	)

	config.DataDir, err = ResolveDataDir(config.DataDir)
	if err != nil {
		return err
	}

	dataDir := config.DataDir

	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return errors.Wrapf(err, "can not mkdir %s", dataDir)
	}

	if err := os.Chdir(dataDir); err != nil {
		return errors.Wrapf(err, "can not chdir %s", dataDir)
	}

	return nil
}

func printToken(httpsPort int, advertiseIP, prefix, cmd string) {
	ip := advertiseIP
	if ip == "" {
		hostIP, err := net.ChooseHostInterface()
		if err != nil {
			logrus.Errorf("Failed to choose interface: %v", err)
		}
		ip = hostIP.String()
	}

	logrus.Infof("%s %s %s -s https://%s:%d -t ${NODE_TOKEN}", prefix, version.Program, cmd, ip, httpsPort)
}

func writeToken(token, file, certs string) error {
	if len(token) == 0 {
		return nil
	}

	token, err := clientaccess.FormatToken(token, certs)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, []byte(token+"\n"), 0600)
}

func setNoProxyEnv(config *config.Control) error {
	splitter := func(c rune) bool {
		return c == ','
	}
	envList := []string{}
	envList = append(envList, strings.FieldsFunc(os.Getenv("NO_PROXY"), splitter)...)
	envList = append(envList, strings.FieldsFunc(os.Getenv("no_proxy"), splitter)...)
	envList = append(envList,
		".svc",
		"."+config.ClusterDomain,
		util.JoinIPNets(config.ClusterIPRanges),
		util.JoinIPNets(config.ServiceIPRanges),
	)
	os.Unsetenv("no_proxy")
	return os.Setenv("NO_PROXY", strings.Join(envList, ","))
}

func writeConfigSymlink(kubeconfig, kubeconfigSymlink string) error {
	if err := os.Remove(kubeconfigSymlink); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove %s file: %v", kubeconfigSymlink, err)
	}
	if err := os.MkdirAll(filepath.Dir(kubeconfigSymlink), 0755); err != nil {
		return fmt.Errorf("failed to create path for symlink: %v", err)
	}
	if err := os.Symlink(kubeconfig, kubeconfigSymlink); err != nil {
		return fmt.Errorf("failed to create symlink: %v", err)
	}
	return nil
}

func isSymlink(config string) bool {
	if fi, err := os.Lstat(config); err == nil && (fi.Mode()&os.ModeSymlink == os.ModeSymlink) {
		return true
	}
	return false
}

func setControlPlaneRoleLabel(ctx context.Context, nodes v1.NodeClient, config *Config) error {
	if config.DisableAgent || config.ControlConfig.DisableAPIServer {
		return nil
	}
	for {
		nodeName := os.Getenv("NODE_NAME")
		if nodeName == "" {
			logrus.Info("Waiting for control-plane node agent startup")
			time.Sleep(1 * time.Second)
			continue
		}
		node, err := nodes.Get(nodeName, metav1.GetOptions{})
		if err != nil {
			logrus.Infof("Waiting for control-plane node %s startup: %v", nodeName, err)
			time.Sleep(1 * time.Second)
			continue
		}
		// remove etcd label if etcd is disabled
		var etcdRoleLabelExists bool
		if config.ControlConfig.DisableETCD {
			if _, ok := node.Labels[ETCDRoleLabelKey]; ok {
				delete(node.Labels, ETCDRoleLabelKey)
				etcdRoleLabelExists = true
			}
		}
		if v, ok := node.Labels[ControlPlaneRoleLabelKey]; ok && v == "true" && !etcdRoleLabelExists {
			break
		}
		if node.Labels == nil {
			node.Labels = make(map[string]string)
		}
		node.Labels[ControlPlaneRoleLabelKey] = "true"
		node.Labels[MasterRoleLabelKey] = "true"

		_, err = nodes.Update(node)
		if err == nil {
			logrus.Infof("Control-plane role label has been set successfully on node: %s", nodeName)
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}

func setClusterDNSConfig(ctx context.Context, controlConfig *Config, configMap v1.ConfigMapClient) error {
	// check if configmap already exists
	_, err := configMap.Get("kube-system", "cluster-dns", metav1.GetOptions{})
	if err == nil {
		logrus.Infof("Cluster dns configmap already exists")
		return nil
	}
	clusterDNS := controlConfig.ControlConfig.ClusterDNS
	clusterDomain := controlConfig.ControlConfig.ClusterDomain
	c := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-dns",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"clusterDNS":    clusterDNS.String(),
			"clusterDomain": clusterDomain,
		},
	}
	for {
		_, err = configMap.Create(c)
		if err == nil {
			logrus.Infof("Cluster dns configmap has been set successfully")
			break
		}
		logrus.Infof("Waiting for control-plane dns startup: %v", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}
