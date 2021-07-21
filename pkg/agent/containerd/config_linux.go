// +build linux

package containerd

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/opencontainers/runc/libcontainer/system"
	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/agent/templates"
	util2 "github.com/rancher/k3s/pkg/agent/util"
	"github.com/rancher/k3s/pkg/cgroups"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/version"
	"github.com/rancher/wharfie/pkg/registries"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/kubernetes/pkg/kubelet/util"
)

func getContainerdArgs(cfg *config.Node) []string {
	args := []string{
		"containerd",
		"-c", cfg.Containerd.Config,
		"-a", cfg.Containerd.Address,
		"--state", cfg.Containerd.State,
		"--root", cfg.Containerd.Root,
	}
	return args
}

// setupContainerdConfig generates the containerd.toml, using a template combined with various
// runtime configurations and registry mirror settings provided by the administrator.
func setupContainerdConfig(ctx context.Context, cfg *config.Node) error {
	privRegistries, err := registries.GetPrivateRegistries(cfg.AgentConfig.PrivateRegistry)
	if err != nil {
		return err
	}

	isRunningInUserNS := system.RunningInUserNS()
	cgroupsCheck := cgroups.CheckCgroups()
	// "/sys/fs/cgroup" is namespaced
	cgroupfsWritable := unix.Access("/sys/fs/cgroup", unix.W_OK) == nil
	disableCgroup := isRunningInUserNS && (!cgroupsCheck.HasCFS || !cgroupsCheck.HasPIDs || !cgroupfsWritable)
	if disableCgroup {
		logrus.Warn("cgroup v2 controllers are not delegated for rootless. Disabling cgroup.")
	}

	var containerdTemplate string
	containerdConfig := templates.ContainerdConfig{
		NodeConfig:            cfg,
		DisableCgroup:         disableCgroup,
		IsRunningInUserNS:     isRunningInUserNS,
		PrivateRegistryConfig: privRegistries.Registry(),
	}

	selEnabled, selConfigured, err := selinuxStatus()
	if err != nil {
		return errors.Wrap(err, "failed to detect selinux")
	}
	switch {
	case !cfg.SELinux && selEnabled:
		logrus.Warn("SELinux is enabled on this host, but " + version.Program + " has not been started with --selinux - containerd SELinux support is disabled")
	case cfg.SELinux && !selConfigured:
		logrus.Warnf("SELinux is enabled for "+version.Program+" but process is not running in context '%s', "+version.Program+"-selinux policy may need to be applied", SELinuxContextType)
	}

	containerdTemplateBytes, err := ioutil.ReadFile(cfg.Containerd.Template)
	if err == nil {
		logrus.Infof("Using containerd template at %s", cfg.Containerd.Template)
		containerdTemplate = string(containerdTemplateBytes)
	} else if os.IsNotExist(err) {
		containerdTemplate = templates.ContainerdConfigTemplate
	} else {
		return err
	}
	parsedTemplate, err := templates.ParseTemplateFromConfig(containerdTemplate, containerdConfig)
	if err != nil {
		return err
	}

	return util2.WriteFile(cfg.Containerd.Config, parsedTemplate)
}

// criConnection connects to a CRI socket at the given path.
func CriConnection(ctx context.Context, address string) (*grpc.ClientConn, error) {
	addr, dialer, err := util.GetAddressAndDialer("unix://" + address)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second), grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	if err != nil {
		return nil, err
	}

	c := runtimeapi.NewRuntimeServiceClient(conn)
	_, err = c.Version(ctx, &runtimeapi.VersionRequest{
		Version: "0.1.0",
	})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
