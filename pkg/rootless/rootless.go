// +build !windows

package rootless

import (
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/opencontainers/runc/libcontainer/cgroups"
	"github.com/pkg/errors"
	"github.com/rootless-containers/rootlesskit/pkg/child"
	"github.com/rootless-containers/rootlesskit/pkg/copyup/tmpfssymlink"
	"github.com/rootless-containers/rootlesskit/pkg/network/slirp4netns"
	"github.com/rootless-containers/rootlesskit/pkg/parent"
	portbuiltin "github.com/rootless-containers/rootlesskit/pkg/port/builtin"
	"github.com/sirupsen/logrus"
)

var (
	pipeFD   = "_K3S_ROOTLESS_FD"
	childEnv = "_K3S_ROOTLESS_SOCK"
	Sock     = ""
)

func Rootless(stateDir string) error {
	defer func() {
		os.Unsetenv(pipeFD)
		os.Unsetenv(childEnv)
	}()

	hasFD := os.Getenv(pipeFD) != ""
	hasChildEnv := os.Getenv(childEnv) != ""

	if hasFD {
		logrus.Debug("Running rootless child")
		childOpt, err := createChildOpt()
		if err != nil {
			logrus.Fatal(err)
		}
		if err := child.Child(*childOpt); err != nil {
			logrus.Fatalf("child died: %v", err)
		}
	}

	if hasChildEnv {
		Sock = os.Getenv(childEnv)
		logrus.Debug("Running rootless process")
		return setupMounts(stateDir)
	}

	logrus.Debug("Running rootless parent")
	if err := validateSysctl(); err != nil {
		logrus.Fatal(err)
	}
	parentOpt, err := createParentOpt(filepath.Join(stateDir, "rootless"))
	if err != nil {
		logrus.Fatal(err)
	}

	os.Setenv(childEnv, filepath.Join(parentOpt.StateDir, parent.StateFileAPISock))
	if err := parent.Parent(*parentOpt); err != nil {
		logrus.Fatal(err)
	}
	os.Exit(0)

	return nil
}

func validateSysctl() error {
	expected := map[string]string{
		// kernel.unprivileged_userns_clone needs to be 1 to allow userns on some distros.
		"kernel.unprivileged_userns_clone": "1",

		// net.ipv4.ip_forward should not need to be 1 in the parent namespace.
		// However, the current k3s implementation has a bug that requires net.ipv4.ip_forward=1
		// https://github.com/rancher/k3s/issues/2420#issuecomment-715051120
		"net.ipv4.ip_forward": "1",
	}
	for key, expectedValue := range expected {
		if actualValue, err := readSysctl(key); err == nil {
			if expectedValue != actualValue {
				return errors.Errorf("expected sysctl value %q to be %q, got %q; try adding \"%s=%s\" to /etc/sysctl.conf and running `sudo sysctl --system`",
					key, expectedValue, actualValue, key, expectedValue)
			}
		}
	}
	return nil
}

func readSysctl(key string) (string, error) {
	p := "/proc/sys/" + strings.ReplaceAll(key, ".", "/")
	b, err := ioutil.ReadFile(p)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func parseCIDR(s string) (*net.IPNet, error) {
	if s == "" {
		return nil, nil
	}
	ip, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		return nil, err
	}
	if !ip.Equal(ipnet.IP) {
		return nil, errors.Errorf("cidr must be like 10.0.2.0/24, not like 10.0.2.100/24")
	}
	return ipnet, nil
}

func createParentOpt(stateDir string) (*parent.Opt, error) {
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to mkdir %s", stateDir)
	}

	stateDir, err := ioutil.TempDir("", "rootless")
	if err != nil {
		return nil, err
	}

	opt := &parent.Opt{
		StateDir:       stateDir,
		CreatePIDNS:    true,
		CreateCgroupNS: true,
		CreateUTSNS:    true,
		CreateIPCNS:    true,
	}

	selfCgroupMap, err := cgroups.ParseCgroupFile("/proc/self/cgroup")
	if err != nil {
		return nil, err
	}
	if selfCgroup2 := selfCgroupMap[""]; selfCgroup2 == "" {
		logrus.Warnf("enabling cgroup2 is highly recommended, see https://rootlesscontaine.rs/getting-started/common/cgroup2/")
	}

	mtu := 0
	ipnet, err := parseCIDR("10.41.0.0/16")
	if err != nil {
		return nil, err
	}
	disableHostLoopback := true
	binary := "slirp4netns"
	if _, err := exec.LookPath(binary); err != nil {
		return nil, err
	}
	debugWriter := &logrusDebugWriter{}
	opt.NetworkDriver, err = slirp4netns.NewParentDriver(debugWriter, binary, mtu, ipnet, "tap0", disableHostLoopback, "", false, false, false)
	if err != nil {
		return nil, err
	}

	opt.PortDriver, err = portbuiltin.NewParentDriver(debugWriter, stateDir)
	if err != nil {
		return nil, err
	}

	opt.PipeFDEnvKey = pipeFD

	return opt, nil
}

type logrusDebugWriter struct {
}

func (w *logrusDebugWriter) Write(p []byte) (int, error) {
	s := strings.TrimSuffix(string(p), "\n")
	logrus.Debug(s)
	return len(p), nil
}

func createChildOpt() (*child.Opt, error) {
	opt := &child.Opt{}
	opt.TargetCmd = os.Args
	opt.PipeFDEnvKey = pipeFD
	opt.NetworkDriver = slirp4netns.NewChildDriver()
	opt.PortDriver = portbuiltin.NewChildDriver(&logrusDebugWriter{})
	opt.CopyUpDirs = []string{"/etc", "/var/run", "/run", "/var/lib"}
	opt.CopyUpDriver = tmpfssymlink.NewChildDriver()
	opt.MountProcfs = true
	opt.Reaper = true
	return opt, nil
}
