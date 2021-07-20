/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package resolver

import (
	"net/http"
	"time"

	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/stargz-snapshotter/fs/source"
)

const defaultRequestTimeoutSec = 30

// Config is config for resolving registries.
type Config struct {
	Host map[string]HostConfig `toml:"host"`
}

type HostConfig struct {
	Mirrors []MirrorConfig `toml:"mirrors"`
}

type MirrorConfig struct {

	// Host is the hostname of the host.
	Host string `toml:"host"`

	// Insecure is true means use http scheme instead of https.
	Insecure bool `toml:"insecure"`

	// RequestTimeoutSec is timeout seconds of each request to the registry.
	// RequestTimeoutSec == 0 indicates the default timeout (defaultRequestTimeoutSec).
	// RequestTimeoutSec < 0 indicates no timeout.
	RequestTimeoutSec int `toml:"request_timeout_sec"`
}

type Credential func(string, reference.Spec) (string, string, error)

// RegistryHostsFromConfig creates RegistryHosts (a set of registry configuration) from Config.
func RegistryHostsFromConfig(cfg Config, credsFuncs ...Credential) source.RegistryHosts {
	return func(ref reference.Spec) (hosts []docker.RegistryHost, _ error) {
		host := ref.Hostname()
		for _, h := range append(cfg.Host[host].Mirrors, MirrorConfig{
			Host: host,
		}) {
			tr := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
			if h.RequestTimeoutSec >= 0 {
				if h.RequestTimeoutSec == 0 {
					tr.Timeout = defaultRequestTimeoutSec * time.Second
				} else {
					tr.Timeout = time.Duration(h.RequestTimeoutSec) * time.Second
				}
			} // h.RequestTimeoutSec < 0 means "no timeout"
			config := docker.RegistryHost{
				Client:       tr,
				Host:         h.Host,
				Scheme:       "https",
				Path:         "/v2",
				Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
				Authorizer: docker.NewDockerAuthorizer(
					docker.WithAuthClient(tr),
					docker.WithAuthCreds(multiCredsFuncs(ref, credsFuncs...))),
			}
			if localhost, _ := docker.MatchLocalhost(config.Host); localhost || h.Insecure {
				config.Scheme = "http"
			}
			if config.Host == "docker.io" {
				config.Host = "registry-1.docker.io"
			}
			hosts = append(hosts, config)
		}
		return
	}
}

func multiCredsFuncs(ref reference.Spec, credsFuncs ...Credential) func(string) (string, string, error) {
	return func(host string) (string, string, error) {
		for _, f := range credsFuncs {
			if username, secret, err := f(host, ref); err != nil {
				return "", "", err
			} else if !(username == "" && secret == "") {
				return username, secret, nil
			}
		}
		return "", "", nil
	}
}
