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

package dockerconfig

import (
	"context"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/stargz-snapshotter/service/resolver"
	"github.com/docker/cli/cli/config"
)

func NewDockerconfigKeychain(ctx context.Context) resolver.Credential {
	cf, err := config.Load("")
	if err != nil {
		log.G(ctx).WithError(err).Warnf("failed to load docker config file")
		return func(string, reference.Spec) (string, string, error) {
			return "", "", nil
		}
	}
	return func(host string, refspec reference.Spec) (string, string, error) {
		if host == "docker.io" || host == "registry-1.docker.io" {
			// Creds of docker.io is stored keyed by "https://index.docker.io/v1/".
			host = "https://index.docker.io/v1/"
		}
		ac, err := cf.GetAuthConfig(host)
		if err != nil {
			return "", "", err
		}
		if ac.IdentityToken != "" {
			return "", ac.IdentityToken, nil
		}
		return ac.Username, ac.Password, nil
	}
}
