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

package cri

import (
	"context"
	"sync"
	"time"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/reference"
	distribution "github.com/containerd/containerd/reference/docker"
	"github.com/containerd/stargz-snapshotter/service/resolver"
	"github.com/pkg/errors"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

// NewCRIKeychain provides creds passed through CRI PullImage API.
// This also returns a CRI image service server that works as a proxy backed by the specified CRI service.
// This server reads all PullImageRequest and uses PullImageRequest.AuthConfig for authenticating snapshots.
func NewCRIKeychain(ctx context.Context, connectCRI func() (runtime.ImageServiceClient, error)) (resolver.Credential, runtime.ImageServiceServer) {
	server := &instrumentedService{config: make(map[string]*runtime.AuthConfig)}
	go func() {
		log.G(ctx).Debugf("Waiting for CRI service is started...")
		for i := 0; i < 100; i++ {
			client, err := connectCRI()
			if err == nil {
				server.criMu.Lock()
				server.cri = client
				server.criMu.Unlock()
				log.G(ctx).Info("connected to backend CRI service")
				return
			}
			log.G(ctx).WithError(err).Warnf("failed to connect to CRI")
			time.Sleep(10 * time.Second)
		}
		log.G(ctx).Warnf("no connection is available to CRI")
	}()
	return server.credentials, server
}

type instrumentedService struct {
	cri   runtime.ImageServiceClient
	criMu sync.Mutex

	config   map[string]*runtime.AuthConfig
	configMu sync.Mutex
}

func (in *instrumentedService) credentials(host string, refspec reference.Spec) (string, string, error) {
	if host == "docker.io" || host == "registry-1.docker.io" {
		// Creds of "docker.io" is stored keyed by "https://index.docker.io/v1/".
		host = "index.docker.io"
	}
	in.configMu.Lock()
	defer in.configMu.Unlock()
	if cfg, ok := in.config[refspec.String()]; ok {
		return resolver.ParseAuth(cfg, host)
	}
	return "", "", nil
}

func (in *instrumentedService) getCRI() (c runtime.ImageServiceClient) {
	in.criMu.Lock()
	c = in.cri
	in.criMu.Unlock()
	return
}

func (in *instrumentedService) ListImages(ctx context.Context, r *runtime.ListImagesRequest) (res *runtime.ListImagesResponse, err error) {
	cri := in.getCRI()
	if cri == nil {
		return nil, errors.New("server is not initialized yet")
	}
	return cri.ListImages(ctx, r)
}

func (in *instrumentedService) ImageStatus(ctx context.Context, r *runtime.ImageStatusRequest) (res *runtime.ImageStatusResponse, err error) {
	cri := in.getCRI()
	if cri == nil {
		return nil, errors.New("server is not initialized yet")
	}
	return cri.ImageStatus(ctx, r)
}

func (in *instrumentedService) PullImage(ctx context.Context, r *runtime.PullImageRequest) (res *runtime.PullImageResponse, err error) {
	cri := in.getCRI()
	if cri == nil {
		return nil, errors.New("server is not initialized yet")
	}
	refspec, err := parseReference(r.GetImage().GetImage())
	if err != nil {
		return nil, err
	}
	in.configMu.Lock()
	in.config[refspec.String()] = r.GetAuth()
	in.configMu.Unlock()
	return cri.PullImage(ctx, r)
}

func (in *instrumentedService) RemoveImage(ctx context.Context, r *runtime.RemoveImageRequest) (_ *runtime.RemoveImageResponse, err error) {
	cri := in.getCRI()
	if cri == nil {
		return nil, errors.New("server is not initialized yet")
	}
	refspec, err := parseReference(r.GetImage().GetImage())
	if err != nil {
		return nil, err
	}
	in.configMu.Lock()
	delete(in.config, refspec.String())
	in.configMu.Unlock()
	return cri.RemoveImage(ctx, r)
}

func (in *instrumentedService) ImageFsInfo(ctx context.Context, r *runtime.ImageFsInfoRequest) (res *runtime.ImageFsInfoResponse, err error) {
	cri := in.getCRI()
	if cri == nil {
		return nil, errors.New("server is not initialized yet")
	}
	return cri.ImageFsInfo(ctx, r)
}

func parseReference(ref string) (reference.Spec, error) {
	namedRef, err := distribution.ParseDockerRef(ref)
	if err != nil {
		return reference.Spec{}, errors.Wrapf(err, "failed to parse image reference %q", ref)
	}
	return reference.Parse(namedRef.String())
}
