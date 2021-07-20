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

package service

import (
	"fmt"
	"strings"

	"github.com/containerd/containerd/reference"
	"github.com/containerd/stargz-snapshotter/fs/source"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// targetRefLabel is a label which contains image reference passed from CRI plugin.
	targetRefLabel = "containerd.io/snapshot/cri.image-ref"

	// targetDigestLabel is a label which contains layer digest passed from CRI plugin.
	targetDigestLabel = "containerd.io/snapshot/cri.layer-digest"

	// targetImageLayersLabel is a label which contains layer digests contained in
	// the target image and is passed from CRI plugin.
	targetImageLayersLabel = "containerd.io/snapshot/cri.image-layers"
)

func sourceFromCRILabels(hosts source.RegistryHosts) source.GetSources {
	return func(labels map[string]string) ([]source.Source, error) {
		refStr, ok := labels[targetRefLabel]
		if !ok {
			return nil, fmt.Errorf("reference hasn't been passed")
		}
		refspec, err := reference.Parse(refStr)
		if err != nil {
			return nil, err
		}

		digestStr, ok := labels[targetDigestLabel]
		if !ok {
			return nil, fmt.Errorf("digest hasn't been passed")
		}
		target, err := digest.Parse(digestStr)
		if err != nil {
			return nil, err
		}

		var layersDgst []digest.Digest
		if l, ok := labels[targetImageLayersLabel]; ok {
			layersStr := strings.Split(l, ",")
			for _, l := range layersStr {
				d, err := digest.Parse(l)
				if err != nil {
					return nil, err
				}
				if d.String() != target.String() {
					layersDgst = append(layersDgst, d)
				}
			}
		}

		var layers []ocispec.Descriptor
		for _, dgst := range append([]digest.Digest{target}, layersDgst...) {
			layers = append(layers, ocispec.Descriptor{Digest: dgst})
		}
		return []source.Source{
			{
				Hosts:    hosts,
				Name:     refspec,
				Target:   ocispec.Descriptor{Digest: target},
				Manifest: ocispec.Manifest{Layers: layers},
			},
		}, nil
	}
}
