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
	"github.com/containerd/stargz-snapshotter/fs/config"
	"github.com/containerd/stargz-snapshotter/service/resolver"
)

type Config struct {
	config.Config

	// KubeconfigKeychainConfig is config for kubeconfig-based keychain.
	KubeconfigKeychainConfig `toml:"kubeconfig_keychain"`

	// CRIKeychainConfig is config for CRI-based keychain.
	CRIKeychainConfig `toml:"cri_keychain"`

	// ResolverConfig is config for resolving registries.
	ResolverConfig `toml:"resolver"`
}

// KubeconfigKeychainConfig is config for kubeconfig-based keychain.
type KubeconfigKeychainConfig struct {
	// EnableKeychain enables kubeconfig-based keychain
	EnableKeychain bool `toml:"enable_keychain"`

	// KubeconfigPath is the path to kubeconfig which can be used to sync
	// secrets on the cluster into this snapshotter.
	KubeconfigPath string `toml:"kubeconfig_path"`
}

// CRIKeychainConfig is config for CRI-based keychain.
type CRIKeychainConfig struct {
	// EnableKeychain enables CRI-based keychain
	EnableKeychain bool `toml:"enable_keychain"`

	// ImageServicePath is the path to the unix socket of backing CRI Image Service (e.g. containerd CRI plugin)
	ImageServicePath string `toml:"image_service_path"`
}

// ResolverConfig is config for resolving registries.
type ResolverConfig resolver.Config
