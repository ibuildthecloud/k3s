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

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package config

const (
	// TargetSkipVerifyLabel is a snapshot label key that indicates to skip content
	// verification for the layer.
	TargetSkipVerifyLabel = "containerd.io/snapshot/remote/stargz.skipverify"

	// TargetPrefetchSizeLabel is a snapshot label key that indicates size to prefetch
	// the layer. If the layer is eStargz and contains prefetch landmarks, these config
	// will be respeced.
	TargetPrefetchSizeLabel = "containerd.io/snapshot/remote/stargz.prefetch"
)

type Config struct {
	HTTPCacheType       string `toml:"http_cache_type"`
	FSCacheType         string `toml:"filesystem_cache_type"`
	ResolveResultEntry  int    `toml:"resolve_result_entry"`
	PrefetchSize        int64  `toml:"prefetch_size"`
	PrefetchTimeoutSec  int64  `toml:"prefetch_timeout_sec"`
	NoPrefetch          bool   `toml:"noprefetch"`
	NoBackgroundFetch   bool   `toml:"no_background_fetch"`
	Debug               bool   `toml:"debug"`
	AllowNoVerification bool   `toml:"allow_no_verification"`
	DisableVerification bool   `toml:"disable_verification"`
	MaxConcurrency      int64  `toml:"max_concurrency"`
	NoPrometheus        bool   `toml:"no_prometheus"`

	// BlobConfig is config for layer blob management.
	BlobConfig `toml:"blob"`

	// DirectoryCacheConfig is config for directory-based cache.
	DirectoryCacheConfig `toml:"directory_cache"`
}

type BlobConfig struct {
	ValidInterval   int64 `toml:"valid_interval"`
	CheckAlways     bool  `toml:"check_always"`
	ChunkSize       int64 `toml:"chunk_size"`
	FetchTimeoutSec int64 `toml:"fetching_timeout_sec"`
}

type DirectoryCacheConfig struct {
	MaxLRUCacheEntry int  `toml:"max_lru_cache_entry"`
	MaxCacheFds      int  `toml:"max_cache_fds"`
	SyncAdd          bool `toml:"sync_add"`
}
