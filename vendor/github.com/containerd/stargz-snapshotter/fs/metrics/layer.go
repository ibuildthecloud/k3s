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

package metrics

import (
	"github.com/containerd/stargz-snapshotter/fs/layer"
	metrics "github.com/docker/go-metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var layerMetrics = []*metric{
	{
		name: "layer_fetched_size",
		help: "Total fetched size of the layer",
		unit: metrics.Bytes,
		vt:   prometheus.CounterValue,
		getValues: func(l layer.Layer) []value {
			return []value{
				{
					v: float64(l.Info().FetchedSize),
				},
			}
		},
	},
	{
		name: "layer_size",
		help: "Total size of the layer",
		unit: metrics.Bytes,
		vt:   prometheus.CounterValue,
		getValues: func(l layer.Layer) []value {
			return []value{
				{
					v: float64(l.Info().Size),
				},
			}
		},
	},
}
