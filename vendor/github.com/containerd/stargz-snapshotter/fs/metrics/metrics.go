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
	"sync"

	"github.com/containerd/stargz-snapshotter/fs/layer"
	metrics "github.com/docker/go-metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func NewLayerMetrics(ns *metrics.Namespace) *Controller {
	if ns == nil {
		return &Controller{}
	}
	c := &Controller{
		ns:    ns,
		layer: make(map[string]layer.Layer),
	}
	c.metrics = append(c.metrics, layerMetrics...)
	ns.Add(c)
	return c
}

type Controller struct {
	ns      *metrics.Namespace
	metrics []*metric

	layer   map[string]layer.Layer
	layerMu sync.RWMutex
}

func (c *Controller) Describe(ch chan<- *prometheus.Desc) {
	for _, e := range c.metrics {
		ch <- e.desc(c.ns)
	}
}

func (c *Controller) Collect(ch chan<- prometheus.Metric) {
	c.layerMu.RLock()
	wg := &sync.WaitGroup{}
	for mp, l := range c.layer {
		mp, l := mp, l
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, e := range c.metrics {
				e.collect(mp, l, c.ns, ch)
			}
		}()
	}
	c.layerMu.RUnlock()
	wg.Wait()
}

func (c *Controller) Add(key string, l layer.Layer) {
	if c.ns == nil {
		return
	}
	c.layerMu.Lock()
	c.layer[key] = l
	c.layerMu.Unlock()
}

func (c *Controller) Remove(key string) {
	if c.ns == nil {
		return
	}
	c.layerMu.Lock()
	delete(c.layer, key)
	c.layerMu.Unlock()
}

type value struct {
	v float64
	l []string
}

type metric struct {
	name   string
	help   string
	unit   metrics.Unit
	vt     prometheus.ValueType
	labels []string
	// getValues returns the value and labels for the data
	getValues func(l layer.Layer) []value
}

func (m *metric) desc(ns *metrics.Namespace) *prometheus.Desc {
	return ns.NewDesc(m.name, m.help, m.unit, append([]string{"digest", "mountpoint"}, m.labels...)...)
}

func (m *metric) collect(mountpoint string, l layer.Layer, ns *metrics.Namespace, ch chan<- prometheus.Metric) {
	values := m.getValues(l)
	for _, v := range values {
		ch <- prometheus.MustNewConstMetric(m.desc(ns), m.vt, v.v, append([]string{l.Info().Digest.String(), mountpoint}, v.l...)...)
	}
}
