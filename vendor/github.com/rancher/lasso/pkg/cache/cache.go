package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/rancher/lasso/pkg/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type Options struct {
	Namespace   string
	Resync      time.Duration
	TweakList   TweakListOptionsFunc
	WaitHealthy func(ctx context.Context)
}

func NewCache(obj, listObj runtime.Object, client *client.Client, opts *Options) cache.SharedIndexInformer {
	indexers := cache.Indexers{}

	if client.Namespaced {
		indexers[cache.NamespaceIndex] = cache.MetaNamespaceIndexFunc
	}

	opts = applyDefaultCacheOptions(opts)

	lw := &deferredListWatcher{
		client:      client,
		tweakList:   opts.TweakList,
		namespace:   opts.Namespace,
		listObj:     listObj,
		waitHealthy: opts.WaitHealthy,
	}

	return &deferredCache{
		SharedIndexInformer: cache.NewSharedIndexInformer(
			lw,
			obj,
			opts.Resync,
			indexers,
		),
		deferredListWatcher: lw,
	}
}

func applyDefaultCacheOptions(opts *Options) *Options {
	var newOpts Options
	if opts != nil {
		newOpts = *opts
	}
	if newOpts.Resync == 0 {
		newOpts.Resync = 10 * time.Hour
	}
	if newOpts.TweakList == nil {
		newOpts.TweakList = func(*metav1.ListOptions) {}
	}
	return &newOpts
}

type deferredCache struct {
	cache.SharedIndexInformer
	deferredListWatcher *deferredListWatcher
}

type deferredListWatcher struct {
	lw          cache.ListerWatcher
	client      *client.Client
	tweakList   TweakListOptionsFunc
	namespace   string
	listObj     runtime.Object
	waitHealthy func(ctx context.Context)
}

func (d *deferredListWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	if d.lw == nil {
		return nil, fmt.Errorf("cache not started")
	}
	return d.lw.List(options)
}

func (d *deferredListWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	if d.lw == nil {
		return nil, fmt.Errorf("cache not started")
	}
	return d.lw.Watch(options)
}

func (d *deferredListWatcher) run(stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()

	d.lw = &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			d.tweakList(&options)
			// If ResourceVersion is set to 0 then the Limit is ignored on the API side. Usually
			// that's not a problem, but with very large counts of a single object type the client will
			// hit it's connection timeout
			if options.ResourceVersion == "0" {
				options.ResourceVersion = ""
			}
			listObj := d.listObj.DeepCopyObject()
			err := d.client.List(ctx, d.namespace, listObj, options)
			if err != nil && d.waitHealthy != nil {
				d.waitHealthy(ctx)
			}
			return listObj, err
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			d.tweakList(&options)
			return d.client.Watch(ctx, d.namespace, options)
		},
	}
}

func (d *deferredCache) Run(stopCh <-chan struct{}) {
	d.deferredListWatcher.run(stopCh)
	d.SharedIndexInformer.Run(stopCh)
}
