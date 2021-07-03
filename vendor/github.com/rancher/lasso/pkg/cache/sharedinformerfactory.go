package cache

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/client"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

type TweakListOptionsFunc func(*v1.ListOptions)

type SharedCacheFactoryOptions struct {
	DefaultResync    time.Duration
	DefaultNamespace string
	DefaultTweakList TweakListOptionsFunc

	KindResync     map[schema.GroupVersionKind]time.Duration
	KindNamespace  map[schema.GroupVersionKind]string
	KindTweakList  map[schema.GroupVersionKind]TweakListOptionsFunc
	HealthCallback func(healthy bool)
}

type sharedCacheFactory struct {
	lock sync.Mutex

	tweakList           TweakListOptionsFunc
	defaultResync       time.Duration
	defaultNamespace    string
	customResync        map[schema.GroupVersionKind]time.Duration
	customNamespaces    map[schema.GroupVersionKind]string
	customTweakList     map[schema.GroupVersionKind]TweakListOptionsFunc
	sharedClientFactory client.SharedClientFactory
	healthcheck         healthcheck

	caches        map[schema.GroupVersionKind]cache.SharedIndexInformer
	startedCaches map[schema.GroupVersionKind]bool
}

// NewSharedInformerFactoryWithOptions constructs a new instance of a SharedInformerFactory with additional options.
func NewSharedCachedFactory(sharedClientFactory client.SharedClientFactory, opts *SharedCacheFactoryOptions) SharedCacheFactory {
	opts = applyDefaults(opts)

	factory := &sharedCacheFactory{
		lock:                sync.Mutex{},
		tweakList:           opts.DefaultTweakList,
		defaultResync:       opts.DefaultResync,
		defaultNamespace:    opts.DefaultNamespace,
		customResync:        opts.KindResync,
		customNamespaces:    opts.KindNamespace,
		customTweakList:     opts.KindTweakList,
		caches:              map[schema.GroupVersionKind]cache.SharedIndexInformer{},
		startedCaches:       map[schema.GroupVersionKind]bool{},
		sharedClientFactory: sharedClientFactory,
		healthcheck: healthcheck{
			callback: opts.HealthCallback,
		},
	}

	return factory
}

func applyDefaults(opts *SharedCacheFactoryOptions) *SharedCacheFactoryOptions {
	var newOpts SharedCacheFactoryOptions
	if opts != nil {
		newOpts = *opts
	}

	return &newOpts
}

func (f *sharedCacheFactory) StartGVK(ctx context.Context, gvk schema.GroupVersionKind) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	informer, ok := f.caches[gvk]
	if !ok {
		return nil
	}

	if !f.startedCaches[gvk] {
		go informer.Run(ctx.Done())
		f.startedCaches[gvk] = true
	}

	return nil
}

func (f *sharedCacheFactory) Start(ctx context.Context) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.healthcheck.start(ctx, f.sharedClientFactory); err != nil {
		return err
	}

	for informerType, informer := range f.caches {
		if !f.startedCaches[informerType] {
			go informer.Run(ctx.Done())
			f.startedCaches[informerType] = true
		}
	}

	return nil
}

func (f *sharedCacheFactory) WaitForCacheSync(ctx context.Context) map[schema.GroupVersionKind]bool {
	informers := func() map[schema.GroupVersionKind]cache.SharedIndexInformer {
		f.lock.Lock()
		defer f.lock.Unlock()

		informers := map[schema.GroupVersionKind]cache.SharedIndexInformer{}
		for informerType, informer := range f.caches {
			if f.startedCaches[informerType] {
				informers[informerType] = informer
			}
		}
		return informers
	}()

	res := map[schema.GroupVersionKind]bool{}
	for informType, informer := range informers {
		res[informType] = cache.WaitForCacheSync(ctx.Done(), informer.HasSynced)
	}
	return res
}

func (f *sharedCacheFactory) ForObject(obj runtime.Object) (cache.SharedIndexInformer, error) {
	return f.ForKind(obj.GetObjectKind().GroupVersionKind())
}

func (f *sharedCacheFactory) ForResource(gvr schema.GroupVersionResource, namespaced bool) (cache.SharedIndexInformer, error) {
	return f.ForResourceKind(gvr, "", namespaced)
}

func (f *sharedCacheFactory) ForKind(gvk schema.GroupVersionKind) (cache.SharedIndexInformer, error) {
	gvr, namespaced, err := f.sharedClientFactory.ResourceForGVK(gvk)
	if err != nil {
		return nil, err
	}
	return f.ForResourceKind(gvr, gvk.Kind, namespaced)
}

func (f *sharedCacheFactory) ForResourceKind(gvr schema.GroupVersionResource, kind string, namespaced bool) (cache.SharedIndexInformer, error) {
	var (
		gvk schema.GroupVersionKind
		err error
	)

	if kind == "" {
		gvk, err = f.sharedClientFactory.GVKForResource(gvr)
		if err != nil {
			return nil, err
		}
	} else {
		gvk = gvr.GroupVersion().WithKind(kind)
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	informer, ok := f.caches[gvk]
	if ok {
		return informer, nil
	}

	resyncPeriod, ok := f.customResync[gvk]
	if !ok {
		resyncPeriod = f.defaultResync
	}

	namespace, ok := f.customNamespaces[gvk]
	if !ok {
		namespace = f.defaultNamespace
	}

	tweakList, ok := f.customTweakList[gvk]
	if !ok {
		tweakList = f.tweakList
	}

	obj, objList, err := f.sharedClientFactory.NewObjects(gvk)
	if err != nil {
		return nil, err
	}

	client := f.sharedClientFactory.ForResourceKind(gvr, kind, namespaced)

	cache := NewCache(obj, objList, client, &Options{
		Namespace:   namespace,
		Resync:      resyncPeriod,
		TweakList:   tweakList,
		WaitHealthy: f.healthcheck.ensureHealthy,
	})
	f.caches[gvk] = cache

	return cache, nil
}

func (f *sharedCacheFactory) SharedClientFactory() client.SharedClientFactory {
	return f.sharedClientFactory
}

type SharedCacheFactory interface {
	Start(ctx context.Context) error
	StartGVK(ctx context.Context, gvk schema.GroupVersionKind) error
	ForObject(obj runtime.Object) (cache.SharedIndexInformer, error)
	ForKind(gvk schema.GroupVersionKind) (cache.SharedIndexInformer, error)
	ForResource(gvr schema.GroupVersionResource, namespaced bool) (cache.SharedIndexInformer, error)
	ForResourceKind(gvr schema.GroupVersionResource, kind string, namespaced bool) (cache.SharedIndexInformer, error)
	WaitForCacheSync(ctx context.Context) map[schema.GroupVersionKind]bool
	SharedClientFactory() client.SharedClientFactory
}
