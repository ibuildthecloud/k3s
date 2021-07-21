package apply

import (
	"context"
	"fmt"
	"sync"

	"github.com/rancher/wrangler/pkg/apply/injectors"
	"github.com/rancher/wrangler/pkg/objectset"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	defaultNamespace = "default"
)

type Patcher func(namespace, name string, pt types.PatchType, data []byte) (runtime.Object, error)

// return false if the Reconciler did not handler this object
type Reconciler func(oldObj runtime.Object, newObj runtime.Object) (bool, error)

type ClientFactory func(gvr schema.GroupVersionResource) (dynamic.NamespaceableResourceInterface, error)

type InformerFactory interface {
	Get(gvk schema.GroupVersionKind, gvr schema.GroupVersionResource) (cache.SharedIndexInformer, error)
}

type InformerGetter interface {
	Informer() cache.SharedIndexInformer
	GroupVersionKind() schema.GroupVersionKind
}

type PatchByGVK map[schema.GroupVersionKind]map[objectset.ObjectKey]string

func (p PatchByGVK) Add(gvk schema.GroupVersionKind, namespace, name, patch string) {
	d, ok := p[gvk]
	if !ok {
		d = map[objectset.ObjectKey]string{}
		p[gvk] = d
	}
	d[objectset.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}] = patch
}

type Plan struct {
	Create  objectset.ObjectKeyByGVK
	Delete  objectset.ObjectKeyByGVK
	Update  PatchByGVK
	Objects []runtime.Object
}

type Apply interface {
	Apply(set *objectset.ObjectSet) error
	ApplyObjects(objs ...runtime.Object) error
	WithContext(ctx context.Context) Apply
	WithCacheTypes(igs ...InformerGetter) Apply
	WithCacheTypeFactory(factory InformerFactory) Apply
	WithSetID(id string) Apply
	WithOwner(obj runtime.Object) Apply
	WithOwnerKey(key string, gvk schema.GroupVersionKind) Apply
	WithInjector(injs ...injectors.ConfigInjector) Apply
	WithInjectorName(injs ...string) Apply
	WithPatcher(gvk schema.GroupVersionKind, patchers Patcher) Apply
	WithReconciler(gvk schema.GroupVersionKind, reconciler Reconciler) Apply
	WithStrictCaching() Apply
	WithDynamicLookup() Apply
	WithRestrictClusterScoped() Apply
	WithDefaultNamespace(ns string) Apply
	WithListerNamespace(ns string) Apply
	WithRateLimiting(ratelimitingQps float32) Apply
	WithNoDelete() Apply
	WithGVK(gvks ...schema.GroupVersionKind) Apply
	WithSetOwnerReference(controller, block bool) Apply
	WithIgnorePreviousApplied() Apply
	WithDiffPatch(gvk schema.GroupVersionKind, namespace, name string, patch []byte) Apply

	FindOwner(obj runtime.Object) (runtime.Object, error)
	PurgeOrphan(obj runtime.Object) error
	DryRun(objs ...runtime.Object) (Plan, error)
}

func NewForConfig(cfg *rest.Config) (Apply, error) {
	discovery, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return New(discovery, NewClientFactory(cfg)), nil
}

func New(discovery discovery.DiscoveryInterface, cf ClientFactory, igs ...InformerGetter) Apply {
	a := &apply{
		clients: &clients{
			clientFactory: cf,
			discovery:     discovery,
			namespaced:    map[schema.GroupVersionKind]bool{},
			gvkToGVR:      map[schema.GroupVersionKind]schema.GroupVersionResource{},
			clients:       map[schema.GroupVersionKind]dynamic.NamespaceableResourceInterface{},
		},
		informers: map[schema.GroupVersionKind]cache.SharedIndexInformer{},
	}

	for _, ig := range igs {
		a.informers[ig.GroupVersionKind()] = ig.Informer()
	}

	return a
}

type apply struct {
	clients   *clients
	informers map[schema.GroupVersionKind]cache.SharedIndexInformer
}

type clients struct {
	sync.Mutex

	clientFactory ClientFactory
	discovery     discovery.DiscoveryInterface
	namespaced    map[schema.GroupVersionKind]bool
	gvkToGVR      map[schema.GroupVersionKind]schema.GroupVersionResource
	clients       map[schema.GroupVersionKind]dynamic.NamespaceableResourceInterface
}

func (c *clients) IsNamespaced(gvk schema.GroupVersionKind) (bool, error) {
	c.Lock()
	ok, exists := c.namespaced[gvk]
	c.Unlock()

	if exists {
		return ok, nil
	}
	_, err := c.client(gvk)
	if err != nil {
		return false, err
	}

	c.Lock()
	defer c.Unlock()
	return c.namespaced[gvk], nil
}

func (c *clients) gvr(gvk schema.GroupVersionKind) schema.GroupVersionResource {
	c.Lock()
	defer c.Unlock()
	return c.gvkToGVR[gvk]
}

func (c *clients) client(gvk schema.GroupVersionKind) (dynamic.NamespaceableResourceInterface, error) {
	c.Lock()
	defer c.Unlock()

	if client, ok := c.clients[gvk]; ok {
		return client, nil
	}

	resources, err := c.discovery.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		return nil, err
	}

	for _, resource := range resources.APIResources {
		if resource.Kind != gvk.Kind {
			continue
		}

		client, err := c.clientFactory(gvk.GroupVersion().WithResource(resource.Name))
		if err != nil {
			return nil, err
		}

		c.namespaced[gvk] = resource.Namespaced
		c.clients[gvk] = client
		c.gvkToGVR[gvk] = gvk.GroupVersion().WithResource(resource.Name)
		return client, nil
	}

	return nil, fmt.Errorf("failed to discover client for %s", gvk)
}

func (a *apply) newDesiredSet() desiredSet {
	return desiredSet{
		a:                a,
		defaultNamespace: defaultNamespace,
		ctx:              context.Background(),
		ratelimitingQps:  1,
		reconcilers:      defaultReconcilers,
		strictCaching:    true,
	}
}

func (a *apply) DryRun(objs ...runtime.Object) (Plan, error) {
	return a.newDesiredSet().DryRun(objs...)
}

func (a *apply) Apply(set *objectset.ObjectSet) error {
	return a.newDesiredSet().Apply(set)
}

func (a *apply) ApplyObjects(objs ...runtime.Object) error {
	os := objectset.NewObjectSet()
	os.Add(objs...)
	return a.newDesiredSet().Apply(os)
}

func (a *apply) WithSetID(id string) Apply {
	return a.newDesiredSet().WithSetID(id)
}

func (a *apply) WithOwner(obj runtime.Object) Apply {
	return a.newDesiredSet().WithOwner(obj)
}

func (a *apply) WithOwnerKey(key string, gvk schema.GroupVersionKind) Apply {
	return a.newDesiredSet().WithOwnerKey(key, gvk)
}

func (a *apply) WithInjector(injs ...injectors.ConfigInjector) Apply {
	return a.newDesiredSet().WithInjector(injs...)
}

func (a *apply) WithInjectorName(injs ...string) Apply {
	return a.newDesiredSet().WithInjectorName(injs...)
}

func (a *apply) WithCacheTypes(igs ...InformerGetter) Apply {
	return a.newDesiredSet().WithCacheTypes(igs...)
}

func (a *apply) WithCacheTypeFactory(factory InformerFactory) Apply {
	return a.newDesiredSet().WithCacheTypeFactory(factory)
}

func (a *apply) WithGVK(gvks ...schema.GroupVersionKind) Apply {
	return a.newDesiredSet().WithGVK(gvks...)
}

func (a *apply) WithPatcher(gvk schema.GroupVersionKind, patcher Patcher) Apply {
	return a.newDesiredSet().WithPatcher(gvk, patcher)
}

func (a *apply) WithReconciler(gvk schema.GroupVersionKind, reconciler Reconciler) Apply {
	return a.newDesiredSet().WithReconciler(gvk, reconciler)
}

func (a *apply) WithStrictCaching() Apply {
	return a.newDesiredSet().WithStrictCaching()
}

func (a *apply) WithDynamicLookup() Apply {
	return a.newDesiredSet().WithDynamicLookup()
}

func (a *apply) WithRestrictClusterScoped() Apply {
	return a.newDesiredSet().WithRestrictClusterScoped()
}

func (a *apply) WithDefaultNamespace(ns string) Apply {
	return a.newDesiredSet().WithDefaultNamespace(ns)
}

func (a *apply) WithListerNamespace(ns string) Apply {
	return a.newDesiredSet().WithListerNamespace(ns)
}

func (a *apply) WithRateLimiting(ratelimitingQps float32) Apply {
	return a.newDesiredSet().WithRateLimiting(ratelimitingQps)
}

func (a *apply) WithNoDelete() Apply {
	return a.newDesiredSet().WithNoDelete()
}

func (a *apply) WithSetOwnerReference(controller, block bool) Apply {
	return a.newDesiredSet().WithSetOwnerReference(controller, block)
}

func (a *apply) WithContext(ctx context.Context) Apply {
	return a.newDesiredSet().WithContext(ctx)
}

func (a *apply) WithIgnorePreviousApplied() Apply {
	return a.newDesiredSet().WithIgnorePreviousApplied()
}

func (a *apply) FindOwner(obj runtime.Object) (runtime.Object, error) {
	return a.newDesiredSet().FindOwner(obj)
}

func (a *apply) PurgeOrphan(obj runtime.Object) error {
	return a.newDesiredSet().PurgeOrphan(obj)
}

func (a *apply) WithDiffPatch(gvk schema.GroupVersionKind, namespace, name string, patch []byte) Apply {
	return a.newDesiredSet().WithDiffPatch(gvk, namespace, name, patch)
}
