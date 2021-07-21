package client

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

type Client struct {
	RESTClient rest.Interface
	timeout    time.Duration
	Namespaced bool
	GVR        schema.GroupVersionResource
	resource   string
	prefix     []string
	apiVersion string
	kind       string
}

func IsNamespaced(gvr schema.GroupVersionResource, mapper meta.RESTMapper) (bool, error) {
	kind, err := mapper.KindFor(gvr)
	if err != nil {
		return false, err
	}

	mapping, err := mapper.RESTMapping(kind.GroupKind(), kind.Version)
	if err != nil {
		return false, err
	}

	return mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

func NewClient(gvr schema.GroupVersionResource, kind string, namespaced bool, client rest.Interface, defaultTimeout time.Duration) *Client {
	var (
		prefix []string
	)

	if gvr.Group == "" {
		prefix = []string{
			"api",
			gvr.Version,
		}
	} else {
		prefix = []string{
			"apis",
			gvr.Group,
			gvr.Version,
		}
	}

	c := &Client{
		RESTClient: client,
		timeout:    defaultTimeout,
		Namespaced: namespaced,
		GVR:        gvr,
		prefix:     prefix,
		resource:   gvr.Resource,
	}
	c.apiVersion, c.kind = gvr.GroupVersion().WithKind(kind).ToAPIVersionAndKind()
	return c
}

func noop() {}

func (c *Client) setupCtx(ctx context.Context, minTimeout time.Duration) (context.Context, func()) {
	if minTimeout == 0 && c.timeout == 0 {
		return ctx, noop
	}

	timeout := c.timeout
	if minTimeout > 0 && timeout < minTimeout {
		timeout = minTimeout
	}

	return context.WithTimeout(ctx, timeout)
}

func (c *Client) Get(ctx context.Context, namespace, name string, result runtime.Object, options metav1.GetOptions) (err error) {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	err = c.RESTClient.Get().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(name).
		VersionedParams(&options, metav1.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

func (c *Client) List(ctx context.Context, namespace string, result runtime.Object, opts metav1.ListOptions) (err error) {
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	r := c.RESTClient.Get()
	if namespace != "" {
		r = r.NamespaceIfScoped(namespace, c.Namespaced)
	}
	err = r.Resource(c.resource).
		Prefix(c.prefix...).
		VersionedParams(&opts, metav1.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

func (c *Client) Watch(ctx context.Context, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.injectKind(c.RESTClient.Get().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		VersionedParams(&opts, metav1.ParameterCodec).
		Timeout(timeout).
		Watch(ctx))
}

func (c *Client) Create(ctx context.Context, namespace string, obj, result runtime.Object, opts metav1.CreateOptions) (err error) {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	err = c.RESTClient.Post().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		VersionedParams(&opts, metav1.ParameterCodec).
		Body(obj).
		Do(ctx).
		Into(result)
	return
}

func (c *Client) Update(ctx context.Context, namespace string, obj, result runtime.Object, opts metav1.UpdateOptions) (err error) {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	err = c.RESTClient.Put().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(m.GetName()).
		VersionedParams(&opts, metav1.ParameterCodec).
		Body(obj).
		Do(ctx).
		Into(result)
	return
}

func (c *Client) UpdateStatus(ctx context.Context, namespace string, obj, result runtime.Object, opts metav1.UpdateOptions) (err error) {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	err = c.RESTClient.Put().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(m.GetName()).
		SubResource("status").
		VersionedParams(&opts, metav1.ParameterCodec).
		Body(obj).
		Do(ctx).
		Into(result)
	return
}

func (c *Client) Delete(ctx context.Context, namespace, name string, opts metav1.DeleteOptions) error {
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	return c.RESTClient.Delete().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

func (c *Client) DeleteCollection(ctx context.Context, namespace string, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.RESTClient.Delete().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		VersionedParams(&listOpts, metav1.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

func (c *Client) Patch(ctx context.Context, namespace, name string, pt types.PatchType, data []byte, result runtime.Object, opts metav1.PatchOptions, subresources ...string) (err error) {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx, 0)
	defer cancel()
	err = c.RESTClient.Patch(pt).
		Prefix(c.prefix...).
		Namespace(namespace).
		Resource(c.resource).
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, metav1.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}

func (c *Client) setKind(obj runtime.Object) {
	if c.kind == "" {
		return
	}
	if _, ok := obj.(*metav1.Status); !ok {
		if meta, err := meta.TypeAccessor(obj); err == nil {
			meta.SetKind(c.kind)
			meta.SetAPIVersion(c.apiVersion)
		}
	}
}

func (c *Client) injectKind(w watch.Interface, err error) (watch.Interface, error) {
	if c.kind == "" || err != nil {
		return w, err
	}

	eventChan := make(chan watch.Event)

	go func() {
		defer close(eventChan)
		for event := range w.ResultChan() {
			c.setKind(event.Object)
			eventChan <- event
		}
	}()

	return &watcher{
		Interface: w,
		eventChan: eventChan,
	}, nil
}

type watcher struct {
	watch.Interface
	eventChan chan watch.Event
}

func (w *watcher) ResultChan() <-chan watch.Event {
	return w.eventChan
}
