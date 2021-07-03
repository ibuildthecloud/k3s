package generators

import (
	"fmt"
	"io"
	"strings"

	args2 "github.com/rancher/wrangler/pkg/controller-gen/args"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/gengo/args"
	"k8s.io/gengo/generator"
	"k8s.io/gengo/namer"
	"k8s.io/gengo/types"
)

func TypeGo(gv schema.GroupVersion, name *types.Name, args *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Generator {
	return &typeGo{
		name:       name,
		gv:         gv,
		args:       args,
		customArgs: customArgs,
		DefaultGen: generator.DefaultGen{
			OptionalName: strings.ToLower(name.Name),
		},
	}
}

type typeGo struct {
	generator.DefaultGen

	name       *types.Name
	gv         schema.GroupVersion
	args       *args.GeneratorArgs
	customArgs *args2.CustomArgs
}

func (f *typeGo) Imports(context *generator.Context) []string {
	packages := append(Imports,
		fmt.Sprintf("%s \"%s\"", f.gv.Version, f.name.Package))

	return packages
}

func (f *typeGo) Init(c *generator.Context, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")

	if err := f.DefaultGen.Init(c, w); err != nil {
		return err
	}

	t := c.Universe.Type(*f.name)
	m := map[string]interface{}{
		"type":       f.name.Name,
		"lowerName":  namer.IL(f.name.Name),
		"plural":     plural.Name(t),
		"version":    f.gv.Version,
		"namespaced": namespaced(t),
		"hasStatus":  hasStatus(t),
		"statusType": statusType(t),
	}

	sw.Do(typeBody, m)
	return sw.Error()
}

func statusType(t *types.Type) string {
	for _, m := range t.Members {
		if m.Name == "Status" {
			return m.Type.Name.Name
		}
	}
	return ""
}

func hasStatus(t *types.Type) bool {
	for _, m := range t.Members {
		if m.Name == "Status" && m.Type.Name.Package == t.Name.Package {
			return true
		}
	}
	return false
}

var typeBody = `
type {{.type}}Handler func(string, *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error)

type {{.type}}Controller interface {
    generic.ControllerMeta
	{{.type}}Client

	OnChange(ctx context.Context, name string, sync {{.type}}Handler)
	OnRemove(ctx context.Context, name string, sync {{.type}}Handler)
	Enqueue({{ if .namespaced}}namespace, {{end}}name string)
	EnqueueAfter({{ if .namespaced}}namespace, {{end}}name string, duration time.Duration)

	Cache() {{.type}}Cache
}

type {{.type}}Client interface {
	Create(*{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error)
	Update(*{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error)
{{ if .hasStatus -}}
	UpdateStatus(*{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error)
{{- end }}
	Delete({{ if .namespaced}}namespace, {{end}}name string, options *metav1.DeleteOptions) error
	Get({{ if .namespaced}}namespace, {{end}}name string, options metav1.GetOptions) (*{{.version}}.{{.type}}, error)
	List({{ if .namespaced}}namespace string, {{end}}opts metav1.ListOptions) (*{{.version}}.{{.type}}List, error)
	Watch({{ if .namespaced}}namespace string, {{end}}opts metav1.ListOptions) (watch.Interface, error)
	Patch({{ if .namespaced}}namespace, {{end}}name string, pt types.PatchType, data []byte, subresources ...string) (result *{{.version}}.{{.type}}, err error)
}

type {{.type}}Cache interface {
	Get({{ if .namespaced}}namespace, {{end}}name string) (*{{.version}}.{{.type}}, error)
	List({{ if .namespaced}}namespace string, {{end}}selector labels.Selector) ([]*{{.version}}.{{.type}}, error)

	AddIndexer(indexName string, indexer {{.type}}Indexer)
	GetByIndex(indexName, key string) ([]*{{.version}}.{{.type}}, error)
}

type {{.type}}Indexer func(obj *{{.version}}.{{.type}}) ([]string, error)

type {{.lowerName}}Controller struct {
	controller controller.SharedController
	client            *client.Client
	gvk               schema.GroupVersionKind
	groupResource     schema.GroupResource
}

func New{{.type}}Controller(gvk schema.GroupVersionKind, resource string, namespaced bool, controller controller.SharedControllerFactory) {{.type}}Controller {
	c := controller.ForResourceKind(gvk.GroupVersion().WithResource(resource), gvk.Kind, namespaced)
	return &{{.lowerName}}Controller{
		controller: c,
		client:     c.Client(),
		gvk:        gvk,
		groupResource: schema.GroupResource{
			Group:    gvk.Group,
			Resource: resource,
		},
	}
}

func From{{.type}}HandlerToHandler(sync {{.type}}Handler) generic.Handler {
	return func(key string, obj runtime.Object) (ret runtime.Object, err error) {
		var v *{{.version}}.{{.type}}
		if obj == nil {
			v, err = sync(key, nil)
		} else {
			v, err = sync(key, obj.(*{{.version}}.{{.type}}))
		}
		if v == nil {
			return nil, err
		}
		return v, err
	}
}

func (c *{{.lowerName}}Controller) Updater() generic.Updater {
	return func(obj runtime.Object) (runtime.Object, error) {
		newObj, err := c.Update(obj.(*{{.version}}.{{.type}}))
		if newObj == nil {
			return nil, err
		}
		return newObj, err
	}
}

func Update{{.type}}DeepCopyOnChange(client {{.type}}Client, obj *{{.version}}.{{.type}}, handler func(obj *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error)) (*{{.version}}.{{.type}}, error) {
	if obj == nil {
		return obj, nil
	}

	copyObj := obj.DeepCopy()
	newObj, err := handler(copyObj)
	if newObj != nil {
		copyObj = newObj
	}
	if obj.ResourceVersion == copyObj.ResourceVersion && !equality.Semantic.DeepEqual(obj, copyObj) {
		return client.Update(copyObj)
	}

	return copyObj, err
}

func (c *{{.lowerName}}Controller) AddGenericHandler(ctx context.Context, name string, handler generic.Handler) {
	c.controller.RegisterHandler(ctx, name, controller.SharedControllerHandlerFunc(handler))
}

func (c *{{.lowerName}}Controller) AddGenericRemoveHandler(ctx context.Context, name string, handler generic.Handler) {
	c.AddGenericHandler(ctx, name, generic.NewRemoveHandler(name, c.Updater(), handler))
}

func (c *{{.lowerName}}Controller) OnChange(ctx context.Context, name string, sync {{.type}}Handler) {
	c.AddGenericHandler(ctx, name, From{{.type}}HandlerToHandler(sync))
}

func (c *{{.lowerName}}Controller) OnRemove(ctx context.Context, name string, sync {{.type}}Handler) {
	c.AddGenericHandler(ctx, name, generic.NewRemoveHandler(name, c.Updater(), From{{.type}}HandlerToHandler(sync)))
}

func (c *{{.lowerName}}Controller) Enqueue({{ if .namespaced}}namespace, {{end}}name string) {
	c.controller.Enqueue({{ if .namespaced }}namespace, {{else}}"", {{end}}name)
}

func (c *{{.lowerName}}Controller) EnqueueAfter({{ if .namespaced}}namespace, {{end}}name string, duration time.Duration) {
	c.controller.EnqueueAfter({{ if .namespaced }}namespace, {{else}}"", {{end}}name, duration)
}

func (c *{{.lowerName}}Controller) Informer() cache.SharedIndexInformer {
	return c.controller.Informer()
}

func (c *{{.lowerName}}Controller) GroupVersionKind() schema.GroupVersionKind {
	return c.gvk
}

func (c *{{.lowerName}}Controller) Cache() {{.type}}Cache {
	return &{{.lowerName}}Cache{
		indexer:  c.Informer().GetIndexer(),
		resource: c.groupResource,
	}
}

func (c *{{.lowerName}}Controller) Create(obj *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error) {
	result := &{{.version}}.{{.type}}{}
	return result, c.client.Create(context.TODO(), {{ if .namespaced}}obj.Namespace,{{else}}"",{{end}} obj, result, metav1.CreateOptions{})
}

func (c *{{.lowerName}}Controller) Update(obj *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error) {
	result := &{{.version}}.{{.type}}{}
	return result, c.client.Update(context.TODO(), {{ if .namespaced}}obj.Namespace,{{else}}"",{{end}} obj, result, metav1.UpdateOptions{})
}

{{ if .hasStatus -}}
func (c *{{.lowerName}}Controller) UpdateStatus(obj *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error) {
	result := &{{.version}}.{{.type}}{}
	return result, c.client.UpdateStatus(context.TODO(), {{ if .namespaced}}obj.Namespace,{{else}}"",{{end}} obj, result, metav1.UpdateOptions{})
}
{{- end }}

func (c *{{.lowerName}}Controller) Delete({{ if .namespaced}}namespace, {{end}}name string, options *metav1.DeleteOptions) error {
	if options == nil {
		options = &metav1.DeleteOptions{}
	}
	return c.client.Delete(context.TODO(), {{ if .namespaced}}namespace,{{else}}"",{{end}} name, *options)
}

func (c *{{.lowerName}}Controller) Get({{ if .namespaced}}namespace, {{end}}name string, options metav1.GetOptions) (*{{.version}}.{{.type}}, error) {
	result := &{{.version}}.{{.type}}{}
	return result, c.client.Get(context.TODO(), {{ if .namespaced}}namespace,{{else}}"",{{end}} name, result, options)
}

func (c *{{.lowerName}}Controller) List({{ if .namespaced}}namespace string, {{end}}opts metav1.ListOptions) (*{{.version}}.{{.type}}List, error) {
	result := &{{.version}}.{{.type}}List{}
	return result, c.client.List(context.TODO(), {{ if .namespaced}}namespace,{{else}}"",{{end}} result, opts)
}

func (c *{{.lowerName}}Controller) Watch({{ if .namespaced}}namespace string, {{end}}opts metav1.ListOptions) (watch.Interface, error) {
	return c.client.Watch(context.TODO(), {{ if .namespaced}}namespace,{{else}}"",{{end}} opts)
}

func (c *{{.lowerName}}Controller) Patch({{ if .namespaced}}namespace, {{end}}name string, pt types.PatchType, data []byte, subresources ...string) (*{{.version}}.{{.type}}, error) {
	result := &{{.version}}.{{.type}}{}
	return result, c.client.Patch(context.TODO(), {{ if .namespaced}}namespace,{{else}}"",{{end}} name, pt, data, result, metav1.PatchOptions{}, subresources...)
}

type {{.lowerName}}Cache struct {
	indexer  cache.Indexer
	resource schema.GroupResource
}

func (c *{{.lowerName}}Cache) Get({{ if .namespaced}}namespace, {{end}}name string) (*{{.version}}.{{.type}}, error) {
	obj, exists, err := c.indexer.GetByKey({{ if .namespaced }}namespace + "/" + {{end}}name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(c.resource, name)
	}
	return obj.(*{{.version}}.{{.type}}), nil
}

func (c *{{.lowerName}}Cache) List({{ if .namespaced}}namespace string, {{end}}selector labels.Selector) (ret []*{{.version}}.{{.type}}, err error) {
	{{ if .namespaced }}
	err = cache.ListAllByNamespace(c.indexer, namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*{{.version}}.{{.type}}))
	})
	{{else}}
	err = cache.ListAll(c.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*{{.version}}.{{.type}}))
	})
	{{end}}
	return ret, err
}

func (c *{{.lowerName}}Cache) AddIndexer(indexName string, indexer {{.type}}Indexer) {
	utilruntime.Must(c.indexer.AddIndexers(map[string]cache.IndexFunc{
		indexName: func(obj interface{}) (strings []string, e error) {
			return indexer(obj.(*{{.version}}.{{.type}}))
		},
	}))
}

func (c *{{.lowerName}}Cache) GetByIndex(indexName, key string) (result []*{{.version}}.{{.type}}, err error) {
	objs, err := c.indexer.ByIndex(indexName, key)
	if err != nil {
		return nil, err
	}
	result = make([]*{{.version}}.{{.type}}, 0, len(objs))
	for _, obj := range objs {
		result = append(result, obj.(*{{.version}}.{{.type}}))
	}
	return result, nil
}

{{ if .hasStatus -}}
type {{.type}}StatusHandler func(obj *{{.version}}.{{.type}}, status {{.version}}.{{.statusType}}) ({{.version}}.{{.statusType}}, error)

type {{.type}}GeneratingHandler func(obj *{{.version}}.{{.type}}, status {{.version}}.{{.statusType}}) ([]runtime.Object, {{.version}}.{{.statusType}}, error)

func Register{{.type}}StatusHandler(ctx context.Context, controller {{.type}}Controller, condition condition.Cond, name string, handler {{.type}}StatusHandler) {
	statusHandler := &{{.lowerName}}StatusHandler{
		client:    controller,
		condition: condition,
		handler:   handler,
	}
	controller.AddGenericHandler(ctx, name, From{{.type}}HandlerToHandler(statusHandler.sync))
}

func Register{{.type}}GeneratingHandler(ctx context.Context, controller {{.type}}Controller, apply apply.Apply,
	condition condition.Cond, name string, handler {{.type}}GeneratingHandler, opts *generic.GeneratingHandlerOptions) {
	statusHandler := &{{.lowerName}}GeneratingHandler{
		{{.type}}GeneratingHandler: handler,
		apply:                            apply,
		name:                             name,
		gvk:                              controller.GroupVersionKind(),
	}
	if opts != nil {
		statusHandler.opts = *opts
	}
	controller.OnChange(ctx, name, statusHandler.Remove)
	Register{{.type}}StatusHandler(ctx, controller, condition, name, statusHandler.Handle)
}

type {{.lowerName}}StatusHandler struct {
	client    {{.type}}Client
	condition condition.Cond
	handler   {{.type}}StatusHandler
}

func (a *{{.lowerName}}StatusHandler) sync(key string, obj *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error) {
	if obj == nil {
		return obj, nil
	}

	origStatus := obj.Status.DeepCopy()
	obj = obj.DeepCopy()
	newStatus, err := a.handler(obj, obj.Status)
	if err != nil {
		// Revert to old status on error
		newStatus = *origStatus.DeepCopy()
	}

	if a.condition != "" {
		if errors.IsConflict(err) {
			a.condition.SetError(&newStatus, "", nil)
		} else {
			a.condition.SetError(&newStatus, "", err)
		}
	}
	if !equality.Semantic.DeepEqual(origStatus, &newStatus) {
		if a.condition != "" {
			// Since status has changed, update the lastUpdatedTime
			a.condition.LastUpdated(&newStatus, time.Now().UTC().Format(time.RFC3339))
		}

		var newErr error
		obj.Status = newStatus
		newObj, newErr := a.client.UpdateStatus(obj)
		if err == nil {
			err = newErr
		}
		if newErr == nil {
			obj = newObj
		}
	}
	return obj, err
}

type {{.lowerName}}GeneratingHandler struct {
	{{.type}}GeneratingHandler
	apply apply.Apply
	opts  generic.GeneratingHandlerOptions
	gvk   schema.GroupVersionKind
	name  string
}

func (a *{{.lowerName}}GeneratingHandler) Remove(key string, obj *{{.version}}.{{.type}}) (*{{.version}}.{{.type}}, error) {
	if obj != nil {
		return obj, nil
	}

	obj = &{{.version}}.{{.type}}{}
	obj.Namespace, obj.Name = kv.RSplit(key, "/")
	obj.SetGroupVersionKind(a.gvk)

	return nil, generic.ConfigureApplyForObject(a.apply, obj, &a.opts).
		WithOwner(obj).
		WithSetID(a.name).
		ApplyObjects()
}

func (a *{{.lowerName}}GeneratingHandler) Handle(obj *{{.version}}.{{.type}}, status {{.version}}.{{.statusType}}) ({{.version}}.{{.statusType}}, error) {
	if !obj.DeletionTimestamp.IsZero() {
		return status, nil
	}

	objs, newStatus, err := a.{{.type}}GeneratingHandler(obj, status)
	if err != nil {
		return newStatus, err
	}

	return newStatus, generic.ConfigureApplyForObject(a.apply, obj, &a.opts).
		WithOwner(obj).
		WithSetID(a.name).
		ApplyObjects(objs...)
}
{{- end }}
`
