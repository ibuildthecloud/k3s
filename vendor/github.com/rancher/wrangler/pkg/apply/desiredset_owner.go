package apply

import (
	"fmt"
	"strings"

	"github.com/rancher/wrangler/pkg/gvk"

	"github.com/rancher/wrangler/pkg/kv"

	"github.com/pkg/errors"
	namer "github.com/rancher/wrangler/pkg/name"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

var (
	ErrOwnerNotFound   = errors.New("owner not found")
	ErrNoInformerFound = errors.New("informer not found")
)

func notFound(name string, gvk schema.GroupVersionKind) error {
	// this is not proper, but does it really matter that much? If you find this
	// line while researching a bug, then the answer is probably yes.
	resource := namer.GuessPluralName(strings.ToLower(gvk.Kind))
	return apierrors.NewNotFound(schema.GroupResource{
		Group:    gvk.Group,
		Resource: resource,
	}, name)
}

func getGVK(gvkLabel string, gvk *schema.GroupVersionKind) error {
	parts := strings.Split(gvkLabel, ", Kind=")
	if len(parts) != 2 {
		return fmt.Errorf("invalid GVK format: %s", gvkLabel)
	}
	gvk.Group, gvk.Version = kv.Split(parts[0], "/")
	gvk.Kind = parts[1]
	return nil
}

func (o desiredSet) FindOwner(obj runtime.Object) (runtime.Object, error) {
	if obj == nil {
		return nil, ErrOwnerNotFound
	}
	meta, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	var (
		debugID   = fmt.Sprintf("%s/%s", meta.GetNamespace(), meta.GetName())
		gvkLabel  = meta.GetAnnotations()[LabelGVK]
		namespace = meta.GetAnnotations()[LabelNamespace]
		name      = meta.GetAnnotations()[LabelName]
		gvk       schema.GroupVersionKind
	)

	if gvkLabel == "" {
		return nil, ErrOwnerNotFound
	}

	if err := getGVK(gvkLabel, &gvk); err != nil {
		return nil, err
	}

	cache, client, err := o.getControllerAndClient(debugID, gvk)
	if err != nil {
		return nil, err
	}

	if cache != nil {
		return o.fromCache(cache, namespace, name, gvk)
	}

	return o.fromClient(client, namespace, name, gvk)
}

func (o *desiredSet) fromClient(client dynamic.NamespaceableResourceInterface, namespace, name string, gvk schema.GroupVersionKind) (runtime.Object, error) {
	var (
		err error
		obj interface{}
	)
	if namespace == "" {
		obj, err = client.Get(o.ctx, name, metav1.GetOptions{})
	} else {
		obj, err = client.Namespace(namespace).Get(o.ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		return nil, err
	}
	if ro, ok := obj.(runtime.Object); ok {
		return ro, nil
	}
	return nil, notFound(name, gvk)
}

func (o *desiredSet) fromCache(cache cache.SharedInformer, namespace, name string, gvk schema.GroupVersionKind) (runtime.Object, error) {
	var key string
	if namespace == "" {
		key = name
	} else {
		key = namespace + "/" + name
	}
	item, ok, err := cache.GetStore().GetByKey(key)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, notFound(name, gvk)
	} else if ro, ok := item.(runtime.Object); ok {
		return ro, nil
	}
	return nil, notFound(name, gvk)
}

func (o desiredSet) PurgeOrphan(obj runtime.Object) error {
	if obj == nil {
		return nil
	}

	meta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	if _, err := o.FindOwner(obj); apierrors.IsNotFound(err) {
		gvk, err := gvk.Get(obj)
		if err != nil {
			return err
		}

		o.strictCaching = false
		_, client, err := o.getControllerAndClient(meta.GetName(), gvk)
		if err != nil {
			return err
		}
		if meta.GetNamespace() == "" {
			return client.Delete(o.ctx, meta.GetName(), metav1.DeleteOptions{})
		} else {
			return client.Namespace(meta.GetNamespace()).Delete(o.ctx, meta.GetName(), metav1.DeleteOptions{})
		}
	} else if err == ErrOwnerNotFound {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}
