package objectset

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/gvk"

	"github.com/rancher/wrangler/pkg/merr"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type ObjectKey struct {
	Name      string
	Namespace string
}

func NewObjectKey(obj v1.Object) ObjectKey {
	return ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

func (o ObjectKey) String() string {
	if o.Namespace == "" {
		return o.Name
	}
	return fmt.Sprintf("%s/%s", o.Namespace, o.Name)
}

type ObjectKeyByGVK map[schema.GroupVersionKind][]ObjectKey

type ObjectByGVK map[schema.GroupVersionKind]map[ObjectKey]runtime.Object

func (o ObjectByGVK) Add(obj runtime.Object) (schema.GroupVersionKind, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	gvk, err := gvk.Get(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	objs := o[gvk]
	if objs == nil {
		objs = map[ObjectKey]runtime.Object{}
		o[gvk] = objs
	}

	objs[ObjectKey{
		Namespace: metadata.GetNamespace(),
		Name:      metadata.GetName(),
	}] = obj

	return gvk, nil
}

type ObjectSet struct {
	errs        []error
	objects     ObjectByGVK
	objectsByGK ObjectByGK
	order       []runtime.Object
	gvkOrder    []schema.GroupVersionKind
	gvkSeen     map[schema.GroupVersionKind]bool
}

func NewObjectSet(objs ...runtime.Object) *ObjectSet {
	os := &ObjectSet{
		objects:     ObjectByGVK{},
		objectsByGK: ObjectByGK{},
		gvkSeen:     map[schema.GroupVersionKind]bool{},
	}
	os.Add(objs...)
	return os
}

func (o *ObjectSet) ObjectsByGVK() ObjectByGVK {
	if o == nil {
		return nil
	}
	return o.objects
}

func (o *ObjectSet) Contains(gk schema.GroupKind, key ObjectKey) bool {
	_, ok := o.objectsByGK[gk][key]
	return ok
}

func (o *ObjectSet) All() []runtime.Object {
	return o.order
}

func (o *ObjectSet) Add(objs ...runtime.Object) *ObjectSet {
	for _, obj := range objs {
		o.add(obj)
	}
	return o
}

func (o *ObjectSet) add(obj runtime.Object) {
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		return
	}

	gvk, err := o.objects.Add(obj)
	if err != nil {
		o.err(errors.Wrapf(err, "failed to add %T", obj))
		return
	}

	_, err = o.objectsByGK.Add(obj)
	if err != nil {
		o.err(errors.Wrapf(err, "failed to add %T", obj))
		return
	}

	o.order = append(o.order, obj)
	if !o.gvkSeen[gvk] {
		o.gvkSeen[gvk] = true
		o.gvkOrder = append(o.gvkOrder, gvk)
	}
}

func (o *ObjectSet) err(err error) error {
	o.errs = append(o.errs, err)
	return o.Err()
}

func (o *ObjectSet) AddErr(err error) {
	o.errs = append(o.errs, err)
}

func (o *ObjectSet) Err() error {
	return merr.NewErrors(o.errs...)
}

func (o *ObjectSet) Len() int {
	return len(o.objects)
}

func (o *ObjectSet) GVKs() []schema.GroupVersionKind {
	return o.GVKOrder()
}

func (o *ObjectSet) GVKOrder(known ...schema.GroupVersionKind) []schema.GroupVersionKind {
	var rest []schema.GroupVersionKind

	for _, gvk := range known {
		if o.gvkSeen[gvk] {
			continue
		}
		rest = append(rest, gvk)
	}

	sort.Slice(rest, func(i, j int) bool {
		return rest[i].String() < rest[j].String()
	})

	return append(o.gvkOrder, rest...)
}

type ObjectByGK map[schema.GroupKind]map[ObjectKey]runtime.Object

func (o ObjectByGK) Add(obj runtime.Object) (schema.GroupKind, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return schema.GroupKind{}, err
	}

	gvk, err := gvk.Get(obj)
	if err != nil {
		return schema.GroupKind{}, err
	}

	gk := gvk.GroupKind()

	objs := o[gk]
	if objs == nil {
		objs = map[ObjectKey]runtime.Object{}
		o[gk] = objs
	}

	objs[ObjectKey{
		Namespace: metadata.GetNamespace(),
		Name:      metadata.GetName(),
	}] = obj

	return gk, nil
}
