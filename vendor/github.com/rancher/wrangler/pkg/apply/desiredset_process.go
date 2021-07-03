package apply

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"
	gvk2 "github.com/rancher/wrangler/pkg/gvk"
	"github.com/rancher/wrangler/pkg/merr"
	"github.com/rancher/wrangler/pkg/objectset"
	"github.com/sirupsen/logrus"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types2 "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

var (
	ErrReplace      = errors.New("replace object with changes")
	ReplaceOnChange = func(name string, o runtime.Object, patchType types2.PatchType, data []byte, subresources ...string) (runtime.Object, error) {
		return nil, ErrReplace
	}
)

func (o *desiredSet) getControllerAndClient(debugID string, gvk schema.GroupVersionKind) (cache.SharedIndexInformer, dynamic.NamespaceableResourceInterface, error) {
	// client needs to be accessed first so that the gvk->gvr mapping gets cached
	client, err := o.a.clients.client(gvk)
	if err != nil {
		return nil, nil, err
	}

	informer, ok := o.pruneTypes[gvk]
	if !ok {
		informer = o.a.informers[gvk]
	}
	if informer == nil && o.informerFactory != nil {
		newInformer, err := o.informerFactory.Get(gvk, o.a.clients.gvr(gvk))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to construct informer for %v for %s", gvk, debugID)
		}
		informer = newInformer
	}
	if informer == nil && o.strictCaching {
		return nil, nil, fmt.Errorf("failed to find informer for %s for %s: %w", gvk, debugID, ErrNoInformerFound)
	}

	return informer, client, nil
}

func (o *desiredSet) assignOwnerReference(gvk schema.GroupVersionKind, objs map[objectset.ObjectKey]runtime.Object) error {
	if o.owner == nil {
		return fmt.Errorf("no owner set to assign owner reference")
	}
	ownerMeta, err := meta.Accessor(o.owner)
	if err != nil {
		return err
	}
	ownerGVK, err := gvk2.Get(o.owner)
	if err != nil {
		return err
	}
	ownerNSed, err := o.a.clients.IsNamespaced(ownerGVK)
	if err != nil {
		return err
	}

	for k, v := range objs {
		// can't set owners across boundaries
		if ownerNSed {
			if nsed, err := o.a.clients.IsNamespaced(gvk); err != nil {
				return err
			} else if !nsed {
				continue
			}
		}

		assignNS := false
		assignOwner := true
		if nsed, err := o.a.clients.IsNamespaced(gvk); err != nil {
			return err
		} else if nsed {
			if k.Namespace == "" {
				assignNS = true
			} else if k.Namespace != ownerMeta.GetNamespace() && ownerNSed {
				assignOwner = false
			}
		}

		if !assignOwner {
			continue
		}

		v = v.DeepCopyObject()
		meta, err := meta.Accessor(v)
		if err != nil {
			return err
		}

		if assignNS {
			meta.SetNamespace(ownerMeta.GetNamespace())
		}

		shouldSet := true
		for _, of := range meta.GetOwnerReferences() {
			if ownerMeta.GetUID() == of.UID {
				shouldSet = false
				break
			}
		}

		if shouldSet {
			meta.SetOwnerReferences(append(meta.GetOwnerReferences(), v1.OwnerReference{
				APIVersion:         ownerGVK.GroupVersion().String(),
				Kind:               ownerGVK.Kind,
				Name:               ownerMeta.GetName(),
				UID:                ownerMeta.GetUID(),
				Controller:         &o.ownerReferenceController,
				BlockOwnerDeletion: &o.ownerReferenceBlock,
			}))
		}

		objs[k] = v

		if assignNS {
			delete(objs, k)
			k.Namespace = ownerMeta.GetNamespace()
			objs[k] = v
		}
	}

	return nil
}

func (o *desiredSet) adjustNamespace(gvk schema.GroupVersionKind, objs map[objectset.ObjectKey]runtime.Object) error {
	for k, v := range objs {
		if k.Namespace != "" {
			continue
		}

		v = v.DeepCopyObject()
		meta, err := meta.Accessor(v)
		if err != nil {
			return err
		}

		meta.SetNamespace(o.defaultNamespace)
		delete(objs, k)
		k.Namespace = o.defaultNamespace
		objs[k] = v
	}

	return nil
}

func (o *desiredSet) clearNamespace(objs map[objectset.ObjectKey]runtime.Object) error {
	for k, v := range objs {
		if k.Namespace == "" {
			continue
		}

		v = v.DeepCopyObject()
		meta, err := meta.Accessor(v)
		if err != nil {
			return err
		}

		meta.SetNamespace("")

		delete(objs, k)
		k.Namespace = ""
		objs[k] = v
	}

	return nil
}

func (o *desiredSet) createPatcher(client dynamic.NamespaceableResourceInterface) Patcher {
	return func(namespace, name string, pt types2.PatchType, data []byte) (object runtime.Object, e error) {
		if namespace != "" {
			return client.Namespace(namespace).Patch(o.ctx, name, pt, data, v1.PatchOptions{})
		}
		return client.Patch(o.ctx, name, pt, data, v1.PatchOptions{})
	}
}

func (o *desiredSet) filterCrossVersion(gvk schema.GroupVersionKind, keys []objectset.ObjectKey) []objectset.ObjectKey {
	result := make([]objectset.ObjectKey, 0, len(keys))
	gk := gvk.GroupKind()
	for _, key := range keys {
		if o.objs.Contains(gk, key) {
			continue
		}
		if key.Namespace == o.defaultNamespace && o.objs.Contains(gk, objectset.ObjectKey{Name: key.Name}) {
			continue
		}
		result = append(result, key)
	}
	return result
}

func (o *desiredSet) process(debugID string, set labels.Selector, gvk schema.GroupVersionKind, objs map[objectset.ObjectKey]runtime.Object) {
	controller, client, err := o.getControllerAndClient(debugID, gvk)
	if err != nil {
		o.err(err)
		return
	}

	nsed, err := o.a.clients.IsNamespaced(gvk)
	if err != nil {
		o.err(err)
		return
	}

	if !nsed && o.restrictClusterScoped {
		o.err(fmt.Errorf("invalid cluster scoped gvk: %v", gvk))
		return
	}

	if o.setOwnerReference && o.owner != nil {
		if err := o.assignOwnerReference(gvk, objs); err != nil {
			o.err(err)
			return
		}
	}

	if nsed {
		if err := o.adjustNamespace(gvk, objs); err != nil {
			o.err(err)
			return
		}
	} else {
		if err := o.clearNamespace(objs); err != nil {
			o.err(err)
			return
		}
	}

	patcher, ok := o.patchers[gvk]
	if !ok {
		patcher = o.createPatcher(client)
	}

	reconciler := o.reconcilers[gvk]

	existing, err := o.list(nsed, controller, client, set)
	if err != nil {
		o.err(errors.Wrapf(err, "failed to list %s for %s", gvk, debugID))
		return
	}

	toCreate, toDelete, toUpdate := compareSets(existing, objs)

	// check for resources in the objectset but under a different version of the same group/kind
	toDelete = o.filterCrossVersion(gvk, toDelete)

	if o.createPlan {
		o.plan.Create[gvk] = toCreate
		o.plan.Delete[gvk] = toDelete

		reconciler = nil
		patcher = func(namespace, name string, pt types2.PatchType, data []byte) (runtime.Object, error) {
			data, err := sanitizePatch(data, true)
			if err != nil {
				return nil, err
			}
			if string(data) != "{}" {
				o.plan.Update.Add(gvk, namespace, name, string(data))
			}
			return nil, nil
		}

		toCreate = nil
		toDelete = nil
	}

	createF := func(k objectset.ObjectKey) {
		obj := objs[k]
		obj, err := prepareObjectForCreate(gvk, obj)
		if err != nil {
			o.err(errors.Wrapf(err, "failed to prepare create %s %s for %s", k, gvk, debugID))
			return
		}

		_, err = o.create(nsed, k.Namespace, client, obj)
		if errors2.IsAlreadyExists(err) {
			// Taking over an object that wasn't previously managed by us
			existingObj, err := o.get(nsed, k.Namespace, k.Name, client)
			if err == nil {
				toUpdate = append(toUpdate, k)
				existing[k] = existingObj
				return
			}
		}
		if err != nil {
			o.err(errors.Wrapf(err, "failed to create %s %s for %s", k, gvk, debugID))
			return
		}
		logrus.Debugf("DesiredSet - Created %s %s for %s", gvk, k, debugID)
	}

	deleteF := func(k objectset.ObjectKey, force bool) {
		if err := o.delete(nsed, k.Namespace, k.Name, client, force); err != nil {
			o.err(errors.Wrapf(err, "failed to delete %s %s for %s", k, gvk, debugID))
			return
		}
		logrus.Debugf("DesiredSet - Delete %s %s for %s", gvk, k, debugID)
	}

	updateF := func(k objectset.ObjectKey) {
		err := o.compareObjects(gvk, reconciler, patcher, client, debugID, existing[k], objs[k], len(toCreate) > 0 || len(toDelete) > 0)
		if err == ErrReplace {
			deleteF(k, true)
			o.err(fmt.Errorf("DesiredSet - Replace Wait %s %s for %s", gvk, k, debugID))
		} else if err != nil {
			o.err(errors.Wrapf(err, "failed to update %s %s for %s", k, gvk, debugID))
		}
	}

	for _, k := range toCreate {
		createF(k)
	}

	for _, k := range toUpdate {
		updateF(k)
	}

	for _, k := range toDelete {
		deleteF(k, false)
	}
}

func (o *desiredSet) list(namespaced bool, informer cache.SharedIndexInformer, client dynamic.NamespaceableResourceInterface, selector labels.Selector) (map[objectset.ObjectKey]runtime.Object, error) {
	var (
		errs []error
		objs = map[objectset.ObjectKey]runtime.Object{}
	)

	if informer == nil {
		var c dynamic.ResourceInterface
		if o.listerNamespace != "" {
			c = client.Namespace(o.listerNamespace)
		} else {
			c = client
		}

		list, err := c.List(o.ctx, v1.ListOptions{
			LabelSelector: selector.String(),
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range list.Items {
			copy := obj
			if err := addObjectToMap(objs, &copy); err != nil {
				errs = append(errs, err)
			}
		}

		return objs, merr.NewErrors(errs...)
	}

	var namespace string
	if namespaced {
		namespace = o.listerNamespace
	}

	err := cache.ListAllByNamespace(informer.GetIndexer(), namespace, selector, func(obj interface{}) {
		if err := addObjectToMap(objs, obj); err != nil {
			errs = append(errs, err)
		}
	})
	if err != nil {
		errs = append(errs, err)
	}

	return objs, merr.NewErrors(errs...)
}

func compareSets(existingSet, newSet map[objectset.ObjectKey]runtime.Object) (toCreate, toDelete, toUpdate []objectset.ObjectKey) {
	for k := range newSet {
		if _, ok := existingSet[k]; ok {
			toUpdate = append(toUpdate, k)
		} else {
			toCreate = append(toCreate, k)
		}
	}

	for k := range existingSet {
		if _, ok := newSet[k]; !ok {
			toDelete = append(toDelete, k)
		}
	}

	sortObjectKeys(toCreate)
	sortObjectKeys(toDelete)
	sortObjectKeys(toUpdate)

	return
}

func sortObjectKeys(keys []objectset.ObjectKey) {
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
}

func addObjectToMap(objs map[objectset.ObjectKey]runtime.Object, obj interface{}) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	objs[objectset.ObjectKey{
		Namespace: metadata.GetNamespace(),
		Name:      metadata.GetName(),
	}] = obj.(runtime.Object)

	return nil
}
