package apply

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	gvk2 "github.com/rancher/wrangler/pkg/gvk"

	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/apply/injectors"
	"github.com/rancher/wrangler/pkg/objectset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	LabelID        = "objectset.rio.cattle.io/id"
	LabelGVK       = "objectset.rio.cattle.io/owner-gvk"
	LabelName      = "objectset.rio.cattle.io/owner-name"
	LabelNamespace = "objectset.rio.cattle.io/owner-namespace"
	LabelHash      = "objectset.rio.cattle.io/hash"
	LabelPrefix    = "objectset.rio.cattle.io/"
)

var (
	hashOrder = []string{
		LabelID,
		LabelGVK,
		LabelName,
		LabelNamespace,
	}
	rls     = map[string]flowcontrol.RateLimiter{}
	rlsLock sync.Mutex
)

func (o *desiredSet) getRateLimit(labelHash string) flowcontrol.RateLimiter {
	var rl flowcontrol.RateLimiter

	rlsLock.Lock()
	defer rlsLock.Unlock()
	if o.remove {
		delete(rls, labelHash)
	} else {
		rl = rls[labelHash]
		if rl == nil {
			rl = flowcontrol.NewTokenBucketRateLimiter(o.ratelimitingQps, 10)
			rls[labelHash] = rl
		}
	}

	return rl
}

func (o *desiredSet) dryRun() (Plan, error) {
	o.createPlan = true
	o.plan.Create = objectset.ObjectKeyByGVK{}
	o.plan.Update = PatchByGVK{}
	o.plan.Delete = objectset.ObjectKeyByGVK{}
	err := o.apply()
	return o.plan, err
}

func (o *desiredSet) apply() error {
	if o.objs == nil || o.objs.Len() == 0 {
		o.remove = true
	}

	if err := o.Err(); err != nil {
		return err
	}

	labelSet, annotationSet, err := GetLabelsAndAnnotations(o.setID, o.owner)
	if err != nil {
		return o.err(err)
	}

	rl := o.getRateLimit(labelSet[LabelHash])
	if rl != nil {
		t := time.Now()
		rl.Accept()
		if d := time.Now().Sub(t); d.Seconds() > 1 {
			logrus.Infof("rate limited %s(%s) %s", o.setID, labelSet, d)
		}
	}

	objList, err := o.injectLabelsAndAnnotations(labelSet, annotationSet)
	if err != nil {
		return o.err(err)
	}

	objList, err = o.runInjectors(objList)
	if err != nil {
		return o.err(err)
	}

	objs := o.collect(objList)

	debugID := o.debugID()
	sel, err := GetSelector(labelSet)
	if err != nil {
		return o.err(err)
	}

	for _, gvk := range o.objs.GVKOrder(o.knownGVK()...) {
		o.process(debugID, sel, gvk, objs[gvk])
	}

	return o.Err()
}

func (o *desiredSet) knownGVK() (ret []schema.GroupVersionKind) {
	for k := range o.pruneTypes {
		ret = append(ret, k)
	}
	return
}

func (o *desiredSet) debugID() string {
	if o.owner == nil {
		return o.setID
	}
	metadata, err := meta.Accessor(o.owner)
	if err != nil {
		return o.setID
	}

	return fmt.Sprintf("%s %s", o.setID, objectset.ObjectKey{
		Namespace: metadata.GetNamespace(),
		Name:      metadata.GetName(),
	})
}

func (o *desiredSet) collect(objList []runtime.Object) objectset.ObjectByGVK {
	result := objectset.ObjectByGVK{}
	for _, obj := range objList {
		result.Add(obj)
	}
	return result
}

func (o *desiredSet) runInjectors(objList []runtime.Object) ([]runtime.Object, error) {
	var err error

	for _, inj := range o.injectors {
		if inj == nil {
			continue
		}

		objList, err = inj(objList)
		if err != nil {
			return nil, err
		}
	}

	for _, name := range o.injectorNames {
		inj := injectors.Get(name)
		if inj == nil {
			continue
		}

		objList, err = inj(objList)
		if err != nil {
			return nil, err
		}
	}

	return objList, nil
}

func GetSelector(labelSet map[string]string) (labels.Selector, error) {
	req, err := labels.NewRequirement(LabelHash, selection.Equals, []string{labelSet[LabelHash]})
	if err != nil {
		return nil, err
	}
	return labels.NewSelector().Add(*req), nil
}

func GetLabelsAndAnnotations(setID string, owner runtime.Object) (map[string]string, map[string]string, error) {
	if setID == "" && owner == nil {
		return nil, nil, fmt.Errorf("set ID or owner must be set")
	}

	annotations := map[string]string{
		LabelID: setID,
	}

	if owner != nil {
		gvk, err := gvk2.Get(owner)
		if err != nil {
			return nil, nil, err
		}
		annotations[LabelGVK] = gvk.String()
		metadata, err := meta.Accessor(owner)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get metadata for %s", gvk)
		}
		annotations[LabelName] = metadata.GetName()
		annotations[LabelNamespace] = metadata.GetNamespace()
	}

	labels := map[string]string{
		LabelHash: objectSetHash(annotations),
	}

	return labels, annotations, nil
}

func (o *desiredSet) injectLabelsAndAnnotations(labels, annotations map[string]string) ([]runtime.Object, error) {
	var result []runtime.Object

	for _, objMap := range o.objs.ObjectsByGVK() {
		for key, obj := range objMap {
			obj = obj.DeepCopyObject()
			meta, err := meta.Accessor(obj)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get metadata for %s", key)
			}

			setLabels(meta, labels)
			setAnnotations(meta, annotations)

			result = append(result, obj)
		}
	}

	return result, nil
}

func setAnnotations(meta metav1.Object, annotations map[string]string) {
	objAnn := meta.GetAnnotations()
	if objAnn == nil {
		objAnn = map[string]string{}
	}
	delete(objAnn, LabelApplied)
	for k, v := range annotations {
		objAnn[k] = v
	}
	meta.SetAnnotations(objAnn)
}

func setLabels(meta metav1.Object, labels map[string]string) {
	objLabels := meta.GetLabels()
	if objLabels == nil {
		objLabels = map[string]string{}
	}
	for k, v := range labels {
		objLabels[k] = v
	}
	meta.SetLabels(objLabels)
}

func objectSetHash(labels map[string]string) string {
	dig := sha1.New()
	for _, key := range hashOrder {
		dig.Write([]byte(labels[key]))
	}
	return hex.EncodeToString(dig.Sum(nil))
}
