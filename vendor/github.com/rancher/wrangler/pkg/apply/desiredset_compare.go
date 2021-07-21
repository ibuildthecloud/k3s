package apply

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io/ioutil"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	data2 "github.com/rancher/wrangler/pkg/data"
	"github.com/rancher/wrangler/pkg/data/convert"
	"github.com/rancher/wrangler/pkg/objectset"
	patch2 "github.com/rancher/wrangler/pkg/patch"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/dynamic"
)

const (
	LabelApplied = "objectset.rio.cattle.io/applied"
)

var (
	knownListKeys = map[string]bool{
		"apiVersion":    true,
		"containerPort": true,
		"devicePath":    true,
		"ip":            true,
		"kind":          true,
		"mountPath":     true,
		"name":          true,
		"port":          true,
		"topologyKey":   true,
		"type":          true,
	}
)

func prepareObjectForCreate(gvk schema.GroupVersionKind, obj runtime.Object) (runtime.Object, error) {
	serialized, err := serializeApplied(obj)
	if err != nil {
		return nil, err
	}

	obj = obj.DeepCopyObject()
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	annotations := m.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	annotations[LabelApplied] = appliedToAnnotation(serialized)
	m.SetAnnotations(annotations)

	typed, err := meta.TypeAccessor(obj)
	if err != nil {
		return nil, err
	}

	apiVersion, kind := gvk.ToAPIVersionAndKind()
	typed.SetAPIVersion(apiVersion)
	typed.SetKind(kind)

	return obj, nil
}

func originalAndModified(gvk schema.GroupVersionKind, oldMetadata v1.Object, newObject runtime.Object) ([]byte, []byte, error) {
	original, err := getOriginalBytes(gvk, oldMetadata)
	if err != nil {
		return nil, nil, err
	}

	newObject, err = prepareObjectForCreate(gvk, newObject)
	if err != nil {
		return nil, nil, err
	}

	modified, err := json.Marshal(newObject)

	return original, modified, err
}

func emptyMaps(data map[string]interface{}, keys ...string) bool {
	for _, key := range append(keys, "__invalid_key__") {
		if len(data) == 0 {
			// map is empty so all children are empty too
			return true
		} else if len(data) > 1 {
			// map has more than one key so not empty
			return false
		}

		value, ok := data[key]
		if !ok {
			// map has one key but not what we are expecting so not considered empty
			return false
		}

		data = convert.ToMapInterface(value)
	}

	return true
}

func sanitizePatch(patch []byte, removeObjectSetAnnotation bool) ([]byte, error) {
	mod := false
	data := map[string]interface{}{}
	err := json.Unmarshal(patch, &data)
	if err != nil {
		return nil, err
	}

	if _, ok := data["kind"]; ok {
		mod = true
		delete(data, "kind")
	}

	if _, ok := data["apiVersion"]; ok {
		mod = true
		delete(data, "apiVersion")
	}

	if _, ok := data["status"]; ok {
		mod = true
		delete(data, "status")
	}

	if deleted := removeCreationTimestamp(data); deleted {
		mod = true
	}

	if removeObjectSetAnnotation {
		metadata := convert.ToMapInterface(data2.GetValueN(data, "metadata"))
		annotations := convert.ToMapInterface(data2.GetValueN(data, "metadata", "annotations"))
		for k := range annotations {
			if strings.HasPrefix(k, LabelPrefix) {
				mod = true
				delete(annotations, k)
			}
		}
		if mod && len(annotations) == 0 {
			delete(metadata, "annotations")
			if len(metadata) == 0 {
				delete(data, "metadata")
			}
		}
	}

	if emptyMaps(data, "metadata", "annotations") {
		return []byte("{}"), nil
	}

	if !mod {
		return patch, nil
	}

	return json.Marshal(data)
}

func applyPatch(gvk schema.GroupVersionKind, reconciler Reconciler, patcher Patcher, debugID string, ignoreOriginal bool, oldObject, newObject runtime.Object, diffPatches [][]byte) (bool, error) {
	oldMetadata, err := meta.Accessor(oldObject)
	if err != nil {
		return false, err
	}

	original, modified, err := originalAndModified(gvk, oldMetadata, newObject)
	if err != nil {
		return false, err
	}

	if ignoreOriginal {
		original = nil
	}

	current, err := json.Marshal(oldObject)
	if err != nil {
		return false, err
	}

	patchType, patch, err := doPatch(gvk, original, modified, current, diffPatches)
	if err != nil {
		return false, errors.Wrap(err, "patch generation")
	}

	if string(patch) == "{}" {
		return false, nil
	}

	patch, err = sanitizePatch(patch, false)
	if err != nil {
		return false, err
	}

	if string(patch) == "{}" {
		return false, nil
	}

	logrus.Debugf("DesiredSet - Patch %s %s/%s for %s -- [%s, %s, %s, %s]", gvk, oldMetadata.GetNamespace(), oldMetadata.GetName(), debugID, patch, original, modified, current)

	if reconciler != nil {
		newObject, err := prepareObjectForCreate(gvk, newObject)
		if err != nil {
			return false, err
		}
		originalObject, err := getOriginalObject(gvk, oldMetadata)
		if err != nil {
			return false, err
		}
		if originalObject == nil {
			originalObject = oldObject
		}
		handled, err := reconciler(originalObject, newObject)
		if err != nil {
			return false, err
		}
		if handled {
			return true, nil
		}
	}

	logrus.Debugf("DesiredSet - Updated %s %s/%s for %s -- %s %s", gvk, oldMetadata.GetNamespace(), oldMetadata.GetName(), debugID, patchType, patch)
	_, err = patcher(oldMetadata.GetNamespace(), oldMetadata.GetName(), patchType, patch)

	return true, err
}

func (o *desiredSet) compareObjects(gvk schema.GroupVersionKind, reconciler Reconciler, patcher Patcher, client dynamic.NamespaceableResourceInterface, debugID string, oldObject, newObject runtime.Object, force bool) error {
	oldMetadata, err := meta.Accessor(oldObject)
	if err != nil {
		return err
	}

	if o.createPlan {
		o.plan.Objects = append(o.plan.Objects, oldObject)
	}

	diffPatches := o.diffPatches[patchKey{
		GroupVersionKind: gvk,
		ObjectKey: objectset.ObjectKey{
			Namespace: oldMetadata.GetNamespace(),
			Name:      oldMetadata.GetName(),
		},
	}]
	diffPatches = append(diffPatches, o.diffPatches[patchKey{
		GroupVersionKind: gvk,
	}]...)

	if ran, err := applyPatch(gvk, reconciler, patcher, debugID, o.ignorePreviousApplied, oldObject, newObject, diffPatches); err != nil {
		return err
	} else if !ran {
		logrus.Debugf("DesiredSet - No change(2) %s %s/%s for %s", gvk, oldMetadata.GetNamespace(), oldMetadata.GetName(), debugID)
	}

	return nil
}

func removeCreationTimestamp(data map[string]interface{}) bool {
	metadata, ok := data["metadata"]
	if !ok {
		return false
	}

	data = convert.ToMapInterface(metadata)
	if _, ok := data["creationTimestamp"]; ok {
		delete(data, "creationTimestamp")
		return true
	}

	return false
}

func getOriginalObject(gvk schema.GroupVersionKind, obj v1.Object) (runtime.Object, error) {
	original := appliedFromAnnotation(obj.GetAnnotations()[LabelApplied])
	if len(original) == 0 {
		return nil, nil
	}

	mapObj := map[string]interface{}{}
	err := json.Unmarshal(original, &mapObj)
	if err != nil {
		return nil, err
	}

	removeCreationTimestamp(mapObj)
	return prepareObjectForCreate(gvk, &unstructured.Unstructured{
		Object: mapObj,
	})
}

func getOriginalBytes(gvk schema.GroupVersionKind, obj v1.Object) ([]byte, error) {
	objCopy, err := getOriginalObject(gvk, obj)
	if err != nil {
		return nil, err
	}
	if objCopy == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(objCopy)
}

func appliedFromAnnotation(str string) []byte {
	if len(str) == 0 || str[0] == '{' {
		return []byte(str)
	}

	b, err := base64.RawStdEncoding.DecodeString(str)
	if err != nil {
		return nil
	}

	r, err := gzip.NewReader(bytes.NewBuffer(b))
	if err != nil {
		return nil
	}

	b, err = ioutil.ReadAll(r)
	if err != nil {
		return nil
	}

	return b
}

func pruneList(data []interface{}) []interface{} {
	result := make([]interface{}, 0, len(data))
	for _, v := range data {
		switch typed := v.(type) {
		case map[string]interface{}:
			result = append(result, pruneValues(typed, true))
		case []interface{}:
			result = append(result, pruneList(typed))
		default:
			result = append(result, v)
		}
	}
	return result
}

func pruneValues(data map[string]interface{}, isList bool) map[string]interface{} {
	result := map[string]interface{}{}
	for k, v := range data {
		switch typed := v.(type) {
		case map[string]interface{}:
			result[k] = pruneValues(typed, false)
		case []interface{}:
			result[k] = pruneList(typed)
		default:
			if isList && knownListKeys[k] {
				result[k] = v
			} else {
				switch x := v.(type) {
				case string:
					if len(x) > 64 {
						result[k] = x[:64]
					} else {
						result[k] = v
					}
				case []byte:
					result[k] = nil
				default:
					result[k] = v
				}
			}
		}
	}
	return result
}

func serializeApplied(obj runtime.Object) ([]byte, error) {
	data, err := convert.EncodeToMap(obj)
	if err != nil {
		return nil, err
	}
	data = pruneValues(data, false)
	return json.Marshal(data)
}

func appliedToAnnotation(b []byte) string {
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	if _, err := w.Write(b); err != nil {
		return string(b)
	}
	if err := w.Close(); err != nil {
		return string(b)
	}
	return base64.RawStdEncoding.EncodeToString(buf.Bytes())
}

func stripIgnores(original, modified, current []byte, patches [][]byte) ([]byte, []byte, []byte, error) {
	for _, patch := range patches {
		patch, err := jsonpatch.DecodePatch(patch)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(original) > 0 {
			b, err := patch.Apply(original)
			if err == nil {
				original = b
			}
		}
		b, err := patch.Apply(modified)
		if err == nil {
			modified = b
		}
		b, err = patch.Apply(current)
		if err == nil {
			current = b
		}
	}

	return original, modified, current, nil
}

// doPatch is adapted from "kubectl apply"
func doPatch(gvk schema.GroupVersionKind, original, modified, current []byte, diffPatch [][]byte) (types.PatchType, []byte, error) {
	var (
		patchType types.PatchType
		patch     []byte
	)

	original, modified, current, err := stripIgnores(original, modified, current, diffPatch)
	if err != nil {
		return patchType, nil, err
	}

	patchType, lookupPatchMeta, err := patch2.GetMergeStyle(gvk)
	if err != nil {
		return patchType, nil, err
	}

	if patchType == types.StrategicMergePatchType {
		patch, err = strategicpatch.CreateThreeWayMergePatch(original, modified, current, lookupPatchMeta, true)
	} else {
		patch, err = jsonmergepatch.CreateThreeWayJSONMergePatch(original, modified, current)
	}

	if err != nil {
		logrus.Errorf("Failed to calcuated patch: %v", err)
	}

	return patchType, patch, err
}
