package yaml

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/data/convert"
	"github.com/rancher/wrangler/pkg/gvk"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	yamlDecoder "k8s.io/apimachinery/pkg/util/yaml"
)

var (
	cleanPrefix = []string{
		"kubectl.kubernetes.io/",
	}
	cleanContains = []string{
		"cattle.io/",
	}
)

func Unmarshal(data []byte, v interface{}) error {
	return yamlDecoder.NewYAMLToJSONDecoder(bytes.NewBuffer(data)).Decode(v)
}

func ToObjects(in io.Reader) ([]runtime.Object, error) {
	var result []runtime.Object
	reader := yamlDecoder.NewYAMLReader(bufio.NewReaderSize(in, 4096))
	for {
		raw, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		obj, err := toObjects(raw)
		if err != nil {
			return nil, err
		}

		result = append(result, obj...)
	}

	return result, nil
}

func toObjects(bytes []byte) ([]runtime.Object, error) {
	bytes, err := yamlDecoder.ToJSON(bytes)
	if err != nil {
		return nil, err
	}

	check := map[string]interface{}{}
	if err := json.Unmarshal(bytes, &check); err != nil || len(check) == 0 {
		return nil, err
	}

	obj, _, err := unstructured.UnstructuredJSONScheme.Decode(bytes, nil, nil)
	if err != nil {
		return nil, err
	}

	if l, ok := obj.(*unstructured.UnstructuredList); ok {
		var result []runtime.Object
		for _, obj := range l.Items {
			copy := obj
			result = append(result, &copy)
		}
		return result, nil
	}

	return []runtime.Object{obj}, nil
}

// Export will attempt to clean up the objects a bit before
// rendering to yaml so that they can easily be imported into another
// cluster
func Export(objects ...runtime.Object) ([]byte, error) {
	if len(objects) == 0 {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	for i, obj := range objects {
		if i > 0 {
			buffer.WriteString("\n---\n")
		}

		obj, err := CleanObjectForExport(obj)
		if err != nil {
			return nil, err
		}

		bytes, err := yaml.Marshal(obj)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to encode %s", obj.GetObjectKind().GroupVersionKind())
		}
		buffer.Write(bytes)
	}

	return buffer.Bytes(), nil
}

func CleanObjectForExport(obj runtime.Object) (runtime.Object, error) {
	obj = obj.DeepCopyObject()
	if obj.GetObjectKind().GroupVersionKind().Kind == "" {
		if gvk, err := gvk.Get(obj); err == nil {
			obj.GetObjectKind().SetGroupVersionKind(gvk)
		} else if err != nil {
			return nil, errors.Wrapf(err, "kind and/or apiVersion is not set on input object: %v", obj)
		}
	}

	data, err := convert.EncodeToMap(obj)
	if err != nil {
		return nil, err
	}

	unstr := &unstructured.Unstructured{
		Object: data,
	}

	metadata := map[string]interface{}{}

	if name := unstr.GetName(); len(name) > 0 {
		metadata["name"] = name
	} else if generated := unstr.GetGenerateName(); len(generated) > 0 {
		metadata["generateName"] = generated
	} else {
		return nil, fmt.Errorf("either name or generateName must be set on obj: %v", obj)
	}

	if unstr.GetNamespace() != "" {
		metadata["namespace"] = unstr.GetNamespace()
	}
	if annotations := unstr.GetAnnotations(); len(annotations) > 0 {
		cleanMap(annotations)
		if len(annotations) > 0 {
			metadata["annotations"] = annotations
		} else {
			delete(metadata, "annotations")
		}
	}
	if labels := unstr.GetLabels(); len(labels) > 0 {
		cleanMap(labels)
		if len(labels) > 0 {
			metadata["labels"] = labels
		} else {
			delete(metadata, "labels")
		}
	}

	if spec, ok := data["spec"]; ok {
		if spec == nil {
			delete(data, "spec")
		} else if m, ok := spec.(map[string]interface{}); ok && len(m) == 0 {
			delete(data, "spec")
		}
	}

	data["metadata"] = metadata
	delete(data, "status")

	return unstr, nil
}

func CleanAnnotationsForExport(annotations map[string]string) map[string]string {
	result := make(map[string]string, len(annotations))

outer:
	for k := range annotations {
		for _, prefix := range cleanPrefix {
			if strings.HasPrefix(k, prefix) {
				continue outer
			}
		}
		for _, contains := range cleanContains {
			if strings.Contains(k, contains) {
				continue outer
			}
		}
		result[k] = annotations[k]
	}
	return result
}

func cleanMap(annoLabels map[string]string) {
	for k := range annoLabels {
		for _, prefix := range cleanPrefix {
			if strings.HasPrefix(k, prefix) {
				delete(annoLabels, k)
			}
		}
	}
}

func ToBytes(objects []runtime.Object) ([]byte, error) {
	if len(objects) == 0 {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	for i, obj := range objects {
		if i > 0 {
			buffer.WriteString("\n---\n")
		}

		bytes, err := yaml.Marshal(obj)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to encode %s", obj.GetObjectKind().GroupVersionKind())
		}
		buffer.Write(bytes)
	}

	return buffer.Bytes(), nil
}
