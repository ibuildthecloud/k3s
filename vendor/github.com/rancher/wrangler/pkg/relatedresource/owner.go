package relatedresource

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

// OwnerResolver Look for owner references that match the apiVersion and kind and resolve to the namespace and
// name of the parent. The namespaced flag is whether the apiVersion/kind referenced is expected to be namespaced
func OwnerResolver(namespaced bool, apiVersion, kind string) Resolver {
	return func(namespace, name string, obj runtime.Object) ([]Key, error) {
		if obj == nil {
			return nil, nil
		}

		meta, err := meta.Accessor(obj)
		if err != nil {
			// ignore err
			return nil, nil
		}

		var result []Key
		for _, owner := range meta.GetOwnerReferences() {
			if owner.Kind == kind && owner.APIVersion == apiVersion {
				ns := ""
				if namespaced {
					ns = meta.GetNamespace()
				}
				result = append(result, Key{
					Namespace: ns,
					Name:      owner.Name,
				})
			}
		}

		return result, nil
	}
}
