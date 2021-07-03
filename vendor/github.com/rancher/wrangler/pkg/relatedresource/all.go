package relatedresource

import "k8s.io/apimachinery/pkg/runtime"

const (
	AllKey = "_all_"
)

func TriggerAllKey(namespace, name string, obj runtime.Object) ([]Key, error) {
	if name != AllKey {
		return []Key{{
			Name: AllKey,
		}}, nil
	}
	return nil, nil
}
