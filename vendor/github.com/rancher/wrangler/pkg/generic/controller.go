package generic

import (
	"context"

	"github.com/rancher/lasso/pkg/controller"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

var ErrSkip = controller.ErrIgnore

type Handler func(key string, obj runtime.Object) (runtime.Object, error)

type ControllerMeta interface {
	Informer() cache.SharedIndexInformer
	GroupVersionKind() schema.GroupVersionKind

	AddGenericHandler(ctx context.Context, name string, handler Handler)
	AddGenericRemoveHandler(ctx context.Context, name string, handler Handler)
	Updater() Updater
}
