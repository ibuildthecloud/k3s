package schemes

import (
	"github.com/rancher/lasso/pkg/scheme"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	All                = scheme.All
	localSchemeBuilder = runtime.NewSchemeBuilder()
)

func Register(addToScheme func(*runtime.Scheme) error) error {
	localSchemeBuilder = append(localSchemeBuilder, addToScheme)
	return addToScheme(All)
}

func AddToScheme(scheme *runtime.Scheme) error {
	return localSchemeBuilder.AddToScheme(scheme)
}
