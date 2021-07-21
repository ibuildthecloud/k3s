package generators

import (
	"strings"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
	"k8s.io/gengo/namer"
	"k8s.io/gengo/types"
)

var (
	Imports = []string{
		"context",
		"time",
		"k8s.io/client-go/rest",
		"github.com/rancher/lasso/pkg/client",
		"github.com/rancher/lasso/pkg/controller",
		"github.com/rancher/wrangler/pkg/apply",
		"github.com/rancher/wrangler/pkg/condition",
		"github.com/rancher/wrangler/pkg/schemes",
		"github.com/rancher/wrangler/pkg/generic",
		"github.com/rancher/wrangler/pkg/kv",
		"k8s.io/apimachinery/pkg/api/equality",
		"k8s.io/apimachinery/pkg/api/errors",
		"metav1 \"k8s.io/apimachinery/pkg/apis/meta/v1\"",
		"k8s.io/apimachinery/pkg/labels",
		"k8s.io/apimachinery/pkg/runtime",
		"k8s.io/apimachinery/pkg/runtime/schema",
		"k8s.io/apimachinery/pkg/types",
		"utilruntime \"k8s.io/apimachinery/pkg/util/runtime\"",
		"k8s.io/apimachinery/pkg/watch",
		"k8s.io/client-go/tools/cache",
	}
)

func namespaced(t *types.Type) bool {
	if util.MustParseClientGenTags(t.SecondClosestCommentLines).NonNamespaced {
		return false
	}

	kubeBuilder := false
	for _, line := range t.SecondClosestCommentLines {
		if strings.HasPrefix(line, "+kubebuilder:resource:path=") {
			kubeBuilder = true
			if strings.Contains(line, "scope=Namespaced") {
				return true
			}
		}
	}

	return !kubeBuilder
}

func groupPath(group string) string {
	g := strings.Replace(strings.Split(group, ".")[0], "-", "", -1)
	return groupPackageName(g, "")
}

func groupPackageName(group, groupPackageName string) string {
	if groupPackageName != "" {
		return groupPackageName
	}
	if group == "" {
		return "core"
	}
	return group
}

func upperLowercase(name string) string {
	return namer.IC(strings.ToLower(groupPath(name)))
}
