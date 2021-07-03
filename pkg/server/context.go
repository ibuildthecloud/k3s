package server

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/k3s-io/helm-controller/pkg/generated/controllers/helm.cattle.io"
	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/deploy"
	"github.com/rancher/k3s/pkg/generated/controllers/k3s.cattle.io"
	"github.com/rancher/k3s/pkg/version"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apps"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/batch"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/core"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/rbac"
	"github.com/rancher/wrangler/pkg/apply"
	"github.com/rancher/wrangler/pkg/crd"
	"github.com/rancher/wrangler/pkg/start"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Context struct {
	K3s   *k3s.Factory
	Helm  *helm.Factory
	Batch *batch.Factory
	Apps  *apps.Factory
	Auth  *rbac.Factory
	Core  *core.Factory
	K8s   kubernetes.Interface
	Apply apply.Apply
}

func (c *Context) Start(ctx context.Context) error {
	return start.All(ctx, 5, c.K3s, c.Helm, c.Apps, c.Auth, c.Batch, c.Core)
}

func NewContext(ctx context.Context, cfg string) (*Context, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", cfg)
	if err != nil {
		return nil, err
	}

	// Construct a custom user-agent string for the apply client used by the deploy controller
	// so that we can track which node's deploy controller most recently modified a resource.
	nodeName := os.Getenv("NODE_NAME")
	managerName := deploy.ControllerName + "@" + nodeName
	if nodeName == "" || len(managerName) > validation.FieldManagerMaxLength {
		logrus.Warn("Deploy controller node name is empty or too long, and will not be tracked via server side apply field management")
		managerName = deploy.ControllerName
	}
	restConfig.UserAgent = fmt.Sprintf("%s/%s (%s/%s) %s/%s", managerName, version.Version, runtime.GOOS, runtime.GOARCH, version.Program, version.GitCommit)

	if err := crds(ctx, restConfig); err != nil {
		return nil, errors.Wrap(err, "failed to register CRDs")
	}

	k8s := kubernetes.NewForConfigOrDie(restConfig)
	return &Context{
		K3s:   k3s.NewFactoryFromConfigOrDie(restConfig),
		Helm:  helm.NewFactoryFromConfigOrDie(restConfig),
		K8s:   k8s,
		Auth:  rbac.NewFactoryFromConfigOrDie(restConfig),
		Apps:  apps.NewFactoryFromConfigOrDie(restConfig),
		Batch: batch.NewFactoryFromConfigOrDie(restConfig),
		Core:  core.NewFactoryFromConfigOrDie(restConfig),
		Apply: apply.New(k8s, apply.NewClientFactory(restConfig)).WithDynamicLookup(),
	}, nil
}

func crds(ctx context.Context, config *rest.Config) error {
	factory, err := crd.NewFactoryFromClient(config)
	if err != nil {
		return err
	}

	factory.BatchCreateCRDs(ctx, crd.NamespacedTypes(
		"Addon.k3s.cattle.io/v1",
		"HelmChart.helm.cattle.io/v1",
		"HelmChartConfig.helm.cattle.io/v1")...)

	return factory.BatchWait()
}
