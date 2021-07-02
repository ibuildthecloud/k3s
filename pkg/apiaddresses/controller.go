package apiaddresses

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/rancher/k3s/pkg/etcd"
	"github.com/rancher/k3s/pkg/util"
	"github.com/rancher/k3s/pkg/version"
	controllerv1 "github.com/rancher/wrangler-api/pkg/generated/controllers/core/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/core/v1"
)

type EndpointsControllerGetter func() controllerv1.EndpointsController

func Register(ctx context.Context, runtime *config.ControlRuntime, endpoints controllerv1.EndpointsController) error {
	h := &handler{
		endpointsController: endpoints,
		runtime:             runtime,
		ctx:                 ctx,
	}
	endpoints.OnChange(ctx, version.Program+"-apiserver-lb-controller", h.sync)

	cl, err := etcd.GetClient(h.ctx, h.runtime, "https://127.0.0.1:2379")
	if err != nil {
		return err
	}

	h.etcdClient = cl

	return nil
}

type handler struct {
	endpointsController controllerv1.EndpointsController
	runtime             *config.ControlRuntime
	ctx                 context.Context
	etcdClient          *clientv3.Client
}

// This controller will update the version.program/apiaddresses etcd key with a list of
// api addresses endpoints found in the kubernetes service in the default namespace
func (h *handler) sync(key string, endpoint *v1.Endpoints) (*v1.Endpoints, error) {
	if endpoint == nil {
		return nil, nil
	}

	if endpoint.Namespace != "default" && endpoint.Name != "kubernetes" {
		return nil, nil
	}

	w := &bytes.Buffer{}
	if err := json.NewEncoder(w).Encode(util.GetAddresses(endpoint)); err != nil {
		return nil, err
	}

	_, err := h.etcdClient.Put(h.ctx, etcd.AddressKey, w.String())
	if err != nil {
		return nil, err
	}

	return endpoint, nil
}
