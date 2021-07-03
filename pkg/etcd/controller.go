package etcd

import (
	"context"
	"os"
	"time"

	controllerv1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

const (
	NodeID       = "etcd.k3s.cattle.io/node-name"
	NodeAddress  = "etcd.k3s.cattle.io/node-address"
	master       = "node-role.kubernetes.io/master"
	controlPlane = "node-role.kubernetes.io/control-plane"
	etcdRole     = "node-role.kubernetes.io/etcd"
)

type NodeControllerGetter func() controllerv1.NodeController

func Register(ctx context.Context, etcd *ETCD, nodes controllerv1.NodeController) {
	h := &handler{
		etcd:           etcd,
		nodeController: nodes,
		ctx:            ctx,
	}
	nodes.OnChange(ctx, "managed-etcd-controller", h.sync)
	nodes.OnRemove(ctx, "managed-etcd-controller", h.onRemove)
}

type handler struct {
	etcd           *ETCD
	nodeController controllerv1.NodeController
	ctx            context.Context
}

func (h *handler) sync(key string, node *v1.Node) (*v1.Node, error) {
	if node == nil {
		return nil, nil
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		logrus.Debug("waiting for node to be assigned for etcd controller")
		h.nodeController.EnqueueAfter(key, 5*time.Second)
		return node, nil
	}

	if key == nodeName {
		return h.handleSelf(node)
	}

	return node, nil
}

func (h *handler) handleSelf(node *v1.Node) (*v1.Node, error) {
	if node.Annotations[NodeID] == h.etcd.name &&
		node.Annotations[NodeAddress] == h.etcd.address &&
		node.Labels[etcdRole] == "true" &&
		node.Labels[controlPlane] == "true" ||
		h.etcd.config.DisableETCD {
		return node, nil
	}

	node = node.DeepCopy()
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[NodeID] = h.etcd.name
	node.Annotations[NodeAddress] = h.etcd.address
	node.Labels[etcdRole] = "true"
	node.Labels[master] = "true"
	node.Labels[controlPlane] = "true"

	return h.nodeController.Update(node)
}

func (h *handler) onRemove(key string, node *v1.Node) (*v1.Node, error) {
	if _, ok := node.Labels[etcdRole]; !ok {
		return node, nil
	}

	id := node.Annotations[NodeID]
	address, ok := node.Annotations[NodeAddress]
	if !ok {
		return node, nil
	}
	return node, h.etcd.removePeer(h.ctx, id, address, false)
}
