package node

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/nodepassword"
	coreclient "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	core "k8s.io/api/core/v1"
)

func Register(ctx context.Context,
	modCoreDNS bool,
	secretClient coreclient.SecretClient,
	configMap coreclient.ConfigMapController,
	nodes coreclient.NodeController,
) error {
	h := &handler{
		modCoreDNS:   modCoreDNS,
		secretClient: secretClient,
		configCache:  configMap.Cache(),
		configClient: configMap,
	}
	nodes.OnChange(ctx, "node", h.onChange)
	nodes.OnRemove(ctx, "node", h.onRemove)

	return nil
}

type handler struct {
	modCoreDNS   bool
	secretClient coreclient.SecretClient
	configCache  coreclient.ConfigMapCache
	configClient coreclient.ConfigMapClient
}

func (h *handler) onChange(key string, node *core.Node) (*core.Node, error) {
	if node == nil {
		return nil, nil
	}
	return h.updateHosts(node, false)
}

func (h *handler) onRemove(key string, node *core.Node) (*core.Node, error) {
	return h.updateHosts(node, true)
}

func (h *handler) updateHosts(node *core.Node, removed bool) (*core.Node, error) {
	var (
		nodeName    string
		nodeAddress string
	)
	nodeName = node.Name
	for _, address := range node.Status.Addresses {
		if address.Type == "InternalIP" {
			nodeAddress = address.Address
			break
		}
	}
	if removed {
		if err := h.removeNodePassword(nodeName); err != nil {
			logrus.Warn(errors.Wrap(err, "Unable to remove node password"))
		}
	}
	if h.modCoreDNS {
		if err := h.updateCoreDNSConfigMap(nodeName, nodeAddress, removed); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (h *handler) updateCoreDNSConfigMap(nodeName, nodeAddress string, removed bool) error {
	if nodeAddress == "" && !removed {
		logrus.Errorf("No InternalIP found for node " + nodeName)
		return nil
	}

	configMapCache, err := h.configCache.Get("kube-system", "coredns")
	if err != nil || configMapCache == nil {
		logrus.Warn(errors.Wrap(err, "Unable to fetch coredns config map"))
		return nil
	}

	configMap := configMapCache.DeepCopy()
	hosts := configMap.Data["NodeHosts"]
	hostsMap := map[string]string{}

	for _, line := range strings.Split(hosts, "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			logrus.Warnf("Unknown format for hosts line [%s]", line)
			continue
		}
		ip := fields[0]
		host := fields[1]
		if host == nodeName {
			if removed {
				continue
			}
			if ip == nodeAddress {
				return nil
			}
		}
		hostsMap[host] = ip
	}

	if !removed {
		hostsMap[nodeName] = nodeAddress
	}

	var newHosts string
	for host, ip := range hostsMap {
		newHosts += ip + " " + host + "\n"
	}
	configMap.Data["NodeHosts"] = newHosts

	if _, err := h.configClient.Update(configMap); err != nil {
		return err
	}

	var actionType string
	if removed {
		actionType = "Removed"
	} else {
		actionType = "Updated"
	}
	logrus.Infof("%s coredns node hosts entry [%s]", actionType, nodeAddress+" "+nodeName)
	return nil
}

func (h *handler) removeNodePassword(nodeName string) error {
	return nodepassword.Delete(h.secretClient, nodeName)
}
