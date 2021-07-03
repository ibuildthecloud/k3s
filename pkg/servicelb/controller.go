package servicelb

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/rancher/k3s/pkg/version"
	"github.com/rancher/wrangler/pkg/apply"
	"github.com/rancher/wrangler/pkg/condition"
	appclient "github.com/rancher/wrangler/pkg/generated/controllers/apps/v1"
	coreclient "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/pkg/objectset"
	"github.com/rancher/wrangler/pkg/relatedresource"
	"github.com/rancher/wrangler/pkg/slice"
	"github.com/sirupsen/logrus"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	v1getter "k8s.io/client-go/kubernetes/typed/apps/v1"
	coregetter "k8s.io/client-go/kubernetes/typed/core/v1"
)

var (
	svcNameLabel       = "svccontroller." + version.Program + ".cattle.io/svcname"
	daemonsetNodeLabel = "svccontroller." + version.Program + ".cattle.io/enablelb"
	nodeSelectorLabel  = "svccontroller." + version.Program + ".cattle.io/nodeselector"
	DefaultLBImage     = "rancher/klipper-lb:v0.2.0"
)

const (
	Ready = condition.Cond("Ready")
)

var (
	trueVal = true
)

func Register(ctx context.Context,
	kubernetes kubernetes.Interface,
	apply apply.Apply,
	daemonSetController appclient.DaemonSetController,
	deployments appclient.DeploymentController,
	nodes coreclient.NodeController,
	pods coreclient.PodController,
	services coreclient.ServiceController,
	endpoints coreclient.EndpointsController,
	enabled, rootless bool) error {
	h := &handler{
		rootless:        rootless,
		enabled:         enabled,
		nodeCache:       nodes.Cache(),
		podCache:        pods.Cache(),
		deploymentCache: deployments.Cache(),
		processor: apply.WithSetID("svccontroller").
			WithCacheTypes(daemonSetController),
		serviceCache: services.Cache(),
		services:     kubernetes.CoreV1(),
		daemonsets:   kubernetes.AppsV1(),
		deployments:  kubernetes.AppsV1(),
	}

	services.OnChange(ctx, "svccontroller", h.onChangeService)
	nodes.OnChange(ctx, "svccontroller", h.onChangeNode)
	relatedresource.Watch(ctx, "svccontroller-watcher",
		h.onResourceChange,
		services,
		pods,
		endpoints)

	return nil
}

type handler struct {
	rootless        bool
	enabled         bool
	nodeCache       coreclient.NodeCache
	podCache        coreclient.PodCache
	deploymentCache appclient.DeploymentCache
	processor       apply.Apply
	serviceCache    coreclient.ServiceCache
	services        coregetter.ServicesGetter
	daemonsets      v1getter.DaemonSetsGetter
	deployments     v1getter.DeploymentsGetter
}

func (h *handler) onResourceChange(name, namespace string, obj runtime.Object) ([]relatedresource.Key, error) {
	if ep, ok := obj.(*core.Endpoints); ok {
		return []relatedresource.Key{
			{
				Name:      ep.Name,
				Namespace: ep.Namespace,
			},
		}, nil
	}

	pod, ok := obj.(*core.Pod)
	if !ok {
		return nil, nil
	}

	serviceName := pod.Labels[svcNameLabel]
	if serviceName == "" {
		return nil, nil
	}

	if pod.Status.PodIP == "" {
		return nil, nil
	}

	return []relatedresource.Key{
		{
			Name:      serviceName,
			Namespace: pod.Namespace,
		},
	}, nil
}

// onChangeService handles changes to Services.
func (h *handler) onChangeService(key string, svc *core.Service) (*core.Service, error) {
	if svc == nil {
		return nil, nil
	}

	if svc.Spec.Type != core.ServiceTypeLoadBalancer || svc.Spec.ClusterIP == "" ||
		svc.Spec.ClusterIP == "None" {
		return svc, nil
	}

	if err := h.deployPod(svc); err != nil {
		return svc, err
	}

	// Don't return service because we don't want another update
	_, err := h.updateService(svc)
	return nil, err
}

// onChangeNode handles changes to Nodes. We need to handle this as we may need to kick the DaemonSet
// to add or remove pods from nodes if labels have changed.
func (h *handler) onChangeNode(key string, node *core.Node) (*core.Node, error) {
	if node == nil {
		return nil, nil
	}
	if _, ok := node.Labels[daemonsetNodeLabel]; !ok {
		return node, nil
	}

	if err := h.updateDaemonSets(); err != nil {
		return node, err
	}

	return node, nil
}

// updateService ensures that the Service ingress IP address list is in sync
// with the Nodes actually running pods for this service.
func (h *handler) updateService(svc *core.Service) (runtime.Object, error) {
	if !h.enabled {
		return svc, nil
	}

	pods, err := h.podCache.List(svc.Namespace, labels.SelectorFromSet(map[string]string{
		svcNameLabel: svc.Name,
	}))

	if err != nil {
		return svc, err
	}

	existingIPs := serviceIPs(svc)
	expectedIPs, err := h.podIPs(pods)
	if err != nil {
		return svc, err
	}

	sort.Strings(expectedIPs)
	sort.Strings(existingIPs)

	if slice.StringsEqual(expectedIPs, existingIPs) {
		return svc, nil
	}

	svc = svc.DeepCopy()
	svc.Status.LoadBalancer.Ingress = nil
	for _, ip := range expectedIPs {
		svc.Status.LoadBalancer.Ingress = append(svc.Status.LoadBalancer.Ingress, core.LoadBalancerIngress{
			IP: ip,
		})
	}

	logrus.Debugf("Setting service loadbalancer %s/%s to IPs %v", svc.Namespace, svc.Name, expectedIPs)
	return h.services.Services(svc.Namespace).UpdateStatus(context.TODO(), svc, meta.UpdateOptions{})
}

// serviceIPs returns the list of ingress IP addresses from the Service
func serviceIPs(svc *core.Service) []string {
	var ips []string

	for _, ingress := range svc.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			ips = append(ips, ingress.IP)
		}
	}

	return ips
}

// podIPs returns a list of IPs for Nodes hosting ServiceLB Pods.
// If at least one node has External IPs available, only external IPs are returned.
// If no nodes have External IPs set, the Internal IPs of all nodes running pods are returned.
func (h *handler) podIPs(pods []*core.Pod) ([]string, error) {
	// Go doesn't have sets so we stuff things into a map of bools and then get lists of keys
	// to determine the unique set of IPs in use by pods.
	extIPs := map[string]bool{}
	intIPs := map[string]bool{}

	for _, pod := range pods {
		if pod.Spec.NodeName == "" || pod.Status.PodIP == "" {
			continue
		}
		if !Ready.IsTrue(pod) {
			continue
		}

		node, err := h.nodeCache.Get(pod.Spec.NodeName)
		if errors.IsNotFound(err) {
			continue
		} else if err != nil {
			return nil, err
		}

		for _, addr := range node.Status.Addresses {
			if addr.Type == core.NodeExternalIP {
				extIPs[addr.Address] = true
			} else if addr.Type == core.NodeInternalIP {
				intIPs[addr.Address] = true
			}
		}
	}

	keys := func(addrs map[string]bool) (ips []string) {
		for k := range addrs {
			ips = append(ips, k)
		}
		return ips
	}

	var ips []string
	if len(extIPs) > 0 {
		ips = keys(extIPs)
	} else {
		ips = keys(intIPs)
	}

	if len(ips) > 0 && h.rootless {
		return []string{"127.0.0.1"}, nil
	}

	return ips, nil
}

// deployPod ensures that there is a DaemonSet for each service.
// It also ensures that any legacy Deployments from older versions of ServiceLB are deleted.
func (h *handler) deployPod(svc *core.Service) error {
	if err := h.deleteOldDeployments(svc); err != nil {
		return err
	}
	objs := objectset.NewObjectSet()
	if !h.enabled {
		return h.processor.WithOwner(svc).Apply(objs)
	}

	ds, err := h.newDaemonSet(svc)
	if err != nil {
		return err
	}
	if ds != nil {
		objs.Add(ds)
	}
	return h.processor.WithOwner(svc).Apply(objs)
}

// newDaemonSet creates a DaemonSet to ensure that ServiceLB pods are run on
// each eligible node.
func (h *handler) newDaemonSet(svc *core.Service) (*apps.DaemonSet, error) {
	name := fmt.Sprintf("svclb-%s", svc.Name)
	oneInt := intstr.FromInt(1)

	ds := &apps.DaemonSet{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: svc.Namespace,
			OwnerReferences: []meta.OwnerReference{
				{
					Name:       svc.Name,
					APIVersion: "v1",
					Kind:       "Service",
					UID:        svc.UID,
					Controller: &trueVal,
				},
			},
			Labels: map[string]string{
				nodeSelectorLabel: "false",
			},
		},
		TypeMeta: meta.TypeMeta{
			Kind:       "DaemonSet",
			APIVersion: "apps/v1",
		},
		Spec: apps.DaemonSetSpec{
			Selector: &meta.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{
					Labels: map[string]string{
						"app":        name,
						svcNameLabel: svc.Name,
					},
				},
			},
			UpdateStrategy: apps.DaemonSetUpdateStrategy{
				Type: apps.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &apps.RollingUpdateDaemonSet{
					MaxUnavailable: &oneInt,
				},
			},
		},
	}

	for _, port := range svc.Spec.Ports {
		portName := fmt.Sprintf("lb-port-%d", port.Port)
		container := core.Container{
			Name:            portName,
			Image:           DefaultLBImage,
			ImagePullPolicy: core.PullIfNotPresent,
			Ports: []core.ContainerPort{
				{
					Name:          portName,
					ContainerPort: port.Port,
					HostPort:      port.Port,
					Protocol:      port.Protocol,
				},
			},
			Env: []core.EnvVar{
				{
					Name:  "SRC_PORT",
					Value: strconv.Itoa(int(port.Port)),
				},
				{
					Name:  "DEST_PROTO",
					Value: string(port.Protocol),
				},
				{
					Name:  "DEST_PORT",
					Value: strconv.Itoa(int(port.Port)),
				},
				{
					Name:  "DEST_IP",
					Value: svc.Spec.ClusterIP,
				},
			},
			SecurityContext: &core.SecurityContext{
				Capabilities: &core.Capabilities{
					Add: []core.Capability{
						"NET_ADMIN",
					},
				},
			},
		}

		ds.Spec.Template.Spec.Containers = append(ds.Spec.Template.Spec.Containers, container)
	}

	// Add toleration to noderole.kubernetes.io/master=*:NoSchedule
	masterToleration := core.Toleration{
		Key:      "node-role.kubernetes.io/master",
		Operator: "Exists",
		Effect:   "NoSchedule",
	}
	ds.Spec.Template.Spec.Tolerations = append(ds.Spec.Template.Spec.Tolerations, masterToleration)

	// Add toleration to noderole.kubernetes.io/control-plane=*:NoSchedule
	controlPlaneToleration := core.Toleration{
		Key:      "node-role.kubernetes.io/control-plane",
		Operator: "Exists",
		Effect:   "NoSchedule",
	}
	ds.Spec.Template.Spec.Tolerations = append(ds.Spec.Template.Spec.Tolerations, controlPlaneToleration)

	// Add toleration to CriticalAddonsOnly
	criticalAddonsOnlyToleration := core.Toleration{
		Key:      "CriticalAddonsOnly",
		Operator: "Exists",
	}
	ds.Spec.Template.Spec.Tolerations = append(ds.Spec.Template.Spec.Tolerations, criticalAddonsOnlyToleration)

	// Add node selector only if label "svccontroller.k3s.cattle.io/enablelb" exists on the nodes
	selector, err := labels.Parse(daemonsetNodeLabel)
	if err != nil {
		return nil, err
	}
	nodesWithLabel, err := h.nodeCache.List(selector)
	if err != nil {
		return nil, err
	}
	if len(nodesWithLabel) > 0 {
		ds.Spec.Template.Spec.NodeSelector = map[string]string{
			daemonsetNodeLabel: "true",
		}
		ds.Labels[nodeSelectorLabel] = "true"
	}
	return ds, nil
}

func (h *handler) updateDaemonSets() error {
	daemonsets, err := h.daemonsets.DaemonSets("").List(context.TODO(), meta.ListOptions{
		LabelSelector: nodeSelectorLabel + "=false",
	})
	if err != nil {
		return err
	}

	for _, ds := range daemonsets.Items {
		ds.Spec.Template.Spec.NodeSelector = map[string]string{
			daemonsetNodeLabel: "true",
		}
		ds.Labels[nodeSelectorLabel] = "true"
		if _, err := h.daemonsets.DaemonSets(ds.Namespace).Update(context.TODO(), &ds, meta.UpdateOptions{}); err != nil {
			return err
		}

	}

	return nil
}

// deleteOldDeployments ensures that there are no legacy Deployments for ServiceLB pods.
// ServiceLB used to use Deployments before switching to DaemonSets in 875ba28
func (h *handler) deleteOldDeployments(svc *core.Service) error {
	name := fmt.Sprintf("svclb-%s", svc.Name)
	if _, err := h.deploymentCache.Get(svc.Namespace, name); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return h.deployments.Deployments(svc.Namespace).Delete(context.TODO(), name, meta.DeleteOptions{})
}
