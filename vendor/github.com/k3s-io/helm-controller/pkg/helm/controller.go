package helm

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	helmv1 "github.com/k3s-io/helm-controller/pkg/apis/helm.cattle.io/v1"
	helmcontroller "github.com/k3s-io/helm-controller/pkg/generated/controllers/helm.cattle.io/v1"
	"github.com/rancher/wrangler/pkg/apply"
	batchcontroller "github.com/rancher/wrangler/pkg/generated/controllers/batch/v1"
	corecontroller "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	rbaccontroller "github.com/rancher/wrangler/pkg/generated/controllers/rbac/v1"
	"github.com/rancher/wrangler/pkg/objectset"
	"github.com/rancher/wrangler/pkg/relatedresource"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	trueVal         = true
	commaRE         = regexp.MustCompile(`\\*,`)
	DefaultJobImage = "rancher/klipper-helm:v0.6.1-build20210616"
)

type Controller struct {
	namespace      string
	helmController helmcontroller.HelmChartController
	confController helmcontroller.HelmChartConfigController
	jobsCache      batchcontroller.JobCache
	apply          apply.Apply
}

const (
	Label         = "helmcharts.helm.cattle.io/chart"
	Annotation    = "helmcharts.helm.cattle.io/configHash"
	CRDName       = "helmcharts.helm.cattle.io"
	ConfigCRDName = "helmchartconfigs.helm.cattle.io"
	Name          = "helm-controller"
)

func Register(ctx context.Context, apply apply.Apply,
	helms helmcontroller.HelmChartController,
	confs helmcontroller.HelmChartConfigController,
	jobs batchcontroller.JobController,
	crbs rbaccontroller.ClusterRoleBindingController,
	sas corecontroller.ServiceAccountController,
	cm corecontroller.ConfigMapController) {
	apply = apply.WithSetID(Name).
		WithCacheTypes(helms, confs, jobs, crbs, sas, cm).
		WithStrictCaching().WithPatcher(batch.SchemeGroupVersion.WithKind("Job"), func(namespace, name string, pt types.PatchType, data []byte) (runtime.Object, error) {
		err := jobs.Delete(namespace, name, &meta.DeleteOptions{})
		if err == nil {
			return nil, fmt.Errorf("replace job")
		}
		return nil, err
	})

	relatedresource.Watch(ctx, "helm-pod-watch",
		func(namespace, name string, obj runtime.Object) ([]relatedresource.Key, error) {
			if job, ok := obj.(*batch.Job); ok {
				name := job.Labels[Label]
				if name != "" {
					return []relatedresource.Key{
						{
							Name:      name,
							Namespace: namespace,
						},
					}, nil
				}
			}
			return nil, nil
		},
		helms,
		confs,
		jobs)

	controller := &Controller{
		helmController: helms,
		confController: confs,
		jobsCache:      jobs.Cache(),
		apply:          apply,
	}

	helms.OnChange(ctx, Name, controller.OnHelmChange)
	helms.OnRemove(ctx, Name, controller.OnHelmRemove)
	confs.OnChange(ctx, Name, controller.OnConfChange)
}

func (c *Controller) OnHelmChange(key string, chart *helmv1.HelmChart) (*helmv1.HelmChart, error) {
	if chart == nil {
		return nil, nil
	}
	if chart.Spec.Chart == "" && chart.Spec.ChartContent == "" {
		return chart, nil
	}

	objs := objectset.NewObjectSet()
	job, valuesConfigMap, contentConfigMap := job(chart)
	objs.Add(serviceAccount(chart))
	objs.Add(roleBinding(chart))

	if config, err := c.confController.Cache().Get(chart.Namespace, chart.Name); err != nil {
		if !errors.IsNotFound(err) {
			return chart, err
		}
	} else if config != nil {
		valuesConfigMapAddConfig(valuesConfigMap, config)
	}

	hashConfigMaps(job, contentConfigMap, valuesConfigMap)

	objs.Add(contentConfigMap)
	objs.Add(valuesConfigMap)
	objs.Add(job)

	if err := c.apply.WithOwner(chart).Apply(objs); err != nil {
		return chart, err
	}

	chartCopy := chart.DeepCopy()
	chartCopy.Status.JobName = job.Name
	return c.helmController.Update(chartCopy)
}

func (c *Controller) OnHelmRemove(key string, chart *helmv1.HelmChart) (*helmv1.HelmChart, error) {
	if chart == nil {
		return nil, nil
	}
	if chart.Spec.Chart == "" {
		return chart, nil
	}

	job, _, _ := job(chart)
	job, err := c.jobsCache.Get(chart.Namespace, job.Name)

	if errors.IsNotFound(err) {
		_, err := c.OnHelmChange(key, chart)
		if err != nil {
			return chart, err
		}
	} else if err != nil {
		return chart, err
	}

	if job.Status.Succeeded <= 0 {
		return chart, fmt.Errorf("waiting for delete of helm chart for %s by %s", key, job.Name)
	}

	chartCopy := chart.DeepCopy()
	chartCopy.Status.JobName = job.Name
	newChart, err := c.helmController.Update(chartCopy)

	if err != nil {
		return newChart, err
	}

	return newChart, c.apply.WithOwner(newChart).Apply(objectset.NewObjectSet())
}

func (c *Controller) OnConfChange(key string, conf *helmv1.HelmChartConfig) (*helmv1.HelmChartConfig, error) {
	if conf == nil {
		return nil, nil
	}

	if chart, err := c.helmController.Cache().Get(conf.Namespace, conf.Name); err != nil {
		if !errors.IsNotFound(err) {
			return conf, err
		}
	} else if chart != nil {
		c.helmController.Enqueue(conf.Namespace, conf.Name)
	}
	return conf, nil
}

func job(chart *helmv1.HelmChart) (*batch.Job, *core.ConfigMap, *core.ConfigMap) {
	oneThousand := int32(1000)

	jobImage := strings.TrimSpace(chart.Spec.JobImage)
	if jobImage == "" {
		jobImage = DefaultJobImage
	}

	action := "install"
	if chart.DeletionTimestamp != nil {
		action = "delete"
	}

	targetNamespace := chart.Namespace
	if len(chart.Spec.TargetNamespace) != 0 {
		targetNamespace = chart.Spec.TargetNamespace
	}

	job := &batch.Job{
		TypeMeta: meta.TypeMeta{
			APIVersion: "batch/v1",
			Kind:       "Job",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      fmt.Sprintf("helm-%s-%s", action, chart.Name),
			Namespace: chart.Namespace,
			Labels: map[string]string{
				Label: chart.Name,
			},
		},
		Spec: batch.JobSpec{
			BackoffLimit: &oneThousand,
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{
					Annotations: map[string]string{},
					Labels: map[string]string{
						Label: chart.Name,
					},
				},
				Spec: core.PodSpec{
					RestartPolicy: core.RestartPolicyOnFailure,
					Containers: []core.Container{
						{
							Name:            "helm",
							Image:           jobImage,
							ImagePullPolicy: core.PullIfNotPresent,
							Args:            args(chart),
							Env: []core.EnvVar{
								{
									Name:  "NAME",
									Value: chart.Name,
								},
								{
									Name:  "VERSION",
									Value: chart.Spec.Version,
								},
								{
									Name:  "REPO",
									Value: chart.Spec.Repo,
								},
								{
									Name:  "HELM_DRIVER",
									Value: "secret",
								},
								{
									Name:  "CHART_NAMESPACE",
									Value: chart.Namespace,
								},
								{
									Name:  "CHART",
									Value: chart.Spec.Chart,
								},
								{
									Name:  "HELM_VERSION",
									Value: chart.Spec.HelmVersion,
								},
								{
									Name:  "TARGET_NAMESPACE",
									Value: targetNamespace,
								},
							},
						},
					},
					ServiceAccountName: fmt.Sprintf("helm-%s", chart.Name),
				},
			},
		},
	}

	if chart.Spec.Timeout != nil {
		job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, core.EnvVar{
			Name:  "TIMEOUT",
			Value: chart.Spec.Timeout.String(),
		})
	}

	if chart.Spec.Bootstrap {
		job.Spec.Template.Spec.HostNetwork = true
		job.Spec.Template.Spec.Tolerations = []core.Toleration{
			{
				Key:    "node.kubernetes.io/not-ready",
				Effect: core.TaintEffectNoSchedule,
			},
			{
				Key:      "node.cloudprovider.kubernetes.io/uninitialized",
				Operator: core.TolerationOpEqual,
				Value:    "true",
				Effect:   core.TaintEffectNoSchedule,
			},
			{
				Key:      "CriticalAddonsOnly",
				Operator: core.TolerationOpExists,
			},
			{
				Key:      "node-role.kubernetes.io/etcd",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoExecute,
			},
			{
				Key:      "node-role.kubernetes.io/control-plane",
				Operator: core.TolerationOpExists,
				Effect:   core.TaintEffectNoSchedule,
			},
		}
		job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, []core.EnvVar{
			{
				Name:  "KUBERNETES_SERVICE_HOST",
				Value: "127.0.0.1"},
			{
				Name:  "KUBERNETES_SERVICE_PORT",
				Value: "6443"},
			{
				Name:  "BOOTSTRAP",
				Value: "true"},
		}...)
		job.Spec.Template.Spec.NodeSelector = make(map[string]string)
		job.Spec.Template.Spec.NodeSelector["node-role.kubernetes.io/master"] = "true"
	}

	setProxyEnv(job)
	valueConfigMap := setValuesConfigMap(job, chart)
	contentConfigMap := setContentConfigMap(job, chart)

	return job, valueConfigMap, contentConfigMap
}

func valuesConfigMap(chart *helmv1.HelmChart) *core.ConfigMap {
	var configMap = &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      fmt.Sprintf("chart-values-%s", chart.Name),
			Namespace: chart.Namespace,
		},
		Data: map[string]string{},
	}

	if chart.Spec.ValuesContent != "" {
		configMap.Data["values-01_HelmChart.yaml"] = chart.Spec.ValuesContent
	}

	return configMap
}

func valuesConfigMapAddConfig(configMap *core.ConfigMap, config *helmv1.HelmChartConfig) {
	if config.Spec.ValuesContent != "" {
		configMap.Data["values-10_HelmChartConfig.yaml"] = config.Spec.ValuesContent
	}
}

func roleBinding(chart *helmv1.HelmChart) *rbac.ClusterRoleBinding {
	return &rbac.ClusterRoleBinding{
		TypeMeta: meta.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: meta.ObjectMeta{
			Name: fmt.Sprintf("helm-%s-%s", chart.Namespace, chart.Name),
		},
		RoleRef: rbac.RoleRef{
			Kind:     "ClusterRole",
			APIGroup: "rbac.authorization.k8s.io",
			Name:     "cluster-admin",
		},
		Subjects: []rbac.Subject{
			{
				Name:      fmt.Sprintf("helm-%s", chart.Name),
				Kind:      "ServiceAccount",
				Namespace: chart.Namespace,
			},
		},
	}
}

func serviceAccount(chart *helmv1.HelmChart) *core.ServiceAccount {
	return &core.ServiceAccount{
		TypeMeta: meta.TypeMeta{
			APIVersion: "v1",
			Kind:       "ServiceAccount",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      fmt.Sprintf("helm-%s", chart.Name),
			Namespace: chart.Namespace,
		},
		AutomountServiceAccountToken: &trueVal,
	}
}

func args(chart *helmv1.HelmChart) []string {
	if chart.DeletionTimestamp != nil {
		return []string{
			"delete",
		}
	}

	spec := chart.Spec
	args := []string{
		"install",
	}
	if spec.TargetNamespace != "" {
		args = append(args, "--namespace", spec.TargetNamespace)
	}
	if spec.Repo != "" {
		args = append(args, "--repo", spec.Repo)
	}
	if spec.Version != "" {
		args = append(args, "--version", spec.Version)
	}

	for _, k := range keys(spec.Set) {
		val := spec.Set[k]
		if typedVal(val) {
			args = append(args, "--set", fmt.Sprintf("%s=%s", k, val.String()))
		} else {
			args = append(args, "--set-string", fmt.Sprintf("%s=%s", k, commaRE.ReplaceAllStringFunc(val.String(), escapeComma)))
		}
	}

	return args
}

func keys(val map[string]intstr.IntOrString) []string {
	var keys []string
	for k := range val {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// typedVal is a modified version of helm's typedVal function that operates on kubernetes IntOrString types.
// Things that look like an integer, boolean, or null should use --set; everything else should use --set-string.
// Ref: https://github.com/helm/helm/blob/v3.5.4/pkg/strvals/parser.go#L415
func typedVal(val intstr.IntOrString) bool {
	if intstr.Int == val.Type {
		return true
	}
	switch strings.ToLower(val.StrVal) {
	case "true", "false", "null":
		return true
	default:
		return false
	}
}

// escapeComma should be passed a string consisting of zero or more backslashes, followed by a comma.
// If there are an even number of characters (such as `\,` or `\\\,`) then the comma is escaped.
// If there are an uneven number of characters (such as `,` or `\\,` then the comma is not escaped,
// and we need to escape it by adding an additional backslash.
// This logic is difficult if not impossible to accomplish with a simple regex submatch replace.
func escapeComma(match string) string {
	if len(match)%2 == 1 {
		match = `\` + match
	}
	return match
}

func setProxyEnv(job *batch.Job) {
	proxySysEnv := []string{
		"all_proxy",
		"ALL_PROXY",
		"http_proxy",
		"HTTP_PROXY",
		"https_proxy",
		"HTTPS_PROXY",
		"no_proxy",
		"NO_PROXY",
	}
	for _, proxyEnv := range proxySysEnv {
		proxyEnvValue := os.Getenv(proxyEnv)
		if len(proxyEnvValue) == 0 {
			continue
		}
		envar := core.EnvVar{
			Name:  proxyEnv,
			Value: proxyEnvValue,
		}
		job.Spec.Template.Spec.Containers[0].Env = append(
			job.Spec.Template.Spec.Containers[0].Env,
			envar)
	}
}

func contentConfigMap(chart *helmv1.HelmChart) *core.ConfigMap {
	configMap := &core.ConfigMap{
		TypeMeta: meta.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      fmt.Sprintf("chart-content-%s", chart.Name),
			Namespace: chart.Namespace,
		},
		Data: map[string]string{},
	}

	if chart.Spec.ChartContent != "" {
		key := fmt.Sprintf("%s.tgz.base64", chart.Name)
		configMap.Data[key] = chart.Spec.ChartContent
	}

	return configMap
}

func setValuesConfigMap(job *batch.Job, chart *helmv1.HelmChart) *core.ConfigMap {
	configMap := valuesConfigMap(chart)

	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, []core.Volume{
		{
			Name: "values",
			VolumeSource: core.VolumeSource{
				ConfigMap: &core.ConfigMapVolumeSource{
					LocalObjectReference: core.LocalObjectReference{
						Name: configMap.Name,
					},
				},
			},
		},
	}...)

	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, []core.VolumeMount{
		{
			MountPath: "/config",
			Name:      "values",
		},
	}...)

	return configMap
}

func setContentConfigMap(job *batch.Job, chart *helmv1.HelmChart) *core.ConfigMap {
	configMap := contentConfigMap(chart)
	if configMap == nil {
		return nil
	}

	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, []core.Volume{
		{
			Name: "content",
			VolumeSource: core.VolumeSource{
				ConfigMap: &core.ConfigMapVolumeSource{
					LocalObjectReference: core.LocalObjectReference{
						Name: configMap.Name,
					},
				},
			},
		},
	}...)

	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, []core.VolumeMount{
		{
			MountPath: "/chart",
			Name:      "content",
		},
	}...)

	return configMap
}

func hashConfigMaps(job *batch.Job, maps ...*core.ConfigMap) {
	hash := sha256.New()

	for _, configMap := range maps {
		for k, v := range configMap.Data {
			hash.Write([]byte(k))
			hash.Write([]byte(v))
		}
		for k, v := range configMap.BinaryData {
			hash.Write([]byte(k))
			hash.Write(v)
		}
	}

	job.Spec.Template.ObjectMeta.Annotations[Annotation] = fmt.Sprintf("SHA256=%X", hash.Sum(nil))
}
