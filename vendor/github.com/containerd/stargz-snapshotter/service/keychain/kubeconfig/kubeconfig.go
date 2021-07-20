/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package kubeconfig

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/stargz-snapshotter/service/resolver"
	dcfile "github.com/docker/cli/cli/config/configfile"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

const dockerconfigSelector = "type=" + string(corev1.SecretTypeDockerConfigJson)

type options struct {
	kubeconfigPath string
}

type Option func(*options)

func WithKubeconfigPath(path string) Option {
	return func(opts *options) {
		opts.kubeconfigPath = path
	}
}

// NewKubeconfigKeychain provides a keychain which can sync its contents with
// kubernetes API server by fetching all `kubernetes.io/dockerconfigjson`
// secrets in the cluster with provided kubeconfig. It's OK that config provides
// kubeconfig path but the file doesn't exist at that moment. In this case, this
// keychain keeps on trying to read the specified path periodically and when the
// file is actually provided, this keychain tries to access API server using the
// file. This is useful for some environments (e.g. single node cluster with
// containerized apiserver) where stargz snapshotter needs to start before
// everything, including booting containerd/kubelet/apiserver and configuring
// users/roles.
// TODO: support update of kubeconfig file
func NewKubeconfigKeychain(ctx context.Context, opts ...Option) resolver.Credential {
	var kcOpts options
	for _, o := range opts {
		o(&kcOpts)
	}
	kc := newKeychain(ctx, kcOpts.kubeconfigPath)
	return kc.credentials
}

func newKeychain(ctx context.Context, kubeconfigPath string) *keychain {
	kc := &keychain{
		config: make(map[string]*dcfile.ConfigFile),
	}
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("kubeconfig", kubeconfigPath))
	go func() {
		if kubeconfigPath != "" {
			log.G(ctx).Debugf("Waiting for kubeconfig being installed...")
			for {
				if _, err := os.Stat(kubeconfigPath); err == nil {
					break
				} else if !os.IsNotExist(err) {
					log.G(ctx).WithError(err).
						Warnf("failed to read; Disabling syncing")
					return
				}
				time.Sleep(10 * time.Second)
			}
		}

		// default loader for KUBECONFIG or `~/.kube/config`
		// if no explicit path provided, KUBECONFIG will be used.
		// if KUBECONFIG doesn't contain paths, `~/.kube/config` will be used.
		loadingRule := clientcmd.NewDefaultClientConfigLoadingRules()

		// explicitly provide path for kubeconfig.
		// if path isn't "", this path will be respected.
		loadingRule.ExplicitPath = kubeconfigPath

		// load and merge config files
		clientcfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRule,                  // loader for config files
			&clientcmd.ConfigOverrides{}, // no overrides for config
		).ClientConfig()
		if err != nil {
			log.G(ctx).WithError(err).Warnf("failed to load config; Disabling syncing")
			return
		}

		client, err := kubernetes.NewForConfig(clientcfg)
		if err != nil {
			log.G(ctx).WithError(err).Warnf("failed to prepare client; Disabling syncing")
			return
		}
		if err := kc.startSyncSecrets(ctx, client); err != nil {
			log.G(ctx).WithError(err).Warnf("failed to sync secrets")
		}
	}()
	return kc
}

type keychain struct {
	config   map[string]*dcfile.ConfigFile
	configMu sync.Mutex

	// the following entries are used for syncing secrets with API server.
	// these fields are lazily filled after kubeconfig file is provided.
	queue    *workqueue.Type
	informer cache.SharedIndexInformer
}

func (kc *keychain) credentials(host string, refspec reference.Spec) (string, string, error) {
	if host == "docker.io" || host == "registry-1.docker.io" {
		// Creds of "docker.io" is stored keyed by "https://index.docker.io/v1/".
		host = "https://index.docker.io/v1/"
	}
	kc.configMu.Lock()
	defer kc.configMu.Unlock()
	for _, cfg := range kc.config {
		if acfg, err := cfg.GetAuthConfig(host); err == nil {
			if acfg.IdentityToken != "" {
				return "", acfg.IdentityToken, nil
			} else if !(acfg.Username == "" && acfg.Password == "") {
				return acfg.Username, acfg.Password, nil
			}
		}
	}
	return "", "", nil
}

func (kc *keychain) startSyncSecrets(ctx context.Context, client kubernetes.Interface) error {

	// don't let panics crash the process
	defer utilruntime.HandleCrash()

	// get informed on `kubernetes.io/dockerconfigjson` secrets in all namespaces
	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				// TODO: support legacy image secret `kubernetes.io/dockercfg`
				options.FieldSelector = dockerconfigSelector
				return client.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				// TODO: support legacy image secret `kubernetes.io/dockercfg`
				options.FieldSelector = dockerconfigSelector
				return client.CoreV1().Secrets(metav1.NamespaceAll).Watch(ctx, options)
			},
		},
		&corev1.Secret{},
		0,
		cache.Indexers{},
	)

	// use workqueue because each task possibly takes long for parsing config,
	// wating for lock, etc...
	queue := workqueue.New()
	defer queue.ShutDown()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	})
	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return fmt.Errorf("Timed out for syncing cache")
	}

	// get informer and queue
	kc.informer = informer
	kc.queue = queue

	// keep on syncing secrets
	wait.Until(kc.runWorker, time.Second, ctx.Done())

	return nil
}

func (kc *keychain) runWorker() {
	for kc.processNextItem() {
		// continue looping
	}
}

// TODO: consider retrying?
func (kc *keychain) processNextItem() bool {
	key, quit := kc.queue.Get()
	if quit {
		return false
	}
	defer kc.queue.Done(key)

	obj, exists, err := kc.informer.GetIndexer().GetByKey(key.(string))
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get object; don't sync %q: %v", key, err))
		return true
	}
	if !exists {
		kc.configMu.Lock()
		delete(kc.config, key.(string))
		kc.configMu.Unlock()
		return true
	}

	// TODO: support legacy image secret `kubernetes.io/dockercfg`
	data, ok := obj.(*corev1.Secret).Data[corev1.DockerConfigJsonKey]
	if !ok {
		utilruntime.HandleError(fmt.Errorf("no secret is provided; don't sync %q", key))
		return true
	}
	configFile := dcfile.New("")
	if err := configFile.LoadFromReader(bytes.NewReader(data)); err != nil {
		utilruntime.HandleError(fmt.Errorf("broken data; don't sync %q: %v", key, err))
		return true
	}
	kc.configMu.Lock()
	kc.config[key.(string)] = configFile
	kc.configMu.Unlock()

	return true
}
