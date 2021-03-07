// Copyright 2019 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package helmrepo

import (
	"errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	appv1alpha1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
	kubesynchronizer "github.com/open-cluster-management/multicloud-operators-subscription/pkg/synchronizer/kubernetes"
)

type SyncSource interface {
	GetInterval() int
	GetLocalClient() client.Client
	GetValidatedGVK(schema.GroupVersionKind) *schema.GroupVersionKind
	IsResourceNamespaced(schema.GroupVersionKind) bool
	AddTemplates(string, types.NamespacedName, []kubesynchronizer.DplUnit) error
	CleanupByHost(types.NamespacedName, string) error
}

type itemmap map[types.NamespacedName]*SubscriberItem

// Subscriber - information to run namespace subscription
type Subscriber struct {
	itemmap
	manager      manager.Manager
	synchronizer SyncSource
	syncinterval int
}

var defaultSubscriber *Subscriber

var helmreposyncsource = "subhelm-"

// Add does nothing for namespace subscriber, it generates cache for each of the item syncid *types.NamespacedName, syncinterval int
func Add(mgr manager.Manager, hubconfig *rest.Config, ops config.SubscriptionCMDoptions) error {
	// No polling, use cache. Add default one for cluster namespace
	var err error

	klog.V(2).Info("Setting up default helmrepo subscriber on ", ops.Syncid)

	sync := kubesynchronizer.GetDefaultSynchronizer()
	if sync == nil {
		err = kubesynchronizer.Add(mgr, hubconfig, ops)
		if err != nil {
			klog.Error("Failed to initialize synchronizer for default namespace channel with error:", err)
			return err
		}

		sync = kubesynchronizer.GetDefaultSynchronizer()
	}

	if err != nil {
		klog.Error("Failed to create synchronizer for subscriber with error:", err)
		return err
	}

	defaultSubscriber = CreateHelmRepoSubsriber(hubconfig, mgr.GetScheme(), mgr, sync, ops.SyncInterval)
	if defaultSubscriber == nil {
		errmsg := "failed to create default namespace subscriber"

		return errors.New(errmsg)
	}

	return nil
}

// SubscribeItem subscribes a subscriber item with namespace channel
func (hrs *Subscriber) SubscribeItem(subitem *appv1alpha1.SubscriberItem) error {
	if hrs.itemmap == nil {
		hrs.itemmap = make(map[types.NamespacedName]*SubscriberItem)
	}

	itemkey := types.NamespacedName{Name: subitem.Subscription.Name, Namespace: subitem.Subscription.Namespace}
	klog.V(2).Info("subscribeItem ", itemkey)

	hrssubitem, ok := hrs.itemmap[itemkey]

	if !ok {
		hrssubitem = &SubscriberItem{}
		hrssubitem.syncinterval = hrs.syncinterval
		hrssubitem.synchronizer = hrs.synchronizer
	}

	subitem.DeepCopyInto(&hrssubitem.SubscriberItem)
	hrssubitem.hash = ""

	hrs.itemmap[itemkey] = hrssubitem

	hrssubitem.Start()

	return nil
}

// UnsubscribeItem uhrsubscribes a namespace subscriber item
func (hrs *Subscriber) UnsubscribeItem(key types.NamespacedName) error {
	klog.Info("helm UnsubscribeItem ", key)

	subitem, ok := hrs.itemmap[key]

	if ok {
		subitem.Stop()
		delete(hrs.itemmap, key)

		if err := hrs.synchronizer.CleanupByHost(key, helmreposyncsource+key.String()); err != nil {
			klog.Errorf("failed to unsubscribe %v, err: %v", key.String(), err)
			return err
		}
	}

	return nil
}

// GetDefaultSubscriber - returns the defajlt namespace subscriber
func GetDefaultSubscriber() appv1alpha1.Subscriber {
	return defaultSubscriber
}

// CreateNamespaceSubsriber - create namespace subscriber with config to hub cluster, scheme of hub cluster and a syncrhonizer to local cluster
func CreateHelmRepoSubsriber(config *rest.Config, scheme *runtime.Scheme, mgr manager.Manager,
	kubesync SyncSource, syncinterval int) *Subscriber {
	if config == nil || kubesync == nil {
		klog.Error("Can not create namespace subscriber with config: ", config, " kubenetes synchronizer: ", kubesync)
		return nil
	}

	hrsubscriber := &Subscriber{
		manager:      mgr,
		synchronizer: kubesync,
	}

	hrsubscriber.itemmap = make(map[types.NamespacedName]*SubscriberItem)
	hrsubscriber.syncinterval = syncinterval

	return hrsubscriber
}
