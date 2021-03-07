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

package kubernetes

import (
	"fmt"
	"sync"
	"time"

	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	syncWorkNum = 5
)

// TemplateUnit defines the basic unit of Template and whether it should be updated or not
type TemplateUnit struct {
	*unstructured.Unstructured
	Source          string
	ResourceUpdated bool
	StatusUpdated   bool
}

// ResourceMap is a registry for all resources
type ResourceMap struct {
	GroupVersionResource schema.GroupVersionResource
	Namespaced           bool
	ServerUpdated        bool
	TemplateMap          map[string]*TemplateUnit
}

// KubeSynchronizer handles resources to a kube endpoint
type KubeSynchronizer struct {
	Interval      int
	LocalClient   client.Client
	RemoteClient  client.Client
	localConfig   *rest.Config
	DynamicClient dynamic.Interface

	kmtx           sync.Mutex // lock the kubeResource
	KubeResources  map[schema.GroupVersionKind]*ResourceMap
	SynchronizerID *types.NamespacedName
	Extension      Extension
	eventrecorder  *utils.EventRecorder
	tplCh          chan resourceOrder

	dmtx           sync.Mutex //this lock protect the dynamicFactory and stopCh
	stopCh         chan struct{}
	dynamicFactory dynamicinformer.DynamicSharedInformerFactory
}

var (
	crdRetryMultiplier = 3
)

var defaultSynchronizer *KubeSynchronizer

// Add creates the default syncrhonizer and add the start function as runnable into manager
func Add(mgr manager.Manager, hubconfig *rest.Config, ops config.SubscriptionCMDoptions) error {
	var err error
	defaultSynchronizer, err = CreateSynchronizer(mgr.GetConfig(), hubconfig, mgr.GetScheme(), ops.Syncid, ops.SyncInterval, defaultExtension)

	if err != nil {
		klog.Error("Failed to create synchronizer with error: ", err)
		return err
	}

	return mgr.Add(defaultSynchronizer)
}

// GetDefaultSynchronizer - return the default kubernetse synchronizer
func GetDefaultSynchronizer() *KubeSynchronizer {
	return defaultSynchronizer
}

// CreateSynchronizer createa an instance of synchrizer with give api-server config
func CreateSynchronizer(config, remoteConfig *rest.Config, scheme *runtime.Scheme, syncid *types.NamespacedName,
	interval int, ext Extension) (*KubeSynchronizer, error) {
	if klog.V(utils.QuiteLogLel) {
		fnName := utils.GetFnName()
		klog.Infof("Entering: %v()", fnName)

		defer klog.Infof("Exiting: %v()", fnName)
	}

	var err error

	dynamicClient := dynamic.NewForConfigOrDie(config)

	s := &KubeSynchronizer{
		Interval:       interval,
		SynchronizerID: syncid,
		DynamicClient:  dynamicClient,
		localConfig:    config,
		kmtx:           sync.Mutex{},
		KubeResources:  make(map[schema.GroupVersionKind]*ResourceMap),
		Extension:      ext,
		tplCh:          make(chan resourceOrder, syncWorkNum),
		dmtx:           sync.Mutex{},
		stopCh:         make(chan struct{}),
	}

	s.LocalClient, err = client.New(config, client.Options{})

	if err != nil {
		klog.Error("Failed to initialize client to update local status. err: ", err)
		return nil, err
	}

	s.RemoteClient = s.LocalClient
	if remoteConfig != nil {
		s.RemoteClient, err = client.New(remoteConfig, client.Options{})

		if err != nil {
			klog.Error("Failed to initialize client to update remote status. err: ", err)
			return nil, err
		}
	}

	defaultExtension.localClient = s.LocalClient
	defaultExtension.remoteClient = s.RemoteClient

	if ext == nil {
		s.Extension = defaultExtension
	}

	s.eventrecorder, err = utils.NewEventRecorder(config, scheme)

	if err != nil {
		klog.Error("Failed to create event recorder. err: ", err)
		return nil, err
	}

	s.discoverResourcesOnce()

	return s, nil
}

// Start the discovery and start caches, this will be triggered by the manager
func (sync *KubeSynchronizer) Start(s <-chan struct{}) error {
	klog.Info("start synchronizer")
	defer klog.Info("stop synchronizer")

	sync.rediscoverResource()

	go sync.processTplChan(s)

	return nil
}

func (sync *KubeSynchronizer) processTplChan(stopCh <-chan struct{}) {
	crdTicker := time.NewTicker(time.Duration(sync.Interval*crdRetryMultiplier) * time.Second)

	defer klog.Info("stop synchronizer channel")

	for {
		select {
		case order, ok := <-sync.tplCh:
			if !ok {
				sync.tplCh = nil
			}

			klog.Infof("order processor: received order %v", order.hostSub.String())

			st := time.Now()
			err := sync.processOrder(order)
			order.err <- err
			close(order.err)

			klog.Infof("order processor: done order %v, took: %v", order.hostSub.String(), time.Since(st))
		case <-crdTicker.C: //discovery CRD resource applied by user
			sync.rediscoverResource()
		case <-stopCh: //this channel is from controller manager
			close(sync.tplCh)

			crdTicker.Stop()

			return
		}
	}
}

func (sync *KubeSynchronizer) purgeSubscribedResource(subType string, hostSub types.NamespacedName) error {
	var err error

	sync.kmtx.Lock()
	defer sync.kmtx.Unlock()

	for _, resmap := range sync.KubeResources {
		for _, tplunit := range resmap.TemplateMap {
			tplhost := sync.Extension.GetHostFromObject(tplunit)
			tpldpl := utils.GetHostDeployableFromObject(tplunit)

			if tplhost != nil && tplhost.String() == hostSub.String() {
				klog.V(10).Infof("Start DeRegister, with host: %s, dpl: %s", tplhost, tpldpl)
				err = sync.DeRegisterTemplate(*tplhost, *tpldpl, subType)

				if err != nil {
					klog.Error("Failed to deregister template for cleanup by host with error: ", err)
				}
			}
		}
	}

	return err
}

func (sync *KubeSynchronizer) processOrder(order resourceOrder) error {
	// meaning clean up all the resource from a source:host
	if len(order.dpls) == 0 {
		return sync.purgeSubscribedResource(order.subType, order.hostSub)
	}

	keySet := make(map[string]bool)
	resSet := make(map[string]map[schema.GroupVersionKind]bool)
	crdFlag := false

	var err error

	//adding update,new resource to cache and create them at cluster
	for _, dplUn := range order.dpls {
		err = sync.RegisterTemplate(order.hostSub, dplUn.Dpl, order.subType)
		if err != nil {
			klog.Error(fmt.Sprintf("failed to register template of %v/%v, error: %v",
				dplUn.Dpl.GetNamespace(), dplUn.Dpl.GetName(), err))
			continue
		}

		dplKey := types.NamespacedName{Name: dplUn.Dpl.GetName(), Namespace: dplUn.Dpl.GetNamespace()}
		rKey := sync.generateResourceMapKey(order.hostSub, dplKey)
		keySet[rKey] = true

		if _, ok := resSet[rKey][dplUn.Gvk]; !ok {
			resSet[rKey] = map[schema.GroupVersionKind]bool{dplUn.Gvk: true}
		}

		if dplUn.Gvk.Kind == crdKind {
			crdFlag = true
		}
	}

	// handle orphan resource
	sync.kmtx.Lock()
	for resgvk, resmap := range sync.KubeResources {
		for reskey, tplunit := range resmap.TemplateMap {
			//if resource's key don't belong to the current key set, then do
			//nothing
			if _, ok := keySet[reskey]; ok {
				continue
			}

			//if the resource map have a key belong to this resource order, then
			//check if the order required the resgvk, if not, delete the resgvk
			if _, ok := resSet[reskey][resgvk]; !ok {
				// will ignore non-syncsource templates
				tplhost := sync.Extension.GetHostFromObject(tplunit)
				tpldpl := utils.GetHostDeployableFromObject(tplunit)

				klog.V(10).Infof("Start DeRegister, with resgvk: %v, reskey: %s", resgvk, reskey)

				err = sync.DeRegisterTemplate(*tplhost, *tpldpl, order.subType)

				if err != nil {
					klog.Error("Failed to deregister template for applying validator with error: ", err)
				}
			}
		}

		sync.applyKindTemplates(resmap)
	}
	sync.kmtx.Unlock()

	if crdFlag {
		sync.rediscoverResource()
	}

	return err
}
