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

package controller

import (
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// AddToManagerMCMFuncs is a list of functions to add all MCM Controllers (with config to hub) to the Manager
var AddToManagerMCMFuncs []func(manager.Manager, *rest.Config, config.SubscriptionCMDoptions) error

// AddToManagerFuncs is a list of functions to add all Controllers to the Manager
var AddToManagerFuncs []func(manager.Manager) error

// AddHelmToManagerFuncs is a list of functions to add helmrelease Controller to the Manager
var AddHelmToManagerFuncs []func(manager.Manager) error

// AddHubToManagerFuncs is a list of functions to add all Hub Controllers to the Manager
var AddHubToManagerFuncs []func(manager.Manager) error

// AddToManager adds all Controllers to the Manager
func AddToManager(m manager.Manager, cfg *rest.Config, ops config.SubscriptionCMDoptions) error {
	for _, f := range AddToManagerFuncs {
		if err := f(m); err != nil {
			return err
		}
	}

	// If remote subscription pod (appmgr) is running in hub, don't add helmrelease controller to the manager,
	// As there has been a helmrelease controller running in standalone subscription pod
	if ops.Standalone || !utils.IsHub(m.GetConfig()) {
		klog.Info("Add helmrelease controller when the remote subscription is NOT running on hub or standalone subscription")

		for _, f := range AddHelmToManagerFuncs {
			if err := f(m); err != nil {
				return err
			}
		}
	}

	for _, f := range AddToManagerMCMFuncs {
		if err := f(m, cfg, ops); err != nil {
			return err
		}
	}

	return nil
}

// AddHubToManager adds all Hub Controllers to the Manager
func AddHubToManager(m manager.Manager) error {
	for _, f := range AddHubToManagerFuncs {
		if err := f(m); err != nil {
			return err
		}
	}

	return nil
}
