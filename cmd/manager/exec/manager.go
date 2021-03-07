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

package exec

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	ansiblejob "github.com/open-cluster-management/ansiblejob-go-lib/api/v1alpha1"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/controller"
	leasectrl "github.com/open-cluster-management/multicloud-operators-subscription/pkg/controller/subscription"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/subscriber"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/synchronizer"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/webhook"

	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
	ocinfrav1 "github.com/openshift/api/config/v1"
)

// Change below variables to serve metrics on different host or port.
var (
	metricsHost             = "0.0.0.0"
	metricsPort         int = 8381
	operatorMetricsPort int = 8684
)

const (
	AddonName               = "application-manager"
	leaseUpdateJitterFactor = 0.25
)

func RunManager() {
	enableLeaderElection := false

	if _, err := rest.InClusterConfig(); err == nil {
		klog.Info("LeaderElection enabled as running in a cluster")

		enableLeaderElection = true
	} else {
		klog.Info("LeaderElection disabled as not running in a cluster")
	}

	// for hub subcription pod
	leaderElectionID := "multicloud-operators-hub-subscription-leader.open-cluster-management.io"

	if options.Standalone {
		// for standalone subcription pod
		leaderElectionID = "multicloud-operators-standalone-subscription-leader.open-cluster-management.io"
		metricsPort = 8389
	} else if !strings.EqualFold(options.ClusterName, "") && !strings.EqualFold(options.ClusterNamespace, "") {
		// for managed cluster pod appmgr. It could run on hub if hub is self-managed cluster
		metricsPort = 8388
		leaderElectionID = "multicloud-operators-remote-subscription-leader.open-cluster-management.io"
	}

	// increase the dafault QPS(5) to 100, only sends 5 requests to API server
	// seems to be unrealistic. Reading some other projects, it seems QPS 100 is
	// a pretty common practice
	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = 100.0
	cfg.Burst = 200

	// Create a new Cmd to provide shared dependencies and start components
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		MetricsBindAddress:      fmt.Sprintf("%s:%d", metricsHost, metricsPort),
		Port:                    operatorMetricsPort,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        leaderElectionID,
		LeaderElectionNamespace: "kube-system",
	})

	if err != nil {
		klog.Error(err, "")
		os.Exit(1)
	}

	// id is the namespacedname of this cluster in hub
	var id = &types.NamespacedName{
		Name:      options.ClusterName,
		Namespace: options.ClusterNamespace,
	}

	options.Syncid = id

	// generate config to hub cluster
	hubconfig := mgr.GetConfig()
	if options.HubConfigFilePathName != "" {
		hubconfig, err = clientcmd.BuildConfigFromFlags("", options.HubConfigFilePathName)

		if err != nil {
			klog.Error("Failed to build config to hub cluster with the pathname provided ", options.HubConfigFilePathName, " err:", err)
			os.Exit(1)
		}
	}

	klog.Info("Starting ... Registering Components for cluster: ", id)

	// Setup ansibleJob Scheme for manager
	if err := ansiblejob.AddToScheme(mgr.GetScheme()); err != nil {
		klog.Error(err, "")
		os.Exit(1)
	}

	// Setup Scheme for all resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		klog.Error(err, "")
		os.Exit(1)
	}

	if !options.Standalone && options.ClusterName == "" && options.ClusterNamespace == "" { //start the appsub controller in hub mode
		// Setup all Hub Controllers
		if err := controller.AddHubToManager(mgr); err != nil {
			klog.Error(err, "")
			os.Exit(1)
		}

		// Setup Webhook listner
		options.CreateService = true
		if err := webhook.AddToManager(mgr, hubconfig, options); err != nil {
			klog.Error("Failed to initialize WebHook listener with error:", err)
			os.Exit(1)
		}
	} else if !strings.EqualFold(options.ClusterName, "") && !strings.EqualFold(options.ClusterNamespace, "") { //start the appsub controller in remote mode
		// Setup ocinfrav1 Scheme for manager
		if err := ocinfrav1.AddToScheme(mgr.GetScheme()); err != nil {
			klog.Error(err, "")
			os.Exit(1)
		}

		options.Standalone = false
		if err := setupStandalone(mgr, hubconfig, options); err != nil {
			klog.Error("Failed to setup managed subscription, error:", err)
			os.Exit(1)
		}

		// set up lease controller for updating the application-manager lease in each managed cluster namespace on hub
		// The application-manager lease resource is jitter updated every 60 seconds by default.
		managedClusterKubeClient, err := kubernetes.NewForConfig(mgr.GetConfig())
		if err != nil {
			klog.Error("Unable to create managed cluster kube client.", err)
			os.Exit(1)
		}

		leaseReconciler := leasectrl.LeaseReconciler{
			KubeClient:           managedClusterKubeClient,
			LeaseName:            AddonName,
			LeaseNamespace:       options.ClusterName,
			LeaseDurationSeconds: int32(options.LeaseDurationSeconds),
			HubKubeConfigPath:    options.HubConfigFilePathName,
			KubeFake:             false,
		}

		go wait.JitterUntilWithContext(context.TODO(), leaseReconciler.Reconcile,
			time.Duration(options.LeaseDurationSeconds)*time.Second, leaseUpdateJitterFactor, true)
	} else if err := setupStandalone(mgr, hubconfig, options); err != nil { //start the appsub controller in standalone mode
		klog.Error("Failed to setup standalone subscription, error:", err)
		os.Exit(1)
	}

	sig := signals.SetupSignalHandler()

	klog.Info("Starting the Cmd.")

	// Start the Cmd
	if err := mgr.Start(sig); err != nil {
		klog.Error(err, "Manager exited non-zero")
		os.Exit(1)
	}
}

func setupStandalone(mgr manager.Manager, hubconfig *rest.Config, ops config.SubscriptionCMDoptions) error {
	// Setup Synchronizer
	if err := synchronizer.AddToManager(mgr, hubconfig, ops); err != nil {
		klog.Error("Failed to initialize synchronizer with error:", err)
		return err
	}

	// Setup Subscribers
	if err := subscriber.AddToManager(mgr, hubconfig, ops); err != nil {
		klog.Error("Failed to initialize subscriber with error:", err)
		return err
	}

	// Setup all Controllers
	if err := controller.AddToManager(mgr, hubconfig, ops); err != nil {
		klog.Error("Failed to initialize controller with error:", err)
		return err
	}

	if ops.Standalone {
		// Setup Webhook listner
		ops.CreateService = false
		if err := webhook.AddToManager(mgr, hubconfig, ops); err != nil {
			klog.Error("Failed to initialize WebHook listener with error:", err)
			return err
		}
	}

	return nil
}
