// Copyright 2020 The Kubernetes Authors.
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

package spoketoken

import (
	"context"
	"encoding/json"
	"time"

	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
	ocinfrav1 "github.com/openshift/api/config/v1"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	secretSuffix             = "-cluster-secret"
	requeuAfter              = 5
	infrastructureConfigName = "cluster"
)

// Add creates a new agent token controller and adds it to the Manager if standalone is false.
func Add(mgr manager.Manager, hubconfig *rest.Config, ops config.SubscriptionCMDoptions) error {
	if !ops.Standalone {
		hubclient, err := client.New(hubconfig, client.Options{})

		if err != nil {
			klog.Error("Failed to generate client to hub cluster with error:", err)
			return err
		}

		return add(mgr, newReconciler(mgr, hubclient, ops.Syncid, mgr.GetConfig().Host))
	}

	return nil
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, hubclient client.Client, syncid *types.NamespacedName, host string) reconcile.Reconciler {
	rec := &ReconcileAgentToken{
		Client:    mgr.GetClient(),
		scheme:    mgr.GetScheme(),
		hubclient: hubclient,
		syncid:    syncid,
		host:      host,
	}

	return rec
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	klog.Info("Adding klusterlet token controller.")
	// Create a new controller
	c, err := controller.New("klusterlet-token-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to klusterlet-addon-appmgr service account in open-cluster-management-agent-addon namespace.
	err = c.Watch(&source.Kind{Type: &corev1.ServiceAccount{}}, &handler.EnqueueRequestForObject{}, utils.ServiceAccountPredicateFunctions)
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileSubscription implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileAgentToken{}

// ReconcileAppMgrToken syncid is the namespaced name of this managed cluster.
// host is the API server URL of this managed cluster.
type ReconcileAgentToken struct {
	client.Client
	hubclient client.Client
	scheme    *runtime.Scheme
	syncid    *types.NamespacedName
	host      string
}

type Config struct {
	BearerToken     string          `json:"bearerToken"`
	TLSClientConfig map[string]bool `json:"tlsClientConfig"`
}

// Reconciles <clusterName>-cluster-secret secret in the managed cluster's namespace
// on the hub cluster to the klusterlet-addon-appmgr service account's token secret.
// If it is running on the hub, don't do anything.
func (r *ReconcileAgentToken) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	klog.Infof("Reconciling %s", request.NamespacedName)

	appmgrsa := &corev1.ServiceAccount{}

	err := r.Client.Get(context.TODO(), request.NamespacedName, appmgrsa)

	if err != nil {
		if kerrors.IsNotFound(err) {
			klog.Infof("%s is not found. Deleting the secret from the hub.", request.NamespacedName)

			err := r.hubclient.Delete(context.TODO(), r.prepareAgentTokenSecret(""))

			if err != nil {
				klog.Error("Failed to delete the secret from the hub.")
				return reconcile.Result{RequeueAfter: requeuAfter * time.Minute}, err
			}
		} else {
			klog.Errorf("Failed to get serviceaccount %v, error: %v", request.NamespacedName, err)
			return reconcile.Result{RequeueAfter: requeuAfter * time.Minute}, err
		}
	}

	// Get the service account token from the service account's secret list
	saSecret := r.getServiceAccountTokenSecret(*appmgrsa)

	if saSecret == nil {
		klog.Error("Failed to find the service account token.")
		return reconcile.Result{}, errors.New("failed to find the klusterlet agent addon service account token secret")
	}

	token, ok := saSecret.Data["token"]

	if !ok {
		klog.Error("Serviceaccount token secret does not contain token.")
		return reconcile.Result{}, errors.New("the klusterlet agent addon service account token secret does not contain token")
	}

	// Prepare the secret to be created/updated in the managed cluster namespace on the hub
	secret := r.prepareAgentTokenSecret(string(token))

	// Get the existing secret in the managed cluster namespace from the hub
	hubSecret := &corev1.Secret{}
	hubSecretName := types.NamespacedName{Namespace: r.syncid.Namespace, Name: r.syncid.Name + secretSuffix}
	err = r.hubclient.Get(context.TODO(), hubSecretName, hubSecret)

	if err != nil {
		if kerrors.IsNotFound(err) {
			klog.Info("Secret " + hubSecretName.String() + " not found on the hub.")

			err := r.hubclient.Create(context.TODO(), secret)

			if err != nil {
				klog.Error(err.Error())
				return reconcile.Result{RequeueAfter: requeuAfter * time.Minute}, err
			}

			klog.Info("The cluster secret " + secret.Name + " was created in " + secret.Namespace + " on the hub successfully.")
		} else {
			klog.Error("Failed to get secret from the hub: ", err)
			return reconcile.Result{RequeueAfter: requeuAfter * time.Minute}, err
		}
	} else {
		// Update if the content has changed
		if secret.StringData["config"] != string(hubSecret.Data["config"]) ||
			secret.StringData["name"] != string(hubSecret.Data["name"]) ||
			secret.StringData["server"] != string(hubSecret.Data["server"]) {
			klog.Infof("The service account klusterlet-addon-appmgr token secret has changed. Updating %s on the hub cluster.", hubSecretName.String())
			err = r.hubclient.Update(context.TODO(), secret)

			if err != nil {
				klog.Error("Failed to update secret : ", err)
				return reconcile.Result{RequeueAfter: time.Duration(requeuAfter * time.Minute.Milliseconds())}, err
			}
		} else {
			klog.Info("The service account klusterlet-addon-appmgr token secret has not changed.")
		}
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileAgentToken) prepareAgentTokenSecret(token string) *corev1.Secret {
	mcSecret := &corev1.Secret{}
	mcSecret.Name = r.syncid.Name + secretSuffix
	mcSecret.Namespace = r.syncid.Namespace

	labels := make(map[string]string)
	labels["argocd.argoproj.io/secret-type"] = "cluster"
	labels["apps.open-cluster-management.io/secret-type"] = "acm-cluster"

	mcSecret.SetLabels(labels)

	configData := &Config{}
	configData.BearerToken = token
	tlsClientConfig := make(map[string]bool)
	tlsClientConfig["insecure"] = true
	configData.TLSClientConfig = tlsClientConfig

	jsonConfigData, err := json.MarshalIndent(configData, "", "  ")

	if err != nil {
		klog.Error(err)
	}

	apiServerURL, err := r.getKubeAPIServerAddress()

	if err != nil {
		klog.Error(err)
	}

	data := make(map[string]string)
	data["name"] = r.syncid.Name
	data["server"] = apiServerURL
	data["config"] = string(jsonConfigData)

	mcSecret.StringData = data

	return mcSecret
}

func (r *ReconcileAgentToken) getServiceAccountTokenSecret(sa corev1.ServiceAccount) *corev1.Secret {
	// Get the service account token from the service account's secret list
	saSecret := &corev1.Secret{}

	for _, secret := range sa.Secrets {
		secretName := types.NamespacedName{
			Name:      secret.Name,
			Namespace: sa.Namespace,
		}

		if err := r.Client.Get(context.TODO(), secretName, saSecret); err != nil {
			continue
		}

		// Get the service account token
		if saSecret.Type == corev1.SecretTypeServiceAccountToken {
			break
		}
	}

	return saSecret
}

// getKubeAPIServerAddress - Get the API server address from OpenShift kubernetes cluster. This does not work with other kubernetes.
func (r *ReconcileAgentToken) getKubeAPIServerAddress() (string, error) {
	infraConfig := &ocinfrav1.Infrastructure{}

	if err := r.Client.Get(context.TODO(), types.NamespacedName{Name: infrastructureConfigName}, infraConfig); err != nil {
		return "", err
	}

	return infraConfig.Status.APIServerURL, nil
}
