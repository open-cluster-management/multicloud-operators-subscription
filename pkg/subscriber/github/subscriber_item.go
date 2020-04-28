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

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"github.com/ghodss/yaml"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/kustomize"
	"k8s.io/helm/pkg/repo"
	"k8s.io/klog"
	"sigs.k8s.io/kustomize/pkg/fs"

	corev1 "k8s.io/api/core/v1"

	dplv1 "github.com/open-cluster-management/multicloud-operators-deployable/pkg/apis/apps/v1"
	appv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	kubesynchronizer "github.com/open-cluster-management/multicloud-operators-subscription/pkg/synchronizer/kubernetes"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
)

const (
	// UserID is key of GitHub user ID in secret
	UserID = "user"
	// AccessToken is key of GitHub user password or personal token in secret
	AccessToken = "accessToken"
	// Path is the key of GitHub package filter config map
	Path = "path"
)

// SubscriberItem - defines the unit of namespace subscription
type SubscriberItem struct {
	appv1.SubscriberItem
	crdsAndNamespaceFiles []string
	rbacFiles             []string
	otherFiles            []string
	repoRoot              string
	commitID              string
	stopch                chan struct{}
	syncinterval          int
	synchronizer          *kubesynchronizer.KubeSynchronizer
	chartDirs             map[string]string
	kustomizeDirs         map[string]string
	indexFile             *repo.IndexFile
	webhookEnabled        bool
	successful            bool
}

type kubeResource struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
}

// Start subscribes a subscriber item with github channel
func (ghsi *SubscriberItem) Start() {
	// do nothing if already started
	if ghsi.stopch != nil {
		klog.V(4).Info("SubscriberItem already started: ", ghsi.Subscription.Name)
		return
	}

	ghsi.stopch = make(chan struct{})

	klog.Info("Polling on SubscriberItem ", ghsi.Subscription.Name)

	go wait.Until(func() {
		tw := ghsi.SubscriberItem.Subscription.Spec.TimeWindow
		if tw != nil {
			nextRun := utils.NextStartPoint(tw, time.Now())
			if nextRun > time.Duration(0) {
				klog.V(1).Infof("Subcription %v/%v will de deploy after %v",
					ghsi.SubscriberItem.Subscription.GetNamespace(),
					ghsi.SubscriberItem.Subscription.GetName(), nextRun)
				return
			}
		}

		err := ghsi.doSubscription()
		if err != nil {
			klog.Error(err, " Subscription error.")
			ghsi.successful = false
		} else {
			ghsi.successful = true
		}
	}, time.Duration(ghsi.syncinterval)*time.Second, ghsi.stopch)
}

// Stop unsubscribes a subscriber item with namespace channel
func (ghsi *SubscriberItem) Stop() {
	klog.V(4).Info("Stopping SubscriberItem ", ghsi.Subscription.Name)
	close(ghsi.stopch)
}

func (ghsi *SubscriberItem) doSubscription() error {
	// If webhook is enabled and successful, don't do anything until next reconcilitation.
	if ghsi.webhookEnabled && ghsi.successful {
		return nil
	}

	klog.V(2).Info("Subscribing ...", ghsi.Subscription.Name)
	//Clone the git repo
	commitID, err := ghsi.cloneGitRepo()
	if err != nil {
		klog.Error(err, " Unable to clone the git repo ", ghsi.Channel.Spec.Pathname)
		return err
	}

	if commitID != ghsi.commitID || !ghsi.successful {
		klog.V(2).Info("The commit ID is different or subscription failed previously. Process the cloned repo")

		err := ghsi.sortClonedGitRepo()
		if err != nil {
			klog.Error(err, " Unable to sort helm charts and kubernetes resources from the cloned git repo.")
			return err
		}

		hostkey := types.NamespacedName{Name: ghsi.Subscription.Name, Namespace: ghsi.Subscription.Namespace}
		syncsource := githubk8ssyncsource + hostkey.String()
		kvalid := ghsi.synchronizer.CreateValiadtor(syncsource)
		rscPkgMap := make(map[string]bool)

		klog.V(4).Info("Applying resources: ", ghsi.crdsAndNamespaceFiles)

		err = ghsi.subscribeResources(hostkey, syncsource, kvalid, rscPkgMap, ghsi.crdsAndNamespaceFiles)

		if err != nil {
			klog.Error(err, " Unable to subscribe resource")
			return err
		}

		klog.V(4).Info("Applying resources: ", ghsi.rbacFiles)

		err = ghsi.subscribeResources(hostkey, syncsource, kvalid, rscPkgMap, ghsi.rbacFiles)

		if err != nil {
			klog.Error(err, " Unable to subscribe resource")
			return err
		}

		klog.V(4).Info("Applying resources: ", ghsi.otherFiles)

		err = ghsi.subscribeResources(hostkey, syncsource, kvalid, rscPkgMap, ghsi.otherFiles)

		if err != nil {
			klog.Error(err, "Unable to subscribe resource")
			return err
		}

		klog.V(4).Info("Applying kustomizations: ", ghsi.kustomizeDirs)

		err = ghsi.subscribeKustomizations(hostkey, syncsource, kvalid, rscPkgMap)

		if err != nil {
			klog.Error(err, " Unable to subscribe resource")
			return err
		}

		ghsi.synchronizer.ApplyValiadtor(kvalid)

		klog.V(4).Info("Applying helm charts..")

		err = ghsi.subscribeHelmCharts(ghsi.indexFile)

		if err != nil {
			klog.Error(err, " Unable to subscribe helm charts")
			return err
		}

		ghsi.commitID = commitID

		ghsi.chartDirs = nil
		ghsi.kustomizeDirs = nil
		ghsi.crdsAndNamespaceFiles = nil
		ghsi.rbacFiles = nil
		ghsi.otherFiles = nil
		ghsi.indexFile = nil
	} else {
		klog.V(2).Info("The commit ID is same as before. Skip processing the cloned repo")
	}

	return nil
}

func (ghsi *SubscriberItem) subscribeKustomizations(hostkey types.NamespacedName,
	syncsource string,
	kvalid *kubesynchronizer.Validator,
	pkgMap map[string]bool) error {
	for _, kustomizeDir := range ghsi.kustomizeDirs {
		klog.Info("Applying kustomization ", kustomizeDir)

		relativePath := kustomizeDir

		if len(strings.SplitAfter(kustomizeDir, ghsi.repoRoot+"/")) > 1 {
			relativePath = strings.SplitAfter(kustomizeDir, ghsi.repoRoot+"/")[1]
		}

		for _, ov := range ghsi.Subscription.Spec.PackageOverrides {
			ovKustomizeDir := strings.Split(ov.PackageName, "kustomization")[0]
			if !strings.EqualFold(ovKustomizeDir, relativePath) {
				continue
			} else {
				klog.Info("Overriding kustomization ", kustomizeDir)
				pov := ov.PackageOverrides[0] // there is only one override for kustomization.yaml
				err := utils.OverrideKustomize(pov, kustomizeDir)
				if err != nil {
					klog.Error("Failed to override kustomization.")
					break
				}
			}
		}

		fSys := fs.MakeRealFS()

		var out bytes.Buffer

		err := kustomize.RunKustomizeBuild(&out, fSys, kustomizeDir)
		if err != nil {
			klog.Error("Failed to applying kustomization, error: ", err.Error())
		}
		// Split the output of kustomize build output into individual kube resource YAML files
		resources := strings.Split(out.String(), "---")
		for _, resource := range resources {
			resourceFile := []byte(strings.Trim(resource, "\t \n"))

			t := kubeResource{}
			err := yaml.Unmarshal(resourceFile, &t)

			if err != nil {
				klog.Error(err, "Failed to unmarshal YAML file")
				continue
			}

			if t.APIVersion == "" || t.Kind == "" {
				klog.Info("Not a Kubernetes resource")
			} else {
				err := ghsi.subscribeResourceFile(hostkey, syncsource, kvalid, resourceFile, pkgMap)
				if err != nil {
					klog.Error("Failed to apply a resource, error: ", err)
					return err
				}
			}
		}
	}

	return nil
}

func (ghsi *SubscriberItem) subscribeResources(hostkey types.NamespacedName,
	syncsource string,
	kvalid *kubesynchronizer.Validator,
	pkgMap map[string]bool,
	rscFiles []string) error {
	// sync kube resource deployables
	for _, rscFile := range rscFiles {
		file, err := ioutil.ReadFile(rscFile) // #nosec G304 rscFile is not user input

		if err != nil {
			klog.Error(err, "Failed to read YAML file "+rscFile)
			continue
		}

		resources := utils.ParseKubeResoures(file)

		if len(resources) > 0 {
			for _, resource := range resources {
				t := kubeResource{}
				err := yaml.Unmarshal(resource, &t)

				if err != nil {
					// Ignore if it does not have apiVersion or kind fields in the YAML
					continue
				}

				klog.V(4).Info("Applying Kubernetes resource of kind ", t.Kind)

				err = ghsi.subscribeResourceFile(hostkey, syncsource, kvalid, resource, pkgMap)
				if err != nil {
					klog.Error("Failed to apply a resource, error: ", err)
					return err
				}
			}
		}
	}

	return nil
}

func (ghsi *SubscriberItem) subscribeResourceFile(hostkey types.NamespacedName,
	syncsource string,
	kvalid *kubesynchronizer.Validator,
	file []byte,
	pkgMap map[string]bool) error {
	dpltosync, validgvk, err := ghsi.subscribeResource(file, pkgMap)
	if err != nil {
		klog.Info("Skipping resource")
		return err
	}

	pkgMap[dpltosync.GetName()] = true

	klog.V(4).Info("Ready to register template:", hostkey, dpltosync, syncsource)

	err = ghsi.synchronizer.RegisterTemplate(hostkey, dpltosync, syncsource)
	if err != nil {
		err = utils.SetInClusterPackageStatus(&(ghsi.Subscription.Status), dpltosync.GetName(), err, nil)

		if err != nil {
			klog.V(4).Info("error in setting in cluster package status :", err)
		}

		pkgMap[dpltosync.GetName()] = true

		return err
	}

	dplkey := types.NamespacedName{
		Name:      dpltosync.Name,
		Namespace: dpltosync.Namespace,
	}
	kvalid.AddValidResource(*validgvk, hostkey, dplkey)

	pkgMap[dplkey.Name] = true

	return nil
}

func (ghsi *SubscriberItem) subscribeResource(file []byte, pkgMap map[string]bool) (*dplv1.Deployable, *schema.GroupVersionKind, error) {
	rsc := &unstructured.Unstructured{}
	err := yaml.Unmarshal(file, &rsc)

	if err != nil {
		klog.Error(err, "Failed to unmarshal Kubernetes resource")
	}

	dpl := &dplv1.Deployable{}
	if ghsi.Channel == nil {
		dpl.Name = ghsi.Subscription.Name + "-" + rsc.GetKind() + "-" + rsc.GetName()
		dpl.Namespace = ghsi.Subscription.Namespace
	} else {
		dpl.Name = ghsi.Channel.Name + "-" + rsc.GetKind() + "-" + rsc.GetName()
		dpl.Namespace = ghsi.Channel.Namespace
	}

	orggvk := rsc.GetObjectKind().GroupVersionKind()
	validgvk := ghsi.synchronizer.GetValidatedGVK(orggvk)

	if validgvk == nil {
		gvkerr := errors.New("Resource " + orggvk.String() + " is not supported")
		err = utils.SetInClusterPackageStatus(&(ghsi.Subscription.Status), dpl.GetName(), gvkerr, nil)

		if err != nil {
			klog.Info("error in setting in cluster package status :", err)
		}

		pkgMap[dpl.GetName()] = true

		return nil, nil, gvkerr
	}

	if ghsi.synchronizer.KubeResources[*validgvk].Namespaced {
		rsc.SetNamespace(ghsi.Subscription.Namespace)
	}

	if ghsi.Subscription.Spec.PackageFilter != nil {
		errMsg := ghsi.checkFilters(rsc)
		if errMsg != "" {
			klog.V(3).Info(errMsg)

			return nil, nil, errors.New(errMsg)
		}
	}

	if ghsi.Subscription.Spec.PackageOverrides != nil {
		rsc, err = utils.OverrideResourceBySubscription(rsc, rsc.GetName(), ghsi.Subscription)
		if err != nil {
			pkgMap[dpl.GetName()] = true
			errmsg := "Failed override package " + dpl.Name + " with error: " + err.Error()
			err = utils.SetInClusterPackageStatus(&(ghsi.Subscription.Status), dpl.GetName(), err, nil)

			if err != nil {
				errmsg += " and failed to set in cluster package status with error: " + err.Error()
			}

			klog.V(2).Info(errmsg)

			return nil, nil, errors.New(errmsg)
		}
	}

	dpl.Spec.Template = &runtime.RawExtension{}
	dpl.Spec.Template.Raw, err = json.Marshal(rsc)

	if err != nil {
		klog.Error(err, "Failed to mashall the resource", rsc)
		return nil, nil, err
	}

	annotations := dpl.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[dplv1.AnnotationLocal] = "true"
	dpl.SetAnnotations(annotations)

	return dpl, validgvk, nil
}

func (ghsi *SubscriberItem) checkFilters(rsc *unstructured.Unstructured) (errMsg string) {
	if ghsi.Subscription.Spec.Package != "" && ghsi.Subscription.Spec.Package != rsc.GetName() {
		errMsg = "Name does not match, skiping:" + ghsi.Subscription.Spec.Package + "|" + rsc.GetName()

		return errMsg
	}

	if ghsi.Subscription.Spec.Package == rsc.GetName() {
		klog.V(4).Info("Name does matches: " + ghsi.Subscription.Spec.Package + "|" + rsc.GetName())
	}

	if ghsi.Subscription.Spec.PackageFilter != nil {
		if utils.LabelChecker(ghsi.Subscription.Spec.PackageFilter.LabelSelector, rsc.GetLabels()) {
			klog.V(4).Info("Passed label check on resource " + rsc.GetName())
		} else {
			errMsg = "Failed to pass label check on resource " + rsc.GetName()

			return errMsg
		}

		annotations := ghsi.Subscription.Spec.PackageFilter.Annotations
		if annotations != nil {
			klog.V(4).Info("checking annotations filter:", annotations)

			rscanno := rsc.GetAnnotations()
			if rscanno == nil {
				rscanno = make(map[string]string)
			}

			matched := true

			for k, v := range annotations {
				if rscanno[k] != v {
					klog.Info("Annotation filter does not match:", k, "|", v, "|", rscanno[k])

					matched = false

					break
				}
			}

			if !matched {
				errMsg = "Failed to pass annotation check to deployable " + rsc.GetName()

				return errMsg
			}
		}
	}

	return ""
}

func (ghsi *SubscriberItem) subscribeHelmCharts(indexFile *repo.IndexFile) (err error) {
	hostkey := types.NamespacedName{Name: ghsi.Subscription.Name, Namespace: ghsi.Subscription.Namespace}
	syncsource := githubhelmsyncsource + hostkey.String()
	pkgMap := make(map[string]bool)

	for packageName, chartVersions := range indexFile.Entries {
		klog.V(4).Infof("chart: %s\n%v", packageName, chartVersions)

		dpl, err := utils.CreateHelmCRDeployable(
			"", packageName, chartVersions, ghsi.synchronizer.LocalClient, ghsi.Channel, ghsi.Subscription)

		if err != nil {
			klog.Error("Failed to create a helmrelease CR deployable, err: ", err)
			return err
		}

		err = ghsi.synchronizer.RegisterTemplate(hostkey, dpl, syncsource)
		if err != nil {
			klog.Info("eror in registering :", err)
			err = utils.SetInClusterPackageStatus(&(ghsi.Subscription.Status), dpl.GetName(), err, nil)

			if err != nil {
				klog.Info("error in setting in cluster package status :", err)
			}

			pkgMap[dpl.GetName()] = true

			return err
		}

		dplkey := types.NamespacedName{
			Name:      dpl.Name,
			Namespace: dpl.Namespace,
		}

		pkgMap[dplkey.Name] = true
	}

	if utils.ValidatePackagesInSubscriptionStatus(ghsi.synchronizer.LocalClient, ghsi.Subscription, pkgMap) != nil {
		err = ghsi.synchronizer.LocalClient.Get(context.TODO(), hostkey, ghsi.Subscription)
		if err != nil {
			klog.Error("Failed to get subscription resource with error:", err)
		}

		err = utils.ValidatePackagesInSubscriptionStatus(ghsi.synchronizer.LocalClient, ghsi.Subscription, pkgMap)
	}

	return err
}

func (ghsi *SubscriberItem) cloneGitRepo() (commitID string, err error) {
	ghsi.repoRoot = utils.GetLocalGitFolder(ghsi.Channel, ghsi.Subscription)

	user, pwd, err := utils.GetChannelSecret(ghsi.synchronizer.LocalClient, ghsi.Channel)

	if err != nil {
		return "", err
	}

	return utils.CloneGitRepo(ghsi.Channel.Spec.Pathname, utils.GetSubscriptionBranch(ghsi.Subscription), user, pwd, ghsi.repoRoot)
}

func (ghsi *SubscriberItem) sortClonedGitRepo() error {
	if ghsi.Subscription.Spec.PackageFilter != nil && ghsi.Subscription.Spec.PackageFilter.FilterRef != nil {
		ghsi.SubscriberItem.SubscriptionConfigMap = &corev1.ConfigMap{}
		subcfgkey := types.NamespacedName{
			Name:      ghsi.Subscription.Spec.PackageFilter.FilterRef.Name,
			Namespace: ghsi.Subscription.Namespace,
		}

		err := ghsi.synchronizer.LocalClient.Get(context.TODO(), subcfgkey, ghsi.SubscriberItem.SubscriptionConfigMap)
		if err != nil {
			klog.Error("Failed to get filterRef configmap, error: ", err)
		}
	}

	resourcePath := ghsi.repoRoot

	annotations := ghsi.Subscription.GetAnnotations()

	if annotations[appv1.AnnotationGithubPath] != "" {
		resourcePath = filepath.Join(ghsi.repoRoot, annotations[appv1.AnnotationGithubPath])
	} else if ghsi.SubscriberItem.SubscriptionConfigMap != nil {
		resourcePath = filepath.Join(ghsi.repoRoot, ghsi.SubscriberItem.SubscriptionConfigMap.Data["path"])
	}

	// chartDirs contains helm chart directories
	// crdsAndNamespaceFiles contains CustomResourceDefinition and Namespace Kubernetes resources file paths
	// rbacFiles contains ServiceAccount, ClusterRole and Role Kubernetes resource file paths
	// otherFiles contains all other Kubernetes resource file paths
	chartDirs, kustomizeDirs, crdsAndNamespaceFiles, rbacFiles, otherFiles, err := utils.SortResources(ghsi.repoRoot, resourcePath)
	if err != nil {
		klog.Error(err, "Failed to sort kubernetes resources and helm charts.")
		return err
	}

	ghsi.chartDirs = chartDirs
	ghsi.kustomizeDirs = kustomizeDirs
	ghsi.crdsAndNamespaceFiles = crdsAndNamespaceFiles
	ghsi.rbacFiles = rbacFiles
	ghsi.otherFiles = otherFiles

	// Build a helm repo index file
	indexFile, err := utils.GenerateHelmIndexFile(ghsi.Subscription, ghsi.repoRoot, chartDirs)

	if err != nil {
		// If package name is not specified in the subscription, filterCharts throws an error. In this case, just return the original index file.
		klog.Error(err, "Failed to generate helm index file.")
		return err
	}

	ghsi.indexFile = indexFile

	b, _ := yaml.Marshal(ghsi.indexFile)
	klog.V(4).Info("New index file ", string(b))

	return nil
}
