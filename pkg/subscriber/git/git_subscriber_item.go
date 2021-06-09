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

package git

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ghodss/yaml"
	"helm.sh/helm/v3/pkg/repo"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"

	chnv1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
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

var (
	helmGvk = schema.GroupVersionKind{
		Group:   appv1.SchemeGroupVersion.Group,
		Version: appv1.SchemeGroupVersion.Version,
		Kind:    "HelmRelease",
	}

	subscriptionGVK = schema.GroupVersionKind{
		Group:   appv1.SchemeGroupVersion.Group,
		Kind:    "Subscription",
		Version: appv1.SchemeGroupVersion.Version}
)

// GitSubscriber - defines the subscribe of git subscription
type GitSubscriberItem struct {
	client client.Client
	scheme *runtime.Scheme
	appv1.SubscriberItem
	crdsAndNamespaceFiles []string
	rbacFiles             []string
	otherFiles            []string
	repoRoot              string
	commitID              string
	reconcileRate         string
	desiredCommit         string
	desiredTag            string
	syncTime              string
	stopch                chan struct{}
	syncinterval          int
	count                 int
	synchronizer          SyncSource
	chartDirs             map[string]string
	kustomizeDirs         map[string]string
	resources             []kubesynchronizer.DplUnit
	indexFile             *repo.IndexFile
	webhookEnabled        bool
	successful            bool
	clusterAdmin          bool
	subResourcesReg       map[types.NamespacedName]*utils.SubResources
	allresources          []*unstructured.Unstructured
}

type kubeResource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// Start subscribes a subscriber item with github channel
func (ghsi *GitSubscriberItem) Start(restart bool) {
	// do nothing if already started
	if ghsi.stopch != nil {
		if restart {
			// restart this goroutine
			klog.Info("Stopping SubscriberItem: ", ghsi.Subscription.Name)
			ghsi.Stop()
		} else {
			klog.Info("SubscriberItem already started: ", ghsi.Subscription.Name)
			return
		}
	}

	ghsi.count = 0 // reset the counter

	ghsi.stopch = make(chan struct{})

	loopPeriod, retryInterval, retries := utils.GetReconcileInterval(ghsi.reconcileRate, chnv1.ChannelTypeGit)

	if strings.EqualFold(ghsi.reconcileRate, "off") {
		klog.Infof("auto-reconcile is OFF")

		ghsi.doSubscriptionWithRetries(retryInterval, retries)

		return
	}

	subKey := types.NamespacedName{
		Name:      ghsi.SubscriberItem.Subscription.GetName(),
		Namespace: ghsi.SubscriberItem.Subscription.GetNamespace(),
	}

	ghsi.subResourcesReg[subKey] = utils.NewSubResources(subKey)

	go wait.Until(func() {
		tw := ghsi.SubscriberItem.Subscription.Spec.TimeWindow
		if tw != nil {
			nextRun := utils.NextStartPoint(tw, time.Now())
			if nextRun > time.Duration(0) {
				klog.Infof("Subscription is currently blocked by the time window. It %s will be deployed after %v",
					subKey, nextRun)
				return
			}
		}

		// if the subscription pause lable is true, stop subscription here.
		if utils.GetPauseLabel(ghsi.SubscriberItem.Subscription) {
			klog.Infof("Git Subscription  %s is paused.", subKey)
			return
		}

		ghsi.doSubscriptionWithRetries(retryInterval, retries)
	}, loopPeriod, ghsi.stopch)
}

// Stop unsubscribes a subscriber item with namespace channel
func (ghsi *GitSubscriberItem) Stop() {
	klog.Info("Stopping SubscriberItem ", ghsi.Subscription.Name)
	close(ghsi.stopch)
}

func (ghsi *GitSubscriberItem) doSubscriptionWithRetries(retryInterval time.Duration, retries int) {
	err := ghsi.doSubscription()

	if err != nil {
		klog.Error(err, "Subscription error.")
	}

	// If the initial subscription fails, retry.
	n := 0

	for n < retries {
		if !ghsi.successful {
			time.Sleep(retryInterval)
			klog.Infof("Re-try #%d: subcribing to the Git repo", n+1)

			err = ghsi.doSubscription()
			if err != nil {
				klog.Error(err, "Subscription error.")
			}

			n++
		} else {
			break
		}
	}
}

func (ghsi *GitSubscriberItem) doSubscription() error {
	hostkey := types.NamespacedName{Name: ghsi.Subscription.Name, Namespace: ghsi.Subscription.Namespace}

	subResourceCm := ghsi.subResourcesReg[hostkey]
	if err := subResourceCm.GetSubResources(ghsi.client); err != nil {
		return fmt.Errorf("failed to get subscription configmap data, err: %w", err)
	}

	klog.Info("enter doSubscription: ", hostkey.String())

	defer klog.Info("exit doSubscription: ", hostkey.String())

	// If webhook is enabled, don't do anything until next reconcilitation.
	if ghsi.webhookEnabled {
		klog.Infof("Git Webhook is enabled on subscription %s.", ghsi.Subscription.Name)

		if ghsi.successful {
			klog.Infof("All resources are reconciled successfully. Waiting for the next Git Webhook event.")
			return nil
		}

		klog.Infof("Resources are not reconciled successfully yet. Continue reconciling.")
	}

	klog.V(2).Info("Subscribing ...", ghsi.Subscription.Name)

	//Clone the git repo
	commitID, err := ghsi.cloneGitRepo()
	if err != nil {
		klog.Error(err, "Unable to clone the git repo ", ghsi.Channel.Spec.Pathname)
		ghsi.successful = false

		return err
	}

	klog.Info("Git commit: ", commitID)

	if strings.EqualFold(ghsi.reconcileRate, "medium") {
		// every 3 minutes, compare commit ID. If changed, reconcile resources.
		// every 15 minutes, reconcile resources without commit ID comparison.
		ghsi.count++

		if ghsi.commitID == "" {
			klog.Infof("No previous commit. DEPLOY")
		} else {
			if ghsi.count < 6 {
				if commitID == ghsi.commitID && ghsi.successful {
					klog.Infof("Appsub %s Git commit: %s hasn't changed. Skip reconcile.", hostkey.String(), commitID)
					return nil
				}
			} else {
				klog.Infof("Reconciling all resources")
				ghsi.count = 0
			}
		}
	}

	ghsi.resources = []kubesynchronizer.DplUnit{}

	ghsi.allresources = []*unstructured.Unstructured{}

	err = ghsi.sortClonedGitRepo()
	if err != nil {
		klog.Error(err, " Unable to sort helm charts and kubernetes resources from the cloned git repo.")

		ghsi.successful = false

		return err
	}

	// syncsource := githubk8ssyncsource + hostkey.String()

	klog.V(4).Info("Applying resources: ", ghsi.crdsAndNamespaceFiles)

	err = ghsi.subscribeResources(ghsi.crdsAndNamespaceFiles)

	if err != nil {
		klog.Error(err, " Unable to subscribe crd and ns resources")

		ghsi.successful = false
	}

	klog.V(4).Info("Applying resources: ", ghsi.rbacFiles)

	err = ghsi.subscribeResources(ghsi.rbacFiles)

	if err != nil {
		klog.Error(err, " Unable to subscribe rbac resources")

		ghsi.successful = false
	}

	klog.V(4).Info("Applying resources: ", ghsi.otherFiles)

	err = ghsi.subscribeResources(ghsi.otherFiles)

	if err != nil {
		klog.Error(err, " Unable to subscribe other resources")

		ghsi.successful = false
	}

	klog.V(4).Info("Applying kustomizations: ", ghsi.kustomizeDirs)

	err = ghsi.subscribeKustomizations()

	if err != nil {
		klog.Error(err, " Unable to subscribe kustomize resources")

		ghsi.successful = false
	}

	klog.V(4).Info("Applying helm charts..")

	err = ghsi.subscribeHelmChartsUnstructured(ghsi.indexFile)

	if err != nil {
		klog.Error(err, "Unable to subscribe helm charts")

		ghsi.successful = false

		return err
	}

	reqSet := utils.CalResourceSet(ghsi.allresources)

	deleteRes := subResourceCm.GetToBeDeletedResources(reqSet)
	if err := operateOnUnstructured(ghsi.client, DELETE_OP, deleteRes); err != nil {
		return err
	}

	createRes := subResourceCm.GetToBeCreatedResources(reqSet)
	if err := operateOnUnstructured(ghsi.client, CREATE_OP, createRes); err != nil {
		return err
	}

	updateRes := subResourceCm.GetToBeUpdatedResources(reqSet)
	if err := operateOnUnstructured(ghsi.client, UPDATE_OP, updateRes); err != nil {
		return err
	}

	if err := subResourceCm.CommitResources(ghsi.client, reqSet); err != nil {
		return err
	}

	//	if err := ghsi.synchronizer.AddTemplates(syncsource, hostkey, ghsi.resources); err != nil {
	//		klog.Error(err)
	//
	//		ghsi.successful = false
	//
	//		return err
	//	}

	ghsi.commitID = commitID

	ghsi.resources = nil
	ghsi.chartDirs = nil
	ghsi.kustomizeDirs = nil
	ghsi.crdsAndNamespaceFiles = nil
	ghsi.rbacFiles = nil
	ghsi.otherFiles = nil
	ghsi.indexFile = nil
	ghsi.successful = true

	return nil
}

const (
	CREATE_OP = "create"
	UPDATE_OP = "update"
	DELETE_OP = "delete"
)

func operateOnUnstructured(clt client.Client, op string, in []*unstructured.Unstructured) error {
	ctx := context.TODO()
	switch op {
	case CREATE_OP:
		for _, item := range in {
			if err := clt.Create(ctx, item); err != nil {
				if !k8serr.IsAlreadyExists(err) {
					return err
				}
			}
		}
	case DELETE_OP:
		for _, item := range in {
			if err := clt.Delete(ctx, item); err != nil {
				return err
			}
		}

	case UPDATE_OP:
		for _, item := range in {
			if err := clt.Update(ctx, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ghsi *GitSubscriberItem) subscribeKustomizations() error {
	for _, kustomizeDir := range ghsi.kustomizeDirs {
		klog.Info("Applying kustomization ", kustomizeDir)

		relativePath := kustomizeDir

		if len(strings.SplitAfter(kustomizeDir, ghsi.repoRoot+"/")) > 1 {
			relativePath = strings.SplitAfter(kustomizeDir, ghsi.repoRoot+"/")[1]
		}

		utils.VerifyAndOverrideKustomize(ghsi.Subscription.Spec.PackageOverrides, relativePath, kustomizeDir)

		out, err := utils.RunKustomizeBuild(kustomizeDir)

		if err != nil {
			klog.Error("Failed to apply kustomization, error: ", err.Error())
			return err
		}

		// Split the output of kustomize build output into individual kube resource YAML files
		resources := utils.ParseYAML(out)
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
				err := checkSubscriptionAnnotation(t)
				if err != nil {
					klog.Errorf("Failed to apply %s/%s resource. err: %s", t.APIVersion, t.Kind, err)
				}

				ghsi.subscribeResourceFile(resourceFile)
			}
		}
	}

	return nil
}

func checkSubscriptionAnnotation(resource kubeResource) error {
	if strings.EqualFold(resource.APIVersion, appv1.SchemeGroupVersion.String()) && strings.EqualFold(resource.Kind, "Subscription") {
		annotations := resource.GetAnnotations()
		if strings.EqualFold(annotations[appv1.AnnotationClusterAdmin], "true") {
			klog.Errorf("%s %s contains annotation %s set to true.", resource.APIVersion, resource.Name, appv1.AnnotationClusterAdmin)
			return errors.New("contains " + appv1.AnnotationClusterAdmin + " = true annotation.")
		}
	}

	return nil
}

func (ghsi *GitSubscriberItem) subscribeResources(rscFiles []string) error {
	// sync kube resource deployables
	for _, rscFile := range rscFiles {
		file, err := ioutil.ReadFile(rscFile) // #nosec G304 rscFile is not user input

		if err != nil {
			klog.Error(err, "Failed to read YAML file "+rscFile)
			return err
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

				ghsi.subscribeResourceFile(resource)
			}
		}
	}

	return nil
}

func (ghsi *GitSubscriberItem) subscribeResourceFile(file []byte) {
	dpltosync, validgvk, err := ghsi.subscribeResource(file)
	if err != nil {
		klog.Error(err)
	}

	if dpltosync == nil || validgvk == nil {
		klog.Info("Skipping resource")
		return
	}

	ghsi.resources = append(ghsi.resources, kubesynchronizer.DplUnit{Dpl: dpltosync, Gvk: *validgvk})
}

func (ghsi *GitSubscriberItem) subscribeResource(file []byte) (*dplv1.Deployable, *schema.GroupVersionKind, error) {
	rsc := &unstructured.Unstructured{}
	err := yaml.Unmarshal(file, &rsc)

	if err != nil {
		klog.Error(err, "Failed to unmarshal Kubernetes resource")
	}

	dpl := &dplv1.Deployable{}

	if ghsi.Channel == nil {
		dpl.Name = ghsi.Subscription.Name + "-" + rsc.GetKind() + "-" + rsc.GetName()
		dpl.Namespace = ghsi.Subscription.Namespace

		if ghsi.clusterAdmin && (rsc.GetNamespace() != "") {
			// With the cluster admin, the same resource with the same name can be applied to multiple namespaces.
			// This avoids name collisions.
			dpl.Namespace = rsc.GetNamespace()
		}
	} else {
		dpl.Name = ghsi.Channel.Name + "-" + rsc.GetKind() + "-" + rsc.GetName()
		dpl.Namespace = ghsi.Channel.Namespace

		if ghsi.clusterAdmin && (rsc.GetNamespace() != "") {
			// With the cluster admin, the same resource with the same name can be applied to multiple namespaces.
			// This avoids name collisions.
			dpl.Namespace = rsc.GetNamespace()
		}
	}

	orggvk := rsc.GetObjectKind().GroupVersionKind()
	validgvk := ghsi.synchronizer.GetValidatedGVK(orggvk)

	if validgvk == nil {
		gvkerr := errors.New("Resource " + orggvk.String() + " is not supported")
		err = utils.SetInClusterPackageStatus(&(ghsi.Subscription.Status), dpl.GetName(), gvkerr, nil)

		if err != nil {
			klog.Info("error in setting in cluster package status :", err)
		}

		return nil, nil, gvkerr
	}

	if ghsi.synchronizer.IsResourceNamespaced(*validgvk) {
		if ghsi.clusterAdmin {
			klog.Info("cluster-admin is true.")

			if rsc.GetNamespace() != "" {
				klog.Info("Using resource's original namespace. Resource namespace is " + rsc.GetNamespace())
			} else {
				klog.Info("Setting it to subscription namespace " + ghsi.Subscription.Namespace)
				rsc.SetNamespace(ghsi.Subscription.Namespace)
			}

			rscAnnotations := rsc.GetAnnotations()

			if rscAnnotations == nil {
				rscAnnotations = make(map[string]string)
			}

			if strings.EqualFold(rsc.GroupVersionKind().Group, "apps.open-cluster-management.io") &&
				strings.EqualFold(rsc.GroupVersionKind().Kind, "Subscription") {
				// Adding cluster-admin=true annotation to child subscription
				rscAnnotations[appv1.AnnotationClusterAdmin] = "true"
				rsc.SetAnnotations(rscAnnotations)
			}
		} else {
			klog.Info("No cluster-admin. Setting it to subscription namespace " + ghsi.Subscription.Namespace)
			rsc.SetNamespace(ghsi.Subscription.Namespace)
		}
	}

	if ghsi.Subscription.Spec.PackageFilter != nil {
		errMsg := ghsi.checkFilters(rsc)
		if errMsg != "" {
			klog.V(3).Info(errMsg)

			return nil, nil, nil
		}
	}

	if ghsi.Subscription.Spec.PackageOverrides != nil {
		rsc, err = utils.OverrideResourceBySubscription(rsc, rsc.GetName(), ghsi.Subscription)
		if err != nil {
			errmsg := "Failed override package " + dpl.Name + " with error: " + err.Error()
			err = utils.SetInClusterPackageStatus(&(ghsi.Subscription.Status), dpl.GetName(), err, nil)

			if err != nil {
				errmsg += " and failed to set in cluster package status with error: " + err.Error()
			}

			klog.V(2).Info(errmsg)

			return nil, nil, errors.New(errmsg)
		}
	}

	subAnnotations := ghsi.Subscription.GetAnnotations()
	if subAnnotations != nil {
		rscAnnotations := rsc.GetAnnotations()
		if rscAnnotations == nil {
			rscAnnotations = make(map[string]string)
		}

		if strings.EqualFold(subAnnotations[appv1.AnnotationClusterAdmin], "true") {
			rscAnnotations[appv1.AnnotationClusterAdmin] = "true"
		}

		// If the reconcile-option is set in the resource, honor that. Otherwise, take the subscription's reconcile-option
		if rscAnnotations[appv1.AnnotationResourceReconcileOption] == "" {
			if subAnnotations[appv1.AnnotationResourceReconcileOption] != "" {
				rscAnnotations[appv1.AnnotationResourceReconcileOption] = subAnnotations[appv1.AnnotationResourceReconcileOption]
			} else {
				// By default, merge reconcile
				rscAnnotations[appv1.AnnotationResourceReconcileOption] = appv1.MergeReconcile
			}
		}

		rsc.SetAnnotations(rscAnnotations)
	}

	// Set app label
	utils.SetPartOfLabel(ghsi.SubscriberItem.Subscription, rsc)

	rsc.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: subscriptionGVK.Version,
		Kind:       subscriptionGVK.Kind,
		Name:       ghsi.Subscription.Name,
		UID:        ghsi.Subscription.UID,
	}})

	ghsi.allresources = append(ghsi.allresources, rsc)

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

func (ghsi *GitSubscriberItem) checkFilters(rsc *unstructured.Unstructured) (errMsg string) {
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

func (ghsi *GitSubscriberItem) subscribeHelmCharts(indexFile *repo.IndexFile) (err error) {
	for packageName, chartVersions := range indexFile.Entries {
		klog.V(4).Infof("chart: %s\n%v", packageName, chartVersions)

		dpl, err := utils.CreateHelmCRDeployable(
			"", packageName, chartVersions, ghsi.synchronizer.GetLocalClient(), ghsi.Channel, ghsi.Subscription)

		if err != nil {
			klog.Error("Failed to create a helmrelease CR deployable, err: ", err)
			return err
		}

		ghsi.resources = append(ghsi.resources, kubesynchronizer.DplUnit{Dpl: dpl, Gvk: helmGvk})
	}

	return err
}

func (ghsi *GitSubscriberItem) subscribeHelmChartsUnstructured(indexFile *repo.IndexFile) (err error) {
	for packageName, chartVersions := range indexFile.Entries {
		klog.V(4).Infof("chart: %s\n%v", packageName, chartVersions)

		uns, err := utils.CreateHelmCRUnstructured(
			"", packageName, chartVersions, ghsi.synchronizer.GetLocalClient(), ghsi.Channel, ghsi.Subscription, ghsi.scheme)

		if err != nil {
			klog.Error("Failed to create a helmrelease CR deployable, err: ", err)
			return err
		}

		ghsi.allresources = append(ghsi.allresources, uns)
	}

	return err
}

func (ghsi *GitSubscriberItem) cloneGitRepo() (commitID string, err error) {
	ghsi.repoRoot = utils.GetLocalGitFolder(ghsi.Channel, ghsi.Subscription)

	user := ""
	token := ""
	sshKey := []byte("")
	passphrase := []byte("")

	if ghsi.SubscriberItem.ChannelSecret != nil {
		user, token, sshKey, passphrase, err = utils.ParseChannelSecret(ghsi.SubscriberItem.ChannelSecret)

		if err != nil {
			return "", err
		}
	}

	caCert := ""

	if ghsi.SubscriberItem.ChannelConfigMap != nil {
		caCert = ghsi.SubscriberItem.ChannelConfigMap.Data[appv1.ChannelCertificateData]
	}

	annotations := ghsi.Subscription.GetAnnotations()

	cloneDepth := 1

	if annotations[appv1.AnnotationGitCloneDepth] != "" {
		cloneDepth, err = strconv.Atoi(annotations[appv1.AnnotationGitCloneDepth])

		if err != nil {
			cloneDepth = 1

			klog.Error(err, " failed to convert git-clone-depth annotation to integer")
		}
	}

	cloneOptions := &utils.GitCloneOption{
		RepoURL:            ghsi.Channel.Spec.Pathname,
		CommitHash:         ghsi.desiredCommit,
		RevisionTag:        ghsi.desiredTag,
		CloneDepth:         cloneDepth,
		Branch:             utils.GetSubscriptionBranch(ghsi.Subscription),
		User:               user,
		Password:           token,
		SSHKey:             sshKey,
		Passphrase:         passphrase,
		DestDir:            ghsi.repoRoot,
		InsecureSkipVerify: ghsi.Channel.Spec.InsecureSkipVerify,
		CaCerts:            caCert,
	}

	return utils.CloneGitRepo(cloneOptions)
}

func (ghsi *GitSubscriberItem) sortClonedGitRepo() error {
	if ghsi.Subscription.Spec.PackageFilter != nil && ghsi.Subscription.Spec.PackageFilter.FilterRef != nil {
		ghsi.SubscriberItem.SubscriptionConfigMap = &corev1.ConfigMap{}
		subcfgkey := types.NamespacedName{
			Name:      ghsi.Subscription.Spec.PackageFilter.FilterRef.Name,
			Namespace: ghsi.Subscription.Namespace,
		}

		err := ghsi.synchronizer.GetLocalClient().Get(context.TODO(), subcfgkey, ghsi.SubscriberItem.SubscriptionConfigMap)
		if err != nil {
			klog.Error("Failed to get filterRef configmap, error: ", err)
		}
	}

	resourcePath := ghsi.repoRoot

	annotations := ghsi.Subscription.GetAnnotations()

	if annotations[appv1.AnnotationGithubPath] != "" {
		resourcePath = filepath.Join(ghsi.repoRoot, annotations[appv1.AnnotationGithubPath])
	} else if annotations[appv1.AnnotationGitPath] != "" {
		resourcePath = filepath.Join(ghsi.repoRoot, annotations[appv1.AnnotationGitPath])
	} else if ghsi.SubscriberItem.SubscriptionConfigMap != nil {
		resourcePath = filepath.Join(ghsi.repoRoot, ghsi.SubscriberItem.SubscriptionConfigMap.Data["path"])
	}

	// chartDirs contains helm chart directories
	// crdsAndNamespaceFiles contains CustomResourceDefinition and Namespace Kubernetes resources file paths
	// rbacFiles contains ServiceAccount, ClusterRole and Role Kubernetes resource file paths
	// otherFiles contains all other Kubernetes resource file paths
	chartDirs, kustomizeDirs, crdsAndNamespaceFiles, rbacFiles, otherFiles, err := utils.SortResources(ghsi.repoRoot, resourcePath, utils.SkipHooksOnManaged)
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
