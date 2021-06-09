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
	"time"

	"github.com/ghodss/yaml"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	chnv1alpha1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	appv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	appv1alpha1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
)

const rsc1 = `apiVersion: v1
data:
  keyOne: "true"
kind: ConfigMap
metadata:
  name: TestConfigMap1
  namespace: default
  labels:
    environment: dev
    city: Toronto`

const rsc2 = `apiVersion: v1
data:
  keyOne: "true"
kind: ConfigMap
metadata:
  name: TestConfigMap2
  namespace: default
  annotations:
    need: this
  labels:
    environment: dev
    city: Toronto`

const invalidRsc = `apiVersion: v1
data:
  keyOne: "true"
kind: IsThisConfigMap
metadata:
  name: TestConfigMap2
  namespace: default
  annotations:
    need: this
  labels:
    environment: dev
    city: Toronto`

const correctSecret = `apiVersion: v1
kind: Secret
metadata:
  name: correct-secret
  namespace: default
type: Opaque
data:
  user: YWRtaW4=
  accessToken: MWYyZDFlMmU2N2Rm`

const incorrectSecret = `apiVersion: v1
kind: Secret
metadata:
  name: incorrect-secret
  namespace: default
type: Opaque
data:
  user: YWRtaW4=
  password: MWYyZDFlMmU2N2Rm`

const incorrectSecret2 = `apiVersion: v1
kind: Secret
metadata:
  name: incorrect-secret
  namespace: default
type: Opaque
data:
  id: YWRtaW4=
  accessToken: MWYyZDFlMmU2N2Rm`

var (
	sharedkey = types.NamespacedName{
		Name:      "githubtest",
		Namespace: "default",
	}
	githubchn = &chnv1alpha1.Channel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sharedkey.Name,
			Namespace: sharedkey.Namespace,
		},
		Spec: chnv1alpha1.ChannelSpec{
			Type:     "Git",
			Pathname: "https://github.com/open-cluster-management/multicloud-operators-subscription.git",
		},
	}
	githubsub = &appv1alpha1.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sharedkey.Name,
			Namespace: sharedkey.Namespace,
		},
		Spec: appv1alpha1.SubscriptionSpec{
			Channel: sharedkey.String(),
		},
	}
	subitem = &appv1alpha1.SubscriberItem{
		Subscription: githubsub,
		Channel:      githubchn,
	}
	bitbucketsharedkey = types.NamespacedName{
		Name:      "bitbuckettest",
		Namespace: "default",
	}
	bitbucketchn = &chnv1alpha1.Channel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bitbucketsharedkey.Name,
			Namespace: bitbucketsharedkey.Namespace,
		},
		Spec: chnv1alpha1.ChannelSpec{
			Type:     "Git",
			Pathname: "https://bitbucket.org/ekdjbdfh/testrepo.git",
		},
	}
	bitbucketsub = &appv1alpha1.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bitbucketsharedkey.Name,
			Namespace: bitbucketsharedkey.Namespace,
		},
		Spec: appv1alpha1.SubscriptionSpec{
			Channel: bitbucketsharedkey.String(),
		},
	}
)

var _ = Describe("github subscriber reconcile logic", func() {
	It("should reconcile on github subscription creation", func() {
		// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
		// channel when it is finished.
		err := k8sClient.Create(context.TODO(), githubchn)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(context.TODO(), githubsub)
		Expect(err).NotTo(HaveOccurred())

		Expect(defaultSubscriber.SubscribeItem(subitem)).NotTo(HaveOccurred())

		time.Sleep(k8swait)

		Expect(defaultSubscriber.UnsubscribeItem(sharedkey)).NotTo(HaveOccurred())

	})

	It("should pass resource label selector", func() {
		subitem := &GitSubscriberItem{}
		subitem.Subscription = githubsub
		subitem.Channel = githubchn

		rsc := &unstructured.Unstructured{}
		err := yaml.Unmarshal([]byte(rsc1), &rsc)
		Expect(err).Should(Succeed())

		// Test kube resource with no package filter
		errMsg := subitem.checkFilters(rsc)
		Expect(errMsg).To(gomega.Equal(""))

		matchLabels := make(map[string]string)
		matchLabels["environment"] = "dev"
		lblSelector := &metav1.LabelSelector{}
		lblSelector.MatchLabels = matchLabels
		pkgFilter := &appv1alpha1.PackageFilter{}
		pkgFilter.LabelSelector = lblSelector
		githubsub.Spec.PackageFilter = pkgFilter

		// Test kube resource with package filter having a matching label
		errMsg = subitem.checkFilters(rsc)
		Expect(errMsg).To(Equal(""))

		matchLabels = make(map[string]string)
		matchLabels["city"] = "Toronto"
		matchLabels["environment"] = "dev"
		lblSelector.MatchLabels = matchLabels

		// Test kube resource with package filter having multiple matching labels
		errMsg = subitem.checkFilters(rsc)
		Expect(errMsg).To(Equal(""))

		matchLabels = make(map[string]string)
		matchLabels["city"] = "paris"
		matchLabels["environment"] = "dev"
		lblSelector.MatchLabels = matchLabels

		// Test kube resource with package filter having some matching labels
		errMsg = subitem.checkFilters(rsc)
		Expect(errMsg).To(Equal("Failed to pass label check on resource " + rsc.GetName()))

		err = yaml.Unmarshal([]byte(rsc2), &rsc)
		//assert.NoError(t, err)
		Expect(err).Should(Succeed())

		matchLabels = make(map[string]string)
		matchLabels["environment"] = "dev"
		lblSelector.MatchLabels = matchLabels

		// Test kube resource with package filter having no annotation
		errMsg = subitem.checkFilters(rsc)
		Expect(errMsg).To(Equal(""))

		annotations := make(map[string]string)
		annotations["need"] = "this"
		githubsub.Spec.PackageFilter.Annotations = annotations

		// Test kube resource with package filter having some matching labels
		errMsg = subitem.checkFilters(rsc)
		Expect(errMsg).To(Equal(""))

		annotations["need"] = "not"

		// Test kube resource with package filter having some matching labels
		errMsg = subitem.checkFilters(rsc)
		Expect(errMsg).To(Equal("Failed to pass annotation check to deployable " + rsc.GetName()))

	})
})

var _ = Describe("test subscribing to bitbucket repository", func() {
	It("should be able to clone the bitbucket repo and sort resources", func() {
		subitem := &GitSubscriberItem{}
		subitem.Subscription = bitbucketsub
		subitem.Channel = bitbucketchn
		subitem.synchronizer = defaultSubscriber.synchronizer
		commitid, err := subitem.cloneGitRepo()
		Expect(commitid).ToNot(Equal(""))
		Expect(err).NotTo(HaveOccurred())

		err = subitem.sortClonedGitRepo()
		Expect(err).NotTo(HaveOccurred())

		Expect(len(subitem.indexFile.Entries)).To(Equal(3))

		Expect(len(subitem.crdsAndNamespaceFiles)).To(Equal(2))
		Expect(len(subitem.rbacFiles)).To(Equal(3))
		Expect(len(subitem.otherFiles)).To(Equal(2))
		Expect(subitem.crdsAndNamespaceFiles[0]).To(ContainSubstring("resources/deploy/crds/crontab.yaml"))
	})
})

var _ = Describe("test subscribing to bitbucket repository", func() {
	It("should be able to clone the bitbucket repo with skip certificate verification and sort resources", func() {
		subitem := &GitSubscriberItem{}
		subitem.Subscription = bitbucketsub
		bitbucketchn.Spec.InsecureSkipVerify = true
		subitem.Channel = bitbucketchn
		subitem.synchronizer = defaultSubscriber.synchronizer
		commitid, err := subitem.cloneGitRepo()
		Expect(commitid).ToNot(Equal(""))
		Expect(err).NotTo(HaveOccurred())

		err = subitem.sortClonedGitRepo()
		Expect(err).NotTo(HaveOccurred())

		Expect(len(subitem.indexFile.Entries)).To(Equal(3))

		Expect(len(subitem.crdsAndNamespaceFiles)).To(Equal(2))
		Expect(len(subitem.rbacFiles)).To(Equal(3))
		Expect(len(subitem.otherFiles)).To(Equal(2))
		Expect(subitem.crdsAndNamespaceFiles[0]).To(ContainSubstring("resources/deploy/crds/crontab.yaml"))
	})
})

var _ = Describe("test subscribe invalid resource", func() {
	It("should not return error or panic", func() {
		subitem := &GitSubscriberItem{}
		subitem.Subscription = githubsub
		subitem.Channel = githubchn
		subitem.synchronizer = defaultSubscriber.synchronizer

		// Test subscribing an invalid kubernetes resource
		_, _, err := subitem.subscribeResource([]byte(invalidRsc))
		Expect(err).To(HaveOccurred())

	})

	It("should clone the target repo", func() {
		subitem := &GitSubscriberItem{}
		subitem.Subscription = githubsub
		subitem.Channel = githubchn
		subitem.synchronizer = defaultSubscriber.synchronizer
		commitid, err := subitem.cloneGitRepo()
		Expect(commitid).ToNot(Equal(""))
		Expect(err).NotTo(HaveOccurred())

		// Test with a fake authentication secret but correct data keys in the secret
		chnSecret := &corev1.Secret{}
		err = yaml.Unmarshal([]byte(correctSecret), &chnSecret)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(context.TODO(), chnSecret)
		Expect(err).NotTo(HaveOccurred())

		secretRef := &corev1.ObjectReference{}
		secretRef.Name = "correct-secret"

		chnIncorrectSecret := &corev1.Secret{}
		err = yaml.Unmarshal([]byte(incorrectSecret), &chnIncorrectSecret)
		Expect(err).NotTo(HaveOccurred())

		subitem.SubscriberItem.ChannelSecret = chnIncorrectSecret
		_, err = subitem.cloneGitRepo()
		Expect(err.Error()).To(Equal("ssh_key (and optionally passphrase) or user and accressToken need to be specified in the channel secret"))

		chnIncorrectSecret2 := &corev1.Secret{}
		err = yaml.Unmarshal([]byte(incorrectSecret2), &chnIncorrectSecret2)
		Expect(err).NotTo(HaveOccurred())
		subitem.SubscriberItem.ChannelSecret = chnIncorrectSecret2

		_, err = subitem.cloneGitRepo()
		Expect(err.Error()).To(Equal("ssh_key (and optionally passphrase) or user and accressToken need to be specified in the channel secret"))

		err = k8sClient.Delete(context.TODO(), chnSecret)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should success on subscription with repo path", func() {
		pathConfigMapYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: path-config-map
  namespace: default
data:
  path: test/github/resources/deploy/crds`

		pathConfigMap := &corev1.ConfigMap{}
		err := yaml.Unmarshal([]byte(pathConfigMapYAML), &pathConfigMap)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(context.TODO(), pathConfigMap)
		Expect(err).NotTo(HaveOccurred())

		filterRef := &corev1.LocalObjectReference{}
		filterRef.Name = "path-config-map"

		packageFilter := &appv1alpha1.PackageFilter{}
		packageFilter.FilterRef = filterRef

		githubsub.Spec.PackageFilter = packageFilter

		subitem := &GitSubscriberItem{}
		subitem.Subscription = githubsub
		subitem.Channel = githubchn
		subitem.synchronizer = defaultSubscriber.synchronizer

		// Set the cloned Git repo root directory to this Git repository root.
		subitem.repoRoot = "../../.."

		err = subitem.sortClonedGitRepo()
		Expect(err).NotTo(HaveOccurred())

		Expect(len(subitem.indexFile.Entries)).To(BeZero())

		Expect(len(subitem.crdsAndNamespaceFiles)).To(Equal(1))
		Expect(subitem.crdsAndNamespaceFiles[0]).To(ContainSubstring("test/github/resources/deploy/crds/crontab.yaml"))

		Expect(len(subitem.otherFiles)).To(BeZero())

		githubsub.Spec.PackageFilter = nil

		err = k8sClient.Delete(context.TODO(), pathConfigMap)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should remove no matching name", func() {
		pathConfigMapYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: path-config-map
  namespace: default
data:
  path: test/github/helmcharts`

		pathConfigMap := &corev1.ConfigMap{}
		err := yaml.Unmarshal([]byte(pathConfigMapYAML), &pathConfigMap)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(context.TODO(), pathConfigMap)
		Expect(err).NotTo(HaveOccurred())

		filterRef := &corev1.LocalObjectReference{}
		filterRef.Name = "path-config-map"

		packageFilter := &appv1alpha1.PackageFilter{}
		packageFilter.FilterRef = filterRef

		githubsub.Spec.PackageFilter = packageFilter

		subanno := make(map[string]string)
		subanno[appv1alpha1.AnnotationGitPath] = "test/github/helmcharts"
		githubsub.SetAnnotations(subanno)

		githubsub.Spec.Package = "chart1"

		subitem := &GitSubscriberItem{}
		subitem.Subscription = githubsub
		subitem.Channel = githubchn
		subitem.synchronizer = defaultSubscriber.synchronizer

		// Set the cloned Git repo root directory to this Git repository root.
		subitem.repoRoot = "../../.."

		err = subitem.sortClonedGitRepo()
		Expect(err).NotTo(HaveOccurred())
		// 4 helm charts: test/github/helmcharts/chart1,
		//test/github/helmcharts/chart2, test/github/helmcharts/otherCharts/chart1,
		// test/github/helmcharts/chart1Upgrade
		Expect(len(subitem.chartDirs)).To(Equal(4))

		// Filter out chart2
		Expect(len(subitem.indexFile.Entries)).To(Equal(1))

		// chart1 has two versions but it will only contain the latest version (test/github/helmcharts/chart1Upgrade)
		chartVersion, err := subitem.indexFile.Get("chart1", "1.2.2")
		Expect(err).NotTo(HaveOccurred())
		Expect(chartVersion.Name).To(Equal("chart1"))
		Expect(chartVersion.Version).To(Equal("1.2.2"))

		// This check needs to commented out first because the test file must be updated first
		// Expect(chartVersion.Digest).To(Equal("fake-digest-chart1-1.2.2"))

		_, err = subitem.indexFile.Get("chart1", "1.1.1")
		Expect(err).To(HaveOccurred())

		packageFilter.Version = "1.1.1"

		err = subitem.sortClonedGitRepo()
		Expect(err).NotTo(HaveOccurred())

		Expect(len(subitem.indexFile.Entries)).To(Equal(1))

		_, err = subitem.indexFile.Get("chart1", "1.1.2")
		Expect(err).To(HaveOccurred())

		chartVersion, err = subitem.indexFile.Get("chart1", "1.1.1")
		Expect(err).NotTo(HaveOccurred())
		Expect(chartVersion.Name).To(Equal("chart1"))
		Expect(chartVersion.Version).To(Equal("1.1.1"))

		// This check needs to commented out first because the test file must be updated first
		// Expect(chartVersion.Digest).To(Equal("fake-digest-chart1-1.1.1"))

		err = k8sClient.Delete(context.TODO(), pathConfigMap)
		Expect(err).NotTo(HaveOccurred())

		githubsub.Spec.Package = ""
		githubsub.Spec.PackageFilter = nil
	})

	It("should check cluster-admin=true annotation in subscription", func() {
		subscriptionYAML := `apiVersion: apps.open-cluster-management.io/v1
kind: Subscription
metadata:
  name: demo-subscription-2
  namespace: demo-ns-2
  annotations:
    apps.open-cluster-management.io/github-path: resources2
    apps.open-cluster-management.io/cluster-admin: "true"
spec:
  channel: demo-ns-2/somechannel-2
  placement:
    local: true`

		resource := kubeResource{}
		err := yaml.Unmarshal([]byte(subscriptionYAML), &resource)

		Expect(err).NotTo(HaveOccurred())

		err = checkSubscriptionAnnotation(resource)
		Expect(err).To(HaveOccurred())

		subscriptionYAML = `apiVersion: apps.open-cluster-management.io/v1
kind: Subscription
metadata:
  name: demo-subscription-2
  namespace: demo-ns-2
  annotations:
    apps.open-cluster-management.io/github-path: resources2
spec:
  channel: demo-ns-2/somechannel-2
  placement:
    local: true`

		resource = kubeResource{}
		err = yaml.Unmarshal([]byte(subscriptionYAML), &resource)

		Expect(err).NotTo(HaveOccurred())

		err = checkSubscriptionAnnotation(resource)
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("github subscriber reconcile options", func() {
	It("should check apps.open-cluster-management.io/reconcile-option and "+
		"apps.open-cluster-management.io/cluster-admin annotations in subscription and "+
		"propagate them to the synchronizer", func() {

		subAnnotations := make(map[string]string)
		subAnnotations[appv1.AnnotationClusterAdmin] = "true"
		subAnnotations[appv1.AnnotationResourceReconcileOption] = "merge"
		githubsub.SetAnnotations(subAnnotations)
		githubsub.Spec.PackageFilter = nil

		subitem := &GitSubscriberItem{}
		subitem.Subscription = githubsub
		subitem.Channel = githubchn
		subitem.synchronizer = defaultSubscriber.synchronizer

		configMapYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: path-config-map
  namespace: default
data:
  path: test/github/helmcharts`

		deployable, _, err := subitem.subscribeResource([]byte(configMapYAML))
		Expect(err).NotTo(HaveOccurred())

		resource := &unstructured.Unstructured{}
		err = json.Unmarshal(deployable.Spec.Template.Raw, resource)
		Expect(err).NotTo(HaveOccurred())

		rscAnnotations := resource.GetAnnotations()
		Expect(rscAnnotations[appv1.AnnotationClusterAdmin]).To(Equal("true"))
		Expect(rscAnnotations[appv1.AnnotationResourceReconcileOption]).To(Equal("merge"))
	})
})
