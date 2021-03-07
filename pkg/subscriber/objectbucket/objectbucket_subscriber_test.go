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

package objectbucket

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	chnv1alpha1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	dplv1alpha1 "github.com/open-cluster-management/multicloud-operators-deployable/pkg/apis/apps/v1"
	appv1alpha1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"

	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
)

var c client.Client

var id = types.NamespacedName{
	Name:      "endpoint",
	Namespace: "default",
}

var ops = config.SubscriptionCMDoptions{
	Syncid:       &id,
	SyncInterval: 2,
}

var (
	sharedkey = types.NamespacedName{
		Name:      "test",
		Namespace: "default",
	}
	helmchn = &chnv1alpha1.Channel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sharedkey.Name,
			Namespace: sharedkey.Namespace,
		},
		Spec: chnv1alpha1.ChannelSpec{},
	}
	helmsub = &appv1alpha1.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sharedkey.Name,
			Namespace: sharedkey.Namespace,
		},
		Spec: appv1alpha1.SubscriptionSpec{
			Channel: sharedkey.String(),
		},
	}
	subitem = &appv1alpha1.SubscriberItem{
		Subscription: helmsub,
		Channel:      helmchn,
	}
)

func TestObjectSubscriber(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup the Manager and Controller.  Wrap the Controller Reconcile function so it writes each request to a
	// channel when it is finished.
	mgr, err := manager.New(cfg, manager.Options{MetricsBindAddress: "0"})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	c = mgr.GetClient()

	g.Expect(Add(mgr, cfg, ops)).NotTo(gomega.HaveOccurred())
	stopMgr, mgrStopped := StartTestManager(mgr, g)

	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	g.Expect(defaultSubscriber.SubscribeItem(subitem)).NotTo(gomega.HaveOccurred())

	time.Sleep(1 * time.Second)

	g.Expect(defaultSubscriber.UnsubscribeItem(sharedkey)).NotTo(gomega.HaveOccurred())
}

func TestDoSubscribeDeployable(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	obsi := &SubscriberItem{}
	dpl := &dplv1alpha1.Deployable{}

	_, _, err := obsi.doSubscribeDeployable(dpl, nil, nil)
	g.Expect(err).To(gomega.HaveOccurred())

	dpl.Spec.Template = &runtime.RawExtension{}
	dpl.Spec.Template.Raw = []byte{65, 65, 65}
	_, _, err = obsi.doSubscribeDeployable(dpl, nil, nil)
	g.Expect(err).To(gomega.HaveOccurred())
}
