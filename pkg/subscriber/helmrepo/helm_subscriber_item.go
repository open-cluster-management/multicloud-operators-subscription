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
	"context"
	"crypto/sha1" // #nosec G505 Used only to generate random value to be used to generate hash string
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	gerr "github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ghodss/yaml"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/helm/pkg/repo"
	"k8s.io/klog"

	chnv1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	releasev1 "github.com/open-cluster-management/multicloud-operators-subscription-release/pkg/apis/apps/v1"
	appv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
	dplpro "github.com/open-cluster-management/multicloud-operators-subscription/pkg/subscriber/processdeployable"
	kubesynchronizer "github.com/open-cluster-management/multicloud-operators-subscription/pkg/synchronizer/kubernetes"
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/utils"
)

// SubscriberItem - defines the unit of namespace subscription
type SubscriberItem struct {
	appv1.SubscriberItem

	hash         string
	stopch       chan struct{}
	syncinterval int
	success      bool
	synchronizer SyncSource
}

var (
	helmGvk = schema.GroupVersionKind{
		Group:   appv1.SchemeGroupVersion.Group,
		Version: appv1.SchemeGroupVersion.Version,
		Kind:    "HelmRelease",
	}
)

// SubscribeItem subscribes a subscriber item with namespace channel
func (hrsi *SubscriberItem) Start() {
	// do nothing if already started
	if hrsi.stopch != nil {
		return
	}

	hrsi.stopch = make(chan struct{})

	go wait.Until(func() {
		tw := hrsi.SubscriberItem.Subscription.Spec.TimeWindow
		if tw != nil {
			nextRun := utils.NextStartPoint(tw, time.Now())
			if nextRun > time.Duration(0) {
				klog.V(1).Infof("Subcription %v/%v will de deploy after %v",
					hrsi.SubscriberItem.Subscription.GetNamespace(),
					hrsi.SubscriberItem.Subscription.GetName(), nextRun)
				return
			}
		}

		hrsi.doSubscription()
	}, time.Duration(hrsi.syncinterval)*time.Second, hrsi.stopch)
}

func (hrsi *SubscriberItem) Stop() {
	if hrsi.stopch != nil {
		close(hrsi.stopch)
		hrsi.stopch = nil
	}
}

func (hrsi *SubscriberItem) doSubscription() {
	//Retrieve the helm repo
	repoURL := hrsi.Channel.Spec.Pathname

	httpClient, err := getHelmRepoClient(hrsi.ChannelConfigMap)

	if err != nil {
		klog.Error(err, "Unable to create client for helm repo", repoURL)
		return
	}

	_, hash, err := getHelmRepoIndex(httpClient, hrsi.Subscription, hrsi.ChannelSecret, repoURL)

	if err != nil {
		klog.Error(err, "Unable to retrieve the helm repo index", repoURL)
		return
	}

	klog.V(4).Infof("Check if helmRepo %s changed with hash %s", repoURL, hash)

	if hash != hrsi.hash || !hrsi.success {
		if err := hrsi.processSubscription(); err != nil {
			klog.Error("Failed to process helm repo subscription with error:", err)

			hrsi.success = false

			return
		}

		hrsi.success = true
	}
}

func (hrsi *SubscriberItem) processSubscription() error {
	repoURL := hrsi.Channel.Spec.Pathname
	klog.V(4).Info("Proecssing HelmRepo:", repoURL)

	httpClient, err := getHelmRepoClient(hrsi.ChannelConfigMap)
	if err != nil {
		klog.Error(err, "Unable to create client for helm repo", repoURL)
		return err
	}

	indexFile, hash, err := getHelmRepoIndex(httpClient, hrsi.Subscription, hrsi.ChannelSecret, repoURL)
	if err != nil {
		klog.Error(err, "Unable to retrieve the helm repo index", repoURL)
		return err
	}

	err = hrsi.manageHelmCR(indexFile, repoURL)

	if err != nil {
		return err
	}

	hrsi.hash = hash

	return nil
}

func getHelmRepoClient(chnCfg *corev1.ConfigMap) (*http.Client, error) {
	client := http.DefaultClient
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
	}

	if chnCfg != nil {
		helmRepoConfigData := chnCfg.Data
		klog.V(5).Infof("s.HelmRepoConfig.Data %v", helmRepoConfigData)

		if helmRepoConfigData["insecureSkipVerify"] != "" {
			b, err := strconv.ParseBool(helmRepoConfigData["insecureSkipVerify"])
			if err != nil {
				klog.Error(err, "Unable to parse insecureSkipVerify: ", helmRepoConfigData["insecureSkipVerify"])
				return nil, err
			}

			transport.TLSClientConfig.InsecureSkipVerify = b
		} else {
			klog.V(5).Info("helmRepoConfigData[\"insecureSkipVerify\"] is empty")
		}
	} else {
		klog.V(5).Info("s.HelmRepoConfig is nil")
	}

	client.Transport = transport

	return client, nil
}

//getHelmRepoIndex retreives the index.yaml, loads it into a repo.IndexFile and filters it
func getHelmRepoIndex(client rest.HTTPClient, sub *appv1.Subscription,
	chnSrt *corev1.Secret, repoURL string) (indexFile *repo.IndexFile, hash string, err error) {
	cleanRepoURL := strings.TrimSuffix(repoURL, "/") + "/index.yaml"
	req, err := http.NewRequest(http.MethodGet, cleanRepoURL, nil)

	if err != nil {
		klog.Error(err, "Can not build request: ", cleanRepoURL)
		return nil, "", err
	}

	if chnSrt != nil && chnSrt.Data != nil {
		if authHeader, ok := chnSrt.Data["authHeader"]; ok {
			req.Header.Set("Authorization", string(authHeader))
		} else if user, ok := chnSrt.Data["user"]; ok {
			if password, ok := chnSrt.Data["password"]; ok {
				req.SetBasicAuth(string(user), string(password))
			} else {
				return nil, "", fmt.Errorf("password not found in secret for basic authentication")
			}
		}
	}

	klog.V(5).Info(req)
	resp, err := client.Do(req)

	if err != nil {
		klog.Error(err, "Http request failed: ", cleanRepoURL)

		return nil, "", err
	}

	klog.V(5).Info("Get succeeded: ", cleanRepoURL)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		klog.Error(err, "Unable to read body: ", cleanRepoURL)
		return nil, "", err
	}

	defer resp.Body.Close()

	hash = hashKey(body)
	indexfile, err := loadIndex(body)

	if err != nil {
		klog.Error(err, "Unable to parse the indexfile: ", cleanRepoURL)
		return nil, "", err
	}

	err = utils.FilterCharts(sub, indexfile)

	return indexfile, hash, err
}

//func CreateOrUpdateHelmChart(
//	packageName string,
//	releaseCRName string,
//	chartVersions repo.ChartVersions,
//	client client.Client,
//	channel *chnv1.Channel,
//	sub *appv1.Subscription) (helmRelease *releasev1.HelmRelease, err error) {
func GetSubscriptionChartsOnHub(hubClt client.Client, sub *appv1.Subscription) ([]*releasev1.HelmRelease, error) {
	chn := &chnv1.Channel{}
	chnkey := utils.NamespacedNameFormat(sub.Spec.Channel)

	if err := hubClt.Get(context.TODO(), chnkey, chn); err != nil {
		return nil, gerr.Wrapf(err, "failed to get channel of subscription %v", sub)
	}

	repoURL := chn.Spec.Pathname
	klog.V(2).Infof("getting resource list of HelmRepo %v", repoURL)

	chSrt := &corev1.Secret{}

	if chn.Spec.SecretRef != nil {
		chnSrtKey := types.NamespacedName{
			Name:      chn.Spec.SecretRef.Name,
			Namespace: chn.Spec.SecretRef.Namespace,
		}

		if err := hubClt.Get(context.TODO(), chnSrtKey, chSrt); err != nil {
			return nil, gerr.Wrap(err, "failed to get reference configmap from channel")
		}
	}

	chnCfg := &corev1.ConfigMap{}

	if chn.Spec.ConfigMapRef != nil {
		chnCfgKey := types.NamespacedName{
			Name:      chn.Spec.ConfigMapRef.Name,
			Namespace: chn.Spec.ConfigMapRef.Namespace,
		}

		if err := hubClt.Get(context.TODO(), chnCfgKey, chnCfg); err != nil {
			return nil, gerr.Wrap(err, "failed to get reference configmap from channel")
		}
	}

	httpClient, err := getHelmRepoClient(chnCfg)
	if err != nil {
		return nil, gerr.Wrapf(err, "Unable to create client for helm repo %v", repoURL)
	}

	indexFile, _, err := getHelmRepoIndex(httpClient, sub, chSrt, repoURL)
	if err != nil {
		return nil, gerr.Wrapf(err, "unable to retrieve the helm repo index %v", repoURL)
	}

	helms := make([]*releasev1.HelmRelease, 0)

	for pkgName, chartVer := range indexFile.Entries {
		releaseCRName, err := utils.PkgToReleaseCRName(sub, pkgName)
		if err != nil {
			return nil, gerr.Wrapf(err, "failed to generate releaseCRName of helm chart %v for subscription %v", pkgName, sub)
		}

		helm, err := utils.CreateOrUpdateHelmChart(pkgName, releaseCRName, chartVer, hubClt, chn, sub)
		if err != nil {
			return nil, gerr.Wrapf(err, "failed to get helm chart of %v for subscription %v", pkgName, sub)
		}

		helms = append(helms, helm)
	}

	return helms, nil
}

//loadIndex loads data into a repo.IndexFile
func loadIndex(data []byte) (*repo.IndexFile, error) {
	i := &repo.IndexFile{}
	if err := yaml.Unmarshal(data, i); err != nil {
		klog.Error(err, "Unmarshal failed. Data: ", data)
		return i, err
	}

	i.SortEntries()

	if i.APIVersion == "" {
		return i, repo.ErrNoAPIVersion
	}

	return i, nil
}

//hashKey Calculate a hash key
func hashKey(b []byte) string {
	h := sha1.New() // #nosec G401 Used only to generate random value to be used to generate hash string
	_, err := h.Write(b)

	if err != nil {
		klog.Error("Failed to hash key with error:", err)
	}

	return string(h.Sum(nil))
}

func (hrsi *SubscriberItem) manageHelmCR(indexFile *repo.IndexFile, repoURL string) error {
	var doErr error

	hostkey := types.NamespacedName{Name: hrsi.Subscription.Name, Namespace: hrsi.Subscription.Namespace}
	syncsource := helmreposyncsource + hostkey.String()
	pkgMap := make(map[string]bool)

	dplUnits := make([]kubesynchronizer.DplUnit, 0)

	//Loop on all packages selected by the subscription
	for packageName, chartVersions := range indexFile.Entries {
		klog.V(5).Infof("chart: %s\n%v", packageName, chartVersions)

		dpl, err := utils.CreateHelmCRDeployable(
			repoURL, packageName, chartVersions, hrsi.synchronizer.GetLocalClient(), hrsi.Channel, hrsi.Subscription)

		if err != nil {
			klog.Error("failed to create a helmrelease CR deployable, err: ", err)

			doErr = err

			continue
		}

		unit := kubesynchronizer.DplUnit{Dpl: dpl, Gvk: helmGvk}
		dplUnits = append(dplUnits, unit)

		dplkey := types.NamespacedName{
			Name:      dpl.Name,
			Namespace: dpl.Namespace,
		}
		pkgMap[dplkey.Name] = true
	}

	if err := dplpro.Units(hrsi.Subscription, hrsi.synchronizer, hostkey, syncsource, pkgMap, dplUnits); err != nil {
		klog.Warningf("failed to put helm deployables to cache (will retry), err: %v", err)
		doErr = err
	}

	return doErr
}
