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

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/blang/semver"
	"github.com/ghodss/yaml"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/repo"
	clientsetx "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	chnv1 "github.com/open-cluster-management/multicloud-operators-channel/pkg/apis/apps/v1"
	dplv1 "github.com/open-cluster-management/multicloud-operators-deployable/pkg/apis/apps/v1"
	dplutils "github.com/open-cluster-management/multicloud-operators-deployable/pkg/utils"
	releasev1 "github.com/open-cluster-management/multicloud-operators-subscription-release/pkg/apis/apps/v1"
	appv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
)

func GetPackageAlias(sub *appv1.Subscription, packageName string) string {
	for _, overrides := range sub.Spec.PackageOverrides {
		if overrides.PackageName == packageName {
			klog.Infof("Overrides for package %s found", packageName)

			if overrides.PackageAlias != "" {
				return overrides.PackageAlias
			}
		}
	}

	return ""
}

// GenerateHelmIndexFile generate helm repo index file
func GenerateHelmIndexFile(sub *appv1.Subscription, repoRoot string, chartDirs map[string]string) (*repo.IndexFile, error) {
	// Build a helm repo index file
	indexFile := repo.NewIndexFile()

	for chartDir := range chartDirs {
		chartDir = strings.TrimSuffix(chartDir, "/")
		// chartFolderName is chart folder name
		chartFolderName := filepath.Base(chartDir)
		// chartParentDir is the chart folder's parent folder.
		chartParentDir := filepath.Dir(chartDir) + "/"

		// Get the relative parent directory from the git repo root
		chartBaseDir := strings.TrimPrefix(chartParentDir, repoRoot+"/")

		chartMetadata, err := chartutil.LoadChartfile(filepath.Join(chartDir, "Chart.yaml"))

		if err != nil {
			klog.Error("There was a problem in generating helm charts index file: ", err.Error())
			return indexFile, err
		}

		err = indexFile.MustAdd(chartMetadata, chartFolderName, chartBaseDir, "generated-by-multicloud-operators-subscription")
		if err != nil {
			klog.Warning("There was a problem in adding content to helm charts index file: ", err.Error())
		}
	}

	indexFile.SortEntries()

	err := FilterCharts(sub, indexFile)

	if err != nil {
		return indexFile, err
	}

	return indexFile, nil
}

func CreateOrUpdateHelmChart(
	packageName string,
	releaseCRName string,
	chartVersions repo.ChartVersions,
	client client.Client,
	channel *chnv1.Channel,
	sub *appv1.Subscription) (helmRelease *releasev1.HelmRelease, err error) {
	helmRelease = &releasev1.HelmRelease{}
	err = client.Get(context.TODO(),
		types.NamespacedName{Name: releaseCRName, Namespace: sub.Namespace}, helmRelease)

	var source *releasev1.Source

	if IsGitChannel(string(channel.Spec.Type)) {
		source = &releasev1.Source{
			SourceType: releasev1.GitSourceType,
			Git: &releasev1.Git{
				Urls:      []string{channel.Spec.Pathname},
				ChartPath: chartVersions[0].URLs[0],
				Branch:    GetSubscriptionBranch(sub).Short(),
			},
		}
	} else {
		var validURLs []string

		for _, url := range chartVersions[0].URLs {
			if IsURL(url) {
				validURLs = append(validURLs, url)
			} else if IsURL(channel.Spec.Pathname + "/" + url) {
				validURLs = append(validURLs, channel.Spec.Pathname+"/"+url)
			}
		}

		if len(validURLs) == 0 {
			return nil, fmt.Errorf("no valid URLs are found for package: %s", packageName)
		}

		source = &releasev1.Source{
			SourceType: releasev1.HelmRepoSourceType,
			HelmRepo: &releasev1.HelmRepo{
				Urls: validURLs,
			},
		}
	}

	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(2).Infof("Create helmRelease %s", releaseCRName)

			version := ""
			digest := ""

			if len(chartVersions) > 0 && chartVersions[0] != nil {
				if chartVersions[0].Metadata != nil {
					version = chartVersions[0].Version
				}

				digest = chartVersions[0].Digest
			}

			helmRelease = &releasev1.HelmRelease{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "apps.open-cluster-management.io/v1",
					Kind:       "HelmRelease",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      releaseCRName,
					Namespace: sub.Namespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps.open-cluster-management.io/v1",
						Kind:       "Subscription",
						Name:       sub.Name,
						UID:        sub.UID,
					}},
				},
				Repo: releasev1.HelmReleaseRepo{
					Source:             source,
					ConfigMapRef:       channel.Spec.ConfigMapRef,
					InsecureSkipVerify: channel.Spec.InsecureSkipVerify,
					SecretRef:          channel.Spec.SecretRef,
					ChartName:          packageName,
					Version:            version,
					Digest:             digest,
				},
			}
		} else {
			klog.Error("Error in getting existing helm release", err)
			return nil, err
		}
	} else {
		// set kind and apiversion, coz it is not in the resource get from k8s
		helmRelease.APIVersion = "apps.open-cluster-management.io/v1"
		helmRelease.Kind = "HelmRelease"
		klog.V(2).Infof("Update helmRelease repo %s", helmRelease.Name)

		version := ""
		digest := ""

		if len(chartVersions) > 0 && chartVersions[0] != nil {
			if chartVersions[0].Metadata != nil {
				version = chartVersions[0].Version
			}

			digest = chartVersions[0].Digest
		}

		helmRelease.Repo = releasev1.HelmReleaseRepo{
			Source:             source,
			ConfigMapRef:       channel.Spec.ConfigMapRef,
			InsecureSkipVerify: channel.Spec.InsecureSkipVerify,
			SecretRef:          channel.Spec.SecretRef,
			ChartName:          packageName,
			Version:            version,
			Digest:             digest,
		}
	}

	return helmRelease, nil
}

func Override(helmRelease *releasev1.HelmRelease, sub *appv1.Subscription) error {
	//Overrides with the values provided in the subscription for that package
	overrides := getOverrides(helmRelease.Repo.ChartName, sub)
	data, err := yaml.Marshal(helmRelease)

	if err != nil {
		klog.Error("Failed to mashall ", helmRelease.Name, " err:", err)
		return err
	}

	template := &unstructured.Unstructured{}
	err = yaml.Unmarshal(data, template)

	if err != nil {
		klog.Warning("Processing local deployable with error template:", helmRelease, err)
	}

	template, err = dplutils.OverrideTemplate(template, overrides.ClusterOverrides)

	if err != nil {
		klog.Error("Failed to apply override for instance: ")
		return err
	}

	data, err = yaml.Marshal(template)

	if err != nil {
		klog.Error("Failed to mashall ", helmRelease.Name, " err:", err)
		return err
	}

	err = yaml.Unmarshal(data, helmRelease)

	if err != nil {
		klog.Error("Failed to unmashall ", helmRelease.Name, " err:", err)
		return err
	}

	return nil
}

func getShortSubUID(subUID string) string {
	shortUID := subUID

	if len(subUID) >= 5 {
		shortUID = subUID[:5]
	}

	return shortUID
}

func PkgToReleaseCRName(sub *appv1.Subscription, packageName string) (string, error) {
	releaseCRName := GetPackageAlias(sub, packageName)
	if releaseCRName == "" {
		releaseCRName = packageName
		subUID := string(sub.UID)

		if subUID != "" {
			releaseCRName += "-" + getShortSubUID(subUID)
		}
	}

	releaseCRName, err := GetReleaseName(releaseCRName)
	if err != nil {
		return "", err
	}

	return releaseCRName, nil
}

func CreateHelmCRDeployable(
	repoURL string,
	packageName string,
	chartVersions repo.ChartVersions,
	client client.Client,
	channel *chnv1.Channel,
	sub *appv1.Subscription) (*dplv1.Deployable, error) {
	releaseCRName, err := PkgToReleaseCRName(sub, packageName)
	if err != nil {
		return nil, err
	}

	if channel == nil || !IsGitChannel(string(channel.Spec.Type)) {
		for i := range chartVersions[0].URLs {
			parsedURL, err := url.Parse(chartVersions[0].URLs[i])

			if err != nil {
				klog.Error("Failed to parse url with error:", err)
				return nil, err
			}

			if parsedURL.Scheme == "local" {
				//make sure there is one and only one slash
				repoURL = strings.TrimSuffix(repoURL, "/") + "/"
				chartVersions[0].URLs[i] = strings.Replace(chartVersions[0].URLs[i], "local://", repoURL, -1)
			}
		}
	}

	helmRelease, err := CreateOrUpdateHelmChart(
		packageName, releaseCRName, chartVersions, client, channel, sub)

	if err != nil {
		klog.Error("Failed to create or update helm chart ", packageName, " err:", err)
		return nil, err
	}

	err = Override(helmRelease, sub)

	if err != nil {
		klog.Error("Failed to override ", helmRelease.Name, " err:", err)
		return nil, err
	}

	if helmRelease.Spec == nil {
		spec := make(map[string]interface{})

		err := yaml.Unmarshal([]byte("{\"\":\"\"}"), &spec)
		if err != nil {
			klog.Error("Failed to create an empty spec for helm release", helmRelease)
			return nil, err
		}

		helmRelease.Spec = spec
	}

	hrLbls := AddPartOfLabel(sub, helmRelease.Labels)
	if hrLbls != nil {
		helmRelease.Labels = hrLbls
	}

	dpl := &dplv1.Deployable{}
	dpl.Name = sub.Name + "-" + getShortSubUID(string(sub.UID)) + "-" + packageName
	dpl.Namespace = sub.Namespace

	dpl.Spec.Template = &runtime.RawExtension{}
	dpl.Spec.Template.Raw, err = json.Marshal(helmRelease)

	if err != nil {
		klog.Error("Failed to mashall helm release", helmRelease)
		return nil, err
	}

	dplanno := make(map[string]string)
	dplanno[dplv1.AnnotationLocal] = "true"
	dpl.SetAnnotations(dplanno)

	return dpl, nil
}

func CreateHelmCRUnstructured(
	repoURL string,
	packageName string,
	chartVersions repo.ChartVersions,
	client client.Client,
	channel *chnv1.Channel,
	sub *appv1.Subscription,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	releaseCRName, err := PkgToReleaseCRName(sub, packageName)
	if err != nil {
		return nil, err
	}

	if channel == nil || !IsGitChannel(string(channel.Spec.Type)) {
		for i := range chartVersions[0].URLs {
			parsedURL, err := url.Parse(chartVersions[0].URLs[i])

			if err != nil {
				klog.Error("Failed to parse url with error:", err)
				return nil, err
			}

			if parsedURL.Scheme == "local" {
				//make sure there is one and only one slash
				repoURL = strings.TrimSuffix(repoURL, "/") + "/"
				chartVersions[0].URLs[i] = strings.Replace(chartVersions[0].URLs[i], "local://", repoURL, -1)
			}
		}
	}

	helmRelease, err := CreateOrUpdateHelmChart(
		packageName, releaseCRName, chartVersions, client, channel, sub)

	if err != nil {
		klog.Error("Failed to create or update helm chart ", packageName, " err:", err)
		return nil, err
	}

	err = Override(helmRelease, sub)

	if err != nil {
		klog.Error("Failed to override ", helmRelease.Name, " err:", err)
		return nil, err
	}

	if helmRelease.Spec == nil {
		spec := make(map[string]interface{})

		err := yaml.Unmarshal([]byte("{\"\":\"\"}"), &spec)
		if err != nil {
			klog.Error("Failed to create an empty spec for helm release", helmRelease)
			return nil, err
		}

		helmRelease.Spec = spec
	}

	hrLbls := AddPartOfLabel(sub, helmRelease.Labels)
	if hrLbls != nil {
		helmRelease.Labels = hrLbls
	}

	//TODO might need to add owner reference between release and subscription
	unsanno := helmRelease.GetAnnotations()
	unsanno[dplv1.AnnotationLocal] = "true"
	helmRelease.SetAnnotations(unsanno)

	uns := &unstructured.Unstructured{}

	err = scheme.Convert(helmRelease, uns, nil)

	return uns, err
}

func getOverrides(packageName string, sub *appv1.Subscription) dplv1.Overrides {
	dploverrides := dplv1.Overrides{}

	for _, overrides := range sub.Spec.PackageOverrides {
		if overrides.PackageName == packageName {
			klog.Infof("Overrides for package %s found", packageName)
			dploverrides.ClusterName = packageName
			dploverrides.ClusterOverrides = make([]dplv1.ClusterOverride, 0)

			for _, override := range overrides.PackageOverrides {
				clusterOverride := dplv1.ClusterOverride{
					RawExtension: runtime.RawExtension{
						Raw: override.RawExtension.Raw,
					},
				}
				dploverrides.ClusterOverrides = append(dploverrides.ClusterOverrides, clusterOverride)
			}

			return dploverrides
		}
	}

	return dploverrides
}

//FilterCharts filters the indexFile by name, version, digest
func FilterCharts(sub *appv1.Subscription, indexFile *repo.IndexFile) error {
	//Removes all entries from the indexFile with non matching name
	err := removeNoMatchingName(sub, indexFile)
	if err != nil {
		klog.Warning(err)
	}
	//Removes non matching version, digest
	filterOnVersion(sub, indexFile)
	//Keep only the lastest version if multiple remains after filtering.
	err = takeLatestVersion(indexFile)
	if err != nil {
		klog.Error("Failed to filter on version with error: ", err)
		return err
	}

	return nil
}

//takeLatestVersion if the indexFile contains multiple versions for a given chart, then
//only the latest is kept.
func takeLatestVersion(indexFile *repo.IndexFile) (err error) {
	indexFile.SortEntries()

	for k := range indexFile.Entries {
		//Get return the latest version when version is empty but
		//there is a bug in the masterminds semver used by helm
		// "*" constraint is not working properly
		// "*" is equivalent to ">=0.0.0"
		chartVersion, err := indexFile.Get(k, ">=0.0.0")
		if err != nil {
			klog.Error(err)
			return err
		}

		indexFile.Entries[k] = []*repo.ChartVersion{chartVersion}
	}

	return nil
}

//checkDigest Checks if the digest matches
func checkDigest(sub *appv1.Subscription, chartVersion *repo.ChartVersion) bool {
	if sub != nil {
		if sub.Spec.PackageFilter != nil {
			if sub.Spec.PackageFilter.Annotations != nil {
				if filterDigest, ok := sub.Spec.PackageFilter.Annotations["digest"]; ok {
					return filterDigest == chartVersion.Digest
				}
			}
		}
	}

	klog.V(4).Info("Digest check passed for:", chartVersion)

	return true
}

//removeNoMatchingName Deletes entries that the name doesn't match the name provided in the subscription
func removeNoMatchingName(sub *appv1.Subscription, indexFile *repo.IndexFile) error {
	if sub.Spec.Package != "" {
		keys := make([]string, 0)
		for k := range indexFile.Entries {
			keys = append(keys, k)
		}

		for _, k := range keys {
			if k != sub.Spec.Package {
				delete(indexFile.Entries, k)
			}
		}
	} else {
		return fmt.Errorf("subsciption.spec.package is missing for subscription: %s/%s", sub.Namespace, sub.Name)
	}

	klog.V(4).Info("After name matching:", indexFile)

	return nil
}

//filterOnVersion filters the indexFile with the version, and Digest provided in the subscription
//The version provided in the subscription can be an expression like ">=1.2.3" (see https://github.com/blang/semver)
func filterOnVersion(sub *appv1.Subscription, indexFile *repo.IndexFile) {
	keys := make([]string, 0)
	for k := range indexFile.Entries {
		keys = append(keys, k)
	}

	for _, k := range keys {
		chartVersions := indexFile.Entries[k]
		newChartVersions := make([]*repo.ChartVersion, 0)

		for index, chartVersion := range chartVersions {
			if checkKeywords(sub, chartVersion) && checkDigest(sub, chartVersion) && checkVersion(sub, chartVersion) {
				newChartVersions = append(newChartVersions, chartVersions[index])
			}
		}

		if len(newChartVersions) > 0 {
			indexFile.Entries[k] = newChartVersions
		} else {
			delete(indexFile.Entries, k)
		}
	}

	klog.V(4).Info("After version matching:", indexFile)
}

//checkKeywords Checks if the charts has at least 1 keyword from the packageFilter.Keywords array
func checkKeywords(sub *appv1.Subscription, chartVersion *repo.ChartVersion) bool {
	var labelSelector *metav1.LabelSelector
	if sub.Spec.PackageFilter != nil {
		labelSelector = sub.Spec.PackageFilter.LabelSelector
	}

	return KeywordsChecker(labelSelector, chartVersion.Keywords)
}

//checkVersion checks if the version matches
func checkVersion(sub *appv1.Subscription, chartVersion *repo.ChartVersion) bool {
	if sub.Spec.PackageFilter != nil {
		if sub.Spec.PackageFilter.Version != "" {
			version := chartVersion.Version
			versionVersion, err := semver.Parse(version)

			if err != nil {
				klog.Error(err)
				return false
			}

			filterVersion, err := semver.ParseRange(sub.Spec.PackageFilter.Version)

			if err != nil {
				klog.Error(err)
				return false
			}

			return filterVersion(versionVersion)
		}
	}

	klog.V(4).Info("Version check passed for:", chartVersion)

	return true
}

//DeleteHelmReleaseCRD deletes the HelmRelease CRD
func DeleteHelmReleaseCRD(runtimeClient client.Client, crdx *clientsetx.Clientset) {
	hrlist := &releasev1.HelmReleaseList{}
	err := runtimeClient.List(context.TODO(), hrlist, &client.ListOptions{})

	if err != nil && !errors.IsNotFound(err) {
		klog.Infof("HelmRelease kind is gone. err: %s", err.Error())
		os.Exit(0)
	} else {
		for _, hr := range hrlist.Items {
			hr := hr
			klog.V(1).Infof("Found %s", hr.SelfLink)
			// remove all finalizers
			hr = *hr.DeepCopy()
			hr.SetFinalizers([]string{})
			err = runtimeClient.Update(context.TODO(), &hr)
			if err != nil {
				klog.Warning(err)
			}
		}
		// now get rid of the crd
		err = crdx.ApiextensionsV1().CustomResourceDefinitions().Delete(context.TODO(), "helmreleases.apps.open-cluster-management.io", v1.DeleteOptions{})
		if err != nil {
			klog.Infof("Deleting helmrelease CRD failed. err: %s", err.Error())
		} else {
			klog.Info("helmrelease CRD removed")
		}
	}
}

//IsURL return true if string is a valid URL
func IsURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
