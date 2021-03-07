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

package utils

import (
	"fmt"

	"github.com/blang/semver"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"

	dplv1alpha1 "github.com/open-cluster-management/multicloud-operators-deployable/pkg/apis/apps/v1"
)

// SemverCheck filter Deployable based on the version annotations from Subscription and Deployable.
// Assume the input version is a string which meets the Semver format(https://semver.org/)
// SemverCheck: if the subscription version is geater than deployable, then return true, otherwise return false
// edge case: if the version of subscription or deployable is empty then return true
func SemverCheck(vSubStr, vDplStr string) bool {
	if len(vSubStr) == 0 {
		klog.V(5).Infof("Subscription doesn't specify a version, process as update to the latest")
		return true
	}

	if len(vDplStr) == 0 {
		klog.V(5).Infof("Deployable doesn't specify a version, process as update with the current Deployable")
		return true
	}

	vSub, err := semver.ParseRange(vSubStr)
	if err != nil {
		klog.V(5).Infof("Version range string %v is invalid due to %v. Won't proceed the update", vSubStr, err)
		return false
	}

	vDpl, err := semver.ParseTolerant(vDplStr)

	if err != nil {
		klog.V(5).Infof("Version string %v is invalid due to %v. Won't proceed the update", vDplStr, err)
		return false
	}

	if vSub(vDpl) {
		return true
	}

	return false
}

// VersionRep represent version
type VersionRep struct {
	DplKey string
	Vrange string
}

//GenerateVersionSet produce a map, key: dpl.GetGenerateName(), value: dpl.NamespacedName.String()
// the value is the largest version which meet the subscription version requirement.
func GenerateVersionSet(dplPointers []*dplv1alpha1.Deployable, vsub string) map[string]VersionRep {
	vset := make(map[string]VersionRep)

	for _, curDpl := range dplPointers {
		vcurDpl := curDpl.GetAnnotations()[dplv1alpha1.AnnotationDeployableVersion]

		// if the deployable doesn't have a version string, then treat it a the base verion of it's own
		// version group, meaning using the dpl name as group key
		if len(vcurDpl) == 0 {
			vset = emptyVersionsField(curDpl, vset)
			continue
		}

		vmatch := SemverCheck(vsub, vcurDpl)

		if !vmatch {
			continue
		}

		curDplKey := types.NamespacedName{Name: curDpl.Name, Namespace: curDpl.Namespace}.String()

		DplGroupName := curDpl.GetGenerateName()

		// if the dpl doesn't have generateName, then use dpl name as the group key
		if DplGroupName == "" {
			DplGroupName = curDpl.GetName()
		}

		r := "%s%s"

		vrangestr := fmt.Sprintf(r, ">", vcurDpl)

		if preDpl, ok := vset[DplGroupName]; !ok {
			vset[DplGroupName] = VersionRep{
				DplKey: curDplKey,
				Vrange: vrangestr,
			}
		} else {
			preRange := preDpl.Vrange
			if SemverCheck(preRange, vcurDpl) {
				vset[DplGroupName] = VersionRep{
					DplKey: curDplKey,
					Vrange: vrangestr,
				}
			}
		}
	}

	return vset
}

func emptyVersionsField(curDpl *dplv1alpha1.Deployable, vset map[string]VersionRep) map[string]VersionRep {
	DplGroupName := curDpl.GetName()

	r := "%s%s"
	// if the deployable doesn't have a version string, then treat it a the base verion
	vcurDpl := "0.0.0"

	vrangestr := fmt.Sprintf(r, ">", vcurDpl)
	curDplKey := types.NamespacedName{Name: curDpl.Name, Namespace: curDpl.Namespace}.String()

	if preDpl, ok := vset[DplGroupName]; !ok {
		vset[DplGroupName] = VersionRep{
			DplKey: curDplKey,
			Vrange: vrangestr,
		}
	} else {
		preRange := preDpl.Vrange
		if SemverCheck(preRange, vcurDpl) {
			vset[DplGroupName] = VersionRep{
				DplKey: curDplKey,
				Vrange: vrangestr,
			}
		}
	}

	return vset
}

// DplArrayToDplPointers covert the array to pointer array
func DplArrayToDplPointers(dplList []dplv1alpha1.Deployable) []*dplv1alpha1.Deployable {
	var dpls []*dplv1alpha1.Deployable

	for _, dpl := range dplList {
		tmp := dpl
		dpls = append(dpls, &tmp)
	}

	return dpls
}

// IsDeployableInVersionSet - check if deployable is in version
func IsDeployableInVersionSet(vMap map[string]VersionRep, dpl *dplv1alpha1.Deployable) bool {
	vdplKey := types.NamespacedName{Name: dpl.Name, Namespace: dpl.Namespace}.String()
	dplAnno := dpl.GetAnnotations()

	klog.V(5).Infof("version map %v, dplkey %v", vMap, vdplKey)

	versionField := dplAnno[dplv1alpha1.AnnotationDeployableVersion]

	dplGroup := dpl.GetGenerateName()
	if dplGroup == "" || versionField == "" {
		dplGroup = dpl.GetName()
	}

	klog.V(5).Infof("dplGroup %v, versionField %v", dplGroup, versionField)

	return vMap[dplGroup].DplKey == vdplKey
}
