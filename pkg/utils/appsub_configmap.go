package utils

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	SUB_RESOURCES_SET = "appsub-resource"
)

type ResourceInfo struct {
	GVK                  schema.GroupVersionKind `json:"gvk"`
	types.NamespacedName `json:"namespacedName"`
}

type ResourceSet map[ResourceInfo]struct{}

func (r ResourceSet) DeleateAllResource(clt client.Client) error {
	ctx := context.TODO()
	for info := range r {
		ins := &unstructured.Unstructured{}
		ins.SetName(info.NamespacedName.Name)
		ins.SetNamespace(info.NamespacedName.Namespace)
		ins.SetGroupVersionKind(info.GVK)

		if err := clt.Delete(ctx, ins); err != nil {
			return fmt.Errorf("failed to delete DeleateAllResource(), err: %w", err)
		}
	}

	return nil
}

// ConfigMapData contains all the resouces deployed by an appsub
type SubResources struct {
	Key types.NamespacedName
	// format store at configmap
	data []ResourceInfo
	Set  ResourceSet
}

func NewSubResources(subKey types.NamespacedName) *SubResources {
	return &SubResources{Key: subKey, Set: ResourceSet{}, data: []ResourceInfo{}}
}

func (s *SubResources) String() string {
	return fmt.Sprintf("%#v", s.Set)
}

// DeleteAppsubConfigMap
func (s *SubResources) GetSubResources(clt client.Client) error {
	cm := &corev1.ConfigMap{}

	if err := clt.Get(context.TODO(), s.Key, cm); err != nil {
		return err
	}

	if len(cm.BinaryData) == 0 || len(cm.BinaryData[SUB_RESOURCES_SET]) == 0 {
		s.Set = ResourceSet{}
		s.data = []ResourceInfo{}
		return nil
	}

	d := &[]ResourceInfo{}
	if err := json.Unmarshal(cm.BinaryData[SUB_RESOURCES_SET], d); err != nil {
		return fmt.Errorf("failed to Unmarshal when GetSubResources(), err: %w", err)
	}

	s.data = *d

	s.toSet()

	return nil
}

func (s *SubResources) toSet() {
	for _, val := range s.data {
		s.Set[val] = struct{}{}
	}
}

// GetToBeDeletedResources output the resource is in the incoming slice but not in SubResources
func (s *SubResources) GetToBeDeletedResources(in map[ResourceInfo]*unstructured.Unstructured) []*unstructured.Unstructured {
	out := []*unstructured.Unstructured{}

	for key := range s.Set {
		if val, ok := in[key]; !ok {
			out = append(out, val)
		}
	}

	return out
}

// GetToBeCreatedResources output the resource is not in the incoming slice but in SubResources
func (s *SubResources) GetToBeCreatedResources(in map[ResourceInfo]*unstructured.Unstructured) []*unstructured.Unstructured {
	out := []*unstructured.Unstructured{}

	for key, val := range in {
		if _, ok := s.Set[key]; !ok {
			out = append(out, val)
		}
	}

	return out
}

// GetToBeUpdateResources output the resource is in the incoming slice and in SubResources
func (s *SubResources) GetToBeUpdatedResources(in map[ResourceInfo]*unstructured.Unstructured) []*unstructured.Unstructured {
	out := []*unstructured.Unstructured{}

	for key, val := range in {
		if _, ok := s.Set[key]; ok {
			out = append(out, val)
		}
	}

	return out
}

//CommitResources update the SubResources set when 1. the incoming data is marshalled correctly
// 2. the configmap is update correctly
func (s *SubResources) CommitResources(clt client.Client, in map[ResourceInfo]*unstructured.Unstructured) error {
	d := &[]ResourceInfo{}

	for key := range in {
		*d = append(*d, key)
	}

	out, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("failed to marshal when CommitResources(), err %w\n", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.Key.Name,
			Namespace: s.Key.Namespace,
		},
		BinaryData: map[string][]byte{SUB_RESOURCES_SET: out},
	}

	if err := clt.Update(context.TODO(), cm); err != nil {
		return fmt.Errorf("failed to Update configmap when CommitResources(), err %w", err)
	}

	s.data = *d

	s.toSet()

	return nil
}

func calResourceInfo(in *unstructured.Unstructured) ResourceInfo {
	return ResourceInfo{
		GVK:            in.GetObjectKind().GroupVersionKind(),
		NamespacedName: types.NamespacedName{Namespace: in.GetNamespace(), Name: in.GetName()}}
}

func CalResourceSet(in []*unstructured.Unstructured) map[ResourceInfo]*unstructured.Unstructured {
	out := map[ResourceInfo]*unstructured.Unstructured{}

	for _, item := range in {
		p := calResourceInfo(item)

		out[p] = item
	}

	return out
}

func CreateAppsubConfigMap(clt client.Client, appsubKey types.NamespacedName) error {
	cm := &corev1.ConfigMap{}

	ctx := context.TODO()

	err := clt.Get(ctx, appsubKey, cm)
	if err == nil {
		return nil
	}

	if k8serr.IsNotFound(err) {
		cm.SetName(appsubKey.Name)
		cm.SetNamespace(appsubKey.Namespace)

		return clt.Create(ctx, cm)
	}

	return err
}

// DeleteAppsubConfigMap
func DeleteAppsubConfigMap(clt client.Client, appsubKey types.NamespacedName) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appsubKey.Name,
			Namespace: appsubKey.Namespace,
		},
	}

	return clt.Delete(context.TODO(), cm)
}
