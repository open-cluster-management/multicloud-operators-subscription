package config

import (
	"k8s.io/apimachinery/pkg/types"
)

// SubscriptionCMDOptions for command line flag parsing
type SubscriptionCMDoptions struct {
	SyncInterval          int
	LeaseDurationSeconds  int
	DisableTLS            bool
	Standalone            bool
	CreateService         bool
	Beta                  bool
	MetricsAddr           string
	ClusterName           string
	ClusterNamespace      string
	HubConfigFilePathName string
	TLSKeyFilePathName    string
	TLSCrtFilePathName    string
	Syncid                *types.NamespacedName
}
