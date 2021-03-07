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

package exec

import (
	"github.com/open-cluster-management/multicloud-operators-subscription/pkg/config"
	pflag "github.com/spf13/pflag"
)

// SubscriptionCMDoptions for command line flag parsing
var options = config.SubscriptionCMDoptions{
	MetricsAddr:          "",
	SyncInterval:         60,
	LeaseDurationSeconds: 60,
	Standalone:           false,
}

// ProcessFlags parses command line parameters into options
func ProcessFlags() {
	flag := pflag.CommandLine
	// add flags
	flag.StringVar(
		&options.MetricsAddr,
		"metrics-addr",
		options.MetricsAddr,
		"The address the metric endpoint binds to.",
	)

	flag.StringVar(
		&options.HubConfigFilePathName,
		"hub-cluster-configfile",
		options.HubConfigFilePathName,
		"Configuration file pathname to hub kubernetes cluster",
	)

	flag.StringVar(
		&options.ClusterName,
		"cluster-name",
		options.ClusterName,
		"Name of this endpoint.",
	)

	flag.StringVar(
		&options.ClusterNamespace,
		"cluster-namespace",
		options.ClusterNamespace,
		"Cluster Namespace of this endpoint in hub.",
	)

	flag.IntVar(
		&options.SyncInterval,
		"sync-interval",
		options.SyncInterval,
		"The interval of housekeeping in seconds.",
	)

	flag.IntVar(
		&options.LeaseDurationSeconds,
		"lease-duration",
		options.LeaseDurationSeconds,
		"The lease duration in seconds.",
	)

	flag.BoolVar(
		&options.Standalone,
		"standalone",
		options.Standalone,
		"Standalone mode.",
	)

	flag.StringVar(
		&options.TLSKeyFilePathName,
		"tls-key-file",
		options.TLSKeyFilePathName,
		"WebHook event listener TLS key file path.",
	)

	flag.StringVar(
		&options.TLSCrtFilePathName,
		"tls-crt-file",
		options.TLSCrtFilePathName,
		"WebHook event listener TLS cert file path.",
	)

	flag.BoolVar(
		&options.DisableTLS,
		"disable-tls",
		options.DisableTLS,
		"Disable TLS on WebHook event listener.",
	)
}
