# Git repository channel subscription

You can subscribe to public or enterprise Git repositories that contain Kubernetes resource YAML files, Helm charts, or both. This document gives examples of connecting to a Git repository through a channel and subscribing to Kubernetes resources and Helm charts from the Git repository.

## Supported Git servers

- GitHub
- GitLab
- BitBucket
- Gogs (Gogs webhook is not supported )

## Prerequisite

Ensure that you have a Kubernetes cluster and this subscription operator running. Ensure that you have a Kubernetes cluster that includes a running instance of this subscription operator.

## Subscribing to a Helm chart from a public Git repository

Use the following example to create a channel that connects to a public IBM Git repository and subscribes to a MongoDB Helm chart:

1. Clone this `multicloud-operators-subscription` GitHub repository.

1. In the root for your cloned repository, run the following command to create a namespace:

   ```shell
   kubectl apply -f ./examples/git-channel/00-namespace.yaml
   ```

   This command creates an `ibmcharts` namespace.

1. Run the following command to create an `ibm-charts-git` channel within the `ibmcharts` namespace:

   ```shell
   kubectl apply -f ./examples/git-channel/01-channel.yaml
   ```

   The following YAML content is used to define this `ibm-charts-git` channel:

   ```yaml
   apiVersion: apps.open-cluster-management.io/v1
   kind: Channel
   metadata:
   name: ibm-charts-git
   namespace: ibmcharts
   spec:
       type: Git
       pathname: https://github.com/IBM/charts.git
   ```

   The value for the `pathname` field is the Git repository HTTPS URL.

1. Run the following command to subscribe to the `ibm-charts-git` channel:

   ```shell
   kubectl apply -f ./examples/git-channel/02-subscription.yaml
   ```

   When you review the `./examples/git-channel/02-subscription.yaml` file, the subscription has the following annotations:

   ```yaml
    annotations:
      apps.open-cluster-management.io/git-path: stable/ibm-mongodb-dev
      apps.open-cluster-management.io/git-branch: branch1
   ```

   The annotation `apps.open-cluster-management.io/git-path` indicates that the subscription subscribes to all Helm charts and Kubernetes resources that are in the `stable/ibm-mongodb-dev` directory for the Git repository channel. The subscription subscribes to `master` branch by default. If you want to subscribe to a different branch, you can use annotation `apps.open-cluster-management.io/git-branch`.

1. Run the following command to place the subscribed resources onto the local cluster:

   ```shell
   kubectl patch subscriptions.apps.open-cluster-management.io git-mongodb-subscription --type='json' -p='[{"op": "replace", "path": "/spec/placement/local", "value": true}]'
   ```

   After a couple of minutes, run the following command to check whether a `helmreleases.apps.open-cluster-management.io` CR is created for the MongoDB Helm chart:

   ```shell
   kubectl get helmreleases.apps.open-cluster-management.io --all-namespaces
   ```

   Then, run the following command in the same namespace as the MongoDB `helmreleases.apps.open-cluster-management.io` CR to find the deployment:

   ```shell
   kubectl get deployments
   ```

## Subscribing to Kubernetes resources from a Git repository

In the following example, you create a channel that connects to a Git repository and subscribes to a sample nginx deployment `examples/git-channel/sample-deployment.yaml` YAML file:

1. Clone this `multicloud-operators-subscription` Git repository.
1. Run the following command to create a `kuberesources` namespace:

   ```shell
   kubectl apply -f ./examples/git-channel/10-namespace.yaml
   ```

1. Run the following command to create a `sample-kube-resources-git` channel in the `kuberesources` namespace:

   ```shell
   kubectl apply -f ./examples/git-channel/11-channel.yaml
   ```

   The following YAML content is used to define this `sample-kube-resources-git` channel:

   ```yaml
   apiVersion: apps.open-cluster-management.io/v1
   kind: Channel
   metadata:
     name: sample-kube-resources-git
     namespace: kuberesources
   spec:
       type: Git
       pathname: https://github.com/open-cluster-management/multicloud-operators-subscription.git
   ```

   The value for the `pathname` field is the Git repository HTTPS URL.

1. Run the following command to subscribe to the `sample-kube-resources-git` channel:

   ```shell
   kubectl apply -f ./examples/git-channel/12-subscription.yaml
   ```

   When you review the `./examples/git-channel/12-subscription.yaml` file, the subscription has the following annotations:

   ```yaml
    annotations:
      apps.open-cluster-management.io/git-path: examples/git-channel
      apps.open-cluster-management.io/git-branch: branch1
   ```

   The annotation `apps.open-cluster-management.io/git-path` indicates that the subscription subscribes to all Helm charts and Kubernetes resources that are in the `examples/git-channel` directory of the Git repository channel.

   In `examples/git-channel`, there are multiple YAML files, however, only the `sample-deployment.yaml` file is applied. The `.kubernetesignore` file that is within the directory that is defined by the `data.path` field indicates that all other files are to be ignored. The subscription then applies only the `sample-deployment.yaml` file to the cluster.

   The subscription subscribes to `master` branch by default. If you want to subscribe to a different branch, you can use annotation `apps.open-cluster-management.io/git-branch`.

1. Run the following command to place the subscribed resources onto the local cluster:

   ```shell
   kubectl patch subscriptions.apps.open-cluster-management.io sample-kube-resources-subscription --type='json' -p='[{"op": "replace", "path": "/spec/placement/local", "value": true}]'
   ```

   After a couple of minutes, run the following command to check whether a `sample-nginx-deployment` deployment is created:

   ```shell
   kubectl get deployment --all-namespaces
   ```

## Subscribing to a Helm chart from an enterprise Git repository that requires authentication

In the previous examples, the Git repository that the channel connects to is a public repository and did not require authentication. If a Git repository does require authentication to connect to the repository, you need to associate the channel with a Kubernetes secret.

The `channel` and `subscription` resources support only basic authentication.

Update your channel resource to reference a Kubernetes secret and define the YAML content to create the secret. Within your YAML content, set the `user` field to be a Git user ID and the `accessToken` field to be a Git personal access token.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-git-secret
  namespace: ibmcharts
data:
  user: dXNlcgo=
  accessToken: cGFzc3dvcmQK
---
apiVersion: apps.open-cluster-management.io/v1
kind: Channel
metadata:
  name: ibm-charts-git
  namespace: ibmcharts
spec:
    type: Git
    pathname: https://github.com/IBM/charts.git
    secretRef:
      name: my-git-secret
```

## Subscribing to a self-hosted Git server with custom or self-signed TLS certificate

If a Git server has a custom or self-signed TLS certificate, you can use `insecureSkipVerify: true` in the channel spec. Otherwise, the connection to the Git server will fail with an error similar to the following.

```
x509: certificate is valid for localhost.com, not localhost
```

```
apiVersion: apps.open-cluster-management.io/v1
ind: Channel
metadata:
labels:
  name: sample-channel
  namespace: sample
spec:
  type: GitHub
  pathname: <Git URL>
  insecureSkipVerify: true
```

## Include a .kubernetesignore file

You can include a `.kubernetesignore` file within your Git repository root directory, or within the `data.path` directory that is specified in the ConfigMap that is defined for your subscription `spec.packageFilter.filterRef` field.

You can use this `.kubernetesignore` file to specify patterns of files or subdirectories, or both, to ignore when the subscription processes and applies Kubernetes resource from the repository.

You can also use the `.kubernetesignore` file for fine-grain filtering to selectively apply Kubernetes resources. The pattern format of the `.kubernetesignore` file is the same as a `.gitignore` file.

If the `data.path` field is not defined in the ConfigMap that is set for the subscription `spec.packageFilter.filterRef` field, the subscription looks for a `.kubernetesignore` file in the repository root directory. If the `data.path` field is defined, the subscription looks for the `.kubernetesignore` file in the `data.path` directory. Subscriptions do not, searching any other directory for a `.kubernetesignore` file.

## Kustomize

If there is `kustomization.yaml` or `kustomization.yml` file in a subscribed Git folder, kustomize will be applied.

You can use `spec.packageOverrides` to override `kustomization` at the subscription deployment time. For example,

```yaml
apiVersion: apps.open-cluster-management.io/v1
kind: Subscription
metadata:
  name: example-subscription
  namespace: default
spec:
  channel: some/channel
  packageOverrides:
    packageName: kustomization
    packageOverrides:
      value: |
the kustomization overrider YAML as string
```

`packageName: kustomization` is required. The override either adds new entries or updates existing entries. It does not remove existing entries.

## Subscribing to a specific branch

The subscription operator that is include in this `multicloud-operators-subscription` repository subscribes to the `master` branch of a Git repository by default. If you want to subscribe to a different branch, you need to specify the branch name annotation in the subscription.

The following example Subscription YAML shows how to specify a different branch:

```yaml
apiVersion: apps.open-cluster-management.io/v1
kind: Subscription
metadata:
  name: git-mongodb-subscription
  annotations:
    apps.open-cluster-management.io/git-path: stable/ibm-mongodb-dev
    apps.open-cluster-management.io/git-branch: branch1
```

## Enabling Git WebHook

By default, a Git channel subscription clones the Git repository specified in the channel every minute and applies changes when the commit ID has changed. Alternatively, you can configure your subscription to apply changes only when the Git repository sends repo PUSH and PULL webhook event notifications.

In order to configure webhook in a Git repository, you need a target webhook payload URL and optionally a secret.

### Payload URL

Create a route (ingress) to expose the subscription operator's webhook event listener service where `<operator namespace>` is the namespace where the subscription operator runs in.

  ```shell
  oc create route passthrough --service=multicloud-operators-subscription -n <operator namespace>
  ```

Then, use `oc get route multicloud-operators-subscription -n <operator namespace>` command to find the externally-reachable hostname. The webhook payload URL is `https://<externally-reachable hostname>/webhook`.

### WebHook secret

WebHook secret is optional. Create a Kubernetes secret in the channel namespace. The secret must contain `data.secret`. For example,

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-git-webhook-secret
data:
  secret: cm9rZWpAY2EuaWJtLmNvbQo=
```

The value of `data.secret` is the base-64 encoded WebHook secret you are going to use.

* Using a unique secret per Git repository is recommended.

### Configuring WebHook in Git repository

Use the payload URL and webhook secret to configure WebHook in your Git repository.

### Enable WebHook event notification in channel

Annotate the subscription's channel with the following command:

```shell
oc annotate channel.apps.open-cluster-management.io <channel name> apps.open-cluster-management.io/webhook-enabled="true"
```

If you used a secret to configure WebHook, annotate the channel with this as well where `<the_secret_name>` is the kubernetes secret name containing webhook secret.

```shell
oc annotate channel.apps.open-cluster-management.io <channel name> apps.open-cluster-management.io/webhook-secret="<the_secret_name>"
```

### Subscriptions of webhook-enabled channel

No webhook specific configuration is needed in subscriptions.

## Limitations

* You cannot subscribe to a specific commit of a branch.
