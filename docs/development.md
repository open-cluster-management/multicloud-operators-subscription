# Development guide

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Development guide](#development-guide)
    - [Required tools and binaries](#required-tools-and-binaries)
    - [Launch dev mode](#launch-dev-mode)
    - [Build a local image](#build-a-local-image)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Required tools and binaries

The Multicloud-operators projects are built with following tools. Use the links provided to install them on your environment:

 - Find an environment, or set up Kubernetes version 1.13 and later: [kubernetes.io](https://kubernetes.io/docs/setup/#learning-environment).

 - Build with [Go verson 1.13 and later](https://golang.org/dl/).

 - Enforce style and formatting with Lint:

    - [hadolint](https://github.com/hadolint/hadolint#install)
    - [shellcheck](https://github.com/koalaman/shellcheck#installing)
    - [yamllint](https://github.com/adrienverge/yamllint#installation)
    - [helm client](https://helm.sh/docs/using_helm/#install-helm)
    - [golangci-lint](https://github.com/golangci/golangci-lint#install)
    - [autopep8](https://github.com/hhatto/autopep8#installation)
    - [mdl](https://github.com/markdownlint/markdownlint#installation)
    - [awesome_bot](https://github.com/dkhamsing/awesome_bot#installation)
    - [sass-lint](https://github.com/sds/scss-lint#installation)
    - [tslint](https://github.com/palantir/tslint#installation--usage)
    - [prototool](https://github.com/uber/prototool/blob/dev/docs/install.md)
    - goimports -  Run `go get -v golang.org/x/tools/cmd/goimports` to install

- Test with [kubebuilder v1.0.8](https://github.com/kubernetes-sigs/kubebuilder/releases/tag/v1.0.8)

## Launch dev mode

Run the following command to launch developer mode:

```shell
git clone git@github.com:open-cluster-management/multicloud-operators-subscription.git
cd multicloud-operators-subscription
export GITHUB_USER=<github_user>
export GITHUB_TOKEN=<github_token>
make
make build
./build/_output/bin/multicluster-operators-subscription
```

## Build a local image

Build a local image by running the following command:

```shell
git clone git@github.com:open-cluster-management/multicloud-operators-subscription.git
cd multicloud-operators-subscription
export GITHUB_USER=<github_user>
export GITHUB_TOKEN=<github_token>
make
make build-images
```