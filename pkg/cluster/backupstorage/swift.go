// Copyright 2017 The etcd-operator Authors
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

package backupstorage

import (
	"path"

	"github.com/rackspace/gophercloud"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	backupswift "github.com/coreos/etcd-operator/pkg/backup/swift"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type swift struct {
	clusterName  string
	namespace    string
	backupPolicy api.BackupPolicy
	kubecli      kubernetes.Interface
	swiftCli     *backupswift.Swift
}

// NewSwiftStorage returns a new Swift Storage implementation using the given kubecli, cluster name, namespace and backup policy
func NewSwiftStorage(kubecli kubernetes.Interface, clusterName, ns string, p api.BackupPolicy) (Storage, error) {
	prefix := path.Join(ns, clusterName)

	swiftCli, err := func() (*backupswift.Swift, error) {
		authOptions, err := setupSwiftCreds(kubecli, ns, p.Swift.SwiftSecret)
		if err != nil {
			return nil, err
		}
		return backupswift.NewFromAuthOpt(p.Swift.SwiftContainer, p.Swift.SwiftRegion, prefix, authOptions)
	}()
	if err != nil {
		return nil, err
	}

	s := &swift{
		kubecli:      kubecli,
		clusterName:  clusterName,
		backupPolicy: p,
		namespace:    ns,
		swiftCli:     swiftCli,
	}
	return s, nil
}

func setupSwiftCreds(kubecli kubernetes.Interface, ns, secret string) (gophercloud.AuthOptions, error) {
	se, err := kubecli.CoreV1().Secrets(ns).Get(secret, metav1.GetOptions{})
	if err != nil {
		return gophercloud.AuthOptions{}, err
	}
	ao := gophercloud.AuthOptions{
		IdentityEndpoint: string(se.Data[api.SwiftIdentityEndpoint]),
		TenantID:         string(se.Data[api.SwiftTenantID]),
		Username:         string(se.Data[api.SwiftUsername]),
		Password:         string(se.Data[api.SwiftPassword]),
		TokenID:          string(se.Data[api.SwiftTokenID]),
	}
	return ao, err
}

func (a *swift) Create() error {
	// TODO: check if container exists?
	return nil
}

func (a *swift) Clone(from string) error {
	prefix := a.namespace + "/" + from
	return a.swiftCli.CopyPrefix(prefix)
}

func (a *swift) Delete() error {
	if a.backupPolicy.AutoDelete {
		names, err := a.swiftCli.List()
		if err != nil {
			return err
		}
		for _, n := range names {
			err = a.swiftCli.Delete(n)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
