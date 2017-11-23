// Copyright 2016 The kube-etcd-etcd-operator Authors
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

package swift

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"path"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
	"github.com/gophercloud/gophercloud/pagination"
)

// Swift is a helper layer to wrap complex Swift logic.
type Swift struct {
	container string
	prefix    string
	client    *gophercloud.ServiceClient
}

// New returns a Swift object for given container fetching the credentails from environment variables
// Please refer https://github.com/gophercloud/gophercloud/blob/db5f840b1d1a595280d643defc09ce277996959e/openstack/auth_env.go#L27
// for environment variables.
func New(container, region, prefix string) (*Swift, error) {
	opts, err := openstack.AuthOptionsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("create options from environment failed: %v", err)
	}
	return NewFromAuthOpt(container, region, prefix, opts)
}

// NewFromAuthOpt returns a new Swift object for a given container using credentials passed in authOptions <ao>.
func NewFromAuthOpt(container, region, prefix string, ao gophercloud.AuthOptions) (*Swift, error) {
	ao.AllowReauth = true
	provider, err := openstack.NewClient(ao.IdentityEndpoint)
	if err != nil {
		return nil, fmt.Errorf("new openstack client creation failed: %v", err)
	}

	provider.HTTPClient = http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	err = openstack.Authenticate(provider, ao)
	if err != nil {
		return nil, fmt.Errorf("new Openstack client authentication failed : %v", err)
	}

	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("create object storage client failed: %v", err)
	}
	return newFromClient(container, prefix, client), nil
}

// newFromClient returns a new Swift object for a given container using provided serviceClient <cli>.
func newFromClient(container, prefix string, cli *gophercloud.ServiceClient) *Swift {
	return &Swift{
		container: container,
		prefix:    prefix,
		client:    cli,
	}
}

// Put stores the object content from <rs> at <key> in swift container.
func (s *Swift) Put(key string, rs io.Reader) error {
	opts := objects.CreateOpts{
		Content: rs,
	}
	res := objects.Create(s.client, s.container, path.Join(s.prefix, key), opts)
	return res.Err
}

// Get reads the content of object identified by <key> in swift container.
func (s *Swift) Get(key string) (io.ReadCloser, error) {
	resp := objects.Download(s.client, s.container, path.Join(s.prefix, key), nil)
	return resp.Body, resp.Err
}

// Delete deletes the object at <key> in swift container.
func (s *Swift) Delete(key string) error {
	result := objects.Delete(s.client, s.container, path.Join(s.prefix, key), nil)
	return result.Err
}

//List fetches the list of object keys in swift container.
func (s *Swift) List() ([]string, error) {
	_, l, err := s.list(s.prefix)
	return l, err
}

// list fetches the list of object keys with specified <prefix> in swift container.
// It also returns the total size of listed objects.
func (s *Swift) list(prefix string) (int64, []string, error) {
	keys := []string{}
	var size int64
	opts := &objects.ListOpts{
		Full:   true,
		Prefix: prefix,
	}
	// Retrieve a pager (i.e. a paginated collection)
	pager := objects.List(s.client, s.container, opts)
	// Define an anonymous function to be executed on each page's iteration
	err := pager.EachPage(func(page pagination.Page) (bool, error) {

		objectList, err := objects.ExtractInfo(page)
		for _, object := range objectList {
			k := (object.Name)[len(fmt.Sprintf("%s/", prefix)):]
			size += object.Bytes
			keys = append(keys, k)
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return -1, nil, err
	}
	return size, keys, nil
}

// TotalSize returns the sum of size of objects in swift container.
func (s *Swift) TotalSize() (int64, error) {
	size, _, err := s.list(s.prefix)
	return size, err
}

// CopyPrefix copies the objects from specified <prefix> to prefix set in swift object.
func (s *Swift) CopyPrefix(from string) error {
	_, keys, err := s.list(from)
	if err != nil {
		return err
	}

	for _, key := range keys {
		opts := &objects.CopyOpts{Destination: path.Join(s.prefix, key)}
		result := objects.Copy(s.client, s.container, path.Join(s.prefix, key), opts)
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}
