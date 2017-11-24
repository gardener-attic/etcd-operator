// Copyright 2016 The etcd-operator Authors
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

package backend

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/coreos/etcd-operator/pkg/backup/swift"
	"github.com/coreos/etcd-operator/pkg/backup/util"

	"github.com/sirupsen/logrus"
)

// ensure swiftBackend satisfies backend interface.
var _ Backend = &swiftBackend{}

// swiftBackend is Openstack Swift backend.
type swiftBackend struct {
	swift *swift.Swift
}

//NewSwiftBackend returns the newly created Backend object for swift.
func NewSwiftBackend(swift *swift.Swift) Backend {
	return &swiftBackend{swift}
}

// Save saves the backup on swift container from the given reader with given etcd version and revision.
// It returns the size of the snapshot saved.
func (sb *swiftBackend) Save(version string, snapRev int64, rc io.Reader) (int64, error) {
	// swift put is atomic, so let's go ahead and put the key directly.
	key := util.MakeBackupName(version, snapRev)
	err := sb.swift.Put(key, rc)
	if err != nil {
		return -1, err
	}
	n, err := sb.getObjectSize(key)
	if err != nil {
		return -1, err
	}
	logrus.Infof("saved backup %s (size: %d) successfully", key, n)
	return n, nil
}

// getObjectSize returns the size of the backup with specfied <key>.
func (sb *swiftBackend) getObjectSize(key string) (int64, error) {
	rc, err := sb.Open(key)
	if err != nil {
		return -1, err
	}

	b, err := ioutil.ReadAll(rc)
	if err != nil {
		rc.Close()
		return -1, err
	}

	return int64(len(b)), rc.Close()
}

// GetLatest gets latest backup's name.
// If no backup is available, returns empty string name.
func (sb *swiftBackend) GetLatest() (string, error) {
	keys, err := sb.swift.List()
	if err != nil {
		return "", fmt.Errorf("failed to list swift container: %v", err)
	}

	return util.GetLatestBackupName(keys), nil
}

// Open opens a backup file for reading
func (sb *swiftBackend) Open(name string) (io.ReadCloser, error) {
	return sb.swift.Get(name)
}

// Purge purges backup files when backups are greater than maxBackupFiles.
func (sb *swiftBackend) Purge(maxBackupFiles int) error {
	names, err := sb.swift.List()
	if err != nil {
		return err
	}
	bnames := util.FilterAndSortBackups(names)
	if len(bnames) < maxBackupFiles {
		return nil
	}
	for i := 0; i < len(bnames)-maxBackupFiles; i++ {
		err := sb.swift.Delete(bnames[i])
		if err != nil {
			logrus.Errorf("fail to delete swift file (%s): %v", bnames[i], err)
		}
	}
	return nil
}

// Total returns the total number of available backups.
func (sb *swiftBackend) Total() (int, error) {
	names, err := sb.swift.List()
	if err != nil {
		return -1, err
	}
	return len(util.FilterAndSortBackups(names)), nil
}

// TotalSize returns the total size of the backups.
func (sb *swiftBackend) TotalSize() (int64, error) {
	return sb.swift.TotalSize()
}
