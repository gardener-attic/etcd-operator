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
	"os"

	"github.com/coreos/etcd-operator/pkg/backup/swift"
	"github.com/coreos/etcd-operator/pkg/backup/util"

	"github.com/sirupsen/logrus"
)

/*const (
	tmpDir              = "/tmp"
	tmpBackupFilePrefix = "etcd-backup-"
)*/

// ensure swiftBackend satisfies backend interface.
var _ Backend = &swiftBackend{}

// swiftBackend is AWS Swift backend.
type swiftBackend struct {
	swift *swift.Swift
}

func NewSwiftBackend(swift *swift.Swift) Backend {
	return &swiftBackend{swift}
}

func (sb *swiftBackend) Save(version string, snapRev int64, rc io.Reader) (int64, error) {
	// make a local file copy of the backup first, since swift requires io.ReadSeeker.
	tmpfile, err := ioutil.TempFile(tmpDir, tmpBackupFilePrefix)
	if err != nil {
		return -1, fmt.Errorf("failed to create snapshot tempfile: %v", err)
	}
	defer func() {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
	}()

	n, err := io.Copy(tmpfile, rc)
	if err != nil {
		return -1, fmt.Errorf("failed to save snapshot to tmpfile: %v", err)
	}
	_, err = tmpfile.Seek(0, os.SEEK_SET)
	if err != nil {
		return -1, err
	}
	// swift put is atomic, so let's go ahead and put the key directly.
	key := util.MakeBackupName(version, snapRev)
	err = sb.swift.Put(key, tmpfile)
	if err != nil {
		return -1, err
	}
	logrus.Infof("saved backup %s (size: %d) successfully", key, n)
	return n, nil
}

func (sb *swiftBackend) GetLatest() (string, error) {
	keys, err := sb.swift.List()
	if err != nil {
		return "", fmt.Errorf("failed to list swift container: %v", err)
	}

	return util.GetLatestBackupName(keys), nil
}

func (sb *swiftBackend) Open(name string) (io.ReadCloser, error) {
	return sb.swift.Get(name)
}

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

func (sb *swiftBackend) Total() (int, error) {
	names, err := sb.swift.List()
	if err != nil {
		return -1, err
	}
	return len(util.FilterAndSortBackups(names)), nil
}

func (sb *swiftBackend) TotalSize() (int64, error) {
	return sb.swift.TotalSize()
}
