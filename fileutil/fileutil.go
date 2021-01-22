// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fileutil provides utility methods used when dealing with the filesystem in tsdb.
// It is largely copied from github.com/coreos/etcd/pkg/fileutil to avoid the
// dependency chain it brings with it.
// Please check github.com/coreos/etcd for licensing information.
package fileutil

import (
	"os"
	"path/filepath"
)

// Rename safely renames a file.
func Rename(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = pdir.Sync(); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
}
