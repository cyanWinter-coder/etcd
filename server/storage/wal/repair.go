// Copyright 2015 The etcd Authors
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

package wal

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

// Repair validates all WAL segments and truncates every damaged segment to its
// last complete record. Original bytes are retained in "<segment>.broken".
//
// The boolean result preserves the historical startup API. Callers that need
// offsets, per-segment status, or rollback details should use RepairDirectory
// and inspect its RepairReport.
func Repair(lg *zap.Logger, dirpath string) bool {
	if lg == nil {
		lg = zap.NewNop()
	}

	report, err := RepairDirectory(lg, dirpath)
	if report != nil {
		lg.Info("WAL repair summary\n" + report.String())
	}
	if err != nil {
		lg.Warn("failed to repair WAL directory", zap.String("path", dirpath), zap.Error(err))
		return false
	}
	return report != nil && report.FailedSegments == 0
}

// openLast opens the last wal file for read and write.
func openLast(lg *zap.Logger, dirpath string) (*fileutil.LockedFile, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, err
	}
	last := filepath.Join(dirpath, names[len(names)-1])
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
