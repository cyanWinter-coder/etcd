// Copyright 2026 The etcd Authors
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

package etcdutl

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
)

func TestWALRepairCommandPrintsReportAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	oldSegmentSize := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 4096
	t.Cleanup(func() { wal.SegmentSizeBytes = oldSegmentSize })

	w, err := wal.Create(lg, dir, nil)
	require.NoError(t, err)
	require.NoError(t, w.Save(&raftpb.HardState{}, []*raftpb.Entry{{Index: new(uint64(1))}}))
	require.NoError(t, w.Close())

	oldFormat := OutputFormat
	OutputFormat = "simple"
	t.Cleanup(func() { OutputFormat = oldFormat })

	cmd := NewWALRepairCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{dir})
	require.NoError(t, cmd.Execute())
	first := out.String()
	require.Contains(t, first, "WAL repair report")
	require.Contains(t, first, "summary:")

	cmd = NewWALRepairCommand()
	out.Reset()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{dir})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "scanned=1 damaged=0 repaired=0 failed=0")
}

func TestWALRepairCommandJSONReport(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	oldSegmentSize := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 4096
	t.Cleanup(func() { wal.SegmentSizeBytes = oldSegmentSize })

	w, err := wal.Create(lg, dir, nil)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	_, err = wal.RepairDirectory(lg, dir)
	require.NoError(t, err)

	oldFormat := OutputFormat
	OutputFormat = "json"
	t.Cleanup(func() { OutputFormat = oldFormat })

	cmd := NewWALRepairCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--dry-run", dir})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), `"dir": "`+dir+`"`)
	require.Contains(t, out.String(), `"dryRun": true`)
	require.Contains(t, out.String(), `"scannedSegments": 1`)
}
