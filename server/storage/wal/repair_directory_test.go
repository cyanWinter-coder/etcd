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

package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

func testUint64p(v uint64) *uint64 { return &v }

func TestRepairDirectoryAllSegmentsIdempotentAndAppend(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()

	oldSegmentSize := SegmentSizeBytes
	SegmentSizeBytes = 64
	t.Cleanup(func() { SegmentSizeBytes = oldSegmentSize })

	w, err := Create(lg, dir, nil)
	require.NoError(t, err)
	for _, es := range makeEnts(40) {
		require.NoError(t, w.Save(&raftpb.HardState{}, es))
	}
	require.NoError(t, w.Close())

	names, err := readWALNames(lg, dir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(names), 3)

	// Normalize any preallocated zero tails before appending deterministic
	// corrupt frames. With very small test segments, rotation may already have
	// truncated every segment, so this first repair may be a no-op.
	_, err = RepairDirectory(lg, dir)
	require.NoError(t, err)

	clean, err := RepairDirectory(lg, dir)
	require.NoError(t, err)
	require.Zero(t, clean.DamagedSegments)
	require.Zero(t, clean.RepairedSegments)
	require.Zero(t, clean.FailedSegments)

	targets := map[string]string{
		names[0]:            "length",
		names[len(names)/2]: "zero",
		names[len(names)-1]: "length",
	}
	expectedOffsets := make(map[string]int64, len(targets))
	corruptedBytes := make(map[string][]byte, len(targets))

	for name, kind := range targets {
		sr := findSegmentReport(t, clean, name)
		require.Equal(t, SegmentClean, sr.Status)
		require.Equal(t, sr.SizeBefore, sr.LastValidOffset)
		expectedOffsets[name] = sr.LastValidOffset

		path := filepath.Join(dir, name)
		f, openErr := os.OpenFile(path, os.O_WRONLY, 0)
		require.NoError(t, openErr)
		_, seekErr := f.Seek(sr.LastValidOffset, 0)
		require.NoError(t, seekErr)

		switch kind {
		case "zero":
			_, err = f.Write(make([]byte, 32))
		default:
			var header [8]byte
			binary.LittleEndian.PutUint64(header[:], math.MaxUint64)
			_, err = f.Write(header[:])
		}
		require.NoError(t, err)
		require.NoError(t, f.Close())

		b, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		corruptedBytes[name] = b
	}

	verified, err := VerifyDirectory(lg, dir)
	require.NoError(t, err)
	require.Equal(t, len(targets), verified.DamagedSegments)
	for name, kind := range targets {
		sr := findSegmentReport(t, verified, name)
		require.Equal(t, SegmentDamaged, sr.Status)
		require.Equal(t, expectedOffsets[name], sr.DamageOffset, "offset for %s", name)
		require.Equal(t, expectedOffsets[name], sr.SizeAfter)
		require.Equal(t, int64(len(corruptedBytes[name]))-expectedOffsets[name], sr.TruncatedBytes)
		if kind == "zero" {
			require.Equal(t, "zero_record_or_padding", sr.Issue)
		} else {
			require.Equal(t, "invalid_record_length", sr.Issue)
		}
		require.Contains(t, verified.String(), name)
	}

	repaired, err := RepairDirectory(lg, dir)
	require.NoError(t, err)
	require.Equal(t, len(targets), repaired.RepairedSegments)
	require.Zero(t, repaired.FailedSegments)
	require.Equal(t, uint64(40), repaired.LastEntryIndex)
	require.Equal(t, uint64(41), repaired.NextEntryIndex)

	for name := range targets {
		offset := expectedOffsets[name]
		sr := findSegmentReport(t, repaired, name)
		require.Equal(t, SegmentRepaired, sr.Status)
		require.Equal(t, offset, sr.LastValidOffset)
		require.Equal(t, offset, sr.SizeAfter)
		require.Equal(t, int64(len(corruptedBytes[name]))-offset, sr.TruncatedBytes)

		path := filepath.Join(dir, name)
		live, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		require.Len(t, live, int(offset))
		require.Equal(t, corruptedBytes[name][:offset], live)

		backupPath := path + ".broken"
		backup, readErr := os.ReadFile(backupPath)
		require.NoError(t, readErr)
		require.Equal(t, corruptedBytes[name], backup)
		require.Equal(t, "600", fileModeString(t, backupPath))
	}

	// Processing the repaired directory again must be a no-op and must not
	// change the retained backup.
	again, err := RepairDirectory(lg, dir)
	require.NoError(t, err)
	require.Zero(t, again.DamagedSegments)
	require.Zero(t, again.RepairedSegments)
	require.Zero(t, again.FailedSegments)
	for name := range targets {
		backup, readErr := os.ReadFile(filepath.Join(dir, name+".broken"))
		require.NoError(t, readErr)
		require.Equal(t, corruptedBytes[name], backup)
	}

	// The repaired WAL must open, contain every original entry because the
	// corrupt frames were appended after them, and accept sequential writes.
	w, err = Open(lg, dir, &walpb.Snapshot{})
	require.NoError(t, err)
	_, _, ents, err := w.ReadAll()
	require.NoError(t, err)
	require.Len(t, ents, 40)
	for i := uint64(41); i <= 50; i++ {
		require.NoError(t, w.Save(&raftpb.HardState{}, []*raftpb.Entry{{Index: testUint64p(i)}}))
	}
	require.NoError(t, w.Close())

	w, err = Open(lg, dir, &walpb.Snapshot{})
	require.NoError(t, err)
	_, _, ents, err = w.ReadAll()
	require.NoError(t, err)
	require.Len(t, ents, 50)
	require.Equal(t, uint64(50), ents[len(ents)-1].GetIndex())
	require.NoError(t, w.Close())
}

func TestRepairDirectoryRollbackWhenFsyncFails(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()

	w, err := Create(lg, dir, nil)
	require.NoError(t, err)
	require.NoError(t, w.Save(&raftpb.HardState{}, []*raftpb.Entry{{Index: testUint64p(1)}}))
	require.NoError(t, w.Close())

	names, err := readWALNames(lg, dir)
	require.NoError(t, err)
	require.Len(t, names, 1)
	name := names[0]
	path := filepath.Join(dir, name)

	normalized, err := RepairDirectory(lg, dir)
	require.NoError(t, err)
	sr := findSegmentReport(t, normalized, name)
	offset := sr.LastValidOffset

	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Seek(offset, 0)
	require.NoError(t, err)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], math.MaxUint64)
	_, err = f.Write(header[:])
	require.NoError(t, err)
	require.NoError(t, f.Close())

	corrupted, err := os.ReadFile(path)
	require.NoError(t, err)

	oldFsync := walRepairFsync
	walRepairFsync = func(*os.File) error { return errors.New("injected fsync failure") }
	t.Cleanup(func() { walRepairFsync = oldFsync })

	report, err := RepairDirectory(lg, dir)
	require.Error(t, err)
	require.NotNil(t, report)
	require.Equal(t, 1, report.FailedSegments)
	got := findSegmentReport(t, report, name)
	require.Equal(t, SegmentFailed, got.Status)
	require.True(t, got.RolledBack)
	require.FileExists(t, path+".broken")

	live, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, corrupted, live, "rollback must restore exact pre-repair bytes")
}

func findSegmentReport(t *testing.T, report *RepairReport, name string) *SegmentReport {
	t.Helper()
	require.NotNil(t, report)
	for _, sr := range report.Segments {
		if sr.Name == name {
			return sr
		}
	}
	t.Fatalf("segment %s not found in report", name)
	return nil
}

func fileModeString(t *testing.T, path string) string {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fmt.Sprintf("%o", fi.Mode().Perm())
}
