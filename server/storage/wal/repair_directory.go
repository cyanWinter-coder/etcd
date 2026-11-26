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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// SegmentClean means the segment decoded to its logical end without a
	// framing, checksum, or zero-fill stop before EOF.
	SegmentClean = "clean"
	// SegmentDamaged is returned by verification when repair has not been run.
	SegmentDamaged = "damaged"
	// SegmentWouldRepair is used by dry-run repair.
	SegmentWouldRepair = "would-repair"
	// SegmentRepaired means the segment was truncated to its last valid record.
	SegmentRepaired = "repaired"
	// SegmentFailed means repair was attempted and failed. The segment may have
	// been rolled back from its backup.
	SegmentFailed = "failed"
)

// walRepairFsync is indirected only to let tests force the post-truncate
// durability step to fail and exercise automatic rollback.
var walRepairFsync = fileutil.Fsync

// RepairOptions configures directory-wide WAL verification and repair.
type RepairOptions struct {
	// DryRun reports what would be changed without modifying segments.
	DryRun bool
	// BackupSuffix is appended to a segment name to create its original-copy
	// backup. It defaults to ".broken", matching the historical WAL repair.
	BackupSuffix string
	// verifyOnly keeps dry-run results as SegmentDamaged instead of converting
	// them to SegmentWouldRepair for "repair --dry-run".
	verifyOnly bool
}

// SegmentReport is the verification/repair result for one WAL segment.
type SegmentReport struct {
	Name            string `json:"name"`
	Sequence        uint64 `json:"sequence"`
	StartIndex      uint64 `json:"startIndex"`
	Status          string `json:"status"`
	SizeBefore      int64  `json:"sizeBefore"`
	SizeAfter       int64  `json:"sizeAfter"`
	ValidRecords    int    `json:"validRecords"`
	LastValidOffset int64  `json:"lastValidOffset"`
	LastEntryIndex  uint64 `json:"lastEntryIndex"`
	DamageOffset    int64  `json:"damageOffset"`
	TruncatedBytes  int64  `json:"truncatedBytes"`
	Issue           string `json:"issue,omitempty"`
	Error           string `json:"error,omitempty"`
	BackupPath      string `json:"backupPath,omitempty"`
	RolledBack      bool   `json:"rolledBack,omitempty"`
}

// RepairReport is a readable, machine-readable summary of processing a WAL
// directory.
type RepairReport struct {
	Dir              string           `json:"dir"`
	DryRun           bool             `json:"dryRun"`
	Segments         []*SegmentReport `json:"segments"`
	ScannedSegments  int              `json:"scannedSegments"`
	DamagedSegments  int              `json:"damagedSegments"`
	RepairedSegments int              `json:"repairedSegments"`
	FailedSegments   int              `json:"failedSegments"`
	LastSequence     uint64           `json:"lastSequence"`
	NextSequence     uint64           `json:"nextSequence"`
	LastEntryIndex   uint64           `json:"lastEntryIndex"`
	NextEntryIndex   uint64           `json:"nextEntryIndex"`
	NextSegmentName  string           `json:"nextSegmentName,omitempty"`
	Warnings         []string         `json:"warnings,omitempty"`
}

// IsRepairableError reports whether err is a framing, zero-fill, checksum, or
// malformed-record corruption that RepairDirectory can truncate past.
func IsRepairableError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, ErrZeroRecord) ||
		errors.Is(err, ErrInvalidRecordLength) ||
		errors.Is(err, ErrInvalidRecord) ||
		errors.Is(err, ErrCRCMismatch)
}

// VerifyDirectory decodes every WAL segment independently and reports the
// first invalid record in each segment. It never modifies the directory.
func VerifyDirectory(lg *zap.Logger, dirpath string) (*RepairReport, error) {
	return RepairDirectoryWithOptions(lg, dirpath, RepairOptions{DryRun: true, verifyOnly: true})
}

// RepairDirectory validates every WAL segment and truncates a damaged segment
// to the offset immediately after its last complete, checksum-valid record.
// Each changed segment is first copied to a ".broken" backup.
func RepairDirectory(lg *zap.Logger, dirpath string) (*RepairReport, error) {
	return RepairDirectoryWithOptions(lg, dirpath, RepairOptions{})
}

// RepairDirectoryWithOptions is RepairDirectory with caller options.
func RepairDirectoryWithOptions(lg *zap.Logger, dirpath string, opts RepairOptions) (*RepairReport, error) {
	if lg == nil {
		lg = zap.NewNop()
	}
	if opts.BackupSuffix == "" {
		opts.BackupSuffix = ".broken"
	}

	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return &RepairReport{Dir: dirpath, DryRun: opts.DryRun}, err
	}

	report := &RepairReport{
		Dir:    dirpath,
		DryRun: opts.DryRun,
	}

	var processErrs []error
	var expectedSeq uint64
	haveExpectedSeq := false

	for _, name := range names {
		seq, startIndex, parseErr := parseWALName(name)
		if parseErr != nil {
			// readWALNames only returns parseable WAL names, so this should be
			// unreachable. Keep it rather than panicking in an operator tool.
			sr := &SegmentReport{Name: name, Status: SegmentFailed, Error: parseErr.Error()}
			report.Segments = append(report.Segments, sr)
			report.FailedSegments++
			processErrs = append(processErrs, parseErr)
			continue
		}

		if haveExpectedSeq && seq != expectedSeq {
			warning := fmt.Sprintf("WAL sequence gap before %s: expected sequence %016x", name, expectedSeq)
			report.Warnings = append(report.Warnings, warning)
			lg.Warn(warning)
		}
		expectedSeq = seq + 1
		haveExpectedSeq = true

		var sr *SegmentReport
		if opts.DryRun {
			sr, err = inspectSegmentReadOnly(dirpath, name, seq, startIndex, opts.BackupSuffix)
			if sr != nil && !opts.verifyOnly && sr.Status == SegmentDamaged {
				sr.Status = SegmentWouldRepair
			}
		} else {
			sr, err = repairSegment(lg, dirpath, name, seq, startIndex, opts.BackupSuffix)
		}
		if err != nil {
			processErrs = append(processErrs, err)
		}
		report.Segments = append(report.Segments, sr)
	}

	report.aggregate()
	return report, errors.Join(processErrs...)
}

func (r *RepairReport) aggregate() {
	r.ScannedSegments = len(r.Segments)
	r.DamagedSegments = 0
	r.RepairedSegments = 0
	r.FailedSegments = 0
	r.LastSequence = 0
	r.LastEntryIndex = 0

	for _, sr := range r.Segments {
		if sr == nil {
			continue
		}
		r.LastSequence = sr.Sequence
		if sr.LastEntryIndex > r.LastEntryIndex {
			r.LastEntryIndex = sr.LastEntryIndex
		}
		switch sr.Status {
		case SegmentDamaged, SegmentWouldRepair:
			r.DamagedSegments++
		case SegmentRepaired:
			r.DamagedSegments++
			r.RepairedSegments++
		case SegmentFailed:
			r.FailedSegments++
		}
	}
	r.NextEntryIndex = r.LastEntryIndex + 1
	r.NextSequence = r.LastSequence + 1
	if len(r.Segments) > 0 {
		r.NextSegmentName = walName(r.NextSequence, r.NextEntryIndex)
	}
}

// String renders a plain-text operator report.
func (r *RepairReport) String() string {
	var b strings.Builder
	mode := "repair"
	if r.DryRun {
		mode = "verify/dry-run"
	}
	fmt.Fprintf(&b, "WAL repair report (%s): %s\n", mode, r.Dir)

	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SEGMENT\tSTATUS\tSEQ\tSTART-INDEX\tSIZE-BEFORE\tSIZE-AFTER\tRECORDS\tLAST-VALID-OFFSET\tDAMAGE-OFFSET\tLAST-ENTRY-INDEX\tISSUE\tBACKUP")
	for _, s := range r.Segments {
		if s == nil {
			continue
		}
		damageOffset := ""
		if s.DamageOffset != 0 || s.Issue != "" {
			damageOffset = fmt.Sprintf("%d", s.DamageOffset)
		}
		sizeAfter := fmt.Sprintf("%d", s.SizeAfter)
		backup := s.BackupPath
		if s.RolledBack {
			backup += " (rollback restored)"
		}
		fmt.Fprintf(w, "%s\t%s\t%016x\t%d\t%d\t%s\t%d\t%d\t%s\t%d\t%s\t%s\n",
			s.Name, s.Status, s.Sequence, s.StartIndex, s.SizeBefore, sizeAfter,
			s.ValidRecords, s.LastValidOffset, damageOffset, s.LastEntryIndex,
			s.Issue, backup)
	}
	_ = w.Flush()

	for _, warning := range r.Warnings {
		fmt.Fprintf(&b, "WARNING: %s\n", warning)
	}
	fmt.Fprintf(&b, "summary: scanned=%d damaged=%d repaired=%d failed=%d lastSequence=%016x nextSequence=%016x lastEntryIndex=%d nextEntryIndex=%d\n",
		r.ScannedSegments, r.DamagedSegments, r.RepairedSegments, r.FailedSegments,
		r.LastSequence, r.NextSequence, r.LastEntryIndex, r.NextEntryIndex)
	if r.NextSegmentName != "" {
		fmt.Fprintf(&b, "next rotated segment would be named: %s\n", r.NextSegmentName)
	}
	return b.String()
}

type segmentInspection struct {
	size            int64
	validRecords    int
	lastValidOffset int64
	lastEntryIndex  uint64
	damageOffset    int64
	issue           string
	problem         error
}

func (in segmentInspection) healthy() bool {
	return in.problem == nil && in.issue == ""
}

func inspectSegmentReadOnly(dirpath, name string, seq, startIndex uint64, backupSuffix string) (*SegmentReport, error) {
	path := filepath.Join(dirpath, name)
	f, err := os.Open(path)
	if err != nil {
		return failedSegmentReport(name, seq, startIndex, err), err
	}
	defer f.Close()

	sr := newSegmentReport(name, seq, startIndex)
	in := inspectOpenSegment(f, name)
	fillSegmentReport(sr, in)
	if _, statErr := os.Stat(path + backupSuffix); statErr == nil {
		sr.BackupPath = path + backupSuffix
	}

	if in.healthy() {
		sr.Status = SegmentClean
		sr.SizeAfter = in.size
		return sr, nil
	}
	sr.Status = SegmentDamaged
	return sr, nil
}

func repairSegment(lg *zap.Logger, dirpath, name string, seq, startIndex uint64, backupSuffix string) (*SegmentReport, error) {
	path := filepath.Join(dirpath, name)
	// TryLock gives an immediate, clear failure if etcd still has the WAL open.
	f, err := fileutil.TryLockFile(path, os.O_RDWR, fileutil.PrivateFileMode)
	if err != nil {
		sr := failedSegmentReport(name, seq, startIndex, err)
		return sr, fmt.Errorf("lock WAL segment %q: %w", path, err)
	}
	defer f.Close()

	sr := newSegmentReport(name, seq, startIndex)
	in := inspectOpenSegment(f.File, name)
	fillSegmentReport(sr, in)

	if _, statErr := os.Stat(path + backupSuffix); statErr == nil {
		sr.BackupPath = path + backupSuffix
	}

	if in.healthy() {
		sr.Status = SegmentClean
		sr.SizeAfter = in.size
		return sr, nil
	}

	backupPath := path + backupSuffix
	if err = backupFileForRepair(f.File, backupPath, dirpath); err != nil {
		sr.Status = SegmentFailed
		sr.Error = err.Error()
		return sr, fmt.Errorf("backup WAL segment %q: %w", path, err)
	}
	sr.BackupPath = backupPath

	if err = f.Truncate(in.lastValidOffset); err != nil {
		rollbackErr := restoreBackup(f.File, backupPath)
		sr.RolledBack = rollbackErr == nil
		sr.Status = SegmentFailed
		sr.Error = errors.Join(err, rollbackErr).Error()
		return sr, errors.Join(
			fmt.Errorf("truncate WAL segment %q to %d: %w", path, in.lastValidOffset, err),
			rollbackErr,
		)
	}

	start := time.Now()
	if err = walRepairFsync(f.File); err != nil {
		rollbackErr := restoreBackup(f.File, backupPath)
		sr.RolledBack = rollbackErr == nil
		sr.Status = SegmentFailed
		sr.Error = errors.Join(err, rollbackErr).Error()
		return sr, errors.Join(
			fmt.Errorf("fsync repaired WAL segment %q: %w", path, err),
			rollbackErr,
		)
	}
	walFsyncSec.Observe(time.Since(start).Seconds())

	fi, statErr := f.Stat()
	if statErr == nil {
		sr.SizeAfter = fi.Size()
	} else {
		sr.SizeAfter = in.lastValidOffset
	}
	sr.TruncatedBytes = sr.SizeBefore - sr.SizeAfter
	if sr.TruncatedBytes < 0 {
		sr.TruncatedBytes = 0
	}
	sr.Status = SegmentRepaired
	lg.Info("repaired WAL segment",
		zap.String("path", path),
		zap.Int64("truncate-offset", in.lastValidOffset),
		zap.Int64("truncated-bytes", sr.TruncatedBytes),
		zap.String("backup", backupPath),
		zap.String("issue", in.issue),
	)
	return sr, nil
}

func inspectOpenSegment(f *os.File, name string) segmentInspection {
	in := segmentInspection{damageOffset: -1}
	fi, err := f.Stat()
	if err != nil {
		in.problem = err
		in.issue = "stat_error"
		return in
	}
	in.size = fi.Size()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		in.problem = err
		in.issue = "seek_error"
		return in
	}

	decoder := NewDecoder(fileutil.NewFileReader(f))
	rec := &walpb.Record{}
	for {
		frameStart := decoder.LastOffset()
		err := decoder.Decode(rec)
		if err == nil {
			in.validRecords++
			switch rec.GetType() {
			case CrcType:
				computedCRC := decoder.LastCRC()
				// A new decoder starts from CRC 0. The CRC record itself resets
				// the chain, but its stored value must still match the chain so
				// far (normally the previous segment's closing CRC).
				if computedCRC != 0 {
					if validateErr := rec.Validate(computedCRC); validateErr != nil {
						in.stopAt(frameStart, "crc_record_mismatch", validateErr)
						return in
					}
				}
				decoder.UpdateCRC(rec.GetCrc())
			case EntryType:
				var entry raftpb.Entry
				if unmarshalErr := proto.Unmarshal(rec.Data, &entry); unmarshalErr != nil {
					in.stopAt(frameStart, "invalid_entry_record", unmarshalErr)
					return in
				}
				in.lastEntryIndex = entry.GetIndex()
			}
			continue
		}

		offset := decoder.LastOffset()
		if offset < frameStart {
			offset = frameStart
		}
		in.lastValidOffset = offset

		if errors.Is(err, io.EOF) {
			if offset >= in.size {
				// Clean logical end of segment.
				in.damageOffset = 0
				return in
			}
			in.stopAt(offset, "zero_record_or_padding", err)
			return in
		}

		in.stopAt(offset, corruptionIssue(err), err)
		return in
	}
}

func (in *segmentInspection) stopAt(offset int64, issue string, err error) {
	in.lastValidOffset = offset
	in.damageOffset = offset
	in.issue = issue
	in.problem = err
}

func corruptionIssue(err error) string {
	switch {
	case errors.Is(err, ErrZeroRecord):
		return "zero_record"
	case errors.Is(err, ErrInvalidRecordLength):
		return "invalid_record_length"
	case errors.Is(err, ErrCRCMismatch):
		return "crc_mismatch"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "zero_record_or_padding"
	default:
		return "decode_error"
	}
}

func newSegmentReport(name string, seq, startIndex uint64) *SegmentReport {
	return &SegmentReport{
		Name:       name,
		Sequence:   seq,
		StartIndex: startIndex,
		Status:     SegmentDamaged,
	}
}

func failedSegmentReport(name string, seq, startIndex uint64, err error) *SegmentReport {
	sr := newSegmentReport(name, seq, startIndex)
	sr.Status = SegmentFailed
	if err != nil {
		sr.Error = err.Error()
	}
	return sr
}

func fillSegmentReport(sr *SegmentReport, in segmentInspection) {
	sr.SizeBefore = in.size
	sr.SizeAfter = in.lastValidOffset
	sr.ValidRecords = in.validRecords
	sr.LastValidOffset = in.lastValidOffset
	sr.LastEntryIndex = in.lastEntryIndex
	sr.TruncatedBytes = in.size - in.lastValidOffset
	if sr.TruncatedBytes < 0 {
		sr.TruncatedBytes = 0
	}
	if in.damageOffset >= 0 && in.problem != nil {
		sr.DamageOffset = in.damageOffset
		sr.Issue = in.issue
		if in.problem != nil {
			sr.Error = in.problem.Error()
		}
	}
}

// backupFileForRepair copies f to backupPath durably before the live segment
// is truncated. The copy is written to a temporary file and renamed so a
// partial backup never occupies the final backup path.
func backupFileForRepair(f *os.File, backupPath, dirpath string) (retErr error) {
	tmpPath := backupPath + ".tmp"
	bf, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileutil.PrivateFileMode)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = bf.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = io.Copy(bf, f); err != nil {
		return err
	}
	if err = bf.Sync(); err != nil {
		return err
	}
	if err = bf.Close(); err != nil {
		return err
	}
	if err = os.Chmod(tmpPath, fileutil.PrivateFileMode); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, backupPath); err != nil {
		return err
	}

	dir, err := fileutil.OpenDir(dirpath)
	if err != nil {
		return err
	}
	if err = fileutil.Fsync(dir); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func restoreBackup(f *os.File, backupPath string) error {
	bf, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("open backup for rollback: %w", err)
	}
	defer bf.Close()

	if err = f.Truncate(0); err != nil {
		return fmt.Errorf("reset segment for rollback: %w", err)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek segment for rollback: %w", err)
	}
	if _, err = io.Copy(f, bf); err != nil {
		return fmt.Errorf("copy backup back to segment: %w", err)
	}
	if err = fileutil.Fsync(f); err != nil {
		return fmt.Errorf("sync rolled-back segment: %w", err)
	}
	return nil
}
