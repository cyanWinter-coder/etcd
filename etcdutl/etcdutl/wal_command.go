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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"go.etcd.io/etcd/server/v3/storage/wal"
)

// NewWALCommand returns the cobra command for "wal".
func NewWALCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wal <subcommand>",
		Short: "Validates and repairs etcd write-ahead log files",
	}
	cmd.AddCommand(NewWALVerifyCommand(), NewWALRepairCommand())
	return cmd
}

// NewWALVerifyCommand returns the cobra command for "wal verify".
func NewWALVerifyCommand() *cobra.Command {
	var walDirFlag string
	cmd := &cobra.Command{
		Use:           "verify [wal-dir]",
		Short:         "Validates every WAL segment without modifying it",
		Args:          cobra.MaximumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			walDir, err := walDirFromArgsOrFlag(args, walDirFlag)
			if err != nil {
				return err
			}
			return walVerifyCommandFunc(cmd, walDir)
		},
	}
	cmd.Flags().StringVar(&walDirFlag, "wal-dir", "", "path to the WAL directory")
	_ = cmd.MarkFlagDirname("wal-dir")
	return cmd
}

// NewWALRepairCommand returns the cobra command for "wal repair".
func NewWALRepairCommand() *cobra.Command {
	return newWALRepairCommand("repair [wal-dir]", false)
}

// NewWALRepairAliasCommand returns a hidden top-level "wal-repair" command
// for operators and scripts that use the older flat etcdutl command style.
func NewWALRepairAliasCommand() *cobra.Command {
	cmd := newWALRepairCommand("wal-repair [wal-dir]", true)
	cmd.Aliases = []string{"repair-wal"}
	cmd.Hidden = true
	return cmd
}

func newWALRepairCommand(use string, topLevel bool) *cobra.Command {
	var (
		dryRun     bool
		walDirFlag string
	)
	cmd := &cobra.Command{
		Use:           use,
		Short:         "Truncates damaged WAL segments to their last complete record",
		Args:          cobra.MaximumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		Long: `Validates every segment in the WAL directory. A damaged segment is
truncated to the offset immediately after its last complete, checksum-valid
record. The original segment is copied to "<segment>.broken" first, and a
failed truncation/fsync is rolled back from that copy.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			walDir, err := walDirFromArgsOrFlag(args, walDirFlag)
			if err != nil {
				return err
			}
			return walRepairCommandFunc(cmd, walDir, dryRun)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report what would be repaired without modifying files")
	cmd.Flags().StringVar(&walDirFlag, "wal-dir", "", "path to the WAL directory")
	_ = cmd.MarkFlagDirname("wal-dir")
	if topLevel {
		// The positional argument is the documented form; keep the flag for
		// parity with scripts that prefer an explicit option.
		_ = cmd.Flags().MarkHidden("wal-dir")
	}
	return cmd
}

func walDirFromArgsOrFlag(args []string, walDirFlag string) (string, error) {
	if len(args) == 1 {
		if walDirFlag != "" && walDirFlag != args[0] {
			return "", errors.New("WAL directory specified both as an argument and --wal-dir")
		}
		return args[0], nil
	}
	if walDirFlag != "" {
		return walDirFlag, nil
	}
	return "", errors.New("missing WAL directory: pass it as an argument or --wal-dir")
}

func walVerifyCommandFunc(cmd *cobra.Command, walDir string) error {
	report, err := wal.VerifyDirectory(GetLogger(), walDir)
	printWALReport(cmd, report)
	if err != nil {
		return fmt.Errorf("failed to verify WAL directory %q: %w", walDir, err)
	}
	if report != nil && report.FailedSegments > 0 {
		return fmt.Errorf("WAL verification failed for %d segment(s)", report.FailedSegments)
	}
	if report != nil && report.DamagedSegments > 0 {
		return fmt.Errorf("found %d damaged WAL segment(s); run 'etcdutl wal repair %s'", report.DamagedSegments, walDir)
	}
	return nil
}

func walRepairCommandFunc(cmd *cobra.Command, walDir string, dryRun bool) error {
	report, err := wal.RepairDirectoryWithOptions(GetLogger(), walDir, wal.RepairOptions{DryRun: dryRun})
	printWALReport(cmd, report)
	if err != nil {
		return fmt.Errorf("failed to repair WAL directory %q: %w", walDir, err)
	}
	if report != nil && report.FailedSegments > 0 {
		return fmt.Errorf("WAL repair failed for %d segment(s); restore the .broken backup if necessary", report.FailedSegments)
	}
	if dryRun && report != nil && report.DamagedSegments > 0 {
		return fmt.Errorf("dry-run found %d damaged WAL segment(s)", report.DamagedSegments)
	}
	return nil
}

func printWALReport(cmd *cobra.Command, report *wal.RepairReport) {
	if report == nil {
		return
	}
	out := cmd.OutOrStdout()
	if OutputFormat == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
		return
	}
	fmt.Fprint(out, report.String())
}
