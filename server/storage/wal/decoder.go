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
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/pkg/v3/crc"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

const minSectorSize = 512

// frameSizeBytes is frame size in bytes, including record size and padding size.
const frameSizeBytes = 8

var (
	// ErrZeroRecord marks a zero length frame. Preallocated WAL tails are full
	// of zero bytes and hit this path.
	ErrZeroRecord = errors.New("wal: zero record or preallocated zero fill")
	// ErrInvalidRecordLength marks a frame whose encoded length cannot fit in
	// the remaining segment or violates frame alignment.
	ErrInvalidRecordLength = errors.New("wal: invalid record length")
	// ErrInvalidRecord marks a frame that could not be unmarshalled.
	ErrInvalidRecord = errors.New("wal: invalid record")
)

// OffsetError carries the segment offset at which decoding stopped.
type OffsetError struct {
	File   string
	Offset int64
	Err    error
}

func (e *OffsetError) Error() string {
	if e.File == "" {
		return fmt.Sprintf("at offset %d: %v", e.Offset, e.Err)
	}
	return fmt.Sprintf("in file %q at offset %d: %v", e.File, e.Offset, e.Err)
}

func (e *OffsetError) Unwrap() error { return e.Err }

// ErrorOffset returns the WAL segment offset carried by err, if any.
func ErrorOffset(err error) (int64, bool) {
	var offsetErr *OffsetError
	if errors.As(err, &offsetErr) {
		return offsetErr.Offset, true
	}
	return 0, false
}

type Decoder interface {
	Decode(rec *walpb.Record) error
	LastOffset() int64
	LastCRC() uint32
	UpdateCRC(prevCrc uint32)
}

type decoder struct {
	mu  sync.Mutex
	brs []*fileutil.FileBufReader

	// lastValidOff file offset following the last valid decoded record
	lastValidOff int64
	crc          hash.Hash32

	// continueOnCrcError - causes the decoder to continue working even in case of crc mismatch.
	// This is a desired mode for tools performing inspection of the corrupted WAL logs.
	// See comments on 'Decode' method for semantic.
	continueOnCrcError bool
}

func NewDecoderAdvanced(continueOnCrcError bool, r ...fileutil.FileReader) Decoder {
	readers := make([]*fileutil.FileBufReader, len(r))
	for i := range r {
		readers[i] = fileutil.NewFileBufReader(r[i])
	}
	return &decoder{
		brs:                readers,
		crc:                crc.New(0, crcTable),
		continueOnCrcError: continueOnCrcError,
	}
}

func NewDecoder(r ...fileutil.FileReader) Decoder {
	return NewDecoderAdvanced(false, r...)
}

// Decode reads the next record out of the file.
// In the success path, fills 'rec' and returns nil.
// When it fails, it returns err and usually resets 'rec' to the defaults.
// When continueOnCrcError is set, the method may return ErrUnexpectedEOF or ErrCRCMismatch, but preserve the read
// (potentially corrupted) record content.
func (d *decoder) Decode(rec *walpb.Record) error {
	rec.Reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	if len(d.brs) == 0 {
		return io.EOF
	}

	fileBufReader := d.brs[0]
	fileInfo := fileBufReader.FileInfo()
	frameStart := d.lastValidOff
	l, err := readInt64(fileBufReader)
	if errors.Is(err, io.EOF) {
		// Hit the actual end of this file. In a multi-file decode the next
		// reader continues the WAL.
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			return &OffsetError{File: fileInfo.Name(), Offset: frameStart, Err: io.EOF}
		}
		d.lastValidOff = 0
		return d.decodeRecord(rec)
	}
	if err != nil {
		return &OffsetError{File: fileInfo.Name(), Offset: frameStart, Err: err}
	}
	if l == 0 {
		// A zero length frame is either the end of logically written data or a
		// preallocated tail full of zeros. Stop at its first byte and report
		// that offset instead of treating it as a decodable record.
		zeroOffset := d.lastValidOff
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			return &OffsetError{
				File:   fileInfo.Name(),
				Offset: zeroOffset,
				Err:    fmt.Errorf("%w: %w", ErrZeroRecord, io.EOF),
			}
		}
		d.lastValidOff = 0
		return d.decodeRecord(rec)
	}

	recBytes, padBytes := decodeFrameSize(l)
	frameBytes := frameSizeBytes + recBytes + padBytes
	fileSize := fileInfo.Size()
	remainingFrameBytes := fileSize - frameStart
	// The length field itself must be sane and the complete frame must fit in
	// the bytes left in this segment. Checking before allocation prevents a
	// corrupted length from triggering an enormous allocation. Alignment is
	// not enforced because older WAL fixtures can contain unpadded frames.
	if recBytes <= 0 || padBytes < 0 ||
		frameBytes > remainingFrameBytes {
		limit := fileSize - frameStart - frameSizeBytes - padBytes
		err := fmt.Errorf("%w: %w: recBytes=%d, padBytes=%d, frameBytes=%d, fileSize=%d, offset=%d, entryLimit=%d",
			io.ErrUnexpectedEOF, ErrInvalidRecordLength, recBytes, padBytes, frameBytes, fileSize, frameStart, limit)
		return &OffsetError{File: fileInfo.Name(), Offset: frameStart, Err: err}
	}

	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(fileBufReader, data); err != nil {
		// ReadFull returns io.EOF only if no bytes were read
		// the decoder should treat this as an ErrUnexpectedEOF instead.
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		err = &OffsetError{File: fileInfo.Name(), Offset: frameStart, Err: err}
		return err
	}
	if err := proto.Unmarshal(data[:recBytes], rec); err != nil {
		if d.isTornEntry(data) {
			err = io.ErrUnexpectedEOF
		} else {
			err = fmt.Errorf("%w: %w", ErrInvalidRecord, err)
		}
		return &OffsetError{File: fileInfo.Name(), Offset: frameStart, Err: err}
	}

	// skip crc checking if the record type is CrcType
	if rec.GetType() != CrcType {
		_, err := d.crc.Write(rec.Data)
		if err != nil {
			return err
		}
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			if !d.continueOnCrcError {
				rec.Reset()
			} else {
				// If we continue, we want to update lastValidOff, such that following errors are consistent
				defer func() { d.lastValidOff += frameSizeBytes + recBytes + padBytes }()
			}

			if d.isTornEntry(data) {
				return &OffsetError{
					File:   fileInfo.Name(),
					Offset: d.lastValidOff,
					Err:    fmt.Errorf("%w: torn write", io.ErrUnexpectedEOF),
				}
			}
			return &OffsetError{
				File:   fileInfo.Name(),
				Offset: d.lastValidOff,
				Err:    fmt.Errorf("%w: checksum validation failed", err),
			}
		}
	}
	// record decoded as valid; point last valid offset to end of record
	d.lastValidOff += frameSizeBytes + recBytes + padBytes
	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// the record size is stored in the lower 56 bits of the 64-bit length
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// non-zero padding is indicated by set MSb / a negative length
	if lenField < 0 {
		// padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (d *decoder) isTornEntry(data []byte) bool {
	if len(d.brs) != 1 {
		return false
	}

	fileOff := d.lastValidOff + frameSizeBytes
	curOff := 0
	var chunks [][]byte
	// split data on sector boundaries
	for curOff < len(data) {
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	// if any data for a sector chunk is all 0, it's a torn write
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

func (d *decoder) UpdateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
}

func (d *decoder) LastCRC() uint32 {
	return d.crc.Sum32()
}

func (d *decoder) LastOffset() int64 { return d.lastValidOff }

func MustUnmarshalEntry(d []byte) *raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshalMessage(&e, d)
	return &e
}

func MustUnmarshalState(d []byte) *raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshalMessage(&s, d)
	return &s
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
