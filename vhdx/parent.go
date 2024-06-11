package vhdx

import (
	"encoding/binary"
	"io"

	"github.com/google/uuid"
)

type ParentLocatorHeader struct {
	LocatorType   uuid.UUID
	Reserved      uint16
	KeyValueCount uint16
}
type ParentLocatorEntry struct {
	KeyOffset   uint32
	ValueOffset uint32
	KeyLength   uint16
	ValueLength uint16
}

type ParentLocator struct {
	fh      io.ReadSeeker
	offset  int64
	header  ParentLocatorHeader
	typeID  uuid.UUID
	entries map[string]string
}

func NewParentLocator(fh io.ReadSeeker) (*ParentLocator, error) {
	offset, err := fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	header := ParentLocatorHeader{}
	if err := binary.Read(fh, binary.LittleEndian, &header); err != nil {
		return nil, err
	}
	plEntries := make([]ParentLocatorEntry, header.KeyValueCount)
	if err := binary.Read(fh, binary.LittleEndian, &plEntries); err != nil {
		return nil, err
	}

	typeID := newUUIDFromBytesLE(header.LocatorType[:])
	entries := make(map[string]string)
	for _, ple := range plEntries {
		key := make([]byte, ple.KeyLength)
		value := make([]byte, ple.ValueLength)
		if _, err := fh.Seek(offset+int64(ple.KeyOffset), 0); err != nil {
			return nil, err
		}
		if _, err := fh.Read(key); err != nil {
			return nil, err
		}
		if _, err := fh.Seek(offset+int64(ple.ValueOffset), 0); err != nil {
			return nil, err
		}
		if _, err := fh.Read(value); err != nil {
			return nil, err
		}
		entries[utf16ToString(key)] = utf16ToString(value)
	}

	return &ParentLocator{
		fh:      fh,
		offset:  offset,
		header:  header,
		typeID:  typeID,
		entries: entries,
	}, nil
}
