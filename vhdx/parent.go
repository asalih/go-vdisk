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
	offset, _ := fh.Seek(0, 1)
	header := ParentLocatorHeader{}
	if err := binary.Read(fh, binary.LittleEndian, &header); err != nil {
		return nil, err
	}
	typeID := header.LocatorType
	entries := make(map[string]string)
	for i := 0; i < int(header.KeyValueCount); i++ {
		entry := ParentLocatorEntry{}
		if err := binary.Read(fh, binary.LittleEndian, &entry); err != nil {
			return nil, err
		}
		key := make([]byte, entry.KeyLength)
		value := make([]byte, entry.ValueLength)
		if _, err := fh.Seek(offset+int64(entry.KeyOffset), 0); err != nil {
			return nil, err
		}
		if _, err := fh.Read(key); err != nil {
			return nil, err
		}
		if _, err := fh.Seek(offset+int64(entry.ValueOffset), 0); err != nil {
			return nil, err
		}
		if _, err := fh.Read(value); err != nil {
			return nil, err
		}
		entries[string(key)] = string(value)
	}

	return &ParentLocator{
		fh:      fh,
		offset:  offset,
		header:  header,
		typeID:  typeID,
		entries: entries,
	}, nil
}
