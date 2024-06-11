package vhdx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/google/uuid"
)

type RegionTable struct {
	fh      io.ReadSeeker
	offset  int64
	header  RegionTableHeader
	entries []RegionTableEntry
	lookup  map[uuid.UUID]RegionTableEntry
}

type RegionTableHeader struct {
	Signature  [4]byte
	Checksum   uint32
	EntryCount uint32
	Reserved   [4]byte
}

type RegionTableEntry struct {
	Guid       uuid.UUID
	FileOffset uint64
	Length     uint32
	Required   uint32
}

func NewRegionTable(fh io.ReadSeeker, offset int64) (*RegionTable, error) {
	regionTable := &RegionTable{fh: fh, offset: offset}

	_, err := fh.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	err = binary.Read(fh, binary.LittleEndian, &regionTable.header)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(regionTable.header.Signature[:], []byte("regi")) {
		return nil, errors.New("invalid region table signature")
	}

	entries := make([]RegionTableEntry, regionTable.header.EntryCount)
	err = binary.Read(fh, binary.LittleEndian, &entries)
	if err != nil {
		return nil, err
	}

	regionTable.lookup = make(map[uuid.UUID]RegionTableEntry)
	for _, entry := range entries {
		regionTable.lookup[newUUIDFromBytesLE(entry.Guid[:])] = entry
	}

	return regionTable, nil
}
