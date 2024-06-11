package vhdx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

type MetadataTable struct {
	fh      io.ReadSeeker
	offset  int64
	length  int64
	header  MetadataTableHeader
	entries []MetadataTableEntry
	lookup  map[uuid.UUID]interface{}
}

type MetadataTableHeader struct {
	Signature  [8]byte
	Reserved   [2]byte
	EntryCount uint16
	Reserved2  [20]byte
}

type MetadataTableEntry struct {
	ItemID     uuid.UUID
	Offset     uint32
	Length     uint32
	Permission Permission
	Reserved   [7]byte
}

func NewMetadataTable(fh io.ReadSeeker, offset, length int64) (*MetadataTable, error) {
	if _, err := fh.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	header := MetadataTableHeader{}
	if err := binary.Read(fh, binary.LittleEndian, &header); err != nil {
		return nil, err
	}
	if !bytes.Equal(header.Signature[:], []byte("metadata")) {
		return nil, errors.New("invalid metadata table signature")
	}

	entries := make([]MetadataTableEntry, header.EntryCount)
	if err := binary.Read(fh, binary.LittleEndian, &entries); err != nil {
		return nil, err
	}

	lookup := make(map[uuid.UUID]interface{})
	for _, entry := range entries {
		itemID := newUUIDFromBytesLE(entry.ItemID[:])
		_, err := fh.Seek(offset+int64(entry.Offset), 0)
		if err != nil {
			return nil, err
		}
		switch itemID {
		case FILE_PARAMETERS_GUID:
			data := make([]byte, 8)
			if err := binary.Read(fh, binary.LittleEndian, data); err != nil {
				return nil, err
			}
			tmp := binary.LittleEndian.Uint32(data[4:])
			fp := FileParameters{
				BlockSize:           binary.LittleEndian.Uint32(data[0:4]),
				LeaveBlockAllocated: tmp&1 == 1,
				HasParent:           (tmp>>1)&1 > 0,
			}
			lookup[itemID] = fp
		case VIRTUAL_DISK_SIZE_GUID:
			var size uint64
			if err := binary.Read(fh, binary.LittleEndian, &size); err != nil {
				return nil, err
			}
			lookup[itemID] = size
		case VIRTUAL_DISK_ID_GUID:
			id := uuid.UUID{}
			if err := binary.Read(fh, binary.LittleEndian, &id); err != nil {
				return nil, err
			}
			lookup[itemID] = id
		case LOGICAL_SECTOR_SIZE_GUID:
			var size uint32
			if err := binary.Read(fh, binary.LittleEndian, &size); err != nil {
				return nil, err
			}
			lookup[itemID] = size
		case PHYSICAL_SECTOR_SIZE_GUID:
			var size uint32
			if err := binary.Read(fh, binary.LittleEndian, &size); err != nil {
				return nil, err
			}
			lookup[itemID] = size
		case PARENT_LOCATOR_GUID:
			pl, err := NewParentLocator(fh)
			if err != nil {
				return nil, err
			}
			lookup[itemID] = pl
		default:
			continue
		}
	}
	return &MetadataTable{
		fh:      fh,
		offset:  offset,
		length:  length,
		header:  header,
		entries: entries,
		lookup:  lookup,
	}, nil
}

type Permission uint8

// IsUser required
func (p Permission) IsUser() bool {
	return p&1 == 1
}

// IsVirtualDisk required
func (p Permission) IsVirtualDisk() bool {
	return p&2 == 2
}

// IsRequired required
func (p Permission) IsRequired() bool {
	return p&4 == 4
}

func (p Permission) String() string {
	return fmt.Sprintf("%d%d%d", p&1/1, p&2/2, p&4/4)
}
