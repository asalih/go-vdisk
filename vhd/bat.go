package vhd

import (
	"encoding/binary"
	"fmt"
	"io"
)

type BlockAllocationTable struct {
	fh         io.ReadSeeker
	offset     int64
	maxEntries int64
}

func NewBlockAllocationTable(fh io.ReadSeeker, offset, maxEntries int64) *BlockAllocationTable {
	return &BlockAllocationTable{fh: fh, offset: offset, maxEntries: maxEntries}
}

func (bat *BlockAllocationTable) Get(block int64) (uint32, error) {
	if block+1 > int64(bat.maxEntries) {
		return 0, fmt.Errorf("Invalid block %d (max block is %d)", block, bat.maxEntries-1)
	}

	_, err := bat.fh.Seek(int64(bat.offset+block*4), io.SeekStart)
	if err != nil {
		return 0, err
	}

	var sectorOffset uint32
	if err := binary.Read(bat.fh, binary.BigEndian, &sectorOffset); err != nil {
		return 0, err
	}

	if sectorOffset == 0xFFFFFFFF {
		sectorOffset = 0
	}

	return sectorOffset, nil
}
