package vhdx

import (
	"encoding/binary"
	"fmt"
)

type batEntry struct {
	State        int
	FileOffsetMb uint64
}

type BlockAllocationTable struct {
	vhdx       *VHDX
	offset     int64
	chunkRatio int64
	pbCount    int64
	sbCount    int64
	entryCount int64
}

func NewBlockAllocationTable(vhdx *VHDX, offset int64) *BlockAllocationTable {
	pbCount := int64((vhdx.size + uint64(vhdx.blockSize) - 1) / uint64(vhdx.blockSize))
	sbCount := (int64(pbCount) + vhdx.chunkRatio - 1) / vhdx.chunkRatio
	entryCount := int64(pbCount) + ((int64(pbCount) - 1) / vhdx.chunkRatio)
	if vhdx.hasParent {
		entryCount = sbCount * (vhdx.chunkRatio + 1)
	}

	return &BlockAllocationTable{
		vhdx:       vhdx,
		offset:     int64(offset),
		chunkRatio: vhdx.chunkRatio,
		pbCount:    pbCount,
		sbCount:    sbCount,
		entryCount: entryCount,
	}
}

func (bat *BlockAllocationTable) get(entry int64) (batEntry, error) {
	if entry+1 > bat.entryCount {
		return batEntry{}, fmt.Errorf("invalid entry for BAT lookup: %d (max entry is %d)", entry, bat.entryCount-1)
	}

	_, err := bat.vhdx.fh.Seek(bat.offset+int64(entry*8), 0)
	if err != nil {
		return batEntry{}, err
	}
	var entryData [8]byte
	if err := binary.Read(bat.vhdx.fh, binary.LittleEndian, &entryData); err != nil {
		return batEntry{}, err
	}

	return batEntry{
		State:        int(entryData[0]),
		FileOffsetMb: binary.LittleEndian.Uint64(append(entryData[2:], make([]byte, 3)...)) >> 4,
	}, nil
}

func (bat *BlockAllocationTable) pb(block int64) (batEntry, error) {
	sbEntries := block / bat.chunkRatio
	return bat.get(block + sbEntries)
}

func (bat *BlockAllocationTable) sb(block int64) (batEntry, error) {
	numSb := block / bat.chunkRatio
	return bat.get((numSb+1)*bat.chunkRatio + numSb)
}
