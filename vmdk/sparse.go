package vmdk

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
)

type SparseDisk struct {
	fh io.ReadSeeker

	parent         *VMDK
	offset         int64
	sectorOffset   int64
	size           int64
	sectorCount    int64
	isSESparse     bool
	header         *SparseExtentHeader
	descriptor     *DiskDescriptor
	grainDirectory []uint64
	grainTableSize int64
}

type run struct {
	SetFlag bool
	Type    int
	Count   int
	Offset  int64
	Parent  int64
}

func NewSparseDisk(fh io.ReadSeeker, parent *VMDK) (*SparseDisk, error) {
	sd := &SparseDisk{fh: fh, parent: parent}
	var err error
	sd.size, err = getSize(fh)
	if err != nil {
		return nil, err
	}

	sd.header, err = ParseSparseExtentHeader(fh)
	if err != nil {
		return nil, err
	}

	capacity := uint64(0)

	switch sd.header.Magic {
	case VMDK_MAGIC, COWD_MAGIC:
		sd.isSESparse = false
		if sd.header.Raw.GrainOffset() == 0xFFFFFFFFFFFFFFFF {
			_, err := fh.Seek(-1024, io.SeekEnd)
			if err != nil {
				return nil, err
			}
			sd.header, err = ParseSparseExtentHeader(fh)
			if err != nil {
				return nil, err
			}
		}

		gdOffset := int64(0)
		if sd.header.Magic == VMDK_MAGIC {
			h, _ := sd.header.AsVMDK()
			grainTableCoverage := uint64(h.NumGrainTableEntries) * h.GrainSize
			sd.grainDirectory = make([]uint64, (h.Capacity+grainTableCoverage-1)/grainTableCoverage)
			sd.grainTableSize = int64(h.NumGrainTableEntries)
			if h.DescriptorSize > 0 {
				_, err := fh.Seek(int64(h.DescriptorOffset)*SECTOR_SIZE, io.SeekStart)
				if err != nil {
					return nil, err
				}

				descriptorBuf := make([]byte, h.DescriptorSize*SECTOR_SIZE)
				_, err = fh.Read(descriptorBuf)
				if err != nil {
					return nil, err
				}
				sd.descriptor, err = ParseDiskDescriptor(string(descriptorBuf))
				if err != nil {
					return nil, err
				}
			}

			gdOffset = int64(h.PrimaryGrainDirectoryOffset)
			capacity = h.Capacity

		} else if sd.header.Magic == COWD_MAGIC {
			h, _ := sd.header.AsCOWD() //ok
			sd.grainDirectory = make([]uint64, h.NumGrainDirectoryEntries)
			sd.grainTableSize = 4096

			gdOffset = int64(h.PrimaryGrainDirectoryOffset)
			capacity = uint64(h.Capacity)
		}

		_, err = fh.Seek(gdOffset*SECTOR_SIZE, io.SeekStart)
		if err != nil {
			return nil, err
		}

		gd := make([]uint32, len(sd.grainDirectory))
		err = binary.Read(fh, binary.LittleEndian, &gd)
		if err != nil {
			return nil, err
		}
		for i, v := range gd {
			sd.grainDirectory[i] = uint64(v)
		}

	case SESPARSE_MAGIC:
		sd.isSESparse = true
		h, _ := sd.header.AsVMDKSES() //ok
		capacity = h.Capacity

		sd.grainDirectory = make([]uint64, h.GrainDirectorySize*SECTOR_SIZE/8)
		sd.grainTableSize = int64(h.GrainTableSize * SECTOR_SIZE / 8)

		_, err := fh.Seek(int64(h.GrainDirectoryOffset)*SECTOR_SIZE, io.SeekStart)
		if err != nil {
			return nil, err
		}
		err = binary.Read(fh, binary.LittleEndian, &sd.grainDirectory)
		if err != nil {
			return nil, err
		}

	}

	sd.size = int64(capacity * SECTOR_SIZE)
	sd.sectorCount = int64(capacity)

	return sd, nil
}

// readSectors method for SparseDisk
func (sd *SparseDisk) ReadSectors(sector int64, count int) ([]byte, error) {
	runs, err := sd.getRuns(sector, count)
	if err != nil {
		return nil, err
	}

	sectorsRead := []byte{}
	for _, run := range runs {
		switch {
		case run.Type == 0:
			if sd.parent == nil {
				sectorsRead = append(sectorsRead, make([]byte, run.Count*SECTOR_SIZE)...)
				continue
			}

			data, err := sd.parent.ReadSectors(int64(run.Parent), run.Count)
			if err != nil {
				return nil, err
			}
			sectorsRead = append(sectorsRead, data...)

		case run.Type == 1:
			sectorsRead = append(sectorsRead, make([]byte, run.Count*SECTOR_SIZE)...)
		default:
			if !sd.header.Raw.IsCompressed() {
				offset := (int64(run.Type) + run.Offset) * SECTOR_SIZE
				_, err = sd.fh.Seek(int64(offset), io.SeekStart)
				if err != nil {
					return nil, err
				}

				data := make([]byte, run.Count*SECTOR_SIZE)
				_, err = sd.fh.Read(data)
				if err != nil {
					return nil, err
				}

				sectorsRead = append(sectorsRead, data...)

				continue
			}

			for run.Count > 0 {
				offset := run.Offset * SECTOR_SIZE
				grainRemaining := int(sd.header.Raw.GetGrainSize() - uint64(run.Offset))
				readCount := min32(run.Count, grainRemaining)

				buf, err := sd.readCompressedGrain(int(run.Type))
				if err != nil {
					return nil, err
				}
				sectorsRead = append(sectorsRead, buf[offset:offset+int64(readCount)*SECTOR_SIZE]...)

				run.Offset = 0
				run.Type += int(sd.header.Raw.GetGrainSize())
				run.Count -= readCount
			}
		}
	}

	return sectorsRead, nil
}

// readCompressedGrain method for SparseDisk
func (sd *SparseDisk) readCompressedGrain(sector int) ([]byte, error) {
	_, err := sd.fh.Seek(int64(sector)*SECTOR_SIZE, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, SECTOR_SIZE)
	_, err = sd.fh.Read(buf)
	if err != nil {
		return nil, err
	}

	var compressedLen int
	if sd.header.Raw.IsEmeddedLBA() {
		compressedLen = int(binary.LittleEndian.Uint32(buf[8:12]))
	} else {
		compressedLen = int(binary.LittleEndian.Uint32(buf[:4]))
	}

	if compressedLen+12 > SECTOR_SIZE {
		_, err := sd.fh.Seek(int64(sector+1)*SECTOR_SIZE, io.SeekStart)
		if err != nil {
			return nil, err
		}

		remainingLen := compressedLen + 12 - SECTOR_SIZE
		nextBuf := make([]byte, remainingLen)
		_, err = sd.fh.Read(nextBuf)
		if err != nil {
			return nil, err
		}
		buf = append(buf, nextBuf...)
	}

	r, err := zlib.NewReader(bytes.NewReader(buf[12 : 12+compressedLen]))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

// getRuns method for SparseDisk
func (sd *SparseDisk) getRuns(sector int64, count int) ([]run, error) {
	nextGrainSector := 0
	readSector := int64(sector) - sd.sectorOffset
	readCount := count

	runs := []run{}

	if readCount == 0 {
		return runs, nil
	}

	rund := run{}
	for readCount > 0 {
		grainSize := int64(sd.header.Raw.GetGrainSize())
		grain := readSector / grainSize
		grainOffset := readSector % grainSize
		grainSector, err := sd.lookupGrain(grain)
		if err != nil {
			return nil, err
		}
		readSectorCount := min32(readCount, int(grainSize-grainOffset))

		switch {
		case rund.SetFlag && rund.Type == 0 && grainSector == 0:
			rund.Count += readSectorCount
		case rund.SetFlag && rund.Type == 1 && grainSector == 1:
			rund.Count += readSectorCount
		case rund.SetFlag && rund.Type > 1 && grainSector == nextGrainSector:
			nextGrainSector += int(grainSize)
			rund.Count += readSectorCount
		default:
			if rund.SetFlag {
				runs = append(runs, rund)
				rund = run{}
			}
			switch grainSector {
			case 0:
				rund.SetFlag = true
				rund.Type = 0
				rund.Count += readSectorCount
				rund.Parent = sd.sectorOffset + readSector
			case 1:
				rund.SetFlag = true
				rund.Type = 1
				rund.Count += readSectorCount
			default:
				rund.SetFlag = true
				rund.Type = grainSector
				rund.Offset = grainOffset
				rund.Count += readSectorCount
				nextGrainSector = grainSector + int(grainSize)
			}
		}
		readCount -= int(readSectorCount)
		readSector += int64(readSectorCount)
	}

	runs = append(runs, rund)
	return runs, nil
}

// lookupGrain method for SparseDisk
func (sd *SparseDisk) lookupGrain(grain int64) (int, error) {
	gdirEntry, gtblEntry := grain/int64(sd.grainTableSize), grain%int64(sd.grainTableSize)
	table, err := sd.lookupGrainTable(gdirEntry)
	if err != nil {
		return 0, err
	}
	if table == nil {
		return 0, nil
	}
	grainEntry := table[gtblEntry]
	if sd.isSESparse {
		grainType := grainEntry & SESPARSE_GRAIN_TYPE_MASK
		switch grainType {
		case SESPARSE_GRAIN_TYPE_UNALLOCATED, SESPARSE_GRAIN_TYPE_FALLTHROUGH:
			return 0, nil
		case SESPARSE_GRAIN_TYPE_ZERO:
			return 1, nil
		case SESPARSE_GRAIN_TYPE_ALLOCATED:
			clusterSectorHi := (grainEntry & 0x0FFF000000000000) >> 48
			clusterSectorLo := (grainEntry & 0x0000FFFFFFFFFFFF) << 12
			clusterSector := clusterSectorHi | clusterSectorLo
			return int(sd.header.Raw.GrainOffset() +
				(clusterSector * sd.header.Raw.GetGrainSize())), nil
		}
	}
	return int(grainEntry), nil
}

// lookupGrainTable method for SparseDisk
func (sd *SparseDisk) lookupGrainTable(directory int64) ([]uint64, error) {
	gtblOffset := sd.grainDirectory[directory]

	if sd.isSESparse {
		if gtblOffset == 0 || gtblOffset&0xFFFFFFFF00000000 != 0x1000000000000000 {
			return nil, nil
		}

		gtblOffset &= 0x00000000FFFFFFFF
		hdr, ok := sd.header.AsVMDKSES()
		if !ok {
			return nil, errors.New("header is not sesparse")
		}
		gtblOffset = hdr.GrainTablesOffset + gtblOffset*uint64(sd.grainTableSize*8)/SECTOR_SIZE

		_, err := sd.fh.Seek(int64(gtblOffset)*SECTOR_SIZE, io.SeekStart)
		if err != nil {
			return nil, err
		}

		table := make([]uint64, sd.grainTableSize)
		err = binary.Read(sd.fh, binary.LittleEndian, &table)
		return table, err
	}

	if gtblOffset != 0 {
		_, err := sd.fh.Seek(int64(gtblOffset)*SECTOR_SIZE, io.SeekStart)
		if err != nil {
			return nil, err
		}

		table := make([]uint32, sd.grainTableSize)
		err = binary.Read(sd.fh, binary.LittleEndian, &table)
		if err != nil {
			return nil, err
		}

		gt := make([]uint64, sd.grainTableSize)
		for i, v := range table {
			gt[i] = uint64(v)
		}
		return gt, nil
	}
	return nil, nil
}

func (rd *SparseDisk) GetSize() int64 {
	return rd.size
}

func (rd *SparseDisk) GetSectorCount() int64 {
	return rd.size / SECTOR_SIZE
}

func (rd *SparseDisk) GetSectorOffset() int64 {
	return rd.sectorOffset
}

func (rd *SparseDisk) SetOffset(offset, sectorOffset int64) {
	rd.offset = offset
	rd.sectorOffset = sectorOffset
}
