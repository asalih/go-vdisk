package vhdx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

const VHDX_MAGIC = "vhdxfile"

const (
	ALIGNMENT = 64 * 1024
	MB        = 1024 * 1024

	PAYLOAD_BLOCK_NOT_PRESENT       = 0
	PAYLOAD_BLOCK_UNDEFINED         = 1
	PAYLOAD_BLOCK_ZERO              = 2
	PAYLOAD_BLOCK_UNMAPPED          = 3
	PAYLOAD_BLOCK_FULLY_PRESENT     = 6
	PAYLOAD_BLOCK_PARTIALLY_PRESENT = 7
)

var (
	BAT_REGION_GUID, _           = uuid.Parse("2DC27766-F623-4200-9D64-115E9BFD4A08")
	FILE_PARAMETERS_GUID, _      = uuid.Parse("CAA16737-FA36-4D43-B3B6-33F0AA44E76B")
	LOGICAL_SECTOR_SIZE_GUID, _  = uuid.Parse("8141BF1D-A96F-4709-BA47-F233A8FAAB5F")
	METADATA_REGION_GUID, _      = uuid.Parse("8B7CA206-4790-4B9A-B8FE-575F050F886E")
	PARENT_LOCATOR_GUID, _       = uuid.Parse("A8D35F2D-B30B-454D-ABF7-D3D84834AB0C")
	PHYSICAL_SECTOR_SIZE_GUID, _ = uuid.Parse("CDA348C7-445D-4471-9CC9-E9885251C556")
	VIRTUAL_DISK_ID_GUID, _      = uuid.Parse("BECA12AB-B2E6-4523-93EF-C309E000C746")
	VIRTUAL_DISK_SIZE_GUID, _    = uuid.Parse("2FA54224-CD1B-4876-B211-5DBED83BF4B8")
	VHDX_PARENT_LOCATOR_GUID, _  = uuid.Parse("B04AEFB7-D19E-4A81-B789-25B8E9445913")
)

type FileParameters struct {
	BlockSize           uint32
	LeaveBlockAllocated bool
	HasParent           bool
}

type FileIdentifier struct {
	Signature [8]byte
	Creator   [512]byte
}

type VirtualDiskID struct {
	VirtualDiskID [16]byte
}

type VHDX struct {
	fh io.ReadSeeker

	fileIdentifier  FileIdentifier
	header          Header
	headers         [2]Header
	regionTable     *RegionTable
	regionTables    [2]*RegionTable
	metadata        *MetadataTable
	size            uint64
	blockSize       uint32
	hasParent       bool
	sectorSize      uint32
	id              uuid.UUID
	parent          *VHDX
	bat             *BlockAllocationTable
	sectorsPerBlock int
	chunkRatio      int64
}

type FileAccessorFn func(string) (io.ReadSeeker, error)

var FileAccessor FileAccessorFn

var ErrFileAccessorNotAvailable = errors.New("file accessor needed to access for parent and extents from file")

func NewVHDX(fh io.ReadSeeker) (*VHDX, error) {
	if FileAccessor == nil {
		return nil, ErrFileAccessorNotAvailable
	}

	vhdx := &VHDX{fh: fh}

	if err := binary.Read(fh, binary.LittleEndian, &vhdx.fileIdentifier); err != nil {
		return nil, err
	}
	if !bytes.Equal(vhdx.fileIdentifier.Signature[:], []byte(VHDX_MAGIC)) {
		return nil, errors.New("invalid file identifier signature")
	}

	// Read headers
	var header1, header2 Header
	if err := readHeader(fh, &header1, 1*ALIGNMENT); err != nil {
		return nil, err
	}
	if err := readHeader(fh, &header2, 2*ALIGNMENT); err != nil {
		return nil, err
	}

	if header1.SequenceNumber > header2.SequenceNumber {
		vhdx.header = header1
	} else {
		vhdx.header = header2
	}
	vhdx.headers = [2]Header{header1, header2}

	if !bytes.Equal(vhdx.header.Signature[:], []byte("head")) {
		return nil, errors.New("invalid header signature")
	}

	// Read region tables
	regionTable1, err := NewRegionTable(fh, 3*ALIGNMENT)
	if err != nil {
		return nil, err
	}
	regionTable2, err := NewRegionTable(fh, 4*ALIGNMENT)
	if err != nil {
		return nil, err
	}

	vhdx.regionTable = regionTable1
	vhdx.regionTables = [2]*RegionTable{regionTable1, regionTable2}

	// Read metadata
	metadataEntry, ok := vhdx.regionTable.lookup[METADATA_REGION_GUID]
	if !ok {
		return nil, errors.New("missing required region: metadata")
	}
	metadataTable, err := NewMetadataTable(fh, int64(metadataEntry.FileOffset), int64(metadataEntry.Length))
	if err != nil {
		return nil, err
	}
	vhdx.metadata = metadataTable

	// Set VHDX properties
	vhdx.size = vhdx.metadata.lookup[VIRTUAL_DISK_SIZE_GUID].(uint64)
	fileParameters := vhdx.metadata.lookup[FILE_PARAMETERS_GUID].(FileParameters)
	vhdx.blockSize = fileParameters.BlockSize
	vhdx.hasParent = fileParameters.HasParent
	vhdx.sectorSize = vhdx.metadata.lookup[LOGICAL_SECTOR_SIZE_GUID].(uint32)
	id := vhdx.metadata.lookup[VIRTUAL_DISK_ID_GUID].(uuid.UUID)
	vhdx.id = newUUIDFromBytesLE(id[:])
	vhdx.sectorsPerBlock = int(vhdx.blockSize / vhdx.sectorSize)
	vhdx.chunkRatio = (int64(math.Pow(2, 23)) * int64(vhdx.sectorSize)) / int64(vhdx.blockSize)

	// Handle parent locator if exists
	if vhdx.hasParent {
		parentLocatorEntry := vhdx.metadata.lookup[PARENT_LOCATOR_GUID].(*ParentLocator)
		if !bytes.Equal(parentLocatorEntry.typeID[:], VHDX_PARENT_LOCATOR_GUID[:]) {
			return nil, fmt.Errorf("unknown parent locator type: %v", parentLocatorEntry.typeID)
		}
		parent, err := openParent(parentLocatorEntry.entries)
		if err != nil {
			return nil, err
		}
		vhdx.parent = parent
	}

	// Read BAT
	batEntry, ok := vhdx.regionTable.lookup[BAT_REGION_GUID]
	if !ok {
		return nil, errors.New("missing required region: BAT")
	}
	vhdx.bat = NewBlockAllocationTable(vhdx, int64(batEntry.FileOffset))

	return vhdx, nil
}

func (vhdx *VHDX) ReadSectors(sector int64, count int64) ([]byte, error) {
	var sectorsRead bytes.Buffer

	for count > 0 {
		readCount := min64(count, int64(vhdx.sectorsPerBlock))
		readSize := readCount * int64(vhdx.sectorSize)
		block, sectorInBlock := divmod(sector, int64(vhdx.sectorsPerBlock))
		batEntry, err := vhdx.bat.pb(block)
		if err != nil {
			return nil, err
		}

		switch batEntry.State {
		case PAYLOAD_BLOCK_NOT_PRESENT:
			if vhdx.parent != nil {
				parentData, err := vhdx.parent.ReadSectors(sector, readCount)
				if err != nil {
					return nil, err
				}
				sectorsRead.Write(parentData)
			} else {
				sectorsRead.Write(make([]byte, readSize))
			}
		case PAYLOAD_BLOCK_UNDEFINED, PAYLOAD_BLOCK_ZERO, PAYLOAD_BLOCK_UNMAPPED:
			sectorsRead.Write(bytes.Repeat([]byte{0x00}, int(readSize)))
		case PAYLOAD_BLOCK_FULLY_PRESENT:
			offset := int64((batEntry.FileOffsetMb * MB)) + sectorInBlock*int64(vhdx.sectorSize)
			_, err := vhdx.fh.Seek(offset, io.SeekStart)
			if err != nil {
				return nil, err
			}
			data := make([]byte, readSize)
			_, err = vhdx.fh.Read(data)
			if err != nil {
				return nil, err
			}
			sectorsRead.Write(data)
		case PAYLOAD_BLOCK_PARTIALLY_PRESENT:
			sectorBitmapEntry, err := vhdx.bat.sb(block)
			if err != nil {
				return nil, err
			}

			blockInChunk := block % vhdx.chunkRatio
			sectorInChunk := (blockInChunk * int64(vhdx.sectorsPerBlock)) + sectorInBlock
			byteIdx, bitIdx := divmod(sectorInChunk, 8)

			off := int64(sectorBitmapEntry.FileOffsetMb * MB)
			if _, err := vhdx.fh.Seek(off+int64(byteIdx), 0); err != nil {
				return nil, err
			}
			sectorBitmap := make([]byte, (readCount+8-1)/8)
			if _, err := vhdx.fh.Read(sectorBitmap); err != nil {
				return nil, err
			}

			relativeSector := int64(0)
			iterator := partialRunIter(sectorBitmap, bitIdx, int64(readCount))
			for iterator.Next() {
				run := iterator.value
				if run.Type == 0 {
					parentData, err := vhdx.parent.ReadSectors(sector+relativeSector, run.Count)
					if err != nil {
						return nil, err
					}
					sectorsRead.Write(parentData)
				} else {
					boff := batEntry.FileOffsetMb * MB
					sec := (sectorInBlock + relativeSector) * int64(vhdx.sectorSize)
					_, err := vhdx.fh.Seek(int64(boff)+int64(sec), io.SeekStart)
					if err != nil {
						return nil, err
					}
					data := make([]byte, run.Count*int64(vhdx.sectorSize))
					_, err = vhdx.fh.Read(data)
					if err != nil {
						return nil, err
					}
					sectorsRead.Write(data)
				}
				relativeSector += run.Count
			}
		}

		sector += readCount
		count -= readCount
	}

	return sectorsRead.Bytes(), nil
}

func (v *VHDX) Size() uint64 {
	return v.size
}

func (v *VHDX) ReadAt(p []byte, offset int64) (int, error) {
	sector := offset / int64(v.sectorSize)
	offsetInSector := int(offset % int64(v.sectorSize))
	totalLength := len(p)
	readData := make([]byte, 0, totalLength)

	for totalLength > 0 {
		sectorCount := (totalLength + offsetInSector + int(v.sectorSize) - 1) / int(v.sectorSize)
		data, err := v.ReadSectors(sector, int64(sectorCount))
		if err != nil {
			return 0, err
		}
		if len(data) < offsetInSector {
			return 0, io.EOF
		}

		data = data[offsetInSector:]
		bytesToRead := min32(len(data), totalLength)

		readData = append(readData, data[:bytesToRead]...)
		totalLength -= bytesToRead
		sector += int64(sectorCount)
		offsetInSector = 0
	}

	copy(p, readData)
	return len(readData), nil
}

func openParent(locator map[string]string) (*VHDX, error) {
	fp := strings.ReplaceAll(locator["relative_path"], "\\", "/")
	fhp, err := FileAccessor(fp)
	if err == nil {
		return NewVHDX(fhp)
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	fp = filepath.Join("/", strings.ReplaceAll(locator["absolute_win32_path"], "\\", "/"))
	fhp, err = FileAccessor(fp)
	if err != nil {
		return nil, err
	}

	return NewVHDX(fhp)
}
