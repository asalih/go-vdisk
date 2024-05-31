package vmdk

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

type Disk interface {
	ReadSectors(sector int64, count int) ([]byte, error)
	GetSize() int64
	GetSectorCount() int64
	GetSectorOffset() int64
	SetOffset(offset, sectorOffset int64)
}

type VMDK struct {
	Disks       []Disk
	Parent      *VMDK
	Descriptor  *DiskDescriptor
	DiskOffsets []int64
	SectorCount int64
	Size        int64
}

type FileAccessorFn func(string) (io.ReadSeeker, error)

var FileAccessor FileAccessorFn

var ErrFileAccessorNotAvailable = errors.New("file accessor needed to access for parent and extents from file")

func NewVMDK(fhs []io.ReadSeeker) (*VMDK, error) {
	if FileAccessor == nil {
		return nil, ErrFileAccessorNotAvailable
	}

	vmdk := &VMDK{}
	for _, fh := range fhs {
		magic := make([]byte, 4)
		_, err := fh.Read(magic)
		if err != nil {
			return nil, err
		}
		_, err = fh.Seek(-4, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		switch string(magic) {
		case CONFIG_FILE_MAGIC:
			if len(fhs) != 1 {
				continue
			}
			// Handle Disk Descriptor case
			data, err := io.ReadAll(fh)
			if err != nil {
				return nil, err
			}
			vmdk.Descriptor, err = ParseDiskDescriptor(string(data))
			if err != nil {
				return nil, err
			}
			if vmdk.Descriptor.Attr["parentCID"] != "ffffffff" {
				vmdk.Parent, err = openParent(vmdk.Descriptor.Attr["parentFileNameHint"])
				if err != nil {
					return nil, err
				}
			}
			for _, extent := range vmdk.Descriptor.Extents {
				extentFile, err := FileAccessor(extent.Filename)
				if err != nil {
					return nil, err
				}
				switch extent.ExtentType {
				case "SPARSE", "VMFSSPARSE", "SESPARSE":
					sd, err := NewSparseDisk(extentFile, vmdk.Parent)
					if err != nil {
						return nil, err
					}
					vmdk.Disks = append(vmdk.Disks, sd)
				case "VMFS", "FLAT":
					rd, err := NewRawDisk(extentFile, extent.Size*SECTOR_SIZE)
					if err != nil {
						return nil, err
					}
					vmdk.Disks = append(vmdk.Disks, rd)
				}
			}
		case COWD_MAGIC, VMDK_MAGIC, SESPARSE_MAGIC:
			sparseDisk, err := NewSparseDisk(fh, nil)
			if err != nil {
				return nil, err
			}
			if sparseDisk.descriptor != nil && sparseDisk.descriptor.Attr["parentCID"] != "ffffffff" {
				sparseDisk.parent, err = openParent(sparseDisk.descriptor.Attr["parentFileNameHint"])
				if err != nil {
					return nil, err
				}
			}
			vmdk.Disks = append(vmdk.Disks, sparseDisk)
		default:
			rd, err := NewRawDisk(fh, 0)
			if err != nil {
				return nil, err
			}
			vmdk.Disks = append(vmdk.Disks, rd)
		}
	}

	var size int64
	for _, disk := range vmdk.Disks {
		if size != 0 {
			vmdk.DiskOffsets = append(vmdk.DiskOffsets, vmdk.SectorCount)
		}
		disk.SetOffset(size, vmdk.SectorCount)
		size += disk.GetSize()
		vmdk.SectorCount += disk.GetSectorCount()
	}

	vmdk.Size = size

	return vmdk, nil
}

func (v *VMDK) ReadSectors(sector int64, count int) ([]byte, error) {
	var sectorsRead []byte

	diskIdx := bisectRight(v.DiskOffsets, int64(sector))

	for count > 0 {
		if diskIdx >= len(v.Disks) {
			return nil, fmt.Errorf("out of bounds disk, disk count: %v, requested: %v", sector, count)
		}
		disk := v.Disks[diskIdx]
		diskRemainingSectors := disk.GetSectorCount() - (int64(sector) - disk.GetSectorOffset())
		diskSectors := min32(int(diskRemainingSectors), count)
		sectorData, err := disk.ReadSectors(sector, diskSectors)
		if err != nil {
			return nil, err
		}
		sectorsRead = append(sectorsRead, sectorData...)

		sector += int64(diskSectors)
		count -= diskSectors
		diskIdx++
	}

	return sectorsRead, nil
}

func (v *VMDK) ReadAt(p []byte, offset int64) (int, error) {
	sector := offset / SECTOR_SIZE
	offsetInSector := int(offset % SECTOR_SIZE)
	totalLength := len(p)
	readData := make([]byte, 0, totalLength)

	for totalLength > 0 {
		sectorCount := (totalLength + offsetInSector + SECTOR_SIZE - 1) / SECTOR_SIZE
		data, err := v.ReadSectors(sector, sectorCount)
		if err != nil {
			return 0, err
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

func openParent(filenameHint string) (*VMDK, error) {
	filenameHint = strings.ReplaceAll(filenameHint, "\\", "/")
	parentFh, err := FileAccessor(filenameHint)
	if err != nil {
		return nil, err
	}

	return NewVMDK([]io.ReadSeeker{parentFh})
}
