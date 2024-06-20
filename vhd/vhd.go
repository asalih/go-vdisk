package vhd

import (
	"bytes"
	"encoding/binary"
	"io"
)

const VHD_MAGIC = "conectix"

type disk interface {
	ReadSectors(sector int64, count int) ([]byte, error)
}

type VHD struct {
	disk disk
	size int64
}

func NewVHD(fh io.ReadSeeker) (*VHD, error) {
	footer, err := readFooter(fh)
	if err != nil {
		return nil, err
	}

	var diskItem disk
	if footer.DataOffset == 0xFFFFFFFFFFFFFFFF {
		diskItem = NewFixedDisk(fh, footer)
	} else {
		diskItem, err = NewDynamicDisk(fh, footer)
		if err != nil {
			return nil, err
		}
	}

	return &VHD{disk: diskItem, size: int64(footer.CurrentSize)}, nil
}

func (v *VHD) ReadAt(p []byte, offset int64) (int, error) {
	sector := offset / SECTOR_SIZE
	offsetInSector := int(offset % SECTOR_SIZE)
	totalLength := len(p)
	readData := make([]byte, 0, totalLength)

	for totalLength > 0 {
		sectorCount := (totalLength + offsetInSector + SECTOR_SIZE - 1) / SECTOR_SIZE
		data, err := v.disk.ReadSectors(sector, sectorCount)
		if err != nil {
			return 0, err
		}
		if len(data) < offsetInSector {
			return 0, io.EOF
		}

		data = data[offsetInSector:]
		bytesToRead := min(len(data), totalLength)

		readData = append(readData, data[:bytesToRead]...)
		totalLength -= bytesToRead
		sector += int64(sectorCount)
		offsetInSector = 0
	}

	copy(p, readData)
	return len(readData), nil
}

func (v *VHD) Size() int64 {
	return v.size
}

type FixedDisk struct {
	fh     io.ReadSeeker
	footer *Footer
}

func NewFixedDisk(fh io.ReadSeeker, footer *Footer) *FixedDisk {
	return &FixedDisk{fh: fh, footer: footer}
}

func (d *FixedDisk) ReadSectors(sector int64, count int) ([]byte, error) {
	buf := make([]byte, count*SECTOR_SIZE)
	d.fh.Seek(int64(sector*SECTOR_SIZE), io.SeekStart)
	_, err := d.fh.Read(buf)
	return buf, err
}

type DynamicDisk struct {
	fh               io.ReadSeeker
	footer           *Footer
	header           *DynamicHeader
	bat              *BlockAllocationTable
	sectorsPerBlock  int
	sectorBitmapSize int
}

func NewDynamicDisk(fh io.ReadSeeker, footer *Footer) (*DynamicDisk, error) {
	d := &DynamicDisk{fh: fh, footer: footer}
	fh.Seek(int64(footer.DataOffset), io.SeekStart)
	header := &DynamicHeader{}
	if err := binary.Read(fh, binary.BigEndian, header); err != nil {
		return nil, err
	}
	d.header = header
	d.bat = NewBlockAllocationTable(fh, int64(header.TableOffset), int64(header.MaxTableEntries))
	d.sectorsPerBlock = int(header.BlockSize) / SECTOR_SIZE
	d.sectorBitmapSize = ((d.sectorsPerBlock / 8) + SECTOR_SIZE - 1) / SECTOR_SIZE
	return d, nil
}

func (d *DynamicDisk) ReadSectors(sector int64, count int) ([]byte, error) {
	var result bytes.Buffer
	for count > 0 {
		block, offset := sector/int64(d.sectorsPerBlock), sector%int64(d.sectorsPerBlock)
		blockRemaining := int64(d.sectorsPerBlock) - offset
		readCount := min(count, int(blockRemaining))
		sectorOffset, err := d.bat.Get(block)
		if err != nil {
			return nil, err
		}

		boff := int64(sectorOffset) + int64(d.sectorBitmapSize) + offset
		_, err = d.fh.Seek(int64(boff*SECTOR_SIZE), io.SeekStart)
		if err != nil {
			return nil, err
		}

		buf := make([]byte, readCount*SECTOR_SIZE)
		_, err = d.fh.Read(buf)
		if err != nil {
			return nil, err
		}

		_, err = result.Write(buf)
		if err != nil {
			return nil, err
		}

		sector += int64(readCount)
		count -= readCount
	}
	return result.Bytes(), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
