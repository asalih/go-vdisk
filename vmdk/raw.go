package vmdk

import "io"

type RawDisk struct {
	fh           io.ReadSeeker
	size         int64
	offset       int64
	sectorOffset int64
}

func NewRawDisk(fh io.ReadSeeker, size int64) (*RawDisk, error) {
	rd := &RawDisk{fh: fh}
	if size == 0 {

	} else {
		rd.size = size
	}
	rd.sectorOffset = rd.size / int64(SECTOR_SIZE)
	return rd, nil
}

func (rd *RawDisk) ReadSectors(sector int64, count int) ([]byte, error) {
	offset := (int64(sector) - rd.sectorOffset) * SECTOR_SIZE
	_, err := rd.fh.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, count*SECTOR_SIZE)
	_, err = rd.fh.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (rd *RawDisk) GetSize() int64 {
	return rd.size
}

func (rd *RawDisk) GetSectorCount() int64 {
	return rd.size / SECTOR_SIZE
}

func (rd *RawDisk) GetSectorOffset() int64 {
	return rd.sectorOffset
}

func (rd *RawDisk) SetOffset(offset, sectorOffset int64) {
	rd.offset = offset
	rd.sectorOffset = sectorOffset
}
