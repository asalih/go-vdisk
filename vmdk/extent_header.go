package vmdk

import (
	"encoding/binary"
	"fmt"
	"io"
)

type RawSparseExtentHeader interface {
	IsCompressed() bool
	GetGrainSize() uint64
	GrainOffset() uint64
	IsEmeddedLBA() bool
}

// VMDKSparseExtentHeader struct
type VMDKSparseExtentHeader struct {
	Magic                         [4]byte
	Version                       uint32
	Flags                         uint32
	Capacity                      uint64
	GrainSize                     uint64
	DescriptorOffset              uint64
	DescriptorSize                uint64
	NumGrainTableEntries          uint32
	SecondaryGrainDirectoryOffset uint64
	PrimaryGrainDirectoryOffset   uint64
	Overhead                      uint64
	IsDirty                       uint8
	SingleEndLineChar             byte
	NonEndLineChar                byte
	DoubleEndLineChars            [2]byte
	CompressAlgorithm             uint16
	Pad                           [433]byte
}

func (r *VMDKSparseExtentHeader) IsCompressed() bool {
	return r.Flags&SPARSEFLAG_COMPRESSED != 0
}
func (r *VMDKSparseExtentHeader) IsEmeddedLBA() bool {
	return r.Flags&SPARSEFLAG_EMBEDDED_LBA != 0
}
func (r *VMDKSparseExtentHeader) GetGrainSize() uint64 {
	return r.GrainSize
}
func (r *VMDKSparseExtentHeader) GetCapacity() uint64 {
	return r.Capacity
}
func (r *VMDKSparseExtentHeader) GrainOffset() uint64 {
	return r.PrimaryGrainDirectoryOffset
}

// COWDSparseExtentHeader struct
type COWDSparseExtentHeader struct {
	Magic                       [4]byte
	Version                     uint32
	Flags                       uint32
	Capacity                    uint32
	GrainSize                   uint32
	PrimaryGrainDirectoryOffset uint32
	NumGrainDirectoryEntries    uint32
	NextFreeGrain               uint32
}

func (r *COWDSparseExtentHeader) IsCompressed() bool {
	return r.Flags&SPARSEFLAG_COMPRESSED != 0
}
func (r *COWDSparseExtentHeader) IsEmeddedLBA() bool {
	return r.Flags&SPARSEFLAG_EMBEDDED_LBA != 0
}
func (r *COWDSparseExtentHeader) GetGrainSize() uint64 {
	return uint64(r.GrainSize)
}
func (r *COWDSparseExtentHeader) GetCapacity() uint64 {
	return uint64(r.Capacity)
}
func (r *COWDSparseExtentHeader) GrainOffset() uint64 {
	return uint64(r.PrimaryGrainDirectoryOffset)
}

// VMDKSESparseConstHeader struct
type VMDKSESparseConstHeader struct {
	Magic                uint64
	Version              uint64
	Capacity             uint64
	GrainSize            uint64
	GrainTableSize       uint64
	Flags                uint64
	Reserved1            uint64
	Reserved2            uint64
	Reserved3            uint64
	Reserved4            uint64
	VolatileHeaderOffset uint64
	VolatileHeaderSize   uint64
	JournalHeaderOffset  uint64
	JournalHeaderSize    uint64
	JournalOffset        uint64
	JournalSize          uint64
	GrainDirectoryOffset uint64
	GrainDirectorySize   uint64
	GrainTablesOffset    uint64
	GrainTablesSize      uint64
	FreeBitmapOffset     uint64
	FreeBitmapSize       uint64
	BackmapOffset        uint64
	BackmapSize          uint64
	GrainsOffset         uint64
	GrainsSize           uint64
	Pad                  [304]byte
}

func (r *VMDKSESparseConstHeader) IsCompressed() bool {
	return r.Flags&SPARSEFLAG_COMPRESSED != 0
}
func (r *VMDKSESparseConstHeader) IsEmeddedLBA() bool {
	return r.Flags&SPARSEFLAG_EMBEDDED_LBA != 0
}
func (r *VMDKSESparseConstHeader) GetGrainSize() uint64 {
	return r.GrainSize
}
func (r *VMDKSESparseConstHeader) GetCapacity() uint64 {
	return uint64(r.Capacity)
}
func (r *VMDKSESparseConstHeader) GrainOffset() uint64 {
	return r.GrainsOffset
}

type SparseExtentHeader struct {
	Magic string
	Flags uint64

	Raw RawSparseExtentHeader
}

// ParseSparseExtentHeader function
func ParseSparseExtentHeader(fh io.ReadSeeker) (*SparseExtentHeader, error) {
	magic := make([]byte, 4)
	_, err := fh.Read(magic)
	if err != nil {
		return nil, err
	}
	//revert
	_, err = fh.Seek(-4, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	switch string(magic) {
	case VMDK_MAGIC:
		var hdr VMDKSparseExtentHeader
		err := binary.Read(fh, binary.LittleEndian, &hdr)
		if err != nil {
			return nil, err
		}
		return &SparseExtentHeader{
			Magic: VMDK_MAGIC,
			Raw:   &hdr,
		}, nil
	case SESPARSE_MAGIC:
		var hdr VMDKSESparseConstHeader
		err := binary.Read(fh, binary.LittleEndian, &hdr)
		if err != nil {
			return nil, err
		}
		return &SparseExtentHeader{
			Magic: SESPARSE_MAGIC,
			Raw:   &hdr,
		}, nil
	case COWD_MAGIC:
		var hdr COWDSparseExtentHeader
		err := binary.Read(fh, binary.LittleEndian, &hdr)
		if err != nil {
			return nil, err
		}
		return &SparseExtentHeader{
			Magic: COWD_MAGIC,
			Raw:   &hdr,
		}, nil
	default:
		return nil, fmt.Errorf("sparse extent not supported: %s", magic)
	}
}

func (s *SparseExtentHeader) AsVMDKSES() (*VMDKSESparseConstHeader, bool) {
	rh, ok := s.Raw.(*VMDKSESparseConstHeader)
	return rh, ok
}

func (s *SparseExtentHeader) AsCOWD() (*COWDSparseExtentHeader, bool) {
	rh, ok := s.Raw.(*COWDSparseExtentHeader)
	return rh, ok
}
func (s *SparseExtentHeader) AsVMDK() (*VMDKSparseExtentHeader, bool) {
	rh, ok := s.Raw.(*VMDKSparseExtentHeader)
	return rh, ok
}
