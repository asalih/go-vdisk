package vhdx

import (
	"encoding/binary"
	"io"
)

type Header struct {
	Signature      [4]byte
	Checksum       uint32
	SequenceNumber uint64
	FileWriteGuid  [16]byte
	DataWriteGuid  [16]byte
	LogGuid        [16]byte
	LogVersion     uint16
	Version        uint16
	LogLength      uint32
	LogOffset      uint64
	Reserved       [4096]byte
}

func readHeader(fh io.ReadSeeker, header *Header, offset int64) error {
	if _, err := fh.Seek(offset, 0); err != nil {
		return err
	}
	return binary.Read(fh, binary.LittleEndian, header)
}
