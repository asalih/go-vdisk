package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/asalih/go-vdisk/vhdx"
)

func runVHDXStructureAnalysis(sourcePath string) {
	if sourcePath == "" {
		log.Fatal("Please provide -source flag")
	}

	fmt.Println("=== VHDX Structure Deep Analysis ===")
	fmt.Println()

	// Open raw file to examine structure
	rawFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer rawFile.Close()

	// Read VHDX regions from raw file
	fmt.Println("=== Reading VHDX Metadata from Raw File ===")

	// Read region table at 0x30000 (192KB)
	regionTableOffset := int64(0x30000)
	fmt.Printf("\nRegion Table at offset 0x%X:\n", regionTableOffset)

	regionBuf := make([]byte, 64*1024)
	_, err = rawFile.ReadAt(regionBuf, regionTableOffset)
	if err != nil {
		log.Fatalf("Error reading region table: %v", err)
	}

	signature := string(regionBuf[0:4])
	fmt.Printf("  Signature: %q\n", signature)

	if signature == "regi" {
		entryCount := binary.LittleEndian.Uint32(regionBuf[8:12])
		fmt.Printf("  Entry Count: %d\n", entryCount)

		// Parse region entries
		for i := uint32(0); i < entryCount; i++ {
			offset := 16 + (i * 32)
			entry := regionBuf[offset : offset+32]

			// GUID (16 bytes)
			guid := entry[0:16]
			fileOffset := binary.LittleEndian.Uint64(entry[16:24])
			length := binary.LittleEndian.Uint32(entry[24:28])
			required := binary.LittleEndian.Uint32(entry[28:32])

			fmt.Printf("\n  Region %d:\n", i+1)
			fmt.Printf("    GUID: %X-%X-%X-%X-%X\n",
				guid[0:4], guid[4:6], guid[6:8], guid[8:10], guid[10:16])
			fmt.Printf("    File Offset: 0x%X (%d MB)\n", fileOffset, fileOffset/(1024*1024))
			fmt.Printf("    Length: 0x%X (%d MB)\n", length, length/(1024*1024))
			fmt.Printf("    Required: %d\n", required)

			// Check if this is BAT or Metadata region
			batGUID := []byte{0x66, 0x77, 0xC2, 0x2D, 0x23, 0xF6, 0x00, 0x42,
				0x9D, 0x64, 0x11, 0x5E, 0x9B, 0xFD, 0x4A, 0x08}
			metadataGUID := []byte{0x06, 0xA2, 0x7C, 0x8B, 0x90, 0x47, 0x9A, 0x4B,
				0xB8, 0xFE, 0x57, 0x5F, 0x05, 0x0F, 0x88, 0x6E}

			if bytesEqual(guid, batGUID) {
				fmt.Printf("    Type: BAT (Block Allocation Table)\n")
				analyzeBATFromRaw(rawFile, int64(fileOffset), int64(length))
			} else if bytesEqual(guid, metadataGUID) {
				fmt.Printf("    Type: Metadata Region\n")
			}
		}
	}

	// Now check with VHDX parser
	fmt.Println("\n=== Comparing with VHDX Parser ===")

	vFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("Error opening VHDX: %v", err)
	}
	defer vFile.Close()

	vhdx.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join(filepath.Dir(sourcePath), s))
	}

	vhdxImage, err := vhdx.NewVHDX(vFile)
	if err != nil {
		log.Fatalf("Error parsing VHDX: %v", err)
	}

	fmt.Printf("Virtual Disk Size: %d bytes\n", vhdxImage.Size())

	// Try to read where we found NTFS in raw file
	ntfsRawOffset := int64(20971520)
	fmt.Printf("\n=== NTFS Boot Sector Found in Raw File ===\n")
	fmt.Printf("Raw file offset: 0x%X (%d bytes, %.2f MB)\n",
		ntfsRawOffset, ntfsRawOffset, float64(ntfsRawOffset)/(1024*1024))

	// Read from raw file
	rawBuf := make([]byte, 512)
	_, err = rawFile.ReadAt(rawBuf, ntfsRawOffset)
	if err != nil {
		log.Fatalf("Error reading raw: %v", err)
	}

	fmt.Printf("Raw file signature: %q\n", rawBuf[3:11])

	// Now try to figure out what virtual sector this corresponds to
	// by reading data through the VHDX parser at various offsets
	fmt.Println("\n=== Searching for NTFS via VHDX Parser ===")

	// Try common partition start locations
	testOffsets := []int64{
		0,             // Sector 0
		512,           // Sector 1
		1024,          // Sector 2
		2048 * 512,    // 1MB (common GPT partition start)
		4096 * 512,    // 2MB
		8192 * 512,    // 4MB
		16384 * 512,   // 8MB
		32768 * 512,   // 16MB
		65536 * 512,   // 32MB
		ntfsRawOffset, // Try the raw offset directly
	}

	for _, offset := range testOffsets {
		buf := make([]byte, 512)
		n, err := vhdxImage.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			continue
		}
		if n > 11 && string(buf[3:11]) == "NTFS    " {
			fmt.Printf("✓ NTFS found at virtual offset 0x%X (sector %d)\n",
				offset, offset/512)
			fmt.Printf("  Bytes per sector: %d\n", binary.LittleEndian.Uint16(buf[11:13]))
			fmt.Printf("  Sectors per cluster: %d\n", buf[13])
			return
		}
	}

	fmt.Println("✗ NTFS not found through VHDX parser at common locations")
	fmt.Println("\n⚠️  This suggests the VHDX parser is not correctly mapping")
	fmt.Println("    the raw file blocks to virtual disk sectors.")
}

func analyzeBATFromRaw(f *os.File, offset int64, length int64) {
	fmt.Println("\n    Analyzing BAT entries:")

	// Read first 4KB of BAT
	batBuf := make([]byte, 4096)
	n, err := f.ReadAt(batBuf, offset)
	if err != nil && err != io.EOF {
		fmt.Printf("      Error reading BAT: %v\n", err)
		return
	}

	// Each BAT entry is 8 bytes
	numEntries := n / 8
	nonZeroEntries := 0

	for i := 0; i < numEntries; i++ {
		entryOffset := i * 8
		entry := binary.LittleEndian.Uint64(batBuf[entryOffset : entryOffset+8])

		if entry != 0 {
			nonZeroEntries++
			if nonZeroEntries <= 10 { // Show first 10 non-zero entries
				state := entry & 0x7
				fileOffsetMB := (entry >> 20) & 0xFFFFFFFFFFF

				stateNames := []string{
					"NOT_PRESENT", "UNDEFINED", "ZERO", "UNMAPPED",
					"RESERVED_4", "RESERVED_5", "FULLY_PRESENT", "PARTIALLY_PRESENT",
				}

				fmt.Printf("      Entry %d: State=%s, FileOffsetMB=%d (0x%X bytes)\n",
					i, stateNames[state], fileOffsetMB, fileOffsetMB*1024*1024)
			}
		}
	}

	fmt.Printf("    Total BAT entries examined: %d\n", numEntries)
	fmt.Printf("    Non-zero entries: %d\n", nonZeroEntries)

	if nonZeroEntries == 0 {
		fmt.Println("    ⚠️  All BAT entries are zero!")
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
