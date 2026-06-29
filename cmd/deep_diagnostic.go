package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/asalih/go-vdisk/vhdx"
	ntfs_parser "www.velocidex.com/golang/go-ntfs/parser"
)

func runVHDXDeepDiagnostic(sourcePath string) {
	if sourcePath == "" {
		log.Fatal("Please provide -source flag")
	}

	// Open VHDX
	vFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("Error opening VHDX file: %v", err)
	}
	defer vFile.Close()

	vhdx.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join(filepath.Dir(sourcePath), s))
	}

	vhdxImage, err := vhdx.NewVHDX(vFile)
	if err != nil {
		log.Fatalf("Error parsing VHDX: %v", err)
	}

	fmt.Println("=== VHDX Information ===")
	fmt.Printf("Virtual Disk Size: %d bytes (%.2f GB)\n", vhdxImage.Size(), float64(vhdxImage.Size())/(1024*1024*1024))
	fmt.Println()

	// Read and dump multiple sectors to see what's actually there
	fmt.Println("=== Raw Sector Analysis ===")
	checkSector(vhdxImage, 0, "Sector 0 (MBR location)")
	checkSector(vhdxImage, 1, "Sector 1 (GPT header location)")
	checkSector(vhdxImage, 2, "Sector 2 (GPT partition entries start)")
	checkSector(vhdxImage, 2048, "Sector 2048 (Common first partition start - 1MB)")
	checkSector(vhdxImage, 4096, "Sector 4096 (Alternative partition start - 2MB)")
	fmt.Println()

	// Check for GPT specifically
	fmt.Println("=== GPT Partition Table Check ===")
	checkGPT(vhdxImage)
	fmt.Println()

	// Check for MBR
	fmt.Println("=== MBR Partition Table Check ===")
	checkMBR(vhdxImage)
	fmt.Println()

	// Try to find NTFS signature anywhere in the first 10MB
	fmt.Println("=== Scanning for NTFS Signatures (first 10MB) ===")
	scanForNTFS(vhdxImage)
	fmt.Println()

	// Try to use go-ntfs to parse
	fmt.Println("=== Attempting NTFS Parse with go-ntfs ===")
	tryNTFSParse(vhdxImage)
}

func checkSector(r io.ReaderAt, sectorNum int64, description string) {
	buf := make([]byte, 512)
	offset := sectorNum * 512
	n, err := r.ReadAt(buf, offset)

	fmt.Printf("\n%s (offset 0x%X):\n", description, offset)
	if err != nil && err != io.EOF {
		fmt.Printf("  Error reading: %v\n", err)
		return
	}
	if n == 0 {
		fmt.Printf("  No data read\n")
		return
	}

	// Check if all zeros
	allZero := true
	for _, b := range buf[:n] {
		if b != 0 {
			allZero = false
			break
		}
	}

	if allZero {
		fmt.Printf("  ⚠️  All zeros - sector appears empty\n")
	} else {
		// Show first 64 bytes
		fmt.Printf("  First 64 bytes:\n")
		fmt.Println(hex.Dump(buf[:64]))

		// Check for specific signatures
		if n >= 8 && string(buf[0:8]) == "EFI PART" {
			fmt.Printf("  ✓ GPT signature detected!\n")
		}
		if n >= 512 && buf[510] == 0x55 && buf[511] == 0xAA {
			fmt.Printf("  ✓ MBR signature detected (0x55AA at offset 510-511)\n")
		}
		if n >= 11 && string(buf[3:11]) == "NTFS    " {
			fmt.Printf("  ✓ NTFS signature detected!\n")
		}
	}
}

func checkGPT(r io.ReaderAt) {
	// GPT header is at LBA 1 (sector 1, offset 512)
	buf := make([]byte, 512)
	_, err := r.ReadAt(buf, 512)
	if err != nil {
		fmt.Printf("Error reading GPT header location: %v\n", err)
		return
	}

	// Check for GPT signature
	if string(buf[0:8]) == "EFI PART" {
		fmt.Printf("✓ GPT Header Found at LBA 1!\n")

		// Parse GPT header
		revision := binary.LittleEndian.Uint32(buf[8:12])
		headerSize := binary.LittleEndian.Uint32(buf[12:16])
		headerCRC := binary.LittleEndian.Uint32(buf[16:20])
		currentLBA := binary.LittleEndian.Uint64(buf[24:32])
		backupLBA := binary.LittleEndian.Uint64(buf[32:40])
		firstUsableLBA := binary.LittleEndian.Uint64(buf[40:48])
		lastUsableLBA := binary.LittleEndian.Uint64(buf[48:56])
		partEntryLBA := binary.LittleEndian.Uint64(buf[72:80])
		numPartEntries := binary.LittleEndian.Uint32(buf[80:84])
		partEntrySize := binary.LittleEndian.Uint32(buf[84:88])

		fmt.Printf("  Revision: %d.%d\n", revision>>16, revision&0xFFFF)
		fmt.Printf("  Header Size: %d bytes\n", headerSize)
		fmt.Printf("  Header CRC32: 0x%08X\n", headerCRC)
		fmt.Printf("  Current LBA: %d\n", currentLBA)
		fmt.Printf("  Backup LBA: %d\n", backupLBA)
		fmt.Printf("  First Usable LBA: %d\n", firstUsableLBA)
		fmt.Printf("  Last Usable LBA: %d\n", lastUsableLBA)
		fmt.Printf("  Partition Entry LBA: %d\n", partEntryLBA)
		fmt.Printf("  Number of Partition Entries: %d\n", numPartEntries)
		fmt.Printf("  Partition Entry Size: %d bytes\n", partEntrySize)

		// Read partition entries
		fmt.Printf("\n  Reading GPT Partition Entries from LBA %d...\n", partEntryLBA)
		partBuf := make([]byte, int(numPartEntries)*int(partEntrySize))
		_, err := r.ReadAt(partBuf, int64(partEntryLBA)*512)
		if err != nil {
			fmt.Printf("  Error reading partition entries: %v\n", err)
			return
		}

		// Parse each partition entry
		validPartitions := 0
		for i := uint32(0); i < numPartEntries; i++ {
			offset := int(i * partEntrySize)
			entry := partBuf[offset : offset+int(partEntrySize)]

			// Check if partition type GUID is non-zero (indicates valid partition)
			isValid := false
			for _, b := range entry[0:16] {
				if b != 0 {
					isValid = true
					break
				}
			}

			if isValid {
				validPartitions++
				firstLBA := binary.LittleEndian.Uint64(entry[32:40])
				lastLBA := binary.LittleEndian.Uint64(entry[40:48])
				size := (lastLBA - firstLBA + 1) * 512

				fmt.Printf("\n  Partition %d:\n", validPartitions)
				fmt.Printf("    First LBA: %d (offset 0x%X)\n", firstLBA, firstLBA*512)
				fmt.Printf("    Last LBA: %d\n", lastLBA)
				fmt.Printf("    Size: %d bytes (%.2f MB)\n", size, float64(size)/(1024*1024))

				// Try to read the partition boot sector
				partBootSector := make([]byte, 512)
				_, err := r.ReadAt(partBootSector, int64(firstLBA)*512)
				if err == nil {
					if string(partBootSector[3:11]) == "NTFS    " {
						fmt.Printf("    Filesystem: NTFS ✓\n")
					} else if string(partBootSector[54:59]) == "FAT32" {
						fmt.Printf("    Filesystem: FAT32 ✓\n")
					} else {
						fmt.Printf("    Filesystem: Unknown (signature: %q)\n", partBootSector[3:11])
					}
				}
			}
		}

		if validPartitions == 0 {
			fmt.Printf("\n  ⚠️  No valid partitions found in GPT\n")
		}
	} else {
		fmt.Printf("✗ No GPT signature at LBA 1\n")
		fmt.Printf("  Found instead: %q\n", string(buf[0:8]))
		fmt.Printf("  First 32 bytes: % X\n", buf[0:32])
	}
}

func checkMBR(r io.ReaderAt) {
	buf := make([]byte, 512)
	_, err := r.ReadAt(buf, 0)
	if err != nil {
		fmt.Printf("Error reading MBR: %v\n", err)
		return
	}

	// Check for MBR signature
	if buf[510] == 0x55 && buf[511] == 0xAA {
		fmt.Printf("✓ MBR Signature Found (0x55AA)\n")

		// Check partition table entries (4 entries, 16 bytes each, starting at offset 446)
		hasPartitions := false
		for i := 0; i < 4; i++ {
			offset := 446 + (i * 16)
			entry := buf[offset : offset+16]

			status := entry[0]
			partType := entry[4]

			if partType != 0 {
				hasPartitions = true
				lbaStart := binary.LittleEndian.Uint32(entry[8:12])
				lbaSize := binary.LittleEndian.Uint32(entry[12:16])

				fmt.Printf("\n  Partition %d:\n", i+1)
				fmt.Printf("    Status: 0x%02X %s\n", status, func() string {
					if status == 0x80 {
						return "(Bootable)"
					}
					return ""
				}())
				fmt.Printf("    Type: 0x%02X %s\n", partType, getPartitionTypeName(partType))
				fmt.Printf("    Start LBA: %d (offset 0x%X)\n", lbaStart, uint64(lbaStart)*512)
				fmt.Printf("    Size: %d sectors (%d bytes, %.2f MB)\n", lbaSize, uint64(lbaSize)*512, float64(lbaSize)*512/(1024*1024))
			}
		}

		if !hasPartitions {
			fmt.Printf("\n  ⚠️  MBR signature present but no valid partitions found\n")

			// Check if this is a protective MBR (GPT)
			if buf[450] == 0xEE {
				fmt.Printf("  ℹ️  This appears to be a Protective MBR (GPT disk)\n")
			}
		}
	} else {
		fmt.Printf("✗ No MBR signature at offset 510-511\n")
		fmt.Printf("  Found: 0x%02X%02X (expected 0x55AA)\n", buf[510], buf[511])
	}
}

func getPartitionTypeName(t byte) string {
	types := map[byte]string{
		0x07: "NTFS/exFAT",
		0x0B: "FAT32",
		0x0C: "FAT32 (LBA)",
		0x83: "Linux",
		0xEE: "GPT Protective",
	}
	if name, ok := types[t]; ok {
		return name
	}
	return "Unknown"
}

func scanForNTFS(r io.ReaderAt) {
	maxOffset := int64(10 * 1024 * 1024) // 10MB
	sectorSize := int64(512)
	buf := make([]byte, sectorSize)

	found := false
	for offset := int64(0); offset < maxOffset; offset += sectorSize {
		n, err := r.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			break
		}
		if n < 11 {
			continue
		}

		if string(buf[3:11]) == "NTFS    " {
			fmt.Printf("✓ NTFS signature found at offset 0x%X (sector %d)\n", offset, offset/512)

			// Read NTFS boot sector details
			bytesPerSector := binary.LittleEndian.Uint16(buf[11:13])
			sectorsPerCluster := buf[13]
			totalSectors := binary.LittleEndian.Uint64(buf[40:48])
			mftCluster := binary.LittleEndian.Uint64(buf[48:56])

			fmt.Printf("  Bytes per sector: %d\n", bytesPerSector)
			fmt.Printf("  Sectors per cluster: %d\n", sectorsPerCluster)
			fmt.Printf("  Total sectors: %d\n", totalSectors)
			fmt.Printf("  MFT cluster: %d\n", mftCluster)
			fmt.Printf("  Volume size: %.2f GB\n", float64(totalSectors*uint64(bytesPerSector))/(1024*1024*1024))

			found = true

			// Try to parse with go-ntfs at this offset
			fmt.Printf("\n  Attempting to parse NTFS at this offset...\n")
			tryNTFSParseAtOffset(r, offset)

			fmt.Println()
		}
	}

	if !found {
		fmt.Printf("✗ No NTFS signatures found in first 10MB\n")
	}
}

func tryNTFSParse(r io.ReaderAt) {
	tryNTFSParseAtOffset(r, 0)
}

func tryNTFSParseAtOffset(r io.ReaderAt, offset int64) {
	// Create an offset reader if needed
	var reader io.ReaderAt = r
	if offset > 0 {
		reader = &offsetReaderAt{r: r, offset: offset}
	}

	ntfs, err := ntfs_parser.GetNTFSContext(reader, 0)
	if err != nil {
		fmt.Printf("  ✗ Failed to create NTFS context: %v\n", err)
		return
	}

	fmt.Printf("  ✓ NTFS context created successfully!\n")

	// Try to get root directory
	root, err := ntfs.GetMFT(5) // MFT entry 5 is root directory
	if err != nil {
		fmt.Printf("  ✗ Failed to get root MFT entry: %v\n", err)
		return
	}

	fmt.Printf("  ✓ Root directory MFT entry retrieved!\n")
	fmt.Printf("  ✓ NTFS filesystem is accessible!\n")

	// Try to list root directory
	fmt.Printf("\n  Listing root directory:\n")
	entries := root.Dir(ntfs)
	count := len(entries)

	for i, entry := range entries {
		if i < 20 { // Show first 20 entries
			filename := entry.File().Name()
			fmt.Printf("    - %s\n", filename)
		}
	}

	if count > 20 {
		fmt.Printf("    ... and %d more entries\n", count-20)
	}
	fmt.Printf("  ✓ Total entries: %d\n", count)
}

// offsetReaderAt wraps an io.ReaderAt and adds an offset
type offsetReaderAt struct {
	r      io.ReaderAt
	offset int64
}

func (o *offsetReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	return o.r.ReadAt(p, off+o.offset)
}
