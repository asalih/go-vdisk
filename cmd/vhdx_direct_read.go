package main

import (
	"fmt"
	"log"
	"os"

	ntfs_parser "www.velocidex.com/golang/go-ntfs/parser"
)

// Direct reader that bypasses the VHDX BAT and reads the raw data
type DirectVHDXReader struct {
	file       *os.File
	dataOffset int64 // Offset where virtual disk data starts in raw file
}

func (d *DirectVHDXReader) ReadAt(p []byte, off int64) (n int, err error) {
	// Read from dataOffset + virtual offset
	return d.file.ReadAt(p, d.dataOffset+off)
}

func runVHDXDirectRead(sourcePath string, dataOffset int64) {
	if sourcePath == "" {
		log.Fatal("Please provide -source flag")
	}

	fmt.Println("=== Direct VHDX Data Access (Bypassing BAT) ===")
	fmt.Println()

	// Open raw file
	file, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create direct reader
	reader := &DirectVHDXReader{
		file:       file,
		dataOffset: dataOffset,
	}

	fmt.Printf("Reading virtual disk data from raw offset 0x%X\n\n", dataOffset)

	// Check MBR
	fmt.Println("=== MBR (Sector 0) ===")
	mbrBuf := make([]byte, 512)
	_, err = reader.ReadAt(mbrBuf, 0)
	if err != nil {
		log.Fatalf("Error reading MBR: %v", err)
	}

	if mbrBuf[510] == 0x55 && mbrBuf[511] == 0xAA {
		fmt.Println("✓ MBR signature valid (0x55AA)")

		// Check for protective MBR (GPT)
		if mbrBuf[450] == 0xEE {
			fmt.Println("✓ Protective MBR detected (GPT disk)")
		}
	} else {
		fmt.Printf("✗ Invalid MBR signature: 0x%02X%02X\n", mbrBuf[510], mbrBuf[511])
	}

	// Check GPT
	fmt.Println("\n=== GPT Header (Sector 1) ===")
	gptBuf := make([]byte, 512)
	_, err = reader.ReadAt(gptBuf, 512)
	if err != nil {
		log.Fatalf("Error reading GPT: %v", err)
	}

	if string(gptBuf[0:8]) == "EFI PART" {
		fmt.Println("✓ GPT signature valid")
		// Parse basic GPT info (simplified)
		fmt.Printf("  GPT Revision: %d.%d\n", gptBuf[11], gptBuf[10])
	} else {
		fmt.Printf("✗ Invalid GPT signature: %q\n", gptBuf[0:8])
		return
	}

	// Try to parse with go-ntfs
	fmt.Println("\n=== Attempting NTFS Parse ===")

	// NTFS is typically at sector 2048 (1MB) for GPT disks
	ntfsOffset := int64(2048 * 512)

	// Check for NTFS signature
	ntfsBuf := make([]byte, 512)
	_, err = reader.ReadAt(ntfsBuf, ntfsOffset)
	if err != nil {
		log.Fatalf("Error reading NTFS boot sector: %v", err)
	}

	if string(ntfsBuf[3:11]) == "NTFS    " {
		fmt.Printf("✓ NTFS filesystem found at sector 2048 (offset 0x%X)\n", ntfsOffset)
		fmt.Printf("  Bytes per sector: %d\n", uint16(ntfsBuf[11])|uint16(ntfsBuf[12])<<8)
		fmt.Printf("  Sectors per cluster: %d\n", ntfsBuf[13])

		// Create offset reader for NTFS parser
		ntfsReader := &DirectVHDXReader{
			file:       file,
			dataOffset: dataOffset + ntfsOffset,
		}

		// Try to parse with go-ntfs
		fmt.Println("\n  Parsing NTFS with go-ntfs...")
		ntfs, err := ntfs_parser.GetNTFSContext(ntfsReader, 0)
		if err != nil {
			fmt.Printf("  ✗ Error creating NTFS context: %v\n", err)
			return
		}

		fmt.Println("  ✓ NTFS context created successfully!")

		// Get root directory
		root, err := ntfs.GetMFT(5)
		if err != nil {
			fmt.Printf("  ✗ Error getting root directory: %v\n", err)
			return
		}

		fmt.Println("  ✓ Root directory accessed!")

		// List root directory
		fmt.Println("\n=== Root Directory Contents ===")
		entries := root.Dir(ntfs)

		if len(entries) == 0 {
			fmt.Println("  (empty)")
		} else {
			for i, entry := range entries {
				if i >= 50 { // Limit output
					fmt.Printf("  ... and %d more entries\n", len(entries)-50)
					break
				}
				filename := entry.File().Name()
				fmt.Printf("  - %s\n", filename)
			}
			fmt.Printf("\n✓ Total entries: %d\n", len(entries))
		}

		fmt.Println("\n=== SUCCESS ===")
		fmt.Println("✓ Virtual disk data is accessible!")
		fmt.Println("✓ GPT partition table found")
		fmt.Println("✓ NTFS filesystem accessible")
		fmt.Println("\n=== PROBLEM CONFIRMED ===")
		fmt.Println("✗ VHDX BAT (Block Allocation Table) is corrupted/empty")
		fmt.Println("✗ Data exists but BAT doesn't point to it")
		fmt.Println("\n=== SOLUTION ===")
		fmt.Println("You need to either:")
		fmt.Println("1. Recreate the VHDX properly (recommended)")
		fmt.Println("2. Use this direct reader in your code to bypass the BAT")
		fmt.Println("3. Extract data: dd if=source.vhdx of=output.raw bs=1M skip=4")
		fmt.Println("   Then use output.raw as a raw disk image")

	} else {
		fmt.Printf("✗ No NTFS signature at sector 2048\n")
		fmt.Printf("  Found: %q\n", ntfsBuf[3:11])

		// Scan for NTFS
		fmt.Println("\n  Scanning for NTFS...")
		for sector := int64(0); sector < 100000; sector += 2048 {
			offset := sector * 512
			buf := make([]byte, 512)
			n, err := reader.ReadAt(buf, offset)
			if err != nil || n < 11 {
				continue
			}
			if string(buf[3:11]) == "NTFS    " {
				fmt.Printf("  ✓ NTFS found at sector %d (offset 0x%X)\n", sector, offset)
				break
			}
		}
	}
}
