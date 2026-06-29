package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/asalih/go-vdisk/vhdx"
)

func runVHDXBatDiagnostic(sourcePath string) {
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

	fmt.Println("=== VHDX BAT (Block Allocation Table) Diagnostic ===")
	fmt.Printf("Virtual Disk Size: %d bytes (%.2f GB)\n", vhdxImage.Size(), float64(vhdxImage.Size())/(1024*1024*1024))
	fmt.Println()

	// Try to read sectors and see what the BAT returns
	fmt.Println("=== Testing Sector Reads ===")
	testSectors := []int64{0, 1, 2, 2048, 4096}

	for _, sector := range testSectors {
		fmt.Printf("\nSector %d (offset 0x%X):\n", sector, sector*512)

		buf := make([]byte, 512)
		n, err := vhdxImage.ReadAt(buf, sector*512)
		if err != nil && err != io.EOF {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		fmt.Printf("  Bytes read: %d\n", n)

		// Check if all zeros
		allZero := true
		for i := 0; i < n; i++ {
			if buf[i] != 0 {
				allZero = false
				break
			}
		}

		if allZero {
			fmt.Printf("  Status: All zeros ⚠️\n")
		} else {
			fmt.Printf("  Status: Contains data ✓\n")
			// Show first 64 bytes
			fmt.Printf("  First 64 bytes: %X...\n", buf[:min(64, n)])
		}
	}

	fmt.Println("\n=== Attempting to Read Full Disk ===")
	// Try to read the entire disk in chunks and see how much actual data there is
	chunkSize := int64(1024 * 1024) // 1MB chunks
	totalSize := int64(vhdxImage.Size())
	nonZeroChunks := 0
	totalChunks := (totalSize + chunkSize - 1) / chunkSize

	fmt.Printf("Scanning %d chunks of %d bytes each...\n", totalChunks, chunkSize)

	buf := make([]byte, chunkSize)
	for offset := int64(0); offset < totalSize; offset += chunkSize {
		readSize := min64(chunkSize, totalSize-offset)
		n, err := vhdxImage.ReadAt(buf[:readSize], offset)
		if err != nil && err != io.EOF {
			fmt.Printf("Error at offset 0x%X: %v\n", offset, err)
			continue
		}

		// Check if chunk has any non-zero data
		hasData := false
		for i := 0; i < n; i++ {
			if buf[i] != 0 {
				hasData = true
				nonZeroChunks++
				fmt.Printf("  ✓ Found data at offset 0x%X (chunk %d/%d)\n", offset, offset/chunkSize+1, totalChunks)

				// Show where the first non-zero byte is
				for j := 0; j < n; j++ {
					if buf[j] != 0 {
						fmt.Printf("    First non-zero byte at offset 0x%X: 0x%02X\n", offset+int64(j), buf[j])
						fmt.Printf("    Context: %X\n", buf[max(0, j-16):min(n, j+48)])
						break
					}
				}
				break
			}
		}

		if !hasData && offset%(10*chunkSize) == 0 {
			fmt.Printf("  Scanned up to offset 0x%X... (still all zeros)\n", offset+readSize)
		}
	}

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Total chunks: %d\n", totalChunks)
	fmt.Printf("Chunks with data: %d\n", nonZeroChunks)
	fmt.Printf("Empty chunks: %d\n", totalChunks-int64(nonZeroChunks))

	if nonZeroChunks == 0 {
		fmt.Println("\n⚠️  PROBLEM IDENTIFIED:")
		fmt.Println("The VHDX is returning all zeros for all sectors!")
		fmt.Println("\nPossible causes:")
		fmt.Println("1. The VHDX is actually empty (no data has been written)")
		fmt.Println("2. The VHDX BAT (Block Allocation Table) indicates all blocks are NOT_PRESENT")
		fmt.Println("3. There's a bug in the VHDX parser's block reading logic")
		fmt.Println("4. The VHDX was created but never initialized/formatted")
		fmt.Println("\nTo verify:")
		fmt.Println("- Try opening the VHDX in Windows Disk Management")
		fmt.Println("- Check if it shows as 'Unallocated' or 'Not Initialized'")
		fmt.Println("- Try reading the raw VHDX file directly (bypass the parser)")
	} else {
		fmt.Printf("\n✓ Found data in %d/%d chunks\n", nonZeroChunks, totalChunks)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
