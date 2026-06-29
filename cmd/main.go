package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/asalih/go-vdisk/vhd"
	"github.com/asalih/go-vdisk/vhdx"
	"github.com/asalih/go-vdisk/vmdk"
	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/backend"
)

func main() {

	sourcePath := flag.String("source", "", "Source path")
	sourceType := flag.String("type", "", "Source type")
	dataOffset := flag.Int64("offset", 0x400000, "Raw file offset for vhdx-direct-read")
	flag.Parse()

	switch *sourceType {
	case "vmdk":
		openVMDK(*sourcePath)
	case "vhdx":
		openVHDX(*sourcePath)
	case "vhd":
		openVHD(*sourcePath)
	case "vhdx-bat-diagnostic":
		runVHDXBatDiagnostic(*sourcePath)
	case "vhdx-direct-read":
		runVHDXDirectRead(*sourcePath, *dataOffset)
	case "vhdx-structure-analysis":
		runVHDXStructureAnalysis(*sourcePath)
	case "vhdx-deep-diagnostic":
		runVHDXDeepDiagnostic(*sourcePath)
	}

	fmt.Println("Disk opening: ", os.Args)

}

// vhdxBackend implements the diskfs Backend interface
type vhdxBackend struct {
	ra     io.ReaderAt
	size   int64
	offset int64
}

func (v *vhdxBackend) ReadAt(p []byte, off int64) (int, error) {
	return v.ra.ReadAt(p, off)
}

func (v *vhdxBackend) Read(p []byte) (int, error) {
	n, err := v.ra.ReadAt(p, v.offset)
	v.offset += int64(n)
	return n, err
}

func (v *vhdxBackend) WriteAt(p []byte, off int64) (int, error) {
	return 0, fmt.Errorf("write not supported for VHDX backend")
}

func (v *vhdxBackend) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("write not supported for VHDX backend")
}

func (v *vhdxBackend) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		v.offset = offset
	case io.SeekCurrent:
		v.offset += offset
	case io.SeekEnd:
		v.offset = v.size + offset
	}
	return v.offset, nil
}

func (v *vhdxBackend) Size() int64 {
	return v.size
}

func (v *vhdxBackend) SectorSize() int64 {
	return 512
}

func (v *vhdxBackend) Close() error {
	return nil
}

func (v *vhdxBackend) Stat() (os.FileInfo, error) {
	return &vhdxFileInfo{size: v.size}, nil
}

func (v *vhdxBackend) Sys() (*os.File, error) {
	return nil, nil
}

func (v *vhdxBackend) Writable() (backend.WritableFile, error) {
	return nil, fmt.Errorf("VHDX backend is read-only")
}

// vhdxFileInfo implements os.FileInfo
type vhdxFileInfo struct {
	size int64
}

func (f *vhdxFileInfo) Name() string       { return "vhdx" }
func (f *vhdxFileInfo) Size() int64        { return f.size }
func (f *vhdxFileInfo) Mode() os.FileMode  { return 0444 }
func (f *vhdxFileInfo) ModTime() time.Time { return time.Time{} }
func (f *vhdxFileInfo) IsDir() bool        { return false }
func (f *vhdxFileInfo) Sys() interface{}   { return nil }

func checkNTFSSignature(r io.ReaderAt, offset int64, desc string) {
	buf := make([]byte, 512)
	_, err := r.ReadAt(buf, offset)
	if err != nil {
		return
	}

	if string(buf[3:11]) == "NTFS    " {
		fmt.Printf("✓ NTFS filesystem found at %s\n", desc)
		// Read some NTFS metadata
		bytesPerSector := uint16(buf[11]) | uint16(buf[12])<<8
		sectorsPerCluster := buf[13]
		fmt.Printf("  Bytes per sector: %d\n", bytesPerSector)
		fmt.Printf("  Sectors per cluster: %d\n", sectorsPerCluster)
	}
}

func openVHDX(sourcePath string) {
	vFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("Error opening VHDX file: %v", err)
	}
	vhdx.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join(filepath.Dir(sourcePath), s))
	}

	vhdxImage, err := vhdx.NewVHDX(vFile)
	if err != nil {
		log.Fatalf("Error creating VHDX: %v", err)
	}

	fmt.Println("✓ VHDX parsed successfully")
	fmt.Printf("  Disk size: %d bytes (%.2f GB)\n\n", vhdxImage.Size(), float64(vhdxImage.Size())/(1024*1024*1024))

	// Read first sector to examine partition structure
	buf := make([]byte, 8192)
	_, err = vhdxImage.ReadAt(buf, 0)
	if err != nil {
		log.Fatalf("Error reading from VHDX: %v", err)
	}

	// Check MBR signature
	fmt.Println("=== Partition Table Analysis ===")
	fmt.Printf("MBR Signature: 0x%02X%02X ", buf[510], buf[511])
	if buf[510] == 0x55 && buf[511] == 0xAA {
		fmt.Println("✓ (Valid)")
	} else {
		fmt.Println("✗ (Invalid - expected 0x55AA)")
		fmt.Println("\n⚠️  WARNING: No valid MBR found at sector 0!")
		fmt.Println("This means the disk was not properly partitioned.")
		fmt.Println("Possible causes:")
		fmt.Println("  1. The VHDX was formatted without creating a partition")
		fmt.Println("  2. The VHDX was created incorrectly")
		fmt.Println("  3. Direct formatting was used instead of partition + format")
	}

	// Check GPT signature
	gptSig := string(buf[512:520])
	fmt.Printf("\nGPT Signature: %q ", gptSig)
	if gptSig == "EFI PART" {
		fmt.Println("✓ (GPT partition table detected)")
	} else {
		fmt.Println("✗ (Not GPT)")
	}

	// Check if NTFS is at sector 0 (direct format without partition)
	fmt.Println("\n=== Checking for Direct Filesystem (No Partition Table) ===")
	if string(buf[3:11]) == "NTFS    " {
		fmt.Println("✓ NTFS filesystem found at sector 0!")
		fmt.Println("\n⚠️  PROBLEM IDENTIFIED:")
		fmt.Println("The disk was formatted directly with NTFS without creating a partition table.")
		fmt.Println("This is why go-diskfs cannot parse it - there's no partition table to read!")
		fmt.Println("\nTo fix this:")
		fmt.Println("  1. In Windows Disk Management:")
		fmt.Println("     - Right-click the disk → Delete Volume")
		fmt.Println("     - Right-click → Initialize Disk (choose MBR or GPT)")
		fmt.Println("     - Create New Simple Volume")
		fmt.Println("     - Format with NTFS")
		fmt.Println("  2. Or use diskpart:")
		fmt.Println("     diskpart")
		fmt.Println("     select vdisk file=\"path\\to\\file.vhdx\"")
		fmt.Println("     attach vdisk")
		fmt.Println("     create partition primary")
		fmt.Println("     format fs=ntfs quick")
		fmt.Println("     assign letter=X")
	} else {
		fmt.Printf("First 16 bytes: % X\n", buf[:16])
		fmt.Printf("Bytes at 510-511: 0x%02X%02X\n", buf[510], buf[511])
	}

	// Check for NTFS signature at various offsets
	fmt.Println("\n=== Filesystem Detection ===")
	// Try reading from common partition start locations
	checkNTFSSignature(vhdxImage, 0, "sector 0")
	checkNTFSSignature(vhdxImage, 2048*512, "sector 2048 (1MB)")
	checkNTFSSignature(vhdxImage, 4096*512, "sector 4096 (2MB)")

	// Try to parse with go-diskfs
	fmt.Println("\n=== Attempting to parse with go-diskfs ===")

	// Use OpenBackend with a custom backend wrapping the VHDX
	d, err := diskfs.OpenBackend(&vhdxBackend{
		ra:   vhdxImage,
		size: int64(vhdxImage.Size()),
	}, diskfs.WithOpenMode(diskfs.ReadOnly))

	if err != nil {
		fmt.Printf("Error opening with go-diskfs: %v\n", err)
		printSummaryAndExit(false, false, true)
		return
	}

	// Try to get partition table
	table, err := d.GetPartitionTable()
	if err != nil {
		fmt.Printf("Error getting partition table: %v\n", err)
		fmt.Println("\n⚠️  CONFIRMED: No valid partition table found!")
		printSummaryAndExit(true, false, false)
		return
	}

	fmt.Printf("✓ Partition table type: %s\n", table.Type())
	partitions := table.GetPartitions()
	fmt.Printf("✓ Number of partitions: %d\n\n", len(partitions))

	hasNTFS := false
	for i, part := range partitions {
		fmt.Printf("=== Partition %d ===\n", i+1)
		fmt.Printf("  Start: sector %d (offset 0x%X)\n", part.GetStart()/512, part.GetStart())
		fmt.Printf("  Size: %d bytes (%.2f MB)\n", part.GetSize(), float64(part.GetSize())/(1024*1024))

		// Read partition boot sector to detect filesystem
		partBuf := make([]byte, 512)
		_, err := vhdxImage.ReadAt(partBuf, part.GetStart())
		if err != nil {
			fmt.Printf("  Error reading partition: %v\n", err)
			continue
		}

		// Check for NTFS
		if string(partBuf[3:11]) == "NTFS    " {
			hasNTFS = true
			fmt.Printf("  Filesystem: NTFS ✓ (detected)\n")
			fmt.Printf("\n  ⚠️  WARNING: go-diskfs does NOT support NTFS!\n")
			fmt.Printf("  ⚠️  go-diskfs only supports FAT32 and ISO9660 filesystems.\n")
			fmt.Printf("  ⚠️  This is why you cannot access the files in this partition.\n")
		} else if string(partBuf[54:59]) == "FAT32" {
			fmt.Printf("  Filesystem: FAT32\n")
			fmt.Printf("  Attempting to read filesystem...\n")

			fs, err := d.GetFilesystem(i + 1)
			if err != nil {
				fmt.Printf("  Error getting filesystem: %v\n", err)
			} else {
				fmt.Printf("  ✓ Filesystem mounted successfully\n")
				files, err := fs.ReadDir("/")
				if err != nil {
					fmt.Printf("  Error reading root directory: %v\n", err)
				} else {
					fmt.Printf("  Root directory files (%d):\n", len(files))
					for _, file := range files {
						fmt.Printf("    - %s\n", file.Name())
					}
				}
			}
		} else {
			fmt.Printf("  Filesystem: Unknown (signature: %q)\n", partBuf[3:11])
		}
		fmt.Println()
	}

	// Print summary and recommendations
	printSummaryAndExit(true, true, hasNTFS)
}

func printSummaryAndExit(validVHDX, hasPartitionTable, hasNTFS bool) {
	fmt.Println("\n=== SUMMARY ===")
	if validVHDX {
		fmt.Println("✓ VHDX file is valid and readable")
	}
	if hasPartitionTable {
		fmt.Println("✓ Partition table exists and can be parsed")
	} else {
		fmt.Println("✗ No valid partition table found")
	}
	if hasNTFS {
		fmt.Println("✗ Filesystem (NTFS) is not supported by go-diskfs")
	}

	fmt.Println("\n=== ROOT CAUSE ===")
	if !hasPartitionTable {
		fmt.Println("The VHDX was formatted without creating a partition table first.")
		fmt.Println("Windows allows direct formatting, but this creates an invalid disk structure.")
		fmt.Println("go-diskfs (and most disk tools) require a proper partition table.")
	} else if hasNTFS {
		fmt.Println("go-diskfs does not support NTFS filesystems.")
		fmt.Println("It only supports FAT32 and ISO9660.")
	}

	fmt.Println("\n=== SOLUTIONS ===")
	if !hasPartitionTable {
		fmt.Println("Option 1: Properly partition the VHDX in Windows")
		fmt.Println("  1. Mount the VHDX: In Disk Management, Action → Attach VHD")
		fmt.Println("  2. Initialize the disk (choose MBR or GPT)")
		fmt.Println("  3. Create a new simple volume")
		fmt.Println("  4. Format with your desired filesystem")
		fmt.Println()
		fmt.Println("Option 2: Use diskpart command line")
		fmt.Println("  diskpart")
		fmt.Println("  select vdisk file=\"C:\\path\\to\\file.vhdx\"")
		fmt.Println("  attach vdisk")
		fmt.Println("  create partition primary")
		fmt.Println("  format fs=ntfs quick  (or fs=fat32 for go-diskfs compatibility)")
		fmt.Println("  detach vdisk")
	} else if hasNTFS {
		fmt.Println("Option 1: Reformat with FAT32 (if file size < 4GB per file is acceptable)")
		fmt.Println()
		fmt.Println("Option 2: Use a different Go library:")
		fmt.Println("  - github.com/Velocidex/go-ntfs (NTFS parser in pure Go)")
		fmt.Println()
		fmt.Println("Option 3: Mount the VHDX natively:")
		fmt.Println("  - Windows: Mount-VHD PowerShell cmdlet or Disk Management")
		fmt.Println("  - Linux: qemu-nbd + mount")
	}
	fmt.Println()
	fmt.Println("For go-diskfs usage, the VHDX must have:")
	fmt.Println("  1. A valid partition table (MBR or GPT)")
	fmt.Println("  2. Partitions formatted with FAT32 or ISO9660")
}

func openVHD(sourcePath string) {
	vFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("%v", err)
	}

	vhdImage, err := vhd.NewVHD(vFile)
	if err != nil {
		log.Fatalf("%v", err)
	}

	buf := make([]byte, 8192)
	_, err = vhdImage.ReadAt(buf, 0)
	if err != nil {
		log.Fatalf("%v", err)
	}

	fmt.Println("Disk size: ", vhdImage.Size())
}

func openVMDK(sourcePath string) {
	vFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("%v", err)
	}
	vmdk.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join(filepath.Dir(sourcePath), s))
	}

	fhs := []io.ReadSeeker{vFile}
	vmdkImage, err := vmdk.NewVMDK(fhs)
	if err != nil {
		log.Fatalf("%v", err)
	}

	buf := make([]byte, 65536)
	_, err = vmdkImage.ReadAt(buf, 0)
	if err != nil {
		log.Fatalf("%v", err)
	}

	fmt.Println("Disk size: ", vmdkImage.Size)
}
