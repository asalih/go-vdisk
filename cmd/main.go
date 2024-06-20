package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/asalih/go-vdisk/vhd"
	"github.com/asalih/go-vdisk/vhdx"
	"github.com/asalih/go-vdisk/vmdk"
)

func main() {

	sourcePath := flag.String("source", "", "Source path")
	sourceType := flag.String("type", "", "Source type")
	flag.Parse()

	switch *sourceType {
	case "vmdk":
		openVMDK(*sourcePath)
	case "vhdx":
		openVHDX(*sourcePath)
	case "vhd":
		openVHD(*sourcePath)
	}

	fmt.Println("Disk opening: ", os.Args)

}

func openVHDX(sourcePath string) {
	vFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("%v", err)
	}
	vhdx.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join(filepath.Dir(sourcePath), s))
	}

	vhdxImage, err := vhdx.NewVHDX(vFile)
	if err != nil {
		log.Fatalf("%v", err)
	}

	buf := make([]byte, 8192)
	_, err = vhdxImage.ReadAt(buf, 0)
	if err != nil {
		log.Fatalf("%v", err)
	}

	fmt.Println("Disk size: ", vhdxImage.Size())
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

	buf := make([]byte, 1024)
	_, err = vmdkImage.ReadAt(buf, 510)
	if err != nil {
		log.Fatalf("%v", err)
	}

	fmt.Println("Disk size: ", vmdkImage.Size)
}
