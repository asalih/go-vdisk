package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/asalih/go-vdisk/vmdk"
)

func main() {

	sourcePath := flag.String("source", "", "Source path")
	flag.Parse()

	fmt.Println("Disk opening: ", os.Args)

	vFile, err := os.Open(*sourcePath)
	if err != nil {
		log.Fatalf("%v", err)
	}
	vmdk.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join(filepath.Dir(*sourcePath), s))
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