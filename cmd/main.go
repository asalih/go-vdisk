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

	vFileMain, err := os.Open("./testdata/acmecl9ubuntu/ACMECL9-Ubuntu.vmdk")
	if err != nil {
		log.Fatalf("%v", err)
	}
	vFileSnap, err := os.Open("./testdata/acmecl9ubuntu/ACMECL9-Ubuntu-000001.vmdk")
	if err != nil {
		log.Fatalf("%v", err)
	}
	vmdk.FileAccessor = func(s string) (io.ReadSeeker, error) {
		return os.Open(filepath.Join("./testdata/acmecl9ubuntu/", s))
	}

	fhs := []io.ReadSeeker{vFileMain}
	vdiskMain, err := vmdk.NewVMDK(fhs)
	if err != nil {
		log.Fatalf("%v", err)
	}

	fhss := []io.ReadSeeker{vFileSnap}
	vdiskSnap, err := vmdk.NewVMDK(fhss)
	if err != nil {
		log.Fatalf("%v", err)
	}

	offs := int64(543293440)
	for i := 0; i < 1; i++ {
		sz := 65536
		buf1 := make([]byte, sz)
		_, err = vdiskMain.ReadAt(buf1, offs)
		if err != nil {
			log.Fatalf("%v", err)
		}

		buf2 := make([]byte, sz)
		_, err = vdiskSnap.ReadAt(buf2, offs)
		if err != nil {
			log.Fatalf("%v", err)
		}

		for j := 0; j < len(buf1); j++ {
			if buf1[j] != buf2[j] {
				fmt.Println("***** DIFFF: ", j, buf1[j], buf2[j])
			}
		}
		offs += int64(i * sz)
	}
	fmt.Println("Completed")

	// fmt.Println("Disk size: ", vmdkImage.Size)
}

func main1() {

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
