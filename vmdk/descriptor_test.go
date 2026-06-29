package vmdk

import "testing"

func TestParseDiskDescriptorSparseExtentWithSpaces(t *testing.T) {
	descriptor := `# Disk DescriptorFile
version=1
CID=fffffffe
parentCID=ffffffff
createType="twoGbMaxExtentSparse"

# Extent description
RW 8323072 SPARSE "Windows 10-s001.vmdk"
RW 983040 SPARSE "Windows 10-s016.vmdk"

# The Disk Data Base
ddb.adapterType = "lsilogic"
`

	parsed, err := ParseDiskDescriptor(descriptor)
	if err != nil {
		t.Fatalf("ParseDiskDescriptor() error = %v", err)
	}

	if len(parsed.Extents) != 2 {
		t.Fatalf("len(Extents) = %d, want 2", len(parsed.Extents))
	}
	if got, want := parsed.Extents[0].Filename, "Windows 10-s001.vmdk"; got != want {
		t.Fatalf("Extents[0].Filename = %q, want %q", got, want)
	}
	if got, want := parsed.Extents[1].Filename, "Windows 10-s016.vmdk"; got != want {
		t.Fatalf("Extents[1].Filename = %q, want %q", got, want)
	}
	if got, want := parsed.Sectors, int64(9306112); got != want {
		t.Fatalf("Sectors = %d, want %d", got, want)
	}
}

func TestParseDiskDescriptorFlatExtentWithOffset(t *testing.T) {
	descriptor := `RW 4096 FLAT "disk image-flat.vmdk" 128
RW 2048 FLAT disk-flat.vmdk 4224`

	parsed, err := ParseDiskDescriptor(descriptor)
	if err != nil {
		t.Fatalf("ParseDiskDescriptor() error = %v", err)
	}

	if len(parsed.Extents) != 2 {
		t.Fatalf("len(Extents) = %d, want 2", len(parsed.Extents))
	}
	extent := parsed.Extents[0]
	if got, want := extent.Filename, "disk image-flat.vmdk"; got != want {
		t.Fatalf("Filename = %q, want %q", got, want)
	}
	if got, want := extent.StartSector, int64(128); got != want {
		t.Fatalf("StartSector = %d, want %d", got, want)
	}
	extent = parsed.Extents[1]
	if got, want := extent.Filename, "disk-flat.vmdk"; got != want {
		t.Fatalf("Filename = %q, want %q", got, want)
	}
	if got, want := extent.StartSector, int64(4224); got != want {
		t.Fatalf("StartSector = %d, want %d", got, want)
	}
}
