package vmdk

import (
	"fmt"
	"strconv"
	"strings"
)

type DiskExtent struct {
	AccessType string
	Size       int64
	ExtentType string
	Filename   string
}

type DiskDescriptor struct {
	Attr    map[string]string
	Extents []DiskExtent
	Ddb     map[string]string
	Sectors int64
	Raw     string
}

func ParseDiskDescriptor(data string) (*DiskDescriptor, error) {
	attr := make(map[string]string)
	extents := []DiskExtent{}
	ddb := make(map[string]string)
	sectors := int64(0)

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "RW ") || strings.HasPrefix(line, "RDONLY ") || strings.HasPrefix(line, "NOACCESS ") {
			parts := strings.SplitN(line, " ", 4)
			if len(parts) < 4 {
				return nil, fmt.Errorf("invalid extent line: %s", line)
			}
			size, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, err
			}
			sectors += size
			extents = append(extents, DiskExtent{
				AccessType: parts[0],
				Size:       size,
				ExtentType: parts[2],
				Filename:   strings.Trim(parts[3], `"`),
			})
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) < 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, `"`)
		if strings.HasPrefix(key, "ddb.") {
			ddb[key] = value
		} else {
			attr[key] = value
		}
	}

	return &DiskDescriptor{
		Attr:    attr,
		Extents: extents,
		Ddb:     ddb,
		Sectors: sectors,
		Raw:     data,
	}, nil
}
