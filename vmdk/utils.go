package vmdk

import "io"

func getSize(fh io.ReadSeeker) (int64, error) {
	_, err := fh.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil
	}
	size, err := fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, nil
	}
	_, err = fh.Seek(0, io.SeekStart)
	if err != nil {
		return 0, nil
	}
	return size, nil
}

func bisectRight(a []int64, x int64) int {
	lo, hi := 0, len(a)
	for lo < hi {
		mid := (lo + hi) / 2
		if x < a[mid] {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func min32(a, b int) int {
	if a < b {
		return a
	}
	return b
}
