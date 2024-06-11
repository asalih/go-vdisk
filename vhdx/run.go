package vhdx

type PartialRun struct {
	Type  byte
	Count int64
}

type partialRunner struct {
	bitmap   []byte
	startIdx int64
	length   int64
	stop     bool
	value    *PartialRun
}

func (p *partialRunner) Next() bool {
	if p.stop {
		return false
	}

	currentType := (p.bitmap[0] & (1 << p.startIdx)) >> p.startIdx
	currentCount := int64(0)

	for _, byteVal := range p.bitmap {
		if (currentType == 0 && byteVal == 0) || (currentType == 1 && byteVal == 0xFF) {
			maxCount := min64(p.length, int64(8-p.startIdx))
			currentCount += maxCount
			p.length -= maxCount
			p.startIdx = 0
		} else {
			for bitIdx := p.startIdx; bitIdx < min64(p.length, 8); bitIdx++ {
				p.length--
				sectorType := (byteVal & (1 << bitIdx)) >> bitIdx

				if sectorType == currentType {
					currentCount++
				} else {
					p.value = &PartialRun{Type: currentType, Count: currentCount}
					currentType = sectorType
					currentCount = 1
					return true
				}
			}
		}
	}

	if currentCount > 0 {
		p.value = &PartialRun{Type: currentType, Count: currentCount}
		return true
	}

	p.stop = true
	return false
}

func partialRunIter(bitmap []byte, startIdx int64, length int64) *partialRunner {
	return &partialRunner{
		bitmap:   bitmap,
		startIdx: startIdx,
		length:   length,
	}
}
