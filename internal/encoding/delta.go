package encoding

import (
	"encoding/binary"
	"io"
)

// DeltaEncoder performs delta encoding on integer data.
type DeltaEncoder struct{}

// NewDeltaEncoder creates a new delta encoder.
func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{}
}

// Encode encodes int64 data using delta encoding.
// Returns the base value and the deltas.
func (e *DeltaEncoder) Encode(data []int64) (base int64, deltas []int64) {
	if len(data) == 0 {
		return 0, nil
	}

	base = data[0]
	deltas = make([]int64, len(data))

	for i, val := range data {
		deltas[i] = val - base
	}

	return base, deltas
}

// EncodeSequential encodes sequential differences (delta of deltas).
// More efficient for sorted or sequential data.
func (e *DeltaEncoder) EncodeSequential(data []int64) (first int64, deltas []int64) {
	if len(data) == 0 {
		return 0, nil
	}

	if len(data) == 1 {
		return data[0], nil
	}

	first = data[0]
	deltas = make([]int64, len(data)-1)

	for i := 1; i < len(data); i++ {
		deltas[i-1] = data[i] - data[i-1]
	}

	return first, deltas
}

// Decode decodes delta-encoded data back to int64 slice.
func (e *DeltaEncoder) Decode(base int64, deltas []int64) []int64 {
	if len(deltas) == 0 {
		return nil
	}

	result := make([]int64, len(deltas))

	for i, delta := range deltas {
		result[i] = base + delta
	}

	return result
}

// DecodeSequential decodes sequentially delta-encoded data.
func (e *DeltaEncoder) DecodeSequential(first int64, deltas []int64) []int64 {
	if deltas == nil {
		if first != 0 {
			return []int64{first}
		}
		return nil
	}

	result := make([]int64, len(deltas)+1)
	result[0] = first

	for i, delta := range deltas {
		result[i+1] = result[i] + delta
	}

	return result
}

// EncodeDeltaOfDelta encodes using delta-of-delta for time series data.
// Returns first value, second delta, and deltas of deltas.
func (e *DeltaEncoder) EncodeDeltaOfDelta(data []int64) (first, secondDelta int64, deltas []int64) {
	if len(data) == 0 {
		return 0, 0, nil
	}

	if len(data) == 1 {
		return data[0], 0, nil
	}

	first = data[0]
	secondDelta = data[1] - data[0]

	if len(data) == 2 {
		return first, secondDelta, nil
	}

	deltas = make([]int64, len(data)-2)
	prevDelta := secondDelta

	for i := 2; i < len(data); i++ {
		currentDelta := data[i] - data[i-1]
		deltas[i-2] = currentDelta - prevDelta
		prevDelta = currentDelta
	}

	return first, secondDelta, deltas
}

// DecodeDeltaOfDelta decodes delta-of-delta encoded data.
func (e *DeltaEncoder) DecodeDeltaOfDelta(first, secondDelta int64, deltas []int64) []int64 {
	if deltas == nil && secondDelta == 0 && first == 0 {
		return nil
	}

	if deltas == nil && secondDelta == 0 {
		return []int64{first}
	}

	if deltas == nil {
		return []int64{first, first + secondDelta}
	}

	result := make([]int64, len(deltas)+2)
	result[0] = first
	result[1] = first + secondDelta

	prevDelta := secondDelta
	for i, dod := range deltas {
		currentDelta := prevDelta + dod
		result[i+2] = result[i+1] + currentDelta
		prevDelta = currentDelta
	}

	return result
}

// WriteTo writes delta-encoded data to a writer.
func (e *DeltaEncoder) WriteTo(w io.Writer, base int64, deltas []int64) error {
	// Write base
	if err := binary.Write(w, binary.LittleEndian, base); err != nil {
		return err
	}

	// Write delta count
	if err := binary.Write(w, binary.LittleEndian, uint32(len(deltas))); err != nil {
		return err
	}

	// Write deltas
	for _, delta := range deltas {
		if err := binary.Write(w, binary.LittleEndian, delta); err != nil {
			return err
		}
	}

	return nil
}

// ReadFrom reads delta-encoded data from a reader.
func (e *DeltaEncoder) ReadFrom(r io.Reader) (base int64, deltas []int64, err error) {
	// Read base
	if err := binary.Read(r, binary.LittleEndian, &base); err != nil {
		return 0, nil, err
	}

	// Read delta count
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return 0, nil, err
	}

	// Read deltas
	deltas = make([]int64, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(r, binary.LittleEndian, &deltas[i]); err != nil {
			return 0, nil, err
		}
	}

	return base, deltas, nil
}

// CompressionRatio calculates the compression ratio.
// For delta encoding, this depends on the range of deltas.
func (e *DeltaEncoder) CompressionRatio(originalCount int, deltas []int64) float64 {
	if originalCount == 0 {
		return 0
	}

	originalSize := originalCount * 8 // int64

	// Delta encoded: base (8) + count (4) + deltas (8 each)
	encodedSize := 8 + 4 + len(deltas)*8

	return float64(originalSize) / float64(encodedSize)
}

// AnalyzeDeltas returns statistics about the deltas.
func AnalyzeDeltas(deltas []int64) (min, max, avg int64) {
	if len(deltas) == 0 {
		return 0, 0, 0
	}

	min = deltas[0]
	max = deltas[0]
	sum := int64(0)

	for _, d := range deltas {
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
		sum += d
	}

	avg = sum / int64(len(deltas))
	return min, max, avg
}

// BitsRequired returns the number of bits required to represent the range.
func BitsRequired(min, max int64) int {
	if min >= 0 && max >= 0 {
		// Unsigned range
		return bitsForUnsigned(uint64(max))
	}

	// Signed range
	absMax := max
	if -min > absMax {
		absMax = -min
	}

	return bitsForUnsigned(uint64(absMax)) + 1 // +1 for sign bit
}

func bitsForUnsigned(val uint64) int {
	if val == 0 {
		return 1
	}

	bits := 0
	for val > 0 {
		bits++
		val >>= 1
	}
	return bits
}
