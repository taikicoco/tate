// Package encoding implements data encoding algorithms for columnar storage.
package encoding

import (
	"encoding/binary"
	"io"
)

// Run represents a run of consecutive identical values.
type Run struct {
	Value int64
	Count int
}

// RLEEncoder performs Run-Length Encoding on integer data.
type RLEEncoder struct {
	runs []Run
}

// NewRLEEncoder creates a new RLE encoder.
func NewRLEEncoder() *RLEEncoder {
	return &RLEEncoder{
		runs: make([]Run, 0),
	}
}

// Encode encodes an int64 slice using RLE.
func (e *RLEEncoder) Encode(data []int64) []Run {
	if len(data) == 0 {
		e.runs = nil
		return nil
	}

	e.runs = make([]Run, 0)
	current := Run{Value: data[0], Count: 1}

	for i := 1; i < len(data); i++ {
		if data[i] == current.Value {
			current.Count++
		} else {
			e.runs = append(e.runs, current)
			current = Run{Value: data[i], Count: 1}
		}
	}
	e.runs = append(e.runs, current)

	return e.runs
}

// Decode decodes RLE data back to an int64 slice.
func (e *RLEEncoder) Decode(runs []Run) []int64 {
	if len(runs) == 0 {
		return nil
	}

	// Calculate total length
	totalLen := 0
	for _, run := range runs {
		totalLen += run.Count
	}

	result := make([]int64, 0, totalLen)

	for _, run := range runs {
		for i := 0; i < run.Count; i++ {
			result = append(result, run.Value)
		}
	}

	return result
}

// Runs returns the encoded runs.
func (e *RLEEncoder) Runs() []Run {
	return e.runs
}

// WriteTo writes encoded data to a writer.
func (e *RLEEncoder) WriteTo(w io.Writer, runs []Run) error {
	// Write run count
	if err := binary.Write(w, binary.LittleEndian, uint32(len(runs))); err != nil {
		return err
	}

	for _, run := range runs {
		// Write value
		if err := binary.Write(w, binary.LittleEndian, run.Value); err != nil {
			return err
		}
		// Write count
		if err := binary.Write(w, binary.LittleEndian, uint32(run.Count)); err != nil {
			return err
		}
	}

	return nil
}

// ReadData reads encoded data from a reader.
func (e *RLEEncoder) ReadData(r io.Reader) ([]Run, error) {
	var runCount uint32
	if err := binary.Read(r, binary.LittleEndian, &runCount); err != nil {
		return nil, err
	}

	runs := make([]Run, runCount)

	for i := uint32(0); i < runCount; i++ {
		var val int64
		var count uint32

		if err := binary.Read(r, binary.LittleEndian, &val); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
			return nil, err
		}

		runs[i] = Run{Value: val, Count: int(count)}
	}

	return runs, nil
}

// CompressionRatio returns the compression ratio (original / compressed).
func (e *RLEEncoder) CompressionRatio(originalCount int) float64 {
	if len(e.runs) == 0 {
		return 0
	}

	// Original: int64 per value = 8 bytes
	originalSize := originalCount * 8

	// Encoded: run count (4) + (value (8) + count (4)) per run
	encodedSize := 4 + len(e.runs)*12

	return float64(originalSize) / float64(encodedSize)
}

// EncodedSize returns the size of the encoded data in bytes.
func (e *RLEEncoder) EncodedSize() int {
	return 4 + len(e.runs)*12
}

// RLEStringEncoder performs RLE on string indices.
type RLEStringEncoder struct {
	runs []StringRun
}

// StringRun represents a run of consecutive identical string indices.
type StringRun struct {
	Index int
	Count int
}

// NewRLEStringEncoder creates a new RLE string encoder.
func NewRLEStringEncoder() *RLEStringEncoder {
	return &RLEStringEncoder{
		runs: make([]StringRun, 0),
	}
}

// Encode encodes string indices using RLE.
func (e *RLEStringEncoder) Encode(indices []int) []StringRun {
	if len(indices) == 0 {
		e.runs = nil
		return nil
	}

	e.runs = make([]StringRun, 0)
	current := StringRun{Index: indices[0], Count: 1}

	for i := 1; i < len(indices); i++ {
		if indices[i] == current.Index {
			current.Count++
		} else {
			e.runs = append(e.runs, current)
			current = StringRun{Index: indices[i], Count: 1}
		}
	}
	e.runs = append(e.runs, current)

	return e.runs
}

// Decode decodes RLE data back to an int slice.
func (e *RLEStringEncoder) Decode(runs []StringRun) []int {
	if len(runs) == 0 {
		return nil
	}

	totalLen := 0
	for _, run := range runs {
		totalLen += run.Count
	}

	result := make([]int, 0, totalLen)

	for _, run := range runs {
		for i := 0; i < run.Count; i++ {
			result = append(result, run.Index)
		}
	}

	return result
}
