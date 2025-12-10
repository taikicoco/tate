package encoding

import (
	"encoding/binary"
	"io"
)

// DictionaryEncoder performs dictionary encoding on string data.
type DictionaryEncoder struct {
	dict    map[string]int
	reverse []string
}

// NewDictionaryEncoder creates a new dictionary encoder.
func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{
		dict:    make(map[string]int),
		reverse: make([]string, 0),
	}
}

// Encode encodes strings using dictionary encoding.
// Returns the indices into the dictionary.
func (e *DictionaryEncoder) Encode(data []string) []int {
	encoded := make([]int, len(data))

	for i, val := range data {
		if idx, exists := e.dict[val]; exists {
			encoded[i] = idx
		} else {
			idx := len(e.reverse)
			e.dict[val] = idx
			e.reverse = append(e.reverse, val)
			encoded[i] = idx
		}
	}

	return encoded
}

// EncodeValue encodes a single value and returns its index.
func (e *DictionaryEncoder) EncodeValue(val string) int {
	if idx, exists := e.dict[val]; exists {
		return idx
	}

	idx := len(e.reverse)
	e.dict[val] = idx
	e.reverse = append(e.reverse, val)
	return idx
}

// Decode decodes indices back to strings.
func (e *DictionaryEncoder) Decode(indices []int) []string {
	result := make([]string, len(indices))

	for i, idx := range indices {
		if idx >= 0 && idx < len(e.reverse) {
			result[i] = e.reverse[idx]
		}
	}

	return result
}

// DecodeValue decodes a single index to a string.
func (e *DictionaryEncoder) DecodeValue(idx int) (string, bool) {
	if idx < 0 || idx >= len(e.reverse) {
		return "", false
	}
	return e.reverse[idx], true
}

// Dictionary returns the dictionary (value -> index mapping).
func (e *DictionaryEncoder) Dictionary() map[string]int {
	return e.dict
}

// Values returns all dictionary values in order.
func (e *DictionaryEncoder) Values() []string {
	return e.reverse
}

// Size returns the number of unique values in the dictionary.
func (e *DictionaryEncoder) Size() int {
	return len(e.reverse)
}

// Contains checks if a value is in the dictionary.
func (e *DictionaryEncoder) Contains(val string) bool {
	_, exists := e.dict[val]
	return exists
}

// GetIndex returns the index for a value, or -1 if not found.
func (e *DictionaryEncoder) GetIndex(val string) int {
	if idx, exists := e.dict[val]; exists {
		return idx
	}
	return -1
}

// WriteTo writes the dictionary and encoded data to a writer.
func (e *DictionaryEncoder) WriteTo(w io.Writer, indices []int) error {
	// Write dictionary size
	if err := binary.Write(w, binary.LittleEndian, uint32(len(e.reverse))); err != nil {
		return err
	}

	// Write dictionary entries
	for _, val := range e.reverse {
		valBytes := []byte(val)
		// Write string length
		if err := binary.Write(w, binary.LittleEndian, uint32(len(valBytes))); err != nil {
			return err
		}
		// Write string data
		if _, err := w.Write(valBytes); err != nil {
			return err
		}
	}

	// Write index count
	if err := binary.Write(w, binary.LittleEndian, uint32(len(indices))); err != nil {
		return err
	}

	// Write indices
	for _, idx := range indices {
		if err := binary.Write(w, binary.LittleEndian, uint32(idx)); err != nil {
			return err
		}
	}

	return nil
}

// ReadFrom reads the dictionary and encoded data from a reader.
func (e *DictionaryEncoder) ReadFrom(r io.Reader) ([]int, error) {
	// Read dictionary size
	var dictSize uint32
	if err := binary.Read(r, binary.LittleEndian, &dictSize); err != nil {
		return nil, err
	}

	// Read dictionary entries
	e.reverse = make([]string, dictSize)
	e.dict = make(map[string]int)

	for i := uint32(0); i < dictSize; i++ {
		var strLen uint32
		if err := binary.Read(r, binary.LittleEndian, &strLen); err != nil {
			return nil, err
		}

		strBytes := make([]byte, strLen)
		if _, err := io.ReadFull(r, strBytes); err != nil {
			return nil, err
		}

		val := string(strBytes)
		e.reverse[i] = val
		e.dict[val] = int(i)
	}

	// Read index count
	var indexCount uint32
	if err := binary.Read(r, binary.LittleEndian, &indexCount); err != nil {
		return nil, err
	}

	// Read indices
	indices := make([]int, indexCount)
	for i := uint32(0); i < indexCount; i++ {
		var idx uint32
		if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
			return nil, err
		}
		indices[i] = int(idx)
	}

	return indices, nil
}

// CompressionRatio returns the compression ratio (original / compressed).
func (e *DictionaryEncoder) CompressionRatio(originalData []string) float64 {
	var originalSize int
	for _, s := range originalData {
		originalSize += len(s)
	}

	if originalSize == 0 {
		return 0
	}

	// Dictionary size
	var dictSize int
	for _, s := range e.reverse {
		dictSize += 4 + len(s) // length prefix + string data
	}

	// Index size
	indexSize := len(originalData) * 4 // 4 bytes per index

	// Total encoded size
	encodedSize := 4 + dictSize + 4 + indexSize // dict count + dict + index count + indices

	return float64(originalSize) / float64(encodedSize)
}

// EncodedSize returns the size of the encoded data in bytes.
func (e *DictionaryEncoder) EncodedSize(indexCount int) int {
	dictSize := 0
	for _, s := range e.reverse {
		dictSize += 4 + len(s)
	}
	return 4 + dictSize + 4 + indexCount*4
}
