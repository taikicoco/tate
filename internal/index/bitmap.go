// Package index implements database index structures.
package index

import (
	"encoding/binary"
	"io"
)

// Bitmap represents a set of bits.
type Bitmap struct {
	bits   []uint64
	length int
}

// NewBitmap creates a new empty bitmap.
func NewBitmap() *Bitmap {
	return &Bitmap{
		bits:   make([]uint64, 0),
		length: 0,
	}
}

// NewBitmapWithCapacity creates a bitmap with pre-allocated capacity.
func NewBitmapWithCapacity(capacity int) *Bitmap {
	words := (capacity + 63) / 64
	return &Bitmap{
		bits:   make([]uint64, words),
		length: 0,
	}
}

// Set sets the bit at the given position.
func (b *Bitmap) Set(pos int) {
	wordIdx := pos / 64
	bitIdx := pos % 64

	// Extend if necessary
	for len(b.bits) <= wordIdx {
		b.bits = append(b.bits, 0)
	}

	b.bits[wordIdx] |= (1 << bitIdx)

	if pos >= b.length {
		b.length = pos + 1
	}
}

// Clear clears the bit at the given position.
func (b *Bitmap) Clear(pos int) {
	wordIdx := pos / 64
	if wordIdx >= len(b.bits) {
		return
	}
	bitIdx := pos % 64
	b.bits[wordIdx] &^= (1 << bitIdx)
}

// Get returns true if the bit at the given position is set.
func (b *Bitmap) Get(pos int) bool {
	wordIdx := pos / 64
	if wordIdx >= len(b.bits) {
		return false
	}
	bitIdx := pos % 64
	return (b.bits[wordIdx] & (1 << bitIdx)) != 0
}

// And returns a new bitmap that is the AND of this bitmap and another.
func (b *Bitmap) And(other *Bitmap) *Bitmap {
	minLen := len(b.bits)
	if len(other.bits) < minLen {
		minLen = len(other.bits)
	}

	result := &Bitmap{
		bits:   make([]uint64, minLen),
		length: b.length,
	}

	for i := 0; i < minLen; i++ {
		result.bits[i] = b.bits[i] & other.bits[i]
	}

	return result
}

// Or returns a new bitmap that is the OR of this bitmap and another.
func (b *Bitmap) Or(other *Bitmap) *Bitmap {
	maxLen := len(b.bits)
	if len(other.bits) > maxLen {
		maxLen = len(other.bits)
	}

	result := &Bitmap{
		bits:   make([]uint64, maxLen),
		length: b.length,
	}
	if other.length > result.length {
		result.length = other.length
	}

	for i := 0; i < maxLen; i++ {
		if i < len(b.bits) {
			result.bits[i] = b.bits[i]
		}
		if i < len(other.bits) {
			result.bits[i] |= other.bits[i]
		}
	}

	return result
}

// AndNot returns a new bitmap that is this AND NOT other.
func (b *Bitmap) AndNot(other *Bitmap) *Bitmap {
	result := &Bitmap{
		bits:   make([]uint64, len(b.bits)),
		length: b.length,
	}

	for i := 0; i < len(b.bits); i++ {
		if i < len(other.bits) {
			result.bits[i] = b.bits[i] &^ other.bits[i]
		} else {
			result.bits[i] = b.bits[i]
		}
	}

	return result
}

// Not returns a new bitmap that is the NOT of this bitmap.
func (b *Bitmap) Not() *Bitmap {
	result := &Bitmap{
		bits:   make([]uint64, len(b.bits)),
		length: b.length,
	}

	for i := 0; i < len(b.bits); i++ {
		result.bits[i] = ^b.bits[i]
	}

	return result
}

// Count returns the number of set bits.
func (b *Bitmap) Count() int {
	count := 0
	for _, word := range b.bits {
		count += popcount(word)
	}
	return count
}

// Positions returns all positions where bits are set.
func (b *Bitmap) Positions() []int {
	var positions []int
	for i := 0; i < b.length; i++ {
		if b.Get(i) {
			positions = append(positions, i)
		}
	}
	return positions
}

// IsEmpty returns true if no bits are set.
func (b *Bitmap) IsEmpty() bool {
	for _, word := range b.bits {
		if word != 0 {
			return false
		}
	}
	return true
}

// Clone returns a copy of the bitmap.
func (b *Bitmap) Clone() *Bitmap {
	result := &Bitmap{
		bits:   make([]uint64, len(b.bits)),
		length: b.length,
	}
	copy(result.bits, b.bits)
	return result
}

// Length returns the logical length (highest bit position + 1).
func (b *Bitmap) Length() int {
	return b.length
}

// WriteTo writes the bitmap to a writer.
func (b *Bitmap) WriteTo(w io.Writer) error {
	// Write length
	if err := binary.Write(w, binary.LittleEndian, uint64(b.length)); err != nil {
		return err
	}

	// Write word count
	if err := binary.Write(w, binary.LittleEndian, uint64(len(b.bits))); err != nil {
		return err
	}

	// Write words
	for _, word := range b.bits {
		if err := binary.Write(w, binary.LittleEndian, word); err != nil {
			return err
		}
	}

	return nil
}

// ReadFrom reads the bitmap from a reader.
func (b *Bitmap) ReadFrom(r io.Reader) error {
	// Read length
	var length uint64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return err
	}
	b.length = int(length)

	// Read word count
	var wordCount uint64
	if err := binary.Read(r, binary.LittleEndian, &wordCount); err != nil {
		return err
	}

	// Read words
	b.bits = make([]uint64, wordCount)
	for i := uint64(0); i < wordCount; i++ {
		if err := binary.Read(r, binary.LittleEndian, &b.bits[i]); err != nil {
			return err
		}
	}

	return nil
}

// popcount returns the number of set bits in a word.
func popcount(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}

// BitmapIndex is an index that uses bitmaps for low-cardinality columns.
type BitmapIndex struct {
	ColumnName string
	Bitmaps    map[interface{}]*Bitmap
}

// NewBitmapIndex creates a new bitmap index.
func NewBitmapIndex(columnName string) *BitmapIndex {
	return &BitmapIndex{
		ColumnName: columnName,
		Bitmaps:    make(map[interface{}]*Bitmap),
	}
}

// Add adds a value at the given row position.
func (bi *BitmapIndex) Add(value interface{}, rowPos int) {
	if bi.Bitmaps[value] == nil {
		bi.Bitmaps[value] = NewBitmap()
	}
	bi.Bitmaps[value].Set(rowPos)
}

// Lookup returns the bitmap for a given value.
func (bi *BitmapIndex) Lookup(value interface{}) *Bitmap {
	if bm, exists := bi.Bitmaps[value]; exists {
		return bm
	}
	return NewBitmap()
}

// LookupEqual returns row positions where column equals value.
func (bi *BitmapIndex) LookupEqual(value interface{}) []int {
	return bi.Lookup(value).Positions()
}

// LookupIn returns row positions where column is in the given values.
func (bi *BitmapIndex) LookupIn(values []interface{}) []int {
	result := NewBitmap()
	for _, v := range values {
		result = result.Or(bi.Lookup(v))
	}
	return result.Positions()
}

// Values returns all distinct values in the index.
func (bi *BitmapIndex) Values() []interface{} {
	values := make([]interface{}, 0, len(bi.Bitmaps))
	for v := range bi.Bitmaps {
		values = append(values, v)
	}
	return values
}

// Cardinality returns the number of distinct values.
func (bi *BitmapIndex) Cardinality() int {
	return len(bi.Bitmaps)
}
