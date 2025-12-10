// Package storage implements the columnar storage engine.
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/taikicoco/tate/internal/types"
)

const (
	// MagicNumber identifies tate column files.
	MagicNumber = "TCOL"
	// FormatVersion is the current file format version.
	FormatVersion = 1
)

// CompressionType represents the compression algorithm used.
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionRLE
	CompressionDictionary
)

// ColumnHeader represents the header of a column file.
type ColumnHeader struct {
	Magic          [4]byte
	Version        uint16
	DataType       types.DataType
	Compression    CompressionType
	RowCount       uint64
	NullBitmapSize uint64
	DataSize       uint64
}

// ColumnFile manages a single column's data.
type ColumnFile struct {
	Header   ColumnHeader
	NullMask []byte
	Data     []byte
	path     string

	// Statistics
	minValue types.Value
	maxValue types.Value
	hasStats bool
}

// NewColumnFile creates a new column file.
func NewColumnFile(path string, dataType types.DataType) *ColumnFile {
	header := ColumnHeader{
		Version:  FormatVersion,
		DataType: dataType,
	}
	copy(header.Magic[:], MagicNumber)

	return &ColumnFile{
		Header:   header,
		NullMask: make([]byte, 0),
		Data:     make([]byte, 0),
		path:     path,
	}
}

// AppendNull appends a NULL value.
func (cf *ColumnFile) AppendNull() {
	cf.appendNullBit(true)
	cf.appendZeroValue()
	cf.Header.RowCount++
}

// AppendBool appends a boolean value.
func (cf *ColumnFile) AppendBool(value bool, isNull bool) {
	cf.appendNullBit(isNull)
	if value {
		cf.Data = append(cf.Data, 1)
	} else {
		cf.Data = append(cf.Data, 0)
	}
	cf.Header.RowCount++
}

// AppendInt64 appends an int64 value.
func (cf *ColumnFile) AppendInt64(value int64, isNull bool) {
	cf.appendNullBit(isNull)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))
	cf.Data = append(cf.Data, buf...)

	if !isNull {
		cf.updateStats(types.NewInt64Value(value))
	}

	cf.Header.RowCount++
}

// AppendFloat64 appends a float64 value.
func (cf *ColumnFile) AppendFloat64(value float64, isNull bool) {
	cf.appendNullBit(isNull)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(value))
	cf.Data = append(cf.Data, buf...)

	if !isNull {
		cf.updateStats(types.NewFloat64Value(value))
	}

	cf.Header.RowCount++
}

// AppendString appends a string value.
func (cf *ColumnFile) AppendString(value string, isNull bool) {
	cf.appendNullBit(isNull)

	var strBytes []byte
	if !isNull {
		strBytes = []byte(value)
		cf.updateStats(types.NewStringValue(value))
	}

	// Write length prefix (4 bytes) + string data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(strBytes)))
	cf.Data = append(cf.Data, lenBuf...)
	cf.Data = append(cf.Data, strBytes...)

	cf.Header.RowCount++
}

// AppendValue appends a value of any type.
func (cf *ColumnFile) AppendValue(v types.Value) error {
	if v.IsNull {
		cf.AppendNull()
		return nil
	}

	switch cf.Header.DataType {
	case types.TypeBool:
		val, _ := v.AsBool()
		cf.AppendBool(val, false)
	case types.TypeInt64:
		val, _ := v.AsInt64()
		cf.AppendInt64(val, false)
	case types.TypeFloat64:
		val, _ := v.AsFloat64()
		cf.AppendFloat64(val, false)
	case types.TypeString:
		val, _ := v.AsString()
		cf.AppendString(val, false)
	default:
		return fmt.Errorf("unsupported data type: %v", cf.Header.DataType)
	}
	return nil
}

func (cf *ColumnFile) appendNullBit(isNull bool) {
	byteIndex := cf.Header.RowCount / 8
	bitIndex := cf.Header.RowCount % 8

	for uint64(len(cf.NullMask)) <= byteIndex {
		cf.NullMask = append(cf.NullMask, 0)
	}

	if isNull {
		cf.NullMask[byteIndex] |= (1 << bitIndex)
	}
}

func (cf *ColumnFile) appendZeroValue() {
	switch cf.Header.DataType {
	case types.TypeBool:
		cf.Data = append(cf.Data, 0)
	case types.TypeInt64, types.TypeFloat64, types.TypeTimestamp:
		cf.Data = append(cf.Data, make([]byte, 8)...)
	case types.TypeString:
		// Zero-length string
		cf.Data = append(cf.Data, 0, 0, 0, 0)
	}
}

func (cf *ColumnFile) updateStats(v types.Value) {
	if !cf.hasStats {
		cf.minValue = v
		cf.maxValue = v
		cf.hasStats = true
		return
	}

	if v.Compare(cf.minValue) < 0 {
		cf.minValue = v
	}
	if v.Compare(cf.maxValue) > 0 {
		cf.maxValue = v
	}
}

// IsNull returns true if the value at the given row index is NULL.
func (cf *ColumnFile) IsNull(rowIndex uint64) bool {
	if rowIndex >= cf.Header.RowCount {
		return true
	}
	byteIndex := rowIndex / 8
	bitIndex := rowIndex % 8
	if byteIndex >= uint64(len(cf.NullMask)) {
		return false
	}
	return (cf.NullMask[byteIndex] & (1 << bitIndex)) != 0
}

// GetBool returns the boolean value at the given row index.
func (cf *ColumnFile) GetBool(rowIndex uint64) (bool, bool) {
	if cf.IsNull(rowIndex) {
		return false, false
	}
	if rowIndex >= cf.Header.RowCount {
		return false, false
	}
	return cf.Data[rowIndex] != 0, true
}

// GetInt64 returns the int64 value at the given row index.
func (cf *ColumnFile) GetInt64(rowIndex uint64) (int64, bool) {
	if cf.IsNull(rowIndex) {
		return 0, false
	}
	if rowIndex >= cf.Header.RowCount {
		return 0, false
	}
	offset := rowIndex * 8
	return int64(binary.LittleEndian.Uint64(cf.Data[offset:])), true
}

// GetFloat64 returns the float64 value at the given row index.
func (cf *ColumnFile) GetFloat64(rowIndex uint64) (float64, bool) {
	if cf.IsNull(rowIndex) {
		return 0, false
	}
	if rowIndex >= cf.Header.RowCount {
		return 0, false
	}
	offset := rowIndex * 8
	bits := binary.LittleEndian.Uint64(cf.Data[offset:])
	return math.Float64frombits(bits), true
}

// GetString returns the string value at the given row index.
func (cf *ColumnFile) GetString(rowIndex uint64) (string, bool) {
	if cf.IsNull(rowIndex) {
		return "", false
	}

	// Find offset by scanning through all previous strings
	offset := uint64(0)
	for i := uint64(0); i < rowIndex; i++ {
		strLen := binary.LittleEndian.Uint32(cf.Data[offset:])
		offset += 4 + uint64(strLen)
	}

	strLen := binary.LittleEndian.Uint32(cf.Data[offset:])
	start := offset + 4
	end := start + uint64(strLen)
	return string(cf.Data[start:end]), true
}

// GetValue returns the value at the given row index.
func (cf *ColumnFile) GetValue(rowIndex uint64) types.Value {
	if cf.IsNull(rowIndex) {
		return types.NewNullValue()
	}

	switch cf.Header.DataType {
	case types.TypeBool:
		v, _ := cf.GetBool(rowIndex)
		return types.NewBoolValue(v)
	case types.TypeInt64:
		v, _ := cf.GetInt64(rowIndex)
		return types.NewInt64Value(v)
	case types.TypeFloat64:
		v, _ := cf.GetFloat64(rowIndex)
		return types.NewFloat64Value(v)
	case types.TypeString:
		v, _ := cf.GetString(rowIndex)
		return types.NewStringValue(v)
	default:
		return types.NewNullValue()
	}
}

// RowCount returns the number of rows.
func (cf *ColumnFile) RowCount() uint64 {
	return cf.Header.RowCount
}

// DataType returns the column's data type.
func (cf *ColumnFile) DataType() types.DataType {
	return cf.Header.DataType
}

// MinValue returns the minimum value (if statistics are available).
func (cf *ColumnFile) MinValue() (types.Value, bool) {
	return cf.minValue, cf.hasStats
}

// MaxValue returns the maximum value (if statistics are available).
func (cf *ColumnFile) MaxValue() (types.Value, bool) {
	return cf.maxValue, cf.hasStats
}

// Save writes the column file to disk.
func (cf *ColumnFile) Save() error {
	file, err := os.Create(cf.path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Update header sizes
	cf.Header.NullBitmapSize = uint64(len(cf.NullMask))
	cf.Header.DataSize = uint64(len(cf.Data))

	// Write header
	if err := binary.Write(file, binary.LittleEndian, cf.Header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write null bitmap
	if _, err := file.Write(cf.NullMask); err != nil {
		return fmt.Errorf("failed to write null bitmap: %w", err)
	}

	// Write data
	if _, err := file.Write(cf.Data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// LoadColumnFile loads a column file from disk.
func LoadColumnFile(path string) (*ColumnFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	cf := &ColumnFile{path: path}

	// Read header
	if err := binary.Read(file, binary.LittleEndian, &cf.Header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Validate magic number
	if string(cf.Header.Magic[:]) != MagicNumber {
		return nil, fmt.Errorf("invalid column file format")
	}

	// Read null bitmap
	cf.NullMask = make([]byte, cf.Header.NullBitmapSize)
	if _, err := io.ReadFull(file, cf.NullMask); err != nil {
		return nil, fmt.Errorf("failed to read null bitmap: %w", err)
	}

	// Read data
	cf.Data = make([]byte, cf.Header.DataSize)
	if _, err := io.ReadFull(file, cf.Data); err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	return cf, nil
}
