package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	MagicNumber   = "TCOL"
	FormatVersion = 1
)

// ColumnFile manages a single column's data.
type ColumnFile struct {
	dataType DataType
	nullMask []byte
	data     []byte
	rowCount uint64
	path     string
}

// NewColumnFile creates a new column file.
func NewColumnFile(path string, dataType DataType) *ColumnFile {
	return &ColumnFile{
		dataType: dataType,
		nullMask: make([]byte, 0),
		data:     make([]byte, 0),
		path:     path,
	}
}

// AppendValue appends a value to the column.
func (cf *ColumnFile) AppendValue(v Value) error {
	if v.IsNull {
		cf.appendNullBit(true)
		cf.appendZeroValue()
		cf.rowCount++
		return nil
	}

	cf.appendNullBit(false)

	switch cf.dataType {
	case TypeBool:
		val, _ := v.AsBool()
		if val {
			cf.data = append(cf.data, 1)
		} else {
			cf.data = append(cf.data, 0)
		}
	case TypeInt64:
		val, _ := v.AsInt64()
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(val))
		cf.data = append(cf.data, buf...)
	case TypeFloat64:
		val, _ := v.AsFloat64()
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(val))
		cf.data = append(cf.data, buf...)
	case TypeString:
		val, _ := v.AsString()
		strBytes := []byte(val)
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(strBytes)))
		cf.data = append(cf.data, lenBuf...)
		cf.data = append(cf.data, strBytes...)
	default:
		return fmt.Errorf("unsupported data type: %v", cf.dataType)
	}

	cf.rowCount++
	return nil
}

func (cf *ColumnFile) appendNullBit(isNull bool) {
	byteIndex := cf.rowCount / 8
	bitIndex := cf.rowCount % 8

	for uint64(len(cf.nullMask)) <= byteIndex {
		cf.nullMask = append(cf.nullMask, 0)
	}

	if isNull {
		cf.nullMask[byteIndex] |= (1 << bitIndex)
	}
}

func (cf *ColumnFile) appendZeroValue() {
	switch cf.dataType {
	case TypeBool:
		cf.data = append(cf.data, 0)
	case TypeInt64, TypeFloat64:
		cf.data = append(cf.data, make([]byte, 8)...)
	case TypeString:
		cf.data = append(cf.data, 0, 0, 0, 0)
	}
}

// IsNull returns true if the value at the given row index is NULL.
func (cf *ColumnFile) IsNull(rowIndex uint64) bool {
	if rowIndex >= cf.rowCount {
		return true
	}
	byteIndex := rowIndex / 8
	bitIndex := rowIndex % 8
	if byteIndex >= uint64(len(cf.nullMask)) {
		return false
	}
	return (cf.nullMask[byteIndex] & (1 << bitIndex)) != 0
}

// GetValue returns the value at the given row index.
func (cf *ColumnFile) GetValue(rowIndex uint64) Value {
	if cf.IsNull(rowIndex) {
		return NewNullValue()
	}

	switch cf.dataType {
	case TypeBool:
		if rowIndex < uint64(len(cf.data)) {
			return NewBoolValue(cf.data[rowIndex] != 0)
		}
	case TypeInt64:
		offset := rowIndex * 8
		if offset+8 <= uint64(len(cf.data)) {
			v := int64(binary.LittleEndian.Uint64(cf.data[offset:]))
			return NewInt64Value(v)
		}
	case TypeFloat64:
		offset := rowIndex * 8
		if offset+8 <= uint64(len(cf.data)) {
			bits := binary.LittleEndian.Uint64(cf.data[offset:])
			return NewFloat64Value(math.Float64frombits(bits))
		}
	case TypeString:
		offset := uint64(0)
		for i := uint64(0); i < rowIndex; i++ {
			if offset+4 > uint64(len(cf.data)) {
				return NewNullValue()
			}
			strLen := binary.LittleEndian.Uint32(cf.data[offset:])
			offset += 4 + uint64(strLen)
		}
		if offset+4 > uint64(len(cf.data)) {
			return NewNullValue()
		}
		strLen := binary.LittleEndian.Uint32(cf.data[offset:])
		start := offset + 4
		end := start + uint64(strLen)
		if end <= uint64(len(cf.data)) {
			return NewStringValue(string(cf.data[start:end]))
		}
	}

	return NewNullValue()
}

// RowCount returns the number of rows.
func (cf *ColumnFile) RowCount() uint64 {
	return cf.rowCount
}

// Save writes the column file to disk.
func (cf *ColumnFile) Save() (err error) {
	file, err := os.Create(cf.path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// Write magic number
	if _, err := file.Write([]byte(MagicNumber)); err != nil {
		return err
	}

	// Write version
	if err := binary.Write(file, binary.LittleEndian, uint16(FormatVersion)); err != nil {
		return err
	}

	// Write data type
	if err := binary.Write(file, binary.LittleEndian, uint8(cf.dataType)); err != nil {
		return err
	}

	// Write row count
	if err := binary.Write(file, binary.LittleEndian, cf.rowCount); err != nil {
		return err
	}

	// Write null mask size and data
	if err := binary.Write(file, binary.LittleEndian, uint64(len(cf.nullMask))); err != nil {
		return err
	}
	if _, err := file.Write(cf.nullMask); err != nil {
		return err
	}

	// Write data size and data
	if err := binary.Write(file, binary.LittleEndian, uint64(len(cf.data))); err != nil {
		return err
	}
	if _, err := file.Write(cf.data); err != nil {
		return err
	}

	return nil
}

// LoadColumnFile loads a column file from disk.
func LoadColumnFile(path string) (_ *ColumnFile, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	cf := &ColumnFile{path: path}

	// Read magic number
	magic := make([]byte, 4)
	if _, err := io.ReadFull(file, magic); err != nil {
		return nil, err
	}
	if string(magic) != MagicNumber {
		return nil, fmt.Errorf("invalid column file format")
	}

	// Read version
	var version uint16
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return nil, err
	}

	// Read data type
	var dt uint8
	if err := binary.Read(file, binary.LittleEndian, &dt); err != nil {
		return nil, err
	}
	cf.dataType = DataType(dt)

	// Read row count
	if err := binary.Read(file, binary.LittleEndian, &cf.rowCount); err != nil {
		return nil, err
	}

	// Read null mask
	var nullMaskSize uint64
	if err := binary.Read(file, binary.LittleEndian, &nullMaskSize); err != nil {
		return nil, err
	}
	cf.nullMask = make([]byte, nullMaskSize)
	if _, err := io.ReadFull(file, cf.nullMask); err != nil {
		return nil, err
	}

	// Read data
	var dataSize uint64
	if err := binary.Read(file, binary.LittleEndian, &dataSize); err != nil {
		return nil, err
	}
	cf.data = make([]byte, dataSize)
	if _, err := io.ReadFull(file, cf.data); err != nil {
		return nil, err
	}

	return cf, nil
}

// Table represents a columnar table.
type Table struct {
	Schema  *TableSchema
	Columns map[string]*ColumnFile
	dataDir string
}

// CreateTable creates a new table with the given schema.
func CreateTable(dataDir string, schema *TableSchema) (*Table, error) {
	tableDir := filepath.Join(dataDir, "tables", schema.Name)

	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	t := &Table{
		Schema:  schema,
		Columns: make(map[string]*ColumnFile),
		dataDir: tableDir,
	}

	for _, col := range schema.Columns {
		colPath := filepath.Join(tableDir, fmt.Sprintf("col_%s.dat", col.Name))
		t.Columns[col.Name] = NewColumnFile(colPath, col.Type)
	}

	if err := t.saveMetadata(); err != nil {
		return nil, err
	}

	return t, nil
}

// LoadTable loads an existing table from disk.
func LoadTable(dataDir string, tableName string) (*Table, error) {
	tableDir := filepath.Join(dataDir, "tables", tableName)

	metaPath := filepath.Join(tableDir, "_meta.json")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read table metadata: %w", err)
	}

	var schema TableSchema
	if err := json.Unmarshal(metaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse table metadata: %w", err)
	}

	t := &Table{
		Schema:  &schema,
		Columns: make(map[string]*ColumnFile),
		dataDir: tableDir,
	}

	for _, col := range schema.Columns {
		colPath := filepath.Join(tableDir, fmt.Sprintf("col_%s.dat", col.Name))
		cf, err := LoadColumnFile(colPath)
		if err != nil {
			if os.IsNotExist(err) {
				t.Columns[col.Name] = NewColumnFile(colPath, col.Type)
				continue
			}
			return nil, fmt.Errorf("failed to load column %q: %w", col.Name, err)
		}
		t.Columns[col.Name] = cf
	}

	return t, nil
}

// Insert inserts a row into the table.
func (t *Table) Insert(values []Value) error {
	if len(values) != len(t.Schema.Columns) {
		return fmt.Errorf("column count mismatch: expected %d, got %d",
			len(t.Schema.Columns), len(values))
	}

	for i, col := range t.Schema.Columns {
		cf := t.Columns[col.Name]
		if err := cf.AppendValue(values[i]); err != nil {
			return fmt.Errorf("failed to append value to column %q: %w", col.Name, err)
		}
	}

	return nil
}

// RowCount returns the number of rows in the table.
func (t *Table) RowCount() uint64 {
	for _, cf := range t.Columns {
		return cf.RowCount()
	}
	return 0
}

// Scan iterates over all rows and calls the callback function for each row.
func (t *Table) Scan(callback func(rowIndex uint64, row []Value) bool) error {
	rowCount := t.RowCount()
	for i := uint64(0); i < rowCount; i++ {
		row := make([]Value, len(t.Schema.Columns))
		for j, col := range t.Schema.Columns {
			cf := t.Columns[col.Name]
			row[j] = cf.GetValue(i)
		}
		if !callback(i, row) {
			break
		}
	}
	return nil
}

// Save persists the table to disk.
func (t *Table) Save() error {
	for name, cf := range t.Columns {
		if err := cf.Save(); err != nil {
			return fmt.Errorf("failed to save column %q: %w", name, err)
		}
	}
	return t.saveMetadata()
}

// Drop deletes the table from disk.
func (t *Table) Drop() error {
	return os.RemoveAll(t.dataDir)
}

func (t *Table) saveMetadata() error {
	metaPath := filepath.Join(t.dataDir, "_meta.json")
	data, err := json.MarshalIndent(t.Schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return os.WriteFile(metaPath, data, 0644)
}
