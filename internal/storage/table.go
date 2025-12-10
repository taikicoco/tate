package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/taikicoco/tate/internal/catalog"
	"github.com/taikicoco/tate/internal/types"
)

// Table represents a columnar table.
type Table struct {
	Schema  *catalog.TableSchema
	Columns map[string]*ColumnFile
	dataDir string
}

// CreateTable creates a new table with the given schema.
func CreateTable(dataDir string, schema *catalog.TableSchema) (*Table, error) {
	tableDir := filepath.Join(dataDir, "tables", schema.Name)

	// Create table directory
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	t := &Table{
		Schema:  schema,
		Columns: make(map[string]*ColumnFile),
		dataDir: tableDir,
	}

	// Initialize column files
	for _, col := range schema.Columns {
		colPath := filepath.Join(tableDir, fmt.Sprintf("col_%s.dat", col.Name))
		t.Columns[col.Name] = NewColumnFile(colPath, col.Type)
	}

	// Save metadata
	if err := t.saveMetadata(); err != nil {
		return nil, err
	}

	return t, nil
}

// LoadTable loads an existing table from disk.
func LoadTable(dataDir string, tableName string) (*Table, error) {
	tableDir := filepath.Join(dataDir, "tables", tableName)

	// Load metadata
	metaPath := filepath.Join(tableDir, "_meta.json")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read table metadata: %w", err)
	}

	var schema catalog.TableSchema
	if err := json.Unmarshal(metaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse table metadata: %w", err)
	}

	t := &Table{
		Schema:  &schema,
		Columns: make(map[string]*ColumnFile),
		dataDir: tableDir,
	}

	// Load column files
	for _, col := range schema.Columns {
		colPath := filepath.Join(tableDir, fmt.Sprintf("col_%s.dat", col.Name))
		cf, err := LoadColumnFile(colPath)
		if err != nil {
			if os.IsNotExist(err) {
				// File doesn't exist, create empty column
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
func (t *Table) Insert(values []types.Value) error {
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

// InsertMap inserts a row using a map of column names to values.
func (t *Table) InsertMap(values map[string]types.Value) error {
	for _, col := range t.Schema.Columns {
		cf := t.Columns[col.Name]
		val, exists := values[col.Name]
		if !exists {
			val = types.NewNullValue()
		}
		if err := cf.AppendValue(val); err != nil {
			return fmt.Errorf("failed to append value to column %q: %w", col.Name, err)
		}
	}
	return nil
}

// GetRow returns all values for a given row index.
func (t *Table) GetRow(rowIndex uint64) ([]types.Value, error) {
	if rowIndex >= t.RowCount() {
		return nil, fmt.Errorf("row index out of range: %d >= %d", rowIndex, t.RowCount())
	}

	row := make([]types.Value, len(t.Schema.Columns))
	for i, col := range t.Schema.Columns {
		cf := t.Columns[col.Name]
		row[i] = cf.GetValue(rowIndex)
	}
	return row, nil
}

// GetColumn returns all values for a given column name.
func (t *Table) GetColumn(columnName string) ([]types.Value, error) {
	cf, exists := t.Columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %q not found", columnName)
	}

	values := make([]types.Value, cf.RowCount())
	for i := uint64(0); i < cf.RowCount(); i++ {
		values[i] = cf.GetValue(i)
	}
	return values, nil
}

// GetValue returns the value at the given row and column.
func (t *Table) GetValue(rowIndex uint64, columnName string) (types.Value, error) {
	cf, exists := t.Columns[columnName]
	if !exists {
		return types.NewNullValue(), fmt.Errorf("column %q not found", columnName)
	}
	return cf.GetValue(rowIndex), nil
}

// RowCount returns the number of rows in the table.
func (t *Table) RowCount() uint64 {
	if len(t.Columns) == 0 {
		return 0
	}
	for _, cf := range t.Columns {
		return cf.RowCount()
	}
	return 0
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

// Scan iterates over all rows and calls the callback function for each row.
func (t *Table) Scan(callback func(rowIndex uint64, row []types.Value) bool) error {
	rowCount := t.RowCount()
	for i := uint64(0); i < rowCount; i++ {
		row, err := t.GetRow(i)
		if err != nil {
			return err
		}
		if !callback(i, row) {
			break
		}
	}
	return nil
}

// ScanColumns iterates over specified columns only.
func (t *Table) ScanColumns(columnNames []string, callback func(rowIndex uint64, values []types.Value) bool) error {
	// Validate column names
	columns := make([]*ColumnFile, len(columnNames))
	for i, name := range columnNames {
		cf, exists := t.Columns[name]
		if !exists {
			return fmt.Errorf("column %q not found", name)
		}
		columns[i] = cf
	}

	rowCount := t.RowCount()
	values := make([]types.Value, len(columnNames))

	for i := uint64(0); i < rowCount; i++ {
		for j, cf := range columns {
			values[j] = cf.GetValue(i)
		}
		if !callback(i, values) {
			break
		}
	}
	return nil
}
