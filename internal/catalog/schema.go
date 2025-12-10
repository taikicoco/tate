// Package catalog manages database metadata including table schemas.
package catalog

import "github.com/taikicoco/tate/internal/types"

// ColumnDef represents a column definition in a table schema.
type ColumnDef struct {
	Name     string         `json:"name"`
	Type     types.DataType `json:"type"`
	Nullable bool           `json:"nullable"`
	Position int            `json:"position"`
}

// TableSchema represents the schema of a table.
type TableSchema struct {
	Name    string      `json:"name"`
	Columns []ColumnDef `json:"columns"`
}

// NewTableSchema creates a new table schema.
func NewTableSchema(name string) *TableSchema {
	return &TableSchema{
		Name:    name,
		Columns: make([]ColumnDef, 0),
	}
}

// AddColumn adds a column to the schema.
func (s *TableSchema) AddColumn(name string, dataType types.DataType, nullable bool) {
	col := ColumnDef{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
		Position: len(s.Columns),
	}
	s.Columns = append(s.Columns, col)
}

// GetColumn returns a column definition by name.
func (s *TableSchema) GetColumn(name string) (*ColumnDef, bool) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i], true
		}
	}
	return nil, false
}

// GetColumnIndex returns the index of a column by name.
// Returns -1 if the column is not found.
func (s *TableSchema) GetColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// ColumnCount returns the number of columns.
func (s *TableSchema) ColumnCount() int {
	return len(s.Columns)
}

// ColumnNames returns all column names.
func (s *TableSchema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return names
}

// HasColumn returns true if the column exists.
func (s *TableSchema) HasColumn(name string) bool {
	_, found := s.GetColumn(name)
	return found
}
