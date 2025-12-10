package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ColumnDef represents a column definition in a table schema.
type ColumnDef struct {
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Nullable bool     `json:"nullable"`
	Position int      `json:"position"`
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
func (s *TableSchema) AddColumn(name string, dataType DataType, nullable bool) {
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
func (s *TableSchema) GetColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// ColumnNames returns all column names.
func (s *TableSchema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		names[i] = col.Name
	}
	return names
}

// Catalog manages database metadata.
type Catalog struct {
	Tables  map[string]*TableSchema `json:"tables"`
	dataDir string
	mu      sync.RWMutex
}

// NewCatalog creates a new catalog.
func NewCatalog(dataDir string) (*Catalog, error) {
	c := &Catalog{
		Tables:  make(map[string]*TableSchema),
		dataDir: dataDir,
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := c.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	return c, nil
}

// RegisterTable registers a new table schema.
func (c *Catalog) RegisterTable(schema *TableSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.Tables[schema.Name]; exists {
		return fmt.Errorf("table %q already exists", schema.Name)
	}

	c.Tables[schema.Name] = schema

	if err := c.save(); err != nil {
		delete(c.Tables, schema.Name)
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.Tables[name]; !exists {
		return fmt.Errorf("table %q does not exist", name)
	}

	delete(c.Tables, name)

	if err := c.save(); err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

// GetTable returns a table schema by name.
func (c *Catalog) GetTable(name string) (*TableSchema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	schema, exists := c.Tables[name]
	return schema, exists
}

// TableExists returns true if the table exists.
func (c *Catalog) TableExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.Tables[name]
	return exists
}

// ListTables returns all table names.
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]string, 0, len(c.Tables))
	for name := range c.Tables {
		tables = append(tables, name)
	}
	return tables
}

// DataDir returns the data directory path.
func (c *Catalog) DataDir() string {
	return c.dataDir
}

func (c *Catalog) catalogPath() string {
	return filepath.Join(c.dataDir, "catalog.json")
}

func (c *Catalog) save() error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.catalogPath(), data, 0644)
}

func (c *Catalog) load() error {
	data, err := os.ReadFile(c.catalogPath())
	if err != nil {
		return err
	}
	return json.Unmarshal(data, c)
}
