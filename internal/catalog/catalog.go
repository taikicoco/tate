package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

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

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Load existing catalog if it exists
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

// TableCount returns the number of tables.
func (c *Catalog) TableCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.Tables)
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
