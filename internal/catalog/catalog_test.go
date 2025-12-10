package catalog

import (
	"os"
	"testing"

	"github.com/taikicoco/tate/internal/types"
)

func TestNewCatalog(t *testing.T) {
	tmpDir := t.TempDir()

	cat, err := NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}

	if cat.TableCount() != 0 {
		t.Errorf("expected 0 tables, got %d", cat.TableCount())
	}
}

func TestRegisterTable(t *testing.T) {
	tmpDir := t.TempDir()
	cat, _ := NewCatalog(tmpDir)

	schema := NewTableSchema("users")
	schema.AddColumn("id", types.TypeInt64, false)
	schema.AddColumn("name", types.TypeString, false)

	if err := cat.RegisterTable(schema); err != nil {
		t.Fatalf("RegisterTable failed: %v", err)
	}

	if !cat.TableExists("users") {
		t.Error("table 'users' should exist")
	}

	// Duplicate registration should fail
	if err := cat.RegisterTable(schema); err == nil {
		t.Error("duplicate registration should fail")
	}
}

func TestGetTable(t *testing.T) {
	tmpDir := t.TempDir()
	cat, _ := NewCatalog(tmpDir)

	schema := NewTableSchema("products")
	schema.AddColumn("id", types.TypeInt64, false)
	schema.AddColumn("price", types.TypeFloat64, true)

	_ = cat.RegisterTable(schema)

	got, exists := cat.GetTable("products")
	if !exists {
		t.Fatal("table 'products' should exist")
	}

	if got.Name != "products" {
		t.Errorf("expected name 'products', got %q", got.Name)
	}

	if len(got.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(got.Columns))
	}

	// Non-existent table
	_, exists = cat.GetTable("nonexistent")
	if exists {
		t.Error("non-existent table should not exist")
	}
}

func TestDropTable(t *testing.T) {
	tmpDir := t.TempDir()
	cat, _ := NewCatalog(tmpDir)

	schema := NewTableSchema("temp")
	schema.AddColumn("id", types.TypeInt64, false)
	_ = cat.RegisterTable(schema)

	if err := cat.DropTable("temp"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	if cat.TableExists("temp") {
		t.Error("table 'temp' should not exist after drop")
	}

	// Drop non-existent table should fail
	if err := cat.DropTable("nonexistent"); err == nil {
		t.Error("dropping non-existent table should fail")
	}
}

func TestListTables(t *testing.T) {
	tmpDir := t.TempDir()
	cat, _ := NewCatalog(tmpDir)

	schema1 := NewTableSchema("table1")
	schema1.AddColumn("id", types.TypeInt64, false)
	_ = cat.RegisterTable(schema1)

	schema2 := NewTableSchema("table2")
	schema2.AddColumn("id", types.TypeInt64, false)
	_ = cat.RegisterTable(schema2)

	tables := cat.ListTables()
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
}

func TestCatalogPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create catalog and add table
	cat1, _ := NewCatalog(tmpDir)
	schema := NewTableSchema("persistent")
	schema.AddColumn("id", types.TypeInt64, false)
	schema.AddColumn("value", types.TypeString, true)
	_ = cat1.RegisterTable(schema)

	// Create new catalog instance (should load from disk)
	cat2, err := NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to reload catalog: %v", err)
	}

	if !cat2.TableExists("persistent") {
		t.Error("table 'persistent' should exist after reload")
	}

	got, _ := cat2.GetTable("persistent")
	if len(got.Columns) != 2 {
		t.Errorf("expected 2 columns after reload, got %d", len(got.Columns))
	}
}

func TestTableSchema(t *testing.T) {
	schema := NewTableSchema("test")
	schema.AddColumn("id", types.TypeInt64, false)
	schema.AddColumn("name", types.TypeString, true)
	schema.AddColumn("age", types.TypeInt64, true)

	// Test ColumnCount
	if schema.ColumnCount() != 3 {
		t.Errorf("expected 3 columns, got %d", schema.ColumnCount())
	}

	// Test GetColumn
	col, found := schema.GetColumn("name")
	if !found {
		t.Error("column 'name' should exist")
	}
	if col.Type != types.TypeString {
		t.Errorf("expected TypeString, got %v", col.Type)
	}

	// Test GetColumnIndex
	idx := schema.GetColumnIndex("age")
	if idx != 2 {
		t.Errorf("expected index 2, got %d", idx)
	}

	idx = schema.GetColumnIndex("nonexistent")
	if idx != -1 {
		t.Errorf("expected -1 for nonexistent column, got %d", idx)
	}

	// Test ColumnNames
	names := schema.ColumnNames()
	expected := []string{"id", "name", "age"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("expected %q at position %d, got %q", expected[i], i, name)
		}
	}

	// Test HasColumn
	if !schema.HasColumn("id") {
		t.Error("HasColumn should return true for 'id'")
	}
	if schema.HasColumn("nonexistent") {
		t.Error("HasColumn should return false for 'nonexistent'")
	}
}

func TestCatalogConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	cat, _ := NewCatalog(tmpDir)

	// Concurrent reads and writes
	done := make(chan bool)

	// Writer
	go func() {
		for i := 0; i < 100; i++ {
			schema := NewTableSchema("concurrent_" + string(rune('a'+i%26)))
			schema.AddColumn("id", types.TypeInt64, false)
			_ = cat.RegisterTable(schema)
		}
		done <- true
	}()

	// Reader
	go func() {
		for i := 0; i < 100; i++ {
			cat.ListTables()
			cat.TableExists("concurrent_a")
		}
		done <- true
	}()

	<-done
	<-done
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
