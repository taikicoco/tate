package storage

import (
	"testing"

	"github.com/taikicoco/tate/internal/catalog"
	"github.com/taikicoco/tate/internal/types"
)

func TestColumnFileInt64(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test_int64.col"

	cf := NewColumnFile(path, types.TypeInt64)

	// Append values
	cf.AppendInt64(100, false)
	cf.AppendInt64(200, false)
	cf.AppendInt64(0, true) // NULL
	cf.AppendInt64(300, false)

	if cf.RowCount() != 4 {
		t.Errorf("expected 4 rows, got %d", cf.RowCount())
	}

	// Read values
	v1, ok := cf.GetInt64(0)
	if !ok || v1 != 100 {
		t.Errorf("expected 100, got %d", v1)
	}

	v2, ok := cf.GetInt64(1)
	if !ok || v2 != 200 {
		t.Errorf("expected 200, got %d", v2)
	}

	// NULL value
	if !cf.IsNull(2) {
		t.Error("row 2 should be NULL")
	}

	_, ok = cf.GetInt64(2)
	if ok {
		t.Error("GetInt64 should return false for NULL")
	}

	v4, ok := cf.GetInt64(3)
	if !ok || v4 != 300 {
		t.Errorf("expected 300, got %d", v4)
	}

	// Save and reload
	if err := cf.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	cf2, err := LoadColumnFile(path)
	if err != nil {
		t.Fatalf("LoadColumnFile failed: %v", err)
	}

	if cf2.RowCount() != 4 {
		t.Errorf("expected 4 rows after reload, got %d", cf2.RowCount())
	}

	v1r, ok := cf2.GetInt64(0)
	if !ok || v1r != 100 {
		t.Errorf("expected 100 after reload, got %d", v1r)
	}

	if !cf2.IsNull(2) {
		t.Error("row 2 should be NULL after reload")
	}
}

func TestColumnFileFloat64(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test_float64.col"

	cf := NewColumnFile(path, types.TypeFloat64)

	cf.AppendFloat64(3.14, false)
	cf.AppendFloat64(2.718, false)
	cf.AppendFloat64(0, true) // NULL

	v1, ok := cf.GetFloat64(0)
	if !ok || v1 != 3.14 {
		t.Errorf("expected 3.14, got %f", v1)
	}

	v2, ok := cf.GetFloat64(1)
	if !ok || v2 != 2.718 {
		t.Errorf("expected 2.718, got %f", v2)
	}

	if !cf.IsNull(2) {
		t.Error("row 2 should be NULL")
	}
}

func TestColumnFileString(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test_string.col"

	cf := NewColumnFile(path, types.TypeString)

	cf.AppendString("hello", false)
	cf.AppendString("world", false)
	cf.AppendString("", true) // NULL
	cf.AppendString("foo", false)

	if cf.RowCount() != 4 {
		t.Errorf("expected 4 rows, got %d", cf.RowCount())
	}

	v1, ok := cf.GetString(0)
	if !ok || v1 != "hello" {
		t.Errorf("expected 'hello', got %q", v1)
	}

	v2, ok := cf.GetString(1)
	if !ok || v2 != "world" {
		t.Errorf("expected 'world', got %q", v2)
	}

	if !cf.IsNull(2) {
		t.Error("row 2 should be NULL")
	}

	v4, ok := cf.GetString(3)
	if !ok || v4 != "foo" {
		t.Errorf("expected 'foo', got %q", v4)
	}

	// Save and reload
	if err := cf.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	cf2, err := LoadColumnFile(path)
	if err != nil {
		t.Fatalf("LoadColumnFile failed: %v", err)
	}

	v1r, ok := cf2.GetString(0)
	if !ok || v1r != "hello" {
		t.Errorf("expected 'hello' after reload, got %q", v1r)
	}
}

func TestColumnFileBool(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test_bool.col"

	cf := NewColumnFile(path, types.TypeBool)

	cf.AppendBool(true, false)
	cf.AppendBool(false, false)
	cf.AppendBool(false, true) // NULL

	v1, ok := cf.GetBool(0)
	if !ok || v1 != true {
		t.Errorf("expected true, got %v", v1)
	}

	v2, ok := cf.GetBool(1)
	if !ok || v2 != false {
		t.Errorf("expected false, got %v", v2)
	}

	if !cf.IsNull(2) {
		t.Error("row 2 should be NULL")
	}
}

func TestColumnFileGetValue(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test_value.col"

	cf := NewColumnFile(path, types.TypeInt64)
	cf.AppendInt64(42, false)
	cf.AppendInt64(0, true) // NULL

	v1 := cf.GetValue(0)
	if v1.IsNull {
		t.Error("value should not be NULL")
	}
	if val, _ := v1.AsInt64(); val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	v2 := cf.GetValue(1)
	if !v2.IsNull {
		t.Error("value should be NULL")
	}
}

func TestColumnFileStats(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test_stats.col"

	cf := NewColumnFile(path, types.TypeInt64)
	cf.AppendInt64(50, false)
	cf.AppendInt64(10, false)
	cf.AppendInt64(30, false)
	cf.AppendInt64(100, false)
	cf.AppendInt64(20, false)

	minVal, hasStats := cf.MinValue()
	if !hasStats {
		t.Fatal("expected stats to be available")
	}
	if min, _ := minVal.AsInt64(); min != 10 {
		t.Errorf("expected min 10, got %d", min)
	}

	maxVal, _ := cf.MaxValue()
	if max, _ := maxVal.AsInt64(); max != 100 {
		t.Errorf("expected max 100, got %d", max)
	}
}

func TestTable(t *testing.T) {
	tmpDir := t.TempDir()

	// Create schema
	schema := catalog.NewTableSchema("users")
	schema.AddColumn("id", types.TypeInt64, false)
	schema.AddColumn("name", types.TypeString, true)
	schema.AddColumn("age", types.TypeInt64, true)

	// Create table
	table, err := CreateTable(tmpDir, schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert rows
	err = table.Insert([]types.Value{
		types.NewInt64Value(1),
		types.NewStringValue("Alice"),
		types.NewInt64Value(30),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = table.Insert([]types.Value{
		types.NewInt64Value(2),
		types.NewStringValue("Bob"),
		types.NewInt64Value(25),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = table.Insert([]types.Value{
		types.NewInt64Value(3),
		types.NewNullValue(), // NULL name
		types.NewInt64Value(35),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Check row count
	if table.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", table.RowCount())
	}

	// Get row
	row, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("GetRow failed: %v", err)
	}

	id, _ := row[0].AsInt64()
	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}

	name, _ := row[1].AsString()
	if name != "Alice" {
		t.Errorf("expected name 'Alice', got %q", name)
	}

	// Get column
	ages, err := table.GetColumn("age")
	if err != nil {
		t.Fatalf("GetColumn failed: %v", err)
	}

	if len(ages) != 3 {
		t.Errorf("expected 3 ages, got %d", len(ages))
	}

	age1, _ := ages[0].AsInt64()
	if age1 != 30 {
		t.Errorf("expected age 30, got %d", age1)
	}

	// Check NULL value
	row3, _ := table.GetRow(2)
	if !row3[1].IsNull {
		t.Error("expected NULL name for row 3")
	}

	// Save table
	if err := table.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Reload table
	table2, err := LoadTable(tmpDir, "users")
	if err != nil {
		t.Fatalf("LoadTable failed: %v", err)
	}

	if table2.RowCount() != 3 {
		t.Errorf("expected 3 rows after reload, got %d", table2.RowCount())
	}

	row2, _ := table2.GetRow(1)
	name2, _ := row2[1].AsString()
	if name2 != "Bob" {
		t.Errorf("expected 'Bob' after reload, got %q", name2)
	}
}

func TestTableInsertMap(t *testing.T) {
	tmpDir := t.TempDir()

	schema := catalog.NewTableSchema("products")
	schema.AddColumn("id", types.TypeInt64, false)
	schema.AddColumn("name", types.TypeString, true)
	schema.AddColumn("price", types.TypeFloat64, true)

	table, _ := CreateTable(tmpDir, schema)

	err := table.InsertMap(map[string]types.Value{
		"id":    types.NewInt64Value(1),
		"name":  types.NewStringValue("Widget"),
		"price": types.NewFloat64Value(19.99),
	})
	if err != nil {
		t.Fatalf("InsertMap failed: %v", err)
	}

	// Insert with missing column (should be NULL)
	err = table.InsertMap(map[string]types.Value{
		"id":   types.NewInt64Value(2),
		"name": types.NewStringValue("Gadget"),
		// price is missing
	})
	if err != nil {
		t.Fatalf("InsertMap failed: %v", err)
	}

	row, _ := table.GetRow(1)
	if !row[2].IsNull {
		t.Error("expected NULL price for row 2")
	}
}

func TestTableScan(t *testing.T) {
	tmpDir := t.TempDir()

	schema := catalog.NewTableSchema("data")
	schema.AddColumn("value", types.TypeInt64, false)

	table, _ := CreateTable(tmpDir, schema)

	// Insert 10 rows
	for i := int64(0); i < 10; i++ {
		_ = table.Insert([]types.Value{types.NewInt64Value(i * 10)})
	}

	// Scan all rows
	sum := int64(0)
	count := 0
	_ = table.Scan(func(rowIndex uint64, row []types.Value) bool {
		val, _ := row[0].AsInt64()
		sum += val
		count++
		return true
	})

	if count != 10 {
		t.Errorf("expected 10 rows scanned, got %d", count)
	}

	expectedSum := int64(0 + 10 + 20 + 30 + 40 + 50 + 60 + 70 + 80 + 90)
	if sum != expectedSum {
		t.Errorf("expected sum %d, got %d", expectedSum, sum)
	}

	// Scan with early termination
	earlyCount := 0
	_ = table.Scan(func(rowIndex uint64, row []types.Value) bool {
		earlyCount++
		return earlyCount < 5 // Stop after 5 rows
	})

	if earlyCount != 5 {
		t.Errorf("expected 5 rows with early termination, got %d", earlyCount)
	}
}

func TestTableScanColumns(t *testing.T) {
	tmpDir := t.TempDir()

	schema := catalog.NewTableSchema("multi")
	schema.AddColumn("a", types.TypeInt64, false)
	schema.AddColumn("b", types.TypeString, false)
	schema.AddColumn("c", types.TypeFloat64, false)

	table, _ := CreateTable(tmpDir, schema)

	_ = table.Insert([]types.Value{
		types.NewInt64Value(1),
		types.NewStringValue("one"),
		types.NewFloat64Value(1.0),
	})
	_ = table.Insert([]types.Value{
		types.NewInt64Value(2),
		types.NewStringValue("two"),
		types.NewFloat64Value(2.0),
	})

	// Scan only columns a and c
	count := 0
	_ = table.ScanColumns([]string{"a", "c"}, func(rowIndex uint64, values []types.Value) bool {
		if len(values) != 2 {
			t.Errorf("expected 2 values, got %d", len(values))
		}
		count++
		return true
	})

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestTableDrop(t *testing.T) {
	tmpDir := t.TempDir()

	schema := catalog.NewTableSchema("temp")
	schema.AddColumn("id", types.TypeInt64, false)

	table, _ := CreateTable(tmpDir, schema)
	_ = table.Insert([]types.Value{types.NewInt64Value(1)})
	_ = table.Save()

	if err := table.Drop(); err != nil {
		t.Fatalf("Drop failed: %v", err)
	}

	// Verify table is gone
	_, err := LoadTable(tmpDir, "temp")
	if err == nil {
		t.Error("expected error when loading dropped table")
	}
}
