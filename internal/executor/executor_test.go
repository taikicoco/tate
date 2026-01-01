package executor

import (
	"os"
	"testing"

	"github.com/taikicoco/tate/internal/parser"
	"github.com/taikicoco/tate/internal/storage"
)

// testEnv holds test environment
type testEnv struct {
	catalog *storage.Catalog
	exec    *Executor
	dataDir string
}

func setupTest(t *testing.T) *testEnv {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "tate_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	catalog, err := storage.NewCatalog(dataDir)
	if err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("failed to create catalog: %v", err)
	}

	exec := New(catalog, dataDir)

	return &testEnv{
		catalog: catalog,
		exec:    exec,
		dataDir: dataDir,
	}
}

func (e *testEnv) cleanup() {
	os.RemoveAll(e.dataDir)
}

func (e *testEnv) execute(t *testing.T, sql string) (*Result, error) {
	t.Helper()
	l := parser.NewLexer(sql)
	p := parser.NewParser(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parse error: %v", p.Errors())
	}

	return e.exec.Execute(stmt)
}

func (e *testEnv) mustExecute(t *testing.T, sql string) *Result {
	t.Helper()
	result, err := e.execute(t, sql)
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	return result
}

// ============================================
// CREATE TABLE Tests
// ============================================

func TestCreateTable(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	result := env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")

	if result.Message == "" {
		t.Error("expected success message")
	}

	if !env.catalog.TableExists("users") {
		t.Error("table should exist in catalog")
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64)")

	_, err := env.execute(t, "CREATE TABLE users (id INT64)")
	if err == nil {
		t.Error("expected error for duplicate table")
	}
}

func TestCreateTableMultipleTypes(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	result := env.mustExecute(t, `
		CREATE TABLE test (
			id INT64,
			score FLOAT64,
			name STRING,
			active BOOL
		)
	`)

	if result.Message == "" {
		t.Error("expected success message")
	}

	schema, ok := env.catalog.GetTable("test")
	if !ok {
		t.Fatal("table not found")
	}

	if len(schema.Columns) != 4 {
		t.Errorf("expected 4 columns, got %d", len(schema.Columns))
	}
}

// ============================================
// DROP TABLE Tests
// ============================================

func TestDropTable(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64)")
	result := env.mustExecute(t, "DROP TABLE users")

	if result.Message == "" {
		t.Error("expected success message")
	}

	if env.catalog.TableExists("users") {
		t.Error("table should not exist after drop")
	}
}

func TestDropTableNotExists(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	_, err := env.execute(t, "DROP TABLE nonexistent")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

// ============================================
// INSERT Tests
// ============================================

func TestInsertAllColumns(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")
	result := env.mustExecute(t, "INSERT INTO users VALUES (1, 'Alice')")

	if result.Message != "1 row inserted" {
		t.Errorf("expected '1 row inserted', got %q", result.Message)
	}
}

func TestInsertSpecificColumns(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	env.mustExecute(t, "INSERT INTO users (id, name) VALUES (1, 'Alice')")

	result := env.mustExecute(t, "SELECT * FROM users")

	if result.RowCount() != 1 {
		t.Fatalf("expected 1 row, got %d", result.RowCount())
	}

	// age should be NULL
	if !result.Rows[0][2].IsNull {
		t.Error("expected NULL for unspecified column")
	}
}

func TestInsertColumnCountMismatch(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")

	_, err := env.execute(t, "INSERT INTO users VALUES (1)")
	if err == nil {
		t.Error("expected error for column count mismatch")
	}
}

func TestInsertIntoNonExistentTable(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	_, err := env.execute(t, "INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestInsertMultipleRows(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")
	env.mustExecute(t, "INSERT INTO users VALUES (1, 'Alice')")
	env.mustExecute(t, "INSERT INTO users VALUES (2, 'Bob')")
	env.mustExecute(t, "INSERT INTO users VALUES (3, 'Charlie')")

	result := env.mustExecute(t, "SELECT * FROM users")

	if result.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", result.RowCount())
	}
}

// ============================================
// SELECT Tests
// ============================================

func TestSelectAll(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")
	env.mustExecute(t, "INSERT INTO users VALUES (1, 'Alice')")
	env.mustExecute(t, "INSERT INTO users VALUES (2, 'Bob')")

	result := env.mustExecute(t, "SELECT * FROM users")

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}

	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestSelectSpecificColumns(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	env.mustExecute(t, "INSERT INTO users VALUES (1, 'Alice', 30)")

	result := env.mustExecute(t, "SELECT name, age FROM users")

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}

	if result.Columns[0] != "name" || result.Columns[1] != "age" {
		t.Errorf("unexpected columns: %v", result.Columns)
	}

	name, ok := result.Rows[0][0].AsString()
	if !ok || name != "Alice" {
		t.Errorf("expected 'Alice', got %v", result.Rows[0][0])
	}

	age, ok := result.Rows[0][1].AsInt64()
	if !ok || age != 30 {
		t.Errorf("expected 30, got %v", result.Rows[0][1])
	}
}

func TestSelectEmptyTable(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")

	result := env.mustExecute(t, "SELECT * FROM users")

	if result.RowCount() != 0 {
		t.Errorf("expected 0 rows, got %d", result.RowCount())
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
}

func TestSelectNonExistentColumn(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")

	_, err := env.execute(t, "SELECT nonexistent FROM users")
	if err == nil {
		t.Error("expected error for non-existent column")
	}
}

func TestSelectFromNonExistentTable(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	_, err := env.execute(t, "SELECT * FROM nonexistent")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

// ============================================
// Integration Tests
// ============================================

func TestFullFlow(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	// CREATE
	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING, active BOOL)")

	// INSERT
	env.mustExecute(t, "INSERT INTO users VALUES (1, 'Alice', TRUE)")
	env.mustExecute(t, "INSERT INTO users VALUES (2, 'Bob', FALSE)")

	// SELECT
	result := env.mustExecute(t, "SELECT * FROM users")
	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}

	// Verify data
	id, _ := result.Rows[0][0].AsInt64()
	name, _ := result.Rows[0][1].AsString()
	active, _ := result.Rows[0][2].AsBool()

	if id != 1 || name != "Alice" || active != true {
		t.Errorf("unexpected first row: %v", result.Rows[0])
	}

	// DROP
	env.mustExecute(t, "DROP TABLE users")

	if env.catalog.TableExists("users") {
		t.Error("table should not exist after drop")
	}
}

func TestDataTypes(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, `
		CREATE TABLE types_test (
			int_col INT64,
			float_col FLOAT64,
			str_col STRING,
			bool_col BOOL
		)
	`)

	env.mustExecute(t, "INSERT INTO types_test VALUES (42, 3.14, 'hello', TRUE)")

	result := env.mustExecute(t, "SELECT * FROM types_test")

	row := result.Rows[0]

	intVal, ok := row[0].AsInt64()
	if !ok || intVal != 42 {
		t.Errorf("expected int 42, got %v", row[0])
	}

	floatVal, ok := row[1].AsFloat64()
	if !ok || floatVal != 3.14 {
		t.Errorf("expected float 3.14, got %v", row[1])
	}

	strVal, ok := row[2].AsString()
	if !ok || strVal != "hello" {
		t.Errorf("expected string 'hello', got %v", row[2])
	}

	boolVal, ok := row[3].AsBool()
	if !ok || boolVal != true {
		t.Errorf("expected bool true, got %v", row[3])
	}
}

func TestNullValues(t *testing.T) {
	env := setupTest(t)
	defer env.cleanup()

	env.mustExecute(t, "CREATE TABLE users (id INT64, name STRING)")
	env.mustExecute(t, "INSERT INTO users VALUES (1, NULL)")

	result := env.mustExecute(t, "SELECT * FROM users")

	if !result.Rows[0][1].IsNull {
		t.Error("expected NULL value")
	}
}
