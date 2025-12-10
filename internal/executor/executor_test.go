package executor

import (
	"testing"

	"github.com/taikicoco/tate/internal/ast"
	"github.com/taikicoco/tate/internal/catalog"
	"github.com/taikicoco/tate/internal/lexer"
	"github.com/taikicoco/tate/internal/parser"
	"github.com/taikicoco/tate/internal/types"
)

func setupExecutor(t *testing.T) (*Executor, string) {
	tmpDir := t.TempDir()
	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	return New(cat, tmpDir), tmpDir
}

func executeSQL(t *testing.T, exec *Executor, sql string) *Result {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("execution error: %v", err)
	}

	return result
}

func TestCreateTable(t *testing.T) {
	exec, _ := setupExecutor(t)

	result := executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")

	if result.Message == "" {
		t.Error("expected success message")
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64)")

	l := lexer.New("CREATE TABLE users (id INT64)")
	p := parser.New(l)
	stmt := p.Parse()

	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("expected error for duplicate table")
	}
}

func TestInsertAndSelect(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT * FROM users")

	if result.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", result.RowCount())
	}

	if result.ColumnCount() != 3 {
		t.Errorf("expected 3 columns, got %d", result.ColumnCount())
	}

	// Check first row
	val, _ := result.GetValue(0, 0)
	if v, _ := val.AsInt64(); v != 1 {
		t.Errorf("expected id 1, got %d", v)
	}

	val, _ = result.GetValue(0, 1)
	if v, _ := val.AsString(); v != "Alice" {
		t.Errorf("expected name 'Alice', got %q", v)
	}
}

func TestSelectColumns(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")

	result := executeSQL(t, exec, "SELECT name, age FROM users")

	if result.ColumnCount() != 2 {
		t.Errorf("expected 2 columns, got %d", result.ColumnCount())
	}

	if result.Columns[0] != "name" || result.Columns[1] != "age" {
		t.Errorf("unexpected columns: %v", result.Columns)
	}
}

func TestSelectWithWhere(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT * FROM users WHERE age > 25")

	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestSelectWithComplexWhere(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT * FROM users WHERE age >= 25 AND age <= 30")

	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestSelectWithOrderBy(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT * FROM users ORDER BY age")

	// Should be ordered: Bob (25), Alice (30), Charlie (35)
	val, _ := result.GetValue(0, 1)
	if v, _ := val.AsString(); v != "Bob" {
		t.Errorf("expected first row to be Bob, got %q", v)
	}

	val, _ = result.GetValue(2, 1)
	if v, _ := val.AsString(); v != "Charlie" {
		t.Errorf("expected last row to be Charlie, got %q", v)
	}
}

func TestSelectWithOrderByDesc(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT * FROM users ORDER BY age DESC")

	// Should be ordered: Charlie (35), Alice (30), Bob (25)
	val, _ := result.GetValue(0, 1)
	if v, _ := val.AsString(); v != "Charlie" {
		t.Errorf("expected first row to be Charlie, got %q", v)
	}
}

func TestSelectWithLimit(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT * FROM users LIMIT 2")

	if result.RowCount() != 2 {
		t.Errorf("expected 2 rows, got %d", result.RowCount())
	}
}

func TestSelectCount(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT COUNT(*) FROM users")

	if result.RowCount() != 1 {
		t.Errorf("expected 1 row, got %d", result.RowCount())
	}

	val, _ := result.GetValue(0, 0)
	if v, _ := val.AsInt64(); v != 3 {
		t.Errorf("expected COUNT(*) = 3, got %d", v)
	}
}

func TestSelectSum(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT SUM(age) FROM users")

	val, _ := result.GetValue(0, 0)
	if v, _ := val.AsFloat64(); v != 90.0 {
		t.Errorf("expected SUM(age) = 90, got %f", v)
	}
}

func TestSelectAvg(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT AVG(age) FROM users")

	val, _ := result.GetValue(0, 0)
	if v, _ := val.AsFloat64(); v != 30.0 {
		t.Errorf("expected AVG(age) = 30, got %f", v)
	}
}

func TestSelectMinMax(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users VALUES (3, 'Charlie', 35)")

	result := executeSQL(t, exec, "SELECT MIN(age), MAX(age) FROM users")

	minVal, _ := result.GetValue(0, 0)
	if v, _ := minVal.AsInt64(); v != 25 {
		t.Errorf("expected MIN(age) = 25, got %d", v)
	}

	maxVal, _ := result.GetValue(0, 1)
	if v, _ := maxVal.AsInt64(); v != 35 {
		t.Errorf("expected MAX(age) = 35, got %d", v)
	}
}

func TestSelectDistinct(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE data (value INT64)")
	executeSQL(t, exec, "INSERT INTO data VALUES (1)")
	executeSQL(t, exec, "INSERT INTO data VALUES (2)")
	executeSQL(t, exec, "INSERT INTO data VALUES (1)")
	executeSQL(t, exec, "INSERT INTO data VALUES (3)")
	executeSQL(t, exec, "INSERT INTO data VALUES (2)")

	result := executeSQL(t, exec, "SELECT DISTINCT value FROM data")

	if result.RowCount() != 3 {
		t.Errorf("expected 3 distinct values, got %d", result.RowCount())
	}
}

func TestDropTable(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE temp (id INT64)")
	result := executeSQL(t, exec, "DROP TABLE temp")

	if result.Message == "" {
		t.Error("expected success message")
	}

	// Try to select from dropped table
	l := lexer.New("SELECT * FROM temp")
	p := parser.New(l)
	stmt := p.Parse()

	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("expected error when selecting from dropped table")
	}
}

func TestInsertWithNull(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING, age INT64)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, NULL, 30)")

	result := executeSQL(t, exec, "SELECT * FROM users")

	val, _ := result.GetValue(0, 1)
	if !val.IsNull {
		t.Error("expected NULL value")
	}
}

func TestSelectWithNullComparison(t *testing.T) {
	exec, _ := setupExecutor(t)

	executeSQL(t, exec, "CREATE TABLE users (id INT64, name STRING)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice')")
	executeSQL(t, exec, "INSERT INTO users VALUES (2, NULL)")

	// NULL comparisons should not match
	result := executeSQL(t, exec, "SELECT * FROM users WHERE name = 'Alice'")

	if result.RowCount() != 1 {
		t.Errorf("expected 1 row, got %d", result.RowCount())
	}
}

func TestResultString(t *testing.T) {
	result := NewResult()
	result.Columns = []string{"id", "name"}
	result.Rows = [][]types.Value{
		{types.NewInt64Value(1), types.NewStringValue("Alice")},
		{types.NewInt64Value(2), types.NewStringValue("Bob")},
	}

	str := result.String()

	if str == "" {
		t.Error("expected non-empty string")
	}

	// Should contain column names
	if !contains(str, "id") || !contains(str, "name") {
		t.Error("result string should contain column names")
	}

	// Should contain values
	if !contains(str, "Alice") || !contains(str, "Bob") {
		t.Error("result string should contain values")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestEvaluateExpression(t *testing.T) {
	exec, _ := setupExecutor(t)

	// Test integer literal
	expr := &ast.IntegerLiteral{Value: 42}
	val, err := exec.evaluateExpression(expr, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := val.AsInt64(); v != 42 {
		t.Errorf("expected 42, got %d", v)
	}

	// Test string literal
	strExpr := &ast.StringLiteral{Value: "hello"}
	val, err = exec.evaluateExpression(strExpr, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := val.AsString(); v != "hello" {
		t.Errorf("expected 'hello', got %q", v)
	}

	// Test binary expression
	binExpr := &ast.BinaryExpression{
		Left:     &ast.IntegerLiteral{Value: 10},
		Operator: "+",
		Right:    &ast.IntegerLiteral{Value: 5},
	}
	val, err = exec.evaluateExpression(binExpr, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := val.AsFloat64(); v != 15.0 {
		t.Errorf("expected 15, got %f", v)
	}
}
