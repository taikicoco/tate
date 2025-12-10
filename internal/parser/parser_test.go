package parser

import (
	"testing"

	"github.com/taikicoco/tate/internal/ast"
	"github.com/taikicoco/tate/internal/lexer"
	"github.com/taikicoco/tate/internal/types"
)

func TestCreateTableStatement(t *testing.T) {
	input := `CREATE TABLE users (id INT64, name STRING, age INT64)`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	createStmt, ok := stmt.(*ast.CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", createStmt.TableName)
	}

	if len(createStmt.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(createStmt.Columns))
	}

	expected := []struct {
		name     string
		dataType types.DataType
	}{
		{"id", types.TypeInt64},
		{"name", types.TypeString},
		{"age", types.TypeInt64},
	}

	for i, col := range createStmt.Columns {
		if col.Name != expected[i].name {
			t.Errorf("column %d: expected name %q, got %q", i, expected[i].name, col.Name)
		}
		if col.DataType != expected[i].dataType {
			t.Errorf("column %d: expected type %v, got %v", i, expected[i].dataType, col.DataType)
		}
	}
}

func TestInsertStatement(t *testing.T) {
	input := `INSERT INTO users VALUES (1, 'Alice', 30)`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	insertStmt, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	if insertStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", insertStmt.TableName)
	}

	if len(insertStmt.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(insertStmt.Values))
	}

	// Check first value (integer)
	intLit, ok := insertStmt.Values[0].(*ast.IntegerLiteral)
	if !ok {
		t.Fatalf("expected IntegerLiteral, got %T", insertStmt.Values[0])
	}
	if intLit.Value != 1 {
		t.Errorf("expected 1, got %d", intLit.Value)
	}

	// Check second value (string)
	strLit, ok := insertStmt.Values[1].(*ast.StringLiteral)
	if !ok {
		t.Fatalf("expected StringLiteral, got %T", insertStmt.Values[1])
	}
	if strLit.Value != "Alice" {
		t.Errorf("expected 'Alice', got %q", strLit.Value)
	}
}

func TestInsertWithColumns(t *testing.T) {
	input := `INSERT INTO users (name, age) VALUES ('Bob', 25)`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	insertStmt, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	if len(insertStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(insertStmt.Columns))
	}

	if insertStmt.Columns[0] != "name" || insertStmt.Columns[1] != "age" {
		t.Errorf("unexpected columns: %v", insertStmt.Columns)
	}
}

func TestSelectAll(t *testing.T) {
	input := `SELECT * FROM users`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", selectStmt.TableName)
	}

	if len(selectStmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(selectStmt.Columns))
	}

	if !selectStmt.Columns[0].IsWildcard {
		t.Error("expected wildcard column")
	}
}

func TestSelectColumns(t *testing.T) {
	input := `SELECT name, age FROM users`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(selectStmt.Columns))
	}

	col1, ok := selectStmt.Columns[0].Expression.(*ast.Identifier)
	if !ok || col1.Name != "name" {
		t.Errorf("expected column 'name', got %v", selectStmt.Columns[0])
	}

	col2, ok := selectStmt.Columns[1].Expression.(*ast.Identifier)
	if !ok || col2.Name != "age" {
		t.Errorf("expected column 'age', got %v", selectStmt.Columns[1])
	}
}

func TestSelectWithWhere(t *testing.T) {
	input := `SELECT * FROM users WHERE age > 25`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}

	binExpr, ok := selectStmt.Where.(*ast.BinaryExpression)
	if !ok {
		t.Fatalf("expected BinaryExpression, got %T", selectStmt.Where)
	}

	if binExpr.Operator != ">" {
		t.Errorf("expected operator '>', got %q", binExpr.Operator)
	}

	left, ok := binExpr.Left.(*ast.Identifier)
	if !ok || left.Name != "age" {
		t.Errorf("expected left operand 'age', got %v", binExpr.Left)
	}

	right, ok := binExpr.Right.(*ast.IntegerLiteral)
	if !ok || right.Value != 25 {
		t.Errorf("expected right operand 25, got %v", binExpr.Right)
	}
}

func TestSelectWithComplexWhere(t *testing.T) {
	input := `SELECT * FROM users WHERE age >= 18 AND age <= 65`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	binExpr, ok := selectStmt.Where.(*ast.BinaryExpression)
	if !ok {
		t.Fatalf("expected BinaryExpression, got %T", selectStmt.Where)
	}

	if binExpr.Operator != "AND" {
		t.Errorf("expected operator 'AND', got %q", binExpr.Operator)
	}
}

func TestSelectWithOrderBy(t *testing.T) {
	input := `SELECT * FROM users ORDER BY age DESC`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY clause, got %d", len(selectStmt.OrderBy))
	}

	if selectStmt.OrderBy[0].Column != "age" {
		t.Errorf("expected column 'age', got %q", selectStmt.OrderBy[0].Column)
	}

	if !selectStmt.OrderBy[0].Desc {
		t.Error("expected DESC order")
	}
}

func TestSelectWithLimit(t *testing.T) {
	input := `SELECT * FROM users LIMIT 10`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if selectStmt.Limit == nil {
		t.Fatal("expected LIMIT clause")
	}

	if *selectStmt.Limit != 10 {
		t.Errorf("expected limit 10, got %d", *selectStmt.Limit)
	}
}

func TestSelectWithAggregates(t *testing.T) {
	input := `SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(selectStmt.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(selectStmt.Columns))
	}

	expectedFuncs := []string{"COUNT", "SUM", "AVG", "MIN", "MAX"}

	for i, col := range selectStmt.Columns {
		fn, ok := col.Expression.(*ast.FunctionCall)
		if !ok {
			t.Fatalf("column %d: expected FunctionCall, got %T", i, col.Expression)
		}
		if fn.Name != expectedFuncs[i] {
			t.Errorf("column %d: expected function %q, got %q", i, expectedFuncs[i], fn.Name)
		}
	}
}

func TestSelectDistinct(t *testing.T) {
	input := `SELECT DISTINCT name FROM users`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if !selectStmt.Distinct {
		t.Error("expected DISTINCT to be true")
	}
}

func TestDropTable(t *testing.T) {
	input := `DROP TABLE users`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	dropStmt, ok := stmt.(*ast.DropTableStatement)
	if !ok {
		t.Fatalf("expected DropTableStatement, got %T", stmt)
	}

	if dropStmt.TableName != "users" {
		t.Errorf("expected table name 'users', got %q", dropStmt.TableName)
	}
}

func TestNullLiteral(t *testing.T) {
	input := `INSERT INTO users VALUES (1, NULL, 30)`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	insertStmt, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	_, ok = insertStmt.Values[1].(*ast.NullLiteral)
	if !ok {
		t.Fatalf("expected NullLiteral, got %T", insertStmt.Values[1])
	}
}

func TestBoolLiteral(t *testing.T) {
	input := `INSERT INTO flags VALUES (TRUE, FALSE)`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	insertStmt, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	trueLit, ok := insertStmt.Values[0].(*ast.BoolLiteral)
	if !ok || !trueLit.Value {
		t.Error("expected TRUE")
	}

	falseLit, ok := insertStmt.Values[1].(*ast.BoolLiteral)
	if !ok || falseLit.Value {
		t.Error("expected FALSE")
	}
}

func TestFloatLiteral(t *testing.T) {
	input := `INSERT INTO products VALUES (1, 19.99)`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	insertStmt, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	floatLit, ok := insertStmt.Values[1].(*ast.FloatLiteral)
	if !ok {
		t.Fatalf("expected FloatLiteral, got %T", insertStmt.Values[1])
	}

	if floatLit.Value != 19.99 {
		t.Errorf("expected 19.99, got %f", floatLit.Value)
	}
}

func TestComplexQuery(t *testing.T) {
	input := `SELECT name, age FROM users WHERE age >= 18 AND age <= 65 ORDER BY age DESC LIMIT 10`

	l := lexer.New(input)
	p := New(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		t.Fatalf("parser errors: %v", p.Errors())
	}

	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	// Verify all parts are present
	if len(selectStmt.Columns) != 2 {
		t.Error("expected 2 columns")
	}
	if selectStmt.TableName != "users" {
		t.Error("expected table 'users'")
	}
	if selectStmt.Where == nil {
		t.Error("expected WHERE clause")
	}
	if len(selectStmt.OrderBy) != 1 {
		t.Error("expected 1 ORDER BY clause")
	}
	if selectStmt.Limit == nil || *selectStmt.Limit != 10 {
		t.Error("expected LIMIT 10")
	}
}
