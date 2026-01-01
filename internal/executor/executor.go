// Package executor implements the query execution engine.
package executor

import (
	"fmt"
	"strings"

	"github.com/taikicoco/tate/internal/parser"
	"github.com/taikicoco/tate/internal/storage"
)

// Result represents a query result.
type Result struct {
	Columns []string
	Rows    [][]storage.Value
	Message string
}

// NewResult creates a new empty result.
func NewResult() *Result {
	return &Result{
		Columns: make([]string, 0),
		Rows:    make([][]storage.Value, 0),
	}
}

// RowCount returns the number of rows.
func (r *Result) RowCount() int {
	return len(r.Rows)
}

// String returns a formatted string representation of the result.
func (r *Result) String() string {
	if len(r.Columns) == 0 {
		return ""
	}

	var sb strings.Builder

	// Calculate column widths
	widths := make([]int, len(r.Columns))
	for i, col := range r.Columns {
		widths[i] = len(col)
	}
	for _, row := range r.Rows {
		for i, val := range row {
			strLen := len(val.String())
			if strLen > widths[i] {
				widths[i] = strLen
			}
		}
	}

	// Build separator
	separator := "+"
	for _, w := range widths {
		separator += strings.Repeat("-", w+2) + "+"
	}

	// Header
	sb.WriteString(separator)
	sb.WriteString("\n|")
	for i, col := range r.Columns {
		sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], col))
	}
	sb.WriteString("\n")
	sb.WriteString(separator)
	sb.WriteString("\n")

	// Data rows
	for _, row := range r.Rows {
		sb.WriteString("|")
		for i, val := range row {
			sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], val.String()))
		}
		sb.WriteString("\n")
	}

	sb.WriteString(separator)

	return sb.String()
}

// Executor executes SQL statements.
type Executor struct {
	catalog *storage.Catalog
	tables  map[string]*storage.Table
	dataDir string
}

// New creates a new Executor.
func New(cat *storage.Catalog, dataDir string) *Executor {
	return &Executor{
		catalog: cat,
		tables:  make(map[string]*storage.Table),
		dataDir: dataDir,
	}
}

// Execute executes a SQL statement and returns the result.
func (e *Executor) Execute(stmt parser.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStatement:
		return e.executeCreateTable(s)
	case *parser.DropTableStatement:
		return e.executeDropTable(s)
	case *parser.InsertStatement:
		return e.executeInsert(s)
	case *parser.SelectStatement:
		return e.executeSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Executor) executeCreateTable(stmt *parser.CreateTableStatement) (*Result, error) {
	if e.catalog.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table %q already exists", stmt.TableName)
	}

	schema := storage.NewTableSchema(stmt.TableName)
	for _, col := range stmt.Columns {
		dataType := storage.ParseDataType(col.DataType)
		schema.AddColumn(col.Name, dataType, col.Nullable)
	}

	if err := e.catalog.RegisterTable(schema); err != nil {
		return nil, err
	}

	table, err := storage.CreateTable(e.dataDir, schema)
	if err != nil {
		_ = e.catalog.DropTable(stmt.TableName)
		return nil, err
	}

	e.tables[stmt.TableName] = table

	return &Result{
		Message: fmt.Sprintf("Table %q created successfully", stmt.TableName),
	}, nil
}

func (e *Executor) executeDropTable(stmt *parser.DropTableStatement) (*Result, error) {
	if !e.catalog.TableExists(stmt.TableName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	if table, exists := e.tables[stmt.TableName]; exists {
		if err := table.Drop(); err != nil {
			return nil, err
		}
		delete(e.tables, stmt.TableName)
	}

	if err := e.catalog.DropTable(stmt.TableName); err != nil {
		return nil, err
	}

	return &Result{
		Message: fmt.Sprintf("Table %q dropped successfully", stmt.TableName),
	}, nil
}

func (e *Executor) executeInsert(stmt *parser.InsertStatement) (*Result, error) {
	table, err := e.getTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	schema := table.Schema
	var values []storage.Value

	if len(stmt.Columns) > 0 {
		values = make([]storage.Value, len(schema.Columns))
		for i := range values {
			values[i] = storage.NewNullValue()
		}

		for i, colName := range stmt.Columns {
			idx := schema.GetColumnIndex(colName)
			if idx == -1 {
				return nil, fmt.Errorf("column %q not found", colName)
			}
			val, err := e.evaluateLiteral(stmt.Values[i])
			if err != nil {
				return nil, err
			}
			values[idx] = val
		}
	} else {
		if len(stmt.Values) != len(schema.Columns) {
			return nil, fmt.Errorf("column count mismatch: expected %d, got %d",
				len(schema.Columns), len(stmt.Values))
		}

		values = make([]storage.Value, len(stmt.Values))
		for i, expr := range stmt.Values {
			val, err := e.evaluateLiteral(expr)
			if err != nil {
				return nil, err
			}
			values[i] = val
		}
	}

	if err := table.Insert(values); err != nil {
		return nil, err
	}

	if err := table.Save(); err != nil {
		return nil, err
	}

	return &Result{Message: "1 row inserted"}, nil
}

func (e *Executor) executeSelect(stmt *parser.SelectStatement) (*Result, error) {
	table, err := e.getTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	schema := table.Schema
	result := NewResult()

	var selectColumns []string
	var columnIndices []int

	for _, col := range stmt.Columns {
		if col.IsWildcard {
			selectColumns = append(selectColumns, schema.ColumnNames()...)
			for i := range schema.Columns {
				columnIndices = append(columnIndices, i)
			}
		} else if ident, ok := col.Expression.(*parser.Identifier); ok {
			idx := schema.GetColumnIndex(ident.Name)
			if idx == -1 {
				return nil, fmt.Errorf("column %q not found", ident.Name)
			}
			selectColumns = append(selectColumns, ident.Name)
			columnIndices = append(columnIndices, idx)
		}
	}

	result.Columns = selectColumns

	_ = table.Scan(func(rowIndex uint64, row []storage.Value) bool {
		resultRow := make([]storage.Value, len(columnIndices))
		for i, idx := range columnIndices {
			resultRow[i] = row[idx]
		}
		result.Rows = append(result.Rows, resultRow)
		return true
	})

	return result, nil
}

func (e *Executor) evaluateLiteral(expr parser.Expression) (storage.Value, error) {
	switch ex := expr.(type) {
	case *parser.IntegerLiteral:
		return storage.NewInt64Value(ex.Value), nil
	case *parser.FloatLiteral:
		return storage.NewFloat64Value(ex.Value), nil
	case *parser.StringLiteral:
		return storage.NewStringValue(ex.Value), nil
	case *parser.BoolLiteral:
		return storage.NewBoolValue(ex.Value), nil
	case *parser.NullLiteral:
		return storage.NewNullValue(), nil
	default:
		return storage.NewNullValue(), fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) getTable(name string) (*storage.Table, error) {
	if table, exists := e.tables[name]; exists {
		return table, nil
	}

	if !e.catalog.TableExists(name) {
		return nil, fmt.Errorf("table %q does not exist", name)
	}

	table, err := storage.LoadTable(e.dataDir, name)
	if err != nil {
		return nil, err
	}

	e.tables[name] = table
	return table, nil
}
