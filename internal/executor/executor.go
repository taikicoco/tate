// Package executor implements the query execution engine.
package executor

import (
	"fmt"
	"sort"
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
			val, err := e.evaluateExpression(stmt.Values[i], nil, nil)
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
			val, err := e.evaluateExpression(expr, nil, nil)
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
	var selectExpressions []parser.Expression
	hasAggregates := false

	for _, col := range stmt.Columns {
		if col.IsWildcard {
			selectColumns = append(selectColumns, schema.ColumnNames()...)
			for _, name := range schema.ColumnNames() {
				selectExpressions = append(selectExpressions, &parser.Identifier{Name: name})
			}
		} else {
			if fn, ok := col.Expression.(*parser.FunctionCall); ok {
				hasAggregates = true
				name := fn.Name
				if len(fn.Arguments) > 0 {
					if ident, ok := fn.Arguments[0].(*parser.Identifier); ok {
						name = fmt.Sprintf("%s(%s)", fn.Name, ident.Name)
					}
				}
				if col.Alias != "" {
					name = col.Alias
				}
				selectColumns = append(selectColumns, name)
			} else if ident, ok := col.Expression.(*parser.Identifier); ok {
				name := ident.Name
				if col.Alias != "" {
					name = col.Alias
				}
				selectColumns = append(selectColumns, name)
			} else {
				selectColumns = append(selectColumns, "?")
			}
			selectExpressions = append(selectExpressions, col.Expression)
		}
	}

	result.Columns = selectColumns

	if hasAggregates {
		return e.executeAggregateSelect(stmt, table, selectExpressions, result)
	}

	var filteredRows [][]storage.Value

	_ = table.Scan(func(rowIndex uint64, row []storage.Value) bool {
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, schema.ColumnNames(), row)
			if err != nil || !match {
				return true
			}
		}

		resultRow := make([]storage.Value, len(selectExpressions))
		for i, expr := range selectExpressions {
			val, _ := e.evaluateExpression(expr, schema.ColumnNames(), row)
			resultRow[i] = val
		}

		filteredRows = append(filteredRows, resultRow)
		return true
	})

	result.Rows = filteredRows

	if stmt.Distinct {
		result.Rows = e.applyDistinct(result.Rows)
	}

	if len(stmt.OrderBy) > 0 {
		e.applyOrderBy(result, stmt.OrderBy)
	}

	if stmt.Offset != nil && *stmt.Offset > 0 {
		offset := int(*stmt.Offset)
		if offset < len(result.Rows) {
			result.Rows = result.Rows[offset:]
		} else {
			result.Rows = nil
		}
	}

	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if limit < len(result.Rows) {
			result.Rows = result.Rows[:limit]
		}
	}

	return result, nil
}

func (e *Executor) executeAggregateSelect(stmt *parser.SelectStatement, table *storage.Table,
	selectExpressions []parser.Expression, result *Result) (*Result, error) {

	schema := table.Schema

	type aggregateState struct {
		count  int64
		sum    float64
		min    storage.Value
		max    storage.Value
		hasMin bool
		hasMax bool
	}

	aggregates := make(map[int]*aggregateState)

	for i, expr := range selectExpressions {
		if _, ok := expr.(*parser.FunctionCall); ok {
			aggregates[i] = &aggregateState{}
		}
	}

	_ = table.Scan(func(rowIndex uint64, row []storage.Value) bool {
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, schema.ColumnNames(), row)
			if err != nil || !match {
				return true
			}
		}

		for i, expr := range selectExpressions {
			fn, ok := expr.(*parser.FunctionCall)
			if !ok {
				continue
			}

			state := aggregates[i]

			switch fn.Name {
			case "COUNT":
				state.count++

			case "SUM", "AVG":
				if len(fn.Arguments) > 0 {
					val, _ := e.evaluateExpression(fn.Arguments[0], schema.ColumnNames(), row)
					if num, ok := val.ToNumeric(); ok {
						state.sum += num
						state.count++
					}
				}

			case "MIN":
				if len(fn.Arguments) > 0 {
					val, _ := e.evaluateExpression(fn.Arguments[0], schema.ColumnNames(), row)
					if !val.IsNull {
						if !state.hasMin || val.Compare(state.min) < 0 {
							state.min = val
							state.hasMin = true
						}
					}
				}

			case "MAX":
				if len(fn.Arguments) > 0 {
					val, _ := e.evaluateExpression(fn.Arguments[0], schema.ColumnNames(), row)
					if !val.IsNull {
						if !state.hasMax || val.Compare(state.max) > 0 {
							state.max = val
							state.hasMax = true
						}
					}
				}
			}
		}

		return true
	})

	resultRow := make([]storage.Value, len(selectExpressions))

	for i, expr := range selectExpressions {
		fn, ok := expr.(*parser.FunctionCall)
		if !ok {
			resultRow[i] = storage.NewNullValue()
			continue
		}

		state := aggregates[i]

		switch fn.Name {
		case "COUNT":
			resultRow[i] = storage.NewInt64Value(state.count)

		case "SUM":
			if state.count == 0 {
				resultRow[i] = storage.NewNullValue()
			} else {
				resultRow[i] = storage.NewFloat64Value(state.sum)
			}

		case "AVG":
			if state.count == 0 {
				resultRow[i] = storage.NewNullValue()
			} else {
				resultRow[i] = storage.NewFloat64Value(state.sum / float64(state.count))
			}

		case "MIN":
			if state.hasMin {
				resultRow[i] = state.min
			} else {
				resultRow[i] = storage.NewNullValue()
			}

		case "MAX":
			if state.hasMax {
				resultRow[i] = state.max
			} else {
				resultRow[i] = storage.NewNullValue()
			}
		}
	}

	result.Rows = [][]storage.Value{resultRow}
	return result, nil
}

func (e *Executor) evaluateExpression(expr parser.Expression, columns []string, row []storage.Value) (storage.Value, error) {
	switch ex := expr.(type) {
	case *parser.Identifier:
		if columns != nil && row != nil {
			for i, col := range columns {
				if col == ex.Name {
					return row[i], nil
				}
			}
		}
		return storage.NewNullValue(), nil

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

	case *parser.BinaryExpression:
		left, err := e.evaluateExpression(ex.Left, columns, row)
		if err != nil {
			return storage.NewNullValue(), err
		}
		right, err := e.evaluateExpression(ex.Right, columns, row)
		if err != nil {
			return storage.NewNullValue(), err
		}

		switch ex.Operator {
		case "+":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok {
					return storage.NewFloat64Value(lv + rv), nil
				}
			}
		case "-":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok {
					return storage.NewFloat64Value(lv - rv), nil
				}
			}
		case "*":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok {
					return storage.NewFloat64Value(lv * rv), nil
				}
			}
		case "/":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok && rv != 0 {
					return storage.NewFloat64Value(lv / rv), nil
				}
			}
		}

		return storage.NewNullValue(), nil

	case *parser.UnaryExpression:
		val, err := e.evaluateExpression(ex.Operand, columns, row)
		if err != nil {
			return storage.NewNullValue(), err
		}

		switch ex.Operator {
		case "-":
			if v, ok := val.AsInt64(); ok {
				return storage.NewInt64Value(-v), nil
			}
			if v, ok := val.AsFloat64(); ok {
				return storage.NewFloat64Value(-v), nil
			}
		case "NOT":
			if v, ok := val.AsBool(); ok {
				return storage.NewBoolValue(!v), nil
			}
		}

		return storage.NewNullValue(), nil

	default:
		return storage.NewNullValue(), fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) evaluateCondition(expr parser.Expression, columns []string, row []storage.Value) (bool, error) {
	switch ex := expr.(type) {
	case *parser.BinaryExpression:
		switch strings.ToUpper(ex.Operator) {
		case "AND":
			left, err := e.evaluateCondition(ex.Left, columns, row)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evaluateCondition(ex.Right, columns, row)

		case "OR":
			left, err := e.evaluateCondition(ex.Left, columns, row)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evaluateCondition(ex.Right, columns, row)

		default:
			left, err := e.evaluateExpression(ex.Left, columns, row)
			if err != nil {
				return false, err
			}
			right, err := e.evaluateExpression(ex.Right, columns, row)
			if err != nil {
				return false, err
			}

			return e.compareValues(left, right, ex.Operator), nil
		}

	case *parser.UnaryExpression:
		if ex.Operator == "NOT" {
			result, err := e.evaluateCondition(ex.Operand, columns, row)
			if err != nil {
				return false, err
			}
			return !result, nil
		}

	case *parser.BoolLiteral:
		return ex.Value, nil
	}

	return false, nil
}

func (e *Executor) compareValues(left, right storage.Value, op string) bool {
	if left.IsNull || right.IsNull {
		return false
	}

	cmp := left.Compare(right)

	switch op {
	case "=":
		return cmp == 0
	case "<>", "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	case ">=":
		return cmp >= 0
	}

	return false
}

func (e *Executor) applyDistinct(rows [][]storage.Value) [][]storage.Value {
	seen := make(map[string]bool)
	var result [][]storage.Value

	for _, row := range rows {
		key := e.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

func (e *Executor) rowKey(row []storage.Value) string {
	var parts []string
	for _, val := range row {
		parts = append(parts, val.String())
	}
	return strings.Join(parts, "\x00")
}

func (e *Executor) applyOrderBy(result *Result, orderBy []parser.OrderByClause) {
	colIndices := make(map[string]int)
	for i, col := range result.Columns {
		colIndices[col] = i
	}

	sort.SliceStable(result.Rows, func(i, j int) bool {
		for _, ob := range orderBy {
			idx, ok := colIndices[ob.Column]
			if !ok {
				continue
			}

			cmp := result.Rows[i][idx].Compare(result.Rows[j][idx])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
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
