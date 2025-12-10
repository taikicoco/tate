package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/taikicoco/tate/internal/ast"
	"github.com/taikicoco/tate/internal/catalog"
	"github.com/taikicoco/tate/internal/storage"
	"github.com/taikicoco/tate/internal/types"
)

// Executor executes SQL statements.
type Executor struct {
	catalog *catalog.Catalog
	tables  map[string]*storage.Table
	dataDir string
}

// New creates a new Executor.
func New(cat *catalog.Catalog, dataDir string) *Executor {
	return &Executor{
		catalog: cat,
		tables:  make(map[string]*storage.Table),
		dataDir: dataDir,
	}
}

// Execute executes a SQL statement and returns the result.
func (e *Executor) Execute(stmt ast.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *ast.CreateTableStatement:
		return e.executeCreateTable(s)
	case *ast.DropTableStatement:
		return e.executeDropTable(s)
	case *ast.InsertStatement:
		return e.executeInsert(s)
	case *ast.SelectStatement:
		return e.executeSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Executor) executeCreateTable(stmt *ast.CreateTableStatement) (*Result, error) {
	// Check if table already exists
	if e.catalog.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table %q already exists", stmt.TableName)
	}

	// Create schema
	schema := catalog.NewTableSchema(stmt.TableName)
	for _, col := range stmt.Columns {
		schema.AddColumn(col.Name, col.DataType, col.Nullable)
	}

	// Register in catalog
	if err := e.catalog.RegisterTable(schema); err != nil {
		return nil, err
	}

	// Create table storage
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

func (e *Executor) executeDropTable(stmt *ast.DropTableStatement) (*Result, error) {
	// Check if table exists
	if !e.catalog.TableExists(stmt.TableName) {
		if stmt.IfExists {
			return &Result{Message: "OK"}, nil
		}
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	// Drop table storage
	if table, exists := e.tables[stmt.TableName]; exists {
		if err := table.Drop(); err != nil {
			return nil, err
		}
		delete(e.tables, stmt.TableName)
	}

	// Remove from catalog
	if err := e.catalog.DropTable(stmt.TableName); err != nil {
		return nil, err
	}

	return &Result{
		Message: fmt.Sprintf("Table %q dropped successfully", stmt.TableName),
	}, nil
}

func (e *Executor) executeInsert(stmt *ast.InsertStatement) (*Result, error) {
	// Get or load table
	table, err := e.getTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	schema := table.Schema

	// Build values array
	var values []types.Value

	if len(stmt.Columns) > 0 {
		// Insert with specified columns
		values = make([]types.Value, len(schema.Columns))
		for i := range values {
			values[i] = types.NewNullValue()
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
		// Insert without column specification
		if len(stmt.Values) != len(schema.Columns) {
			return nil, fmt.Errorf("column count mismatch: expected %d, got %d",
				len(schema.Columns), len(stmt.Values))
		}

		values = make([]types.Value, len(stmt.Values))
		for i, expr := range stmt.Values {
			val, err := e.evaluateExpression(expr, nil, nil)
			if err != nil {
				return nil, err
			}
			values[i] = val
		}
	}

	// Insert into table
	if err := table.Insert(values); err != nil {
		return nil, err
	}

	// Save table
	if err := table.Save(); err != nil {
		return nil, err
	}

	return &Result{
		Message: "1 row inserted",
	}, nil
}

func (e *Executor) executeSelect(stmt *ast.SelectStatement) (*Result, error) {
	// Get or load table
	table, err := e.getTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	schema := table.Schema
	result := NewResult()

	// Determine columns to select
	var selectColumns []string
	var selectExpressions []ast.Expression
	hasAggregates := false

	for _, col := range stmt.Columns {
		if col.IsWildcard {
			selectColumns = append(selectColumns, schema.ColumnNames()...)
			for _, name := range schema.ColumnNames() {
				selectExpressions = append(selectExpressions, &ast.Identifier{Name: name})
			}
		} else {
			// Check if it's an aggregate function
			if fn, ok := col.Expression.(*ast.FunctionCall); ok {
				hasAggregates = true
				name := fn.Name
				if len(fn.Arguments) > 0 {
					if ident, ok := fn.Arguments[0].(*ast.Identifier); ok {
						name = fmt.Sprintf("%s(%s)", fn.Name, ident.Name)
					}
				}
				if col.Alias != "" {
					name = col.Alias
				}
				selectColumns = append(selectColumns, name)
			} else if ident, ok := col.Expression.(*ast.Identifier); ok {
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

	// Scan table and apply WHERE filter
	var filteredRows [][]types.Value

	err = table.Scan(func(rowIndex uint64, row []types.Value) bool {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, schema.ColumnNames(), row)
			if err != nil || !match {
				return true // continue scanning
			}
		}

		// Build result row
		resultRow := make([]types.Value, len(selectExpressions))
		for i, expr := range selectExpressions {
			val, _ := e.evaluateExpression(expr, schema.ColumnNames(), row)
			resultRow[i] = val
		}

		filteredRows = append(filteredRows, resultRow)
		return true
	})

	if err != nil {
		return nil, err
	}

	result.Rows = filteredRows

	// Apply DISTINCT
	if stmt.Distinct {
		result.Rows = e.applyDistinct(result.Rows)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.applyOrderBy(result, stmt.OrderBy)
	}

	// Apply LIMIT and OFFSET
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

func (e *Executor) executeAggregateSelect(stmt *ast.SelectStatement, table *storage.Table,
	selectExpressions []ast.Expression, result *Result) (*Result, error) {

	schema := table.Schema

	// Collect values for aggregation
	type aggregateState struct {
		count int64
		sum   float64
		min   types.Value
		max   types.Value
		hasMin bool
		hasMax bool
	}

	aggregates := make(map[int]*aggregateState)

	// Initialize aggregates
	for i, expr := range selectExpressions {
		if _, ok := expr.(*ast.FunctionCall); ok {
			aggregates[i] = &aggregateState{}
		}
	}

	// Scan table
	totalRows := int64(0)
	err := table.Scan(func(rowIndex uint64, row []types.Value) bool {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, schema.ColumnNames(), row)
			if err != nil || !match {
				return true
			}
		}

		totalRows++

		// Update aggregates
		for i, expr := range selectExpressions {
			fn, ok := expr.(*ast.FunctionCall)
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

	if err != nil {
		return nil, err
	}

	// Build result row
	resultRow := make([]types.Value, len(selectExpressions))

	for i, expr := range selectExpressions {
		fn, ok := expr.(*ast.FunctionCall)
		if !ok {
			resultRow[i] = types.NewNullValue()
			continue
		}

		state := aggregates[i]

		switch fn.Name {
		case "COUNT":
			resultRow[i] = types.NewInt64Value(state.count)

		case "SUM":
			if state.count == 0 {
				resultRow[i] = types.NewNullValue()
			} else {
				resultRow[i] = types.NewFloat64Value(state.sum)
			}

		case "AVG":
			if state.count == 0 {
				resultRow[i] = types.NewNullValue()
			} else {
				resultRow[i] = types.NewFloat64Value(state.sum / float64(state.count))
			}

		case "MIN":
			if state.hasMin {
				resultRow[i] = state.min
			} else {
				resultRow[i] = types.NewNullValue()
			}

		case "MAX":
			if state.hasMax {
				resultRow[i] = state.max
			} else {
				resultRow[i] = types.NewNullValue()
			}
		}
	}

	result.Rows = [][]types.Value{resultRow}
	return result, nil
}

func (e *Executor) evaluateExpression(expr ast.Expression, columns []string, row []types.Value) (types.Value, error) {
	switch ex := expr.(type) {
	case *ast.Identifier:
		if columns != nil && row != nil {
			for i, col := range columns {
				if col == ex.Name {
					return row[i], nil
				}
			}
		}
		return types.NewNullValue(), nil

	case *ast.IntegerLiteral:
		return types.NewInt64Value(ex.Value), nil

	case *ast.FloatLiteral:
		return types.NewFloat64Value(ex.Value), nil

	case *ast.StringLiteral:
		return types.NewStringValue(ex.Value), nil

	case *ast.BoolLiteral:
		return types.NewBoolValue(ex.Value), nil

	case *ast.NullLiteral:
		return types.NewNullValue(), nil

	case *ast.BinaryExpression:
		left, err := e.evaluateExpression(ex.Left, columns, row)
		if err != nil {
			return types.NewNullValue(), err
		}
		right, err := e.evaluateExpression(ex.Right, columns, row)
		if err != nil {
			return types.NewNullValue(), err
		}

		switch ex.Operator {
		case "+":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok {
					return types.NewFloat64Value(lv + rv), nil
				}
			}
		case "-":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok {
					return types.NewFloat64Value(lv - rv), nil
				}
			}
		case "*":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok {
					return types.NewFloat64Value(lv * rv), nil
				}
			}
		case "/":
			if lv, ok := left.ToNumeric(); ok {
				if rv, ok := right.ToNumeric(); ok && rv != 0 {
					return types.NewFloat64Value(lv / rv), nil
				}
			}
		}

		return types.NewNullValue(), nil

	case *ast.UnaryExpression:
		val, err := e.evaluateExpression(ex.Operand, columns, row)
		if err != nil {
			return types.NewNullValue(), err
		}

		switch ex.Operator {
		case "-":
			if v, ok := val.AsInt64(); ok {
				return types.NewInt64Value(-v), nil
			}
			if v, ok := val.AsFloat64(); ok {
				return types.NewFloat64Value(-v), nil
			}
		case "NOT":
			if v, ok := val.AsBool(); ok {
				return types.NewBoolValue(!v), nil
			}
		}

		return types.NewNullValue(), nil

	default:
		return types.NewNullValue(), fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) evaluateCondition(expr ast.Expression, columns []string, row []types.Value) (bool, error) {
	switch ex := expr.(type) {
	case *ast.BinaryExpression:
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
			// Comparison operators
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

	case *ast.UnaryExpression:
		if ex.Operator == "NOT" {
			result, err := e.evaluateCondition(ex.Operand, columns, row)
			if err != nil {
				return false, err
			}
			return !result, nil
		}

	case *ast.BoolLiteral:
		return ex.Value, nil
	}

	return false, nil
}

func (e *Executor) compareValues(left, right types.Value, op string) bool {
	// Handle NULL comparisons
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

func (e *Executor) applyDistinct(rows [][]types.Value) [][]types.Value {
	seen := make(map[string]bool)
	var result [][]types.Value

	for _, row := range rows {
		key := e.rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

func (e *Executor) rowKey(row []types.Value) string {
	var parts []string
	for _, val := range row {
		parts = append(parts, val.String())
	}
	return strings.Join(parts, "\x00")
}

func (e *Executor) applyOrderBy(result *Result, orderBy []ast.OrderByClause) {
	// Build column index map
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
	// Check cache
	if table, exists := e.tables[name]; exists {
		return table, nil
	}

	// Check catalog
	if !e.catalog.TableExists(name) {
		return nil, fmt.Errorf("table %q does not exist", name)
	}

	// Load table
	table, err := storage.LoadTable(e.dataDir, name)
	if err != nil {
		return nil, err
	}

	e.tables[name] = table
	return table, nil
}

// Close saves all tables and releases resources.
func (e *Executor) Close() error {
	for _, table := range e.tables {
		if err := table.Save(); err != nil {
			return err
		}
	}
	return nil
}
