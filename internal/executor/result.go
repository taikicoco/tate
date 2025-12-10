// Package executor implements the query execution engine.
package executor

import (
	"fmt"
	"strings"

	"github.com/taikicoco/tate/internal/types"
)

// Result represents a query result.
type Result struct {
	Columns []string
	Rows    [][]types.Value
	Message string // For non-SELECT statements
}

// NewResult creates a new empty result.
func NewResult() *Result {
	return &Result{
		Columns: make([]string, 0),
		Rows:    make([][]types.Value, 0),
	}
}

// RowCount returns the number of rows.
func (r *Result) RowCount() int {
	return len(r.Rows)
}

// ColumnCount returns the number of columns.
func (r *Result) ColumnCount() int {
	return len(r.Columns)
}

// GetValue returns the value at the given row and column index.
func (r *Result) GetValue(row, col int) (types.Value, error) {
	if row < 0 || row >= len(r.Rows) {
		return types.NewNullValue(), fmt.Errorf("row index out of range")
	}
	if col < 0 || col >= len(r.Rows[row]) {
		return types.NewNullValue(), fmt.Errorf("column index out of range")
	}
	return r.Rows[row][col], nil
}

// GetColumnIndex returns the index of a column by name.
func (r *Result) GetColumnIndex(name string) int {
	for i, col := range r.Columns {
		if col == name {
			return i
		}
	}
	return -1
}

// String returns a formatted string representation of the result.
func (r *Result) String() string {
	if r.Message != "" {
		return r.Message
	}

	if len(r.Columns) == 0 {
		return "(no results)"
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
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("%d row(s)\n", len(r.Rows)))

	return sb.String()
}
