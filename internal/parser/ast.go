package parser

// Node is the base interface for all AST nodes.
type Node interface {
	node()
}

// Statement represents a SQL statement.
type Statement interface {
	Node
	statementNode()
}

// Expression represents a SQL expression.
type Expression interface {
	Node
	expressionNode()
}

// CreateTableStatement represents a CREATE TABLE statement.
type CreateTableStatement struct {
	TableName string
	Columns   []ColumnDefinition
}

func (s *CreateTableStatement) node()          {}
func (s *CreateTableStatement) statementNode() {}

// ColumnDefinition represents a column definition in CREATE TABLE.
type ColumnDefinition struct {
	Name     string
	DataType string
	Nullable bool
}

// DropTableStatement represents a DROP TABLE statement.
type DropTableStatement struct {
	TableName string
	IfExists  bool
}

func (s *DropTableStatement) node()          {}
func (s *DropTableStatement) statementNode() {}

// InsertStatement represents an INSERT statement.
type InsertStatement struct {
	TableName string
	Columns   []string
	Values    []Expression
}

func (s *InsertStatement) node()          {}
func (s *InsertStatement) statementNode() {}

// SelectStatement represents a SELECT statement.
type SelectStatement struct {
	Columns   []SelectColumn
	TableName string
}

func (s *SelectStatement) node()          {}
func (s *SelectStatement) statementNode() {}

// SelectColumn represents a column in SELECT clause.
type SelectColumn struct {
	Expression Expression
	IsWildcard bool
}

// Identifier represents an identifier (column or table name).
type Identifier struct {
	Name string
}

func (e *Identifier) node()           {}
func (e *Identifier) expressionNode() {}

// IntegerLiteral represents an integer literal.
type IntegerLiteral struct {
	Value int64
}

func (e *IntegerLiteral) node()           {}
func (e *IntegerLiteral) expressionNode() {}

// FloatLiteral represents a floating-point literal.
type FloatLiteral struct {
	Value float64
}

func (e *FloatLiteral) node()           {}
func (e *FloatLiteral) expressionNode() {}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Value string
}

func (e *StringLiteral) node()           {}
func (e *StringLiteral) expressionNode() {}

// BoolLiteral represents a boolean literal.
type BoolLiteral struct {
	Value bool
}

func (e *BoolLiteral) node()           {}
func (e *BoolLiteral) expressionNode() {}

// NullLiteral represents a NULL literal.
type NullLiteral struct{}

func (e *NullLiteral) node()           {}
func (e *NullLiteral) expressionNode() {}
