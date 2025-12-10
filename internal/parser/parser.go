// Package parser implements the SQL parser.
package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/taikicoco/tate/internal/ast"
	"github.com/taikicoco/tate/internal/lexer"
	"github.com/taikicoco/tate/internal/types"
)

// Parser parses SQL statements into AST.
type Parser struct {
	l         *lexer.Lexer
	curToken  lexer.Token
	peekToken lexer.Token
	errors    []string
}

// New creates a new Parser.
func New(l *lexer.Lexer) *Parser {
	p := &Parser{l: l, errors: []string{}}
	p.nextToken()
	p.nextToken()
	return p
}

// Errors returns parsing errors.
func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekError(t lexer.TokenType) {
	msg := fmt.Sprintf("line %d: expected %v, got %v instead",
		p.peekToken.Line, t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, fmt.Sprintf("line %d: %s", p.curToken.Line, msg))
}

// Parse parses a SQL statement and returns the AST.
func (p *Parser) Parse() ast.Statement {
	switch p.curToken.Type {
	case lexer.TOKEN_SELECT:
		return p.parseSelectStatement()
	case lexer.TOKEN_INSERT:
		return p.parseInsertStatement()
	case lexer.TOKEN_CREATE:
		return p.parseCreateStatement()
	case lexer.TOKEN_DROP:
		return p.parseDropStatement()
	default:
		p.addError(fmt.Sprintf("unexpected token: %s", p.curToken.Literal))
		return nil
	}
}

// parseSelectStatement parses a SELECT statement.
func (p *Parser) parseSelectStatement() *ast.SelectStatement {
	stmt := &ast.SelectStatement{}

	p.nextToken() // move past SELECT

	// Check for DISTINCT
	if p.curTokenIs(lexer.TOKEN_DISTINCT) {
		stmt.Distinct = true
		p.nextToken()
	}

	// Parse SELECT columns
	stmt.Columns = p.parseSelectColumns()

	// FROM clause
	if !p.expectPeek(lexer.TOKEN_FROM) {
		return nil
	}
	p.nextToken()

	if !p.curTokenIs(lexer.TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	// WHERE clause (optional)
	if p.peekTokenIs(lexer.TOKEN_WHERE) {
		p.nextToken() // move to WHERE
		p.nextToken() // move to condition
		stmt.Where = p.parseExpression(LOWEST)
	}

	// GROUP BY clause (optional)
	if p.peekTokenIs(lexer.TOKEN_GROUP) {
		p.nextToken() // move to GROUP
		if !p.expectPeek(lexer.TOKEN_BY) {
			return nil
		}
		stmt.GroupBy = p.parseGroupByClause()
	}

	// HAVING clause (optional)
	if p.peekTokenIs(lexer.TOKEN_HAVING) {
		p.nextToken() // move to HAVING
		p.nextToken() // move to condition
		stmt.Having = p.parseExpression(LOWEST)
	}

	// ORDER BY clause (optional)
	if p.peekTokenIs(lexer.TOKEN_ORDER) {
		p.nextToken() // move to ORDER
		if !p.expectPeek(lexer.TOKEN_BY) {
			return nil
		}
		stmt.OrderBy = p.parseOrderByClause()
	}

	// LIMIT clause (optional)
	if p.peekTokenIs(lexer.TOKEN_LIMIT) {
		p.nextToken() // move to LIMIT
		p.nextToken() // move to number
		if p.curTokenIs(lexer.TOKEN_INT) {
			limit, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
			stmt.Limit = &limit
		}
	}

	// OFFSET clause (optional)
	if p.peekTokenIs(lexer.TOKEN_OFFSET) {
		p.nextToken() // move to OFFSET
		p.nextToken() // move to number
		if p.curTokenIs(lexer.TOKEN_INT) {
			offset, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
			stmt.Offset = &offset
		}
	}

	return stmt
}

func (p *Parser) parseSelectColumns() []ast.SelectColumn {
	var columns []ast.SelectColumn

	for {
		if p.curTokenIs(lexer.TOKEN_ASTERISK) {
			columns = append(columns, ast.SelectColumn{IsWildcard: true})
		} else {
			col := ast.SelectColumn{
				Expression: p.parseExpression(LOWEST),
			}
			// Check for alias (AS or just identifier)
			if p.peekTokenIs(lexer.TOKEN_AS) {
				p.nextToken() // move to AS
				p.nextToken() // move to alias
				col.Alias = p.curToken.Literal
			} else if p.peekTokenIs(lexer.TOKEN_IDENT) {
				p.nextToken()
				col.Alias = p.curToken.Literal
			}
			columns = append(columns, col)
		}

		if !p.peekTokenIs(lexer.TOKEN_COMMA) {
			break
		}
		p.nextToken() // move to comma
		p.nextToken() // move to next column
	}

	return columns
}

func (p *Parser) parseGroupByClause() []string {
	var columns []string

	p.nextToken() // move past BY

	for {
		if !p.curTokenIs(lexer.TOKEN_IDENT) {
			break
		}
		columns = append(columns, p.curToken.Literal)

		if !p.peekTokenIs(lexer.TOKEN_COMMA) {
			break
		}
		p.nextToken() // comma
		p.nextToken() // next column
	}

	return columns
}

func (p *Parser) parseOrderByClause() []ast.OrderByClause {
	var clauses []ast.OrderByClause

	p.nextToken() // move past BY

	for {
		clause := ast.OrderByClause{}

		if !p.curTokenIs(lexer.TOKEN_IDENT) {
			break
		}
		clause.Column = p.curToken.Literal

		if p.peekTokenIs(lexer.TOKEN_DESC) {
			clause.Desc = true
			p.nextToken()
		} else if p.peekTokenIs(lexer.TOKEN_ASC) {
			p.nextToken()
		}

		clauses = append(clauses, clause)

		if !p.peekTokenIs(lexer.TOKEN_COMMA) {
			break
		}
		p.nextToken() // comma
		p.nextToken() // next column
	}

	return clauses
}

// parseInsertStatement parses an INSERT statement.
func (p *Parser) parseInsertStatement() *ast.InsertStatement {
	stmt := &ast.InsertStatement{}

	if !p.expectPeek(lexer.TOKEN_INTO) {
		return nil
	}
	p.nextToken()

	if !p.curTokenIs(lexer.TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	// Optional column list
	if p.peekTokenIs(lexer.TOKEN_LPAREN) {
		p.nextToken() // move to (
		stmt.Columns = p.parseIdentifierList()
		if !p.expectPeek(lexer.TOKEN_RPAREN) {
			return nil
		}
	}

	// VALUES keyword
	if !p.expectPeek(lexer.TOKEN_VALUES) {
		return nil
	}

	if !p.expectPeek(lexer.TOKEN_LPAREN) {
		return nil
	}

	p.nextToken()
	stmt.Values = p.parseExpressionList()

	if !p.expectPeek(lexer.TOKEN_RPAREN) {
		return nil
	}

	return stmt
}

// parseCreateStatement parses a CREATE statement.
func (p *Parser) parseCreateStatement() *ast.CreateTableStatement {
	if !p.expectPeek(lexer.TOKEN_TABLE) {
		return nil
	}
	p.nextToken()

	stmt := &ast.CreateTableStatement{}

	if !p.curTokenIs(lexer.TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(lexer.TOKEN_LPAREN) {
		return nil
	}

	stmt.Columns = p.parseColumnDefinitions()

	if !p.expectPeek(lexer.TOKEN_RPAREN) {
		return nil
	}

	return stmt
}

// parseDropStatement parses a DROP statement.
func (p *Parser) parseDropStatement() *ast.DropTableStatement {
	if !p.expectPeek(lexer.TOKEN_TABLE) {
		return nil
	}
	p.nextToken()

	stmt := &ast.DropTableStatement{}

	if !p.curTokenIs(lexer.TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	return stmt
}

func (p *Parser) parseColumnDefinitions() []ast.ColumnDefinition {
	var defs []ast.ColumnDefinition

	p.nextToken() // move past (

	for !p.curTokenIs(lexer.TOKEN_RPAREN) && !p.curTokenIs(lexer.TOKEN_EOF) {
		def := ast.ColumnDefinition{Nullable: true}

		if !p.curTokenIs(lexer.TOKEN_IDENT) {
			break
		}
		def.Name = p.curToken.Literal

		p.nextToken() // move to type
		def.DataType = p.parseDataType()

		// Check for NOT NULL
		if p.peekTokenIs(lexer.TOKEN_NOT) {
			p.nextToken() // NOT
			if p.peekTokenIs(lexer.TOKEN_NULL) {
				p.nextToken() // NULL
				def.Nullable = false
			}
		}

		defs = append(defs, def)

		if p.peekTokenIs(lexer.TOKEN_COMMA) {
			p.nextToken() // comma
			p.nextToken() // next column
		} else {
			break
		}
	}

	return defs
}

func (p *Parser) parseDataType() types.DataType {
	switch p.curToken.Type {
	case lexer.TOKEN_INT64:
		return types.TypeInt64
	case lexer.TOKEN_FLOAT64:
		return types.TypeFloat64
	case lexer.TOKEN_STRING_TYPE:
		return types.TypeString
	case lexer.TOKEN_BOOL:
		return types.TypeBool
	case lexer.TOKEN_TIMESTAMP:
		return types.TypeTimestamp
	default:
		// Try to parse as identifier (INT64, etc.)
		switch strings.ToUpper(p.curToken.Literal) {
		case "INT64", "INT", "INTEGER", "BIGINT":
			return types.TypeInt64
		case "FLOAT64", "FLOAT", "DOUBLE", "REAL":
			return types.TypeFloat64
		case "STRING", "VARCHAR", "TEXT":
			return types.TypeString
		case "BOOL", "BOOLEAN":
			return types.TypeBool
		case "TIMESTAMP", "DATETIME":
			return types.TypeTimestamp
		}
		return types.TypeNull
	}
}

func (p *Parser) parseIdentifierList() []string {
	var idents []string

	p.nextToken() // move past (

	for !p.curTokenIs(lexer.TOKEN_RPAREN) && !p.curTokenIs(lexer.TOKEN_EOF) {
		if p.curTokenIs(lexer.TOKEN_IDENT) {
			idents = append(idents, p.curToken.Literal)
		}

		if p.peekTokenIs(lexer.TOKEN_COMMA) {
			p.nextToken() // comma
			p.nextToken() // next ident
		} else {
			break
		}
	}

	return idents
}

func (p *Parser) parseExpressionList() []ast.Expression {
	var exprs []ast.Expression

	for !p.curTokenIs(lexer.TOKEN_RPAREN) && !p.curTokenIs(lexer.TOKEN_EOF) {
		exprs = append(exprs, p.parseExpression(LOWEST))

		if p.peekTokenIs(lexer.TOKEN_COMMA) {
			p.nextToken() // comma
			p.nextToken() // next expr
		} else {
			break
		}
	}

	return exprs
}

// Operator precedence levels
const (
	LOWEST = iota
	OR_PREC
	AND_PREC
	NOT_PREC
	EQUALS
	LESSGREATER
	SUM
	PRODUCT
	PREFIX
	CALL
)

var precedences = map[lexer.TokenType]int{
	lexer.TOKEN_OR:       OR_PREC,
	lexer.TOKEN_AND:      AND_PREC,
	lexer.TOKEN_EQ:       EQUALS,
	lexer.TOKEN_NEQ:      EQUALS,
	lexer.TOKEN_LT:       LESSGREATER,
	lexer.TOKEN_GT:       LESSGREATER,
	lexer.TOKEN_LTE:      LESSGREATER,
	lexer.TOKEN_GTE:      LESSGREATER,
	lexer.TOKEN_PLUS:     SUM,
	lexer.TOKEN_MINUS:    SUM,
	lexer.TOKEN_ASTERISK: PRODUCT,
	lexer.TOKEN_SLASH:    PRODUCT,
	lexer.TOKEN_LPAREN:   CALL,
}

func (p *Parser) peekPrecedence() int {
	if prec, ok := precedences[p.peekToken.Type]; ok {
		return prec
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	if prec, ok := precedences[p.curToken.Type]; ok {
		return prec
	}
	return LOWEST
}

// parseExpression parses an expression using Pratt parsing.
func (p *Parser) parseExpression(precedence int) ast.Expression {
	left := p.parsePrefixExpression()
	if left == nil {
		return nil
	}

	for !p.peekTokenIs(lexer.TOKEN_SEMICOLON) && precedence < p.peekPrecedence() {
		if !p.isInfixOperator(p.peekToken.Type) {
			return left
		}

		p.nextToken()
		left = p.parseInfixExpression(left)
	}

	return left
}

func (p *Parser) parsePrefixExpression() ast.Expression {
	switch p.curToken.Type {
	case lexer.TOKEN_INT:
		val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			p.addError(fmt.Sprintf("could not parse %q as integer", p.curToken.Literal))
			return nil
		}
		return &ast.IntegerLiteral{Value: val}

	case lexer.TOKEN_FLOAT:
		val, err := strconv.ParseFloat(p.curToken.Literal, 64)
		if err != nil {
			p.addError(fmt.Sprintf("could not parse %q as float", p.curToken.Literal))
			return nil
		}
		return &ast.FloatLiteral{Value: val}

	case lexer.TOKEN_STRING:
		return &ast.StringLiteral{Value: p.curToken.Literal}

	case lexer.TOKEN_TRUE:
		return &ast.BoolLiteral{Value: true}

	case lexer.TOKEN_FALSE:
		return &ast.BoolLiteral{Value: false}

	case lexer.TOKEN_NULL:
		return &ast.NullLiteral{}

	case lexer.TOKEN_NOT:
		p.nextToken()
		return &ast.UnaryExpression{
			Operator: "NOT",
			Operand:  p.parseExpression(NOT_PREC),
		}

	case lexer.TOKEN_MINUS:
		p.nextToken()
		return &ast.UnaryExpression{
			Operator: "-",
			Operand:  p.parseExpression(PREFIX),
		}

	case lexer.TOKEN_LPAREN:
		p.nextToken()
		expr := p.parseExpression(LOWEST)
		if !p.expectPeek(lexer.TOKEN_RPAREN) {
			return nil
		}
		return expr

	case lexer.TOKEN_COUNT, lexer.TOKEN_SUM, lexer.TOKEN_AVG,
		lexer.TOKEN_MIN, lexer.TOKEN_MAX:
		return p.parseFunctionCall()

	case lexer.TOKEN_IDENT:
		return &ast.Identifier{Name: p.curToken.Literal}

	default:
		p.addError(fmt.Sprintf("no prefix parse function for %v", p.curToken.Type))
		return nil
	}
}

func (p *Parser) parseInfixExpression(left ast.Expression) ast.Expression {
	operator := p.curToken.Literal
	// Normalize operator
	switch p.curToken.Type {
	case lexer.TOKEN_AND:
		operator = "AND"
	case lexer.TOKEN_OR:
		operator = "OR"
	}

	precedence := p.curPrecedence()
	p.nextToken()
	right := p.parseExpression(precedence)

	return &ast.BinaryExpression{
		Left:     left,
		Operator: operator,
		Right:    right,
	}
}

func (p *Parser) parseFunctionCall() *ast.FunctionCall {
	fn := &ast.FunctionCall{Name: strings.ToUpper(p.curToken.Literal)}

	if !p.expectPeek(lexer.TOKEN_LPAREN) {
		return nil
	}

	p.nextToken()

	// Handle DISTINCT
	if p.curTokenIs(lexer.TOKEN_DISTINCT) {
		fn.Distinct = true
		p.nextToken()
	}

	// Handle COUNT(*)
	if p.curTokenIs(lexer.TOKEN_ASTERISK) {
		fn.Arguments = []ast.Expression{&ast.Identifier{Name: "*"}}
	} else if !p.curTokenIs(lexer.TOKEN_RPAREN) {
		fn.Arguments = p.parseExpressionList()
	}

	if !p.expectPeek(lexer.TOKEN_RPAREN) {
		return nil
	}

	return fn
}

func (p *Parser) isInfixOperator(t lexer.TokenType) bool {
	switch t {
	case lexer.TOKEN_EQ, lexer.TOKEN_NEQ,
		lexer.TOKEN_LT, lexer.TOKEN_GT,
		lexer.TOKEN_LTE, lexer.TOKEN_GTE,
		lexer.TOKEN_AND, lexer.TOKEN_OR,
		lexer.TOKEN_PLUS, lexer.TOKEN_MINUS,
		lexer.TOKEN_ASTERISK, lexer.TOKEN_SLASH:
		return true
	}
	return false
}
