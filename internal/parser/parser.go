package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses SQL statements into AST.
type Parser struct {
	l         *Lexer
	curToken  Token
	peekToken Token
	errors    []string
}

// NewParser creates a new Parser.
func NewParser(l *Lexer) *Parser {
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

func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.errors = append(p.errors, fmt.Sprintf("line %d: expected %d, got %d",
		p.peekToken.Line, t, p.peekToken.Type))
	return false
}

func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, fmt.Sprintf("line %d: %s", p.curToken.Line, msg))
}

// Parse parses a SQL statement and returns the AST.
func (p *Parser) Parse() Statement {
	switch p.curToken.Type {
	case TOKEN_SELECT:
		return p.parseSelectStatement()
	case TOKEN_INSERT:
		return p.parseInsertStatement()
	case TOKEN_CREATE:
		return p.parseCreateStatement()
	case TOKEN_DROP:
		return p.parseDropStatement()
	default:
		p.addError(fmt.Sprintf("unexpected token: %s", p.curToken.Literal))
		return nil
	}
}

func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{}

	p.nextToken() // move past SELECT

	if p.curTokenIs(TOKEN_DISTINCT) {
		stmt.Distinct = true
		p.nextToken()
	}

	stmt.Columns = p.parseSelectColumns()

	if !p.expectPeek(TOKEN_FROM) {
		return nil
	}
	p.nextToken()

	if !p.curTokenIs(TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if p.peekTokenIs(TOKEN_WHERE) {
		p.nextToken()
		p.nextToken()
		stmt.Where = p.parseExpression(LOWEST)
	}

	if p.peekTokenIs(TOKEN_ORDER) {
		p.nextToken()
		if !p.expectPeek(TOKEN_BY) {
			return nil
		}
		stmt.OrderBy = p.parseOrderByClause()
	}

	if p.peekTokenIs(TOKEN_LIMIT) {
		p.nextToken()
		p.nextToken()
		if p.curTokenIs(TOKEN_INT) {
			limit, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
			stmt.Limit = &limit
		}
	}

	if p.peekTokenIs(TOKEN_OFFSET) {
		p.nextToken()
		p.nextToken()
		if p.curTokenIs(TOKEN_INT) {
			offset, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
			stmt.Offset = &offset
		}
	}

	return stmt
}

func (p *Parser) parseSelectColumns() []SelectColumn {
	var columns []SelectColumn

	for {
		if p.curTokenIs(TOKEN_ASTERISK) {
			columns = append(columns, SelectColumn{IsWildcard: true})
		} else {
			col := SelectColumn{
				Expression: p.parseExpression(LOWEST),
			}
			if p.peekTokenIs(TOKEN_AS) {
				p.nextToken()
				p.nextToken()
				col.Alias = p.curToken.Literal
			} else if p.peekTokenIs(TOKEN_IDENT) {
				p.nextToken()
				col.Alias = p.curToken.Literal
			}
			columns = append(columns, col)
		}

		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
		p.nextToken()
	}

	return columns
}

func (p *Parser) parseOrderByClause() []OrderByClause {
	var clauses []OrderByClause

	p.nextToken()

	for {
		clause := OrderByClause{}

		if !p.curTokenIs(TOKEN_IDENT) {
			break
		}
		clause.Column = p.curToken.Literal

		if p.peekTokenIs(TOKEN_DESC) {
			clause.Desc = true
			p.nextToken()
		} else if p.peekTokenIs(TOKEN_ASC) {
			p.nextToken()
		}

		clauses = append(clauses, clause)

		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
		p.nextToken()
	}

	return clauses
}

func (p *Parser) parseInsertStatement() *InsertStatement {
	stmt := &InsertStatement{}

	if !p.expectPeek(TOKEN_INTO) {
		return nil
	}
	p.nextToken()

	if !p.curTokenIs(TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if p.peekTokenIs(TOKEN_LPAREN) {
		p.nextToken()
		stmt.Columns = p.parseIdentifierList()
		if !p.expectPeek(TOKEN_RPAREN) {
			return nil
		}
	}

	if !p.expectPeek(TOKEN_VALUES) {
		return nil
	}

	if !p.expectPeek(TOKEN_LPAREN) {
		return nil
	}

	p.nextToken()
	stmt.Values = p.parseExpressionList()

	if !p.expectPeek(TOKEN_RPAREN) {
		return nil
	}

	return stmt
}

func (p *Parser) parseCreateStatement() *CreateTableStatement {
	if !p.expectPeek(TOKEN_TABLE) {
		return nil
	}
	p.nextToken()

	stmt := &CreateTableStatement{}

	if !p.curTokenIs(TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(TOKEN_LPAREN) {
		return nil
	}

	stmt.Columns = p.parseColumnDefinitions()

	if !p.expectPeek(TOKEN_RPAREN) {
		return nil
	}

	return stmt
}

func (p *Parser) parseDropStatement() *DropTableStatement {
	if !p.expectPeek(TOKEN_TABLE) {
		return nil
	}
	p.nextToken()

	stmt := &DropTableStatement{}

	if !p.curTokenIs(TOKEN_IDENT) {
		p.addError("expected table name")
		return nil
	}
	stmt.TableName = p.curToken.Literal

	return stmt
}

func (p *Parser) parseColumnDefinitions() []ColumnDefinition {
	var defs []ColumnDefinition

	p.nextToken()

	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		def := ColumnDefinition{Nullable: true}

		if !p.curTokenIs(TOKEN_IDENT) {
			break
		}
		def.Name = p.curToken.Literal

		p.nextToken()
		def.DataType = p.parseDataType()

		if p.peekTokenIs(TOKEN_NOT) {
			p.nextToken()
			if p.peekTokenIs(TOKEN_NULL) {
				p.nextToken()
				def.Nullable = false
			}
		}

		defs = append(defs, def)

		if p.peekTokenIs(TOKEN_COMMA) {
			p.nextToken()
			p.nextToken()
		} else {
			break
		}
	}

	return defs
}

func (p *Parser) parseDataType() string {
	switch p.curToken.Type {
	case TOKEN_TYPE_INT64:
		return "INT64"
	case TOKEN_TYPE_FLOAT64:
		return "FLOAT64"
	case TOKEN_TYPE_STRING:
		return "STRING"
	case TOKEN_TYPE_BOOL:
		return "BOOL"
	case TOKEN_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	default:
		return strings.ToUpper(p.curToken.Literal)
	}
}

func (p *Parser) parseIdentifierList() []string {
	var idents []string

	p.nextToken()

	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		if p.curTokenIs(TOKEN_IDENT) {
			idents = append(idents, p.curToken.Literal)
		}

		if p.peekTokenIs(TOKEN_COMMA) {
			p.nextToken()
			p.nextToken()
		} else {
			break
		}
	}

	return idents
}

func (p *Parser) parseExpressionList() []Expression {
	var exprs []Expression

	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		exprs = append(exprs, p.parseExpression(LOWEST))

		if p.peekTokenIs(TOKEN_COMMA) {
			p.nextToken()
			p.nextToken()
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

var precedences = map[TokenType]int{
	TOKEN_OR:       OR_PREC,
	TOKEN_AND:      AND_PREC,
	TOKEN_EQ:       EQUALS,
	TOKEN_NEQ:      EQUALS,
	TOKEN_LT:       LESSGREATER,
	TOKEN_GT:       LESSGREATER,
	TOKEN_LTE:      LESSGREATER,
	TOKEN_GTE:      LESSGREATER,
	TOKEN_PLUS:     SUM,
	TOKEN_MINUS:    SUM,
	TOKEN_ASTERISK: PRODUCT,
	TOKEN_SLASH:    PRODUCT,
	TOKEN_LPAREN:   CALL,
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

func (p *Parser) parseExpression(precedence int) Expression {
	left := p.parsePrefixExpression()
	if left == nil {
		return nil
	}

	for !p.peekTokenIs(TOKEN_SEMICOLON) && precedence < p.peekPrecedence() {
		if !p.isInfixOperator(p.peekToken.Type) {
			return left
		}

		p.nextToken()
		left = p.parseInfixExpression(left)
	}

	return left
}

func (p *Parser) parsePrefixExpression() Expression {
	switch p.curToken.Type {
	case TOKEN_INT:
		val, err := strconv.ParseInt(p.curToken.Literal, 10, 64)
		if err != nil {
			p.addError(fmt.Sprintf("could not parse %q as integer", p.curToken.Literal))
			return nil
		}
		return &IntegerLiteral{Value: val}

	case TOKEN_FLOAT:
		val, err := strconv.ParseFloat(p.curToken.Literal, 64)
		if err != nil {
			p.addError(fmt.Sprintf("could not parse %q as float", p.curToken.Literal))
			return nil
		}
		return &FloatLiteral{Value: val}

	case TOKEN_STRING:
		return &StringLiteral{Value: p.curToken.Literal}

	case TOKEN_TRUE:
		return &BoolLiteral{Value: true}

	case TOKEN_FALSE:
		return &BoolLiteral{Value: false}

	case TOKEN_NULL:
		return &NullLiteral{}

	case TOKEN_NOT:
		p.nextToken()
		return &UnaryExpression{
			Operator: "NOT",
			Operand:  p.parseExpression(NOT_PREC),
		}

	case TOKEN_MINUS:
		p.nextToken()
		return &UnaryExpression{
			Operator: "-",
			Operand:  p.parseExpression(PREFIX),
		}

	case TOKEN_LPAREN:
		p.nextToken()
		expr := p.parseExpression(LOWEST)
		if !p.expectPeek(TOKEN_RPAREN) {
			return nil
		}
		return expr

	case TOKEN_COUNT, TOKEN_SUM, TOKEN_AVG, TOKEN_MIN, TOKEN_MAX:
		return p.parseFunctionCall()

	case TOKEN_IDENT:
		return &Identifier{Name: p.curToken.Literal}

	default:
		p.addError(fmt.Sprintf("no prefix parse function for %v", p.curToken.Type))
		return nil
	}
}

func (p *Parser) parseInfixExpression(left Expression) Expression {
	operator := p.curToken.Literal
	switch p.curToken.Type {
	case TOKEN_AND:
		operator = "AND"
	case TOKEN_OR:
		operator = "OR"
	}

	precedence := p.curPrecedence()
	p.nextToken()
	right := p.parseExpression(precedence)

	return &BinaryExpression{
		Left:     left,
		Operator: operator,
		Right:    right,
	}
}

func (p *Parser) parseFunctionCall() *FunctionCall {
	fn := &FunctionCall{Name: strings.ToUpper(p.curToken.Literal)}

	if !p.expectPeek(TOKEN_LPAREN) {
		return nil
	}

	p.nextToken()

	if p.curTokenIs(TOKEN_DISTINCT) {
		fn.Distinct = true
		p.nextToken()
	}

	if p.curTokenIs(TOKEN_ASTERISK) {
		fn.Arguments = []Expression{&Identifier{Name: "*"}}
	} else if !p.curTokenIs(TOKEN_RPAREN) {
		fn.Arguments = p.parseExpressionList()
	}

	if !p.expectPeek(TOKEN_RPAREN) {
		return nil
	}

	return fn
}

func (p *Parser) isInfixOperator(t TokenType) bool {
	switch t {
	case TOKEN_EQ, TOKEN_NEQ,
		TOKEN_LT, TOKEN_GT,
		TOKEN_LTE, TOKEN_GTE,
		TOKEN_AND, TOKEN_OR,
		TOKEN_PLUS, TOKEN_MINUS,
		TOKEN_ASTERISK, TOKEN_SLASH:
		return true
	}
	return false
}
