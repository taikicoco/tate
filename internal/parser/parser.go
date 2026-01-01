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

	return stmt
}

func (p *Parser) parseSelectColumns() []SelectColumn {
	var columns []SelectColumn

	for {
		if p.curTokenIs(TOKEN_ASTERISK) {
			columns = append(columns, SelectColumn{IsWildcard: true})
		} else if p.curTokenIs(TOKEN_IDENT) {
			columns = append(columns, SelectColumn{
				Expression: &Identifier{Name: p.curToken.Literal},
			})
		} else {
			break
		}

		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
		p.nextToken()
	}

	return columns
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
	stmt.Values = p.parseValueList()

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

func (p *Parser) parseValueList() []Expression {
	var exprs []Expression

	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		exprs = append(exprs, p.parseLiteral())

		if p.peekTokenIs(TOKEN_COMMA) {
			p.nextToken()
			p.nextToken()
		} else {
			break
		}
	}

	return exprs
}

func (p *Parser) parseLiteral() Expression {
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

	default:
		p.addError(fmt.Sprintf("unexpected token in value: %s", p.curToken.Literal))
		return nil
	}
}
