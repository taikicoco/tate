// Package parser implements the SQL parser.
package parser

import (
	"strings"
	"unicode"
)

// TokenType represents the type of a token.
type TokenType int

const (
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF

	// Literals
	TOKEN_IDENT
	TOKEN_INT
	TOKEN_FLOAT
	TOKEN_STRING

	// Operators
	TOKEN_ASTERISK // *

	// Delimiters
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )

	// Keywords
	TOKEN_SELECT
	TOKEN_FROM
	TOKEN_INSERT
	TOKEN_INTO
	TOKEN_VALUES
	TOKEN_CREATE
	TOKEN_TABLE
	TOKEN_DROP
	TOKEN_NULL
	TOKEN_TRUE
	TOKEN_FALSE

	// Data types
	TOKEN_TYPE_INT64
	TOKEN_TYPE_FLOAT64
	TOKEN_TYPE_STRING
	TOKEN_TYPE_BOOL
)

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

var keywords = map[string]TokenType{
	"SELECT":  TOKEN_SELECT,
	"FROM":    TOKEN_FROM,
	"INSERT":  TOKEN_INSERT,
	"INTO":    TOKEN_INTO,
	"VALUES":  TOKEN_VALUES,
	"CREATE":  TOKEN_CREATE,
	"TABLE":   TOKEN_TABLE,
	"DROP":    TOKEN_DROP,
	"NULL":    TOKEN_NULL,
	"TRUE":    TOKEN_TRUE,
	"FALSE":   TOKEN_FALSE,
	"INT64":   TOKEN_TYPE_INT64,
	"FLOAT64": TOKEN_TYPE_FLOAT64,
	"STRING":  TOKEN_TYPE_STRING,
	"BOOL":    TOKEN_TYPE_BOOL,
}

// LookupIdent checks if an identifier is a keyword.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TOKEN_IDENT
}

// Lexer performs lexical analysis on SQL input.
type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
	line         int
	column       int
}

// NewLexer creates a new Lexer.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input, line: 1, column: 0}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++

	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	tok := Token{Line: l.line, Column: l.column}

	switch l.ch {
	case '*':
		tok.Type = TOKEN_ASTERISK
		tok.Literal = string(l.ch)
	case ',':
		tok.Type = TOKEN_COMMA
		tok.Literal = string(l.ch)
	case ';':
		tok.Type = TOKEN_SEMICOLON
		tok.Literal = string(l.ch)
	case '(':
		tok.Type = TOKEN_LPAREN
		tok.Literal = string(l.ch)
	case ')':
		tok.Type = TOKEN_RPAREN
		tok.Literal = string(l.ch)
	case '\'':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString()
	case '-':
		if isDigit(l.peekChar()) {
			l.readChar()
			literal, isFloat := l.readNumber()
			tok.Literal = "-" + literal
			if isFloat {
				tok.Type = TOKEN_FLOAT
			} else {
				tok.Type = TOKEN_INT
			}
			return tok
		}
		tok.Type = TOKEN_ILLEGAL
		tok.Literal = string(l.ch)
	case 0:
		tok.Type = TOKEN_EOF
		tok.Literal = ""
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			literal, isFloat := l.readNumber()
			tok.Literal = literal
			if isFloat {
				tok.Type = TOKEN_FLOAT
			} else {
				tok.Type = TOKEN_INT
			}
			return tok
		} else {
			tok.Type = TOKEN_ILLEGAL
			tok.Literal = string(l.ch)
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}

	// Skip SQL comments
	if l.ch == '-' && l.peekChar() == '-' {
		for l.ch != '\n' && l.ch != 0 {
			l.readChar()
		}
		l.skipWhitespace()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() (string, bool) {
	position := l.position
	isFloat := false

	for isDigit(l.ch) {
		l.readChar()
	}

	if l.ch == '.' && isDigit(l.peekChar()) {
		isFloat = true
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return l.input[position:l.position], isFloat
}

func (l *Lexer) readString() string {
	l.readChar() // skip opening quote
	position := l.position

	for {
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				l.readChar()
				l.readChar()
				continue
			}
			break
		}
		if l.ch == 0 {
			break
		}
		l.readChar()
	}

	str := l.input[position:l.position]
	str = strings.ReplaceAll(str, "''", "'")
	return str
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
