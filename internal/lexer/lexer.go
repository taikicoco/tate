package lexer

import (
	"strings"
	"unicode"
)

// Lexer performs lexical analysis on SQL input.
type Lexer struct {
	input        string
	position     int  // current position
	readPosition int  // next read position
	ch           byte // current character
	line         int
	column       int
}

// New creates a new Lexer.
func New(input string) *Lexer {
	l := &Lexer{input: input, line: 1, column: 0}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // EOF
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
	case '=':
		tok.Type = TOKEN_EQ
		tok.Literal = string(l.ch)
	case '+':
		tok.Type = TOKEN_PLUS
		tok.Literal = string(l.ch)
	case '-':
		// Check if it's a negative number
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
		tok.Type = TOKEN_MINUS
		tok.Literal = string(l.ch)
	case '*':
		tok.Type = TOKEN_ASTERISK
		tok.Literal = string(l.ch)
	case '/':
		tok.Type = TOKEN_SLASH
		tok.Literal = string(l.ch)
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok.Type = TOKEN_LTE
			tok.Literal = "<="
		} else if l.peekChar() == '>' {
			l.readChar()
			tok.Type = TOKEN_NEQ
			tok.Literal = "<>"
		} else {
			tok.Type = TOKEN_LT
			tok.Literal = string(l.ch)
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok.Type = TOKEN_GTE
			tok.Literal = ">="
		} else {
			tok.Type = TOKEN_GT
			tok.Literal = string(l.ch)
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok.Type = TOKEN_NEQ
			tok.Literal = "!="
		} else {
			tok.Type = TOKEN_ILLEGAL
			tok.Literal = string(l.ch)
		}
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
	case '"':
		// Double-quoted identifier
		tok.Type = TOKEN_IDENT
		tok.Literal = l.readQuotedIdentifier()
	case 0:
		tok.Type = TOKEN_EOF
		tok.Literal = ""
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(strings.ToUpper(tok.Literal))
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

	// Skip comments
	if l.ch == '-' && l.peekChar() == '-' {
		// Single-line comment
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
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	// Scientific notation (e.g., 1e10, 1.5e-3)
	if l.ch == 'e' || l.ch == 'E' {
		isFloat = true
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}
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
			// Check for escaped quote ('')
			if l.peekChar() == '\'' {
				l.readChar()
				l.readChar()
				continue
			}
			break
		}
		if l.ch == 0 {
			break // EOF
		}
		l.readChar()
	}

	str := l.input[position:l.position]
	// Unescape double quotes
	str = strings.ReplaceAll(str, "''", "'")
	return str
}

func (l *Lexer) readQuotedIdentifier() string {
	l.readChar() // skip opening quote
	position := l.position

	for l.ch != '"' && l.ch != 0 {
		l.readChar()
	}

	return l.input[position:l.position]
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF {
			break
		}
	}
	return tokens
}
