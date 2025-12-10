// Package lexer implements the SQL lexical analyzer.
package lexer

// TokenType represents the type of a token.
type TokenType int

const (
	// Special tokens
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF
	TOKEN_WS

	// Literals
	TOKEN_IDENT  // identifier
	TOKEN_INT    // integer
	TOKEN_FLOAT  // floating point
	TOKEN_STRING // string

	// Operators
	TOKEN_EQ       // =
	TOKEN_NEQ      // != or <>
	TOKEN_LT       // <
	TOKEN_GT       // >
	TOKEN_LTE      // <=
	TOKEN_GTE      // >=
	TOKEN_PLUS     // +
	TOKEN_MINUS    // -
	TOKEN_ASTERISK // *
	TOKEN_SLASH    // /

	// Delimiters
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )

	// Keywords
	TOKEN_SELECT
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_INSERT
	TOKEN_INTO
	TOKEN_VALUES
	TOKEN_CREATE
	TOKEN_TABLE
	TOKEN_DROP
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_ASC
	TOKEN_DESC
	TOKEN_LIMIT
	TOKEN_OFFSET
	TOKEN_AND
	TOKEN_OR
	TOKEN_NOT
	TOKEN_NULL
	TOKEN_TRUE
	TOKEN_FALSE
	TOKEN_AS
	TOKEN_DISTINCT
	TOKEN_GROUP
	TOKEN_HAVING

	// Aggregate functions
	TOKEN_COUNT
	TOKEN_SUM
	TOKEN_AVG
	TOKEN_MIN
	TOKEN_MAX

	// Data types
	TOKEN_INT64
	TOKEN_FLOAT64
	TOKEN_STRING_TYPE
	TOKEN_BOOL
	TOKEN_TIMESTAMP
)

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

var keywords = map[string]TokenType{
	"SELECT":    TOKEN_SELECT,
	"FROM":      TOKEN_FROM,
	"WHERE":     TOKEN_WHERE,
	"INSERT":    TOKEN_INSERT,
	"INTO":      TOKEN_INTO,
	"VALUES":    TOKEN_VALUES,
	"CREATE":    TOKEN_CREATE,
	"TABLE":     TOKEN_TABLE,
	"DROP":      TOKEN_DROP,
	"ORDER":     TOKEN_ORDER,
	"BY":        TOKEN_BY,
	"ASC":       TOKEN_ASC,
	"DESC":      TOKEN_DESC,
	"LIMIT":     TOKEN_LIMIT,
	"OFFSET":    TOKEN_OFFSET,
	"AND":       TOKEN_AND,
	"OR":        TOKEN_OR,
	"NOT":       TOKEN_NOT,
	"NULL":      TOKEN_NULL,
	"TRUE":      TOKEN_TRUE,
	"FALSE":     TOKEN_FALSE,
	"AS":        TOKEN_AS,
	"DISTINCT":  TOKEN_DISTINCT,
	"GROUP":     TOKEN_GROUP,
	"HAVING":    TOKEN_HAVING,
	"COUNT":     TOKEN_COUNT,
	"SUM":       TOKEN_SUM,
	"AVG":       TOKEN_AVG,
	"MIN":       TOKEN_MIN,
	"MAX":       TOKEN_MAX,
	"INT64":     TOKEN_INT64,
	"FLOAT64":   TOKEN_FLOAT64,
	"STRING":    TOKEN_STRING_TYPE,
	"BOOL":      TOKEN_BOOL,
	"TIMESTAMP": TOKEN_TIMESTAMP,
}

// LookupIdent checks if an identifier is a keyword.
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}

// String returns the string representation of a token type.
func (t TokenType) String() string {
	switch t {
	case TOKEN_ILLEGAL:
		return "ILLEGAL"
	case TOKEN_EOF:
		return "EOF"
	case TOKEN_IDENT:
		return "IDENT"
	case TOKEN_INT:
		return "INT"
	case TOKEN_FLOAT:
		return "FLOAT"
	case TOKEN_STRING:
		return "STRING"
	case TOKEN_EQ:
		return "="
	case TOKEN_NEQ:
		return "!="
	case TOKEN_LT:
		return "<"
	case TOKEN_GT:
		return ">"
	case TOKEN_LTE:
		return "<="
	case TOKEN_GTE:
		return ">="
	case TOKEN_PLUS:
		return "+"
	case TOKEN_MINUS:
		return "-"
	case TOKEN_ASTERISK:
		return "*"
	case TOKEN_SLASH:
		return "/"
	case TOKEN_COMMA:
		return ","
	case TOKEN_SEMICOLON:
		return ";"
	case TOKEN_LPAREN:
		return "("
	case TOKEN_RPAREN:
		return ")"
	case TOKEN_SELECT:
		return "SELECT"
	case TOKEN_FROM:
		return "FROM"
	case TOKEN_WHERE:
		return "WHERE"
	case TOKEN_INSERT:
		return "INSERT"
	case TOKEN_INTO:
		return "INTO"
	case TOKEN_VALUES:
		return "VALUES"
	case TOKEN_CREATE:
		return "CREATE"
	case TOKEN_TABLE:
		return "TABLE"
	case TOKEN_DROP:
		return "DROP"
	case TOKEN_ORDER:
		return "ORDER"
	case TOKEN_BY:
		return "BY"
	case TOKEN_ASC:
		return "ASC"
	case TOKEN_DESC:
		return "DESC"
	case TOKEN_LIMIT:
		return "LIMIT"
	case TOKEN_OFFSET:
		return "OFFSET"
	case TOKEN_AND:
		return "AND"
	case TOKEN_OR:
		return "OR"
	case TOKEN_NOT:
		return "NOT"
	case TOKEN_NULL:
		return "NULL"
	case TOKEN_TRUE:
		return "TRUE"
	case TOKEN_FALSE:
		return "FALSE"
	case TOKEN_COUNT:
		return "COUNT"
	case TOKEN_SUM:
		return "SUM"
	case TOKEN_AVG:
		return "AVG"
	case TOKEN_MIN:
		return "MIN"
	case TOKEN_MAX:
		return "MAX"
	default:
		return "UNKNOWN"
	}
}
