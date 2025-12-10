package lexer

import (
	"testing"
)

func TestNextToken(t *testing.T) {
	input := `SELECT * FROM users WHERE age > 25`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_SELECT, "SELECT"},
		{TOKEN_ASTERISK, "*"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "users"},
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "age"},
		{TOKEN_GT, ">"},
		{TOKEN_INT, "25"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestCreateTable(t *testing.T) {
	input := `CREATE TABLE users (id INT64, name STRING, age INT64)`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_CREATE, "CREATE"},
		{TOKEN_TABLE, "TABLE"},
		{TOKEN_IDENT, "users"},
		{TOKEN_LPAREN, "("},
		{TOKEN_IDENT, "id"},
		{TOKEN_INT64, "INT64"},
		{TOKEN_COMMA, ","},
		{TOKEN_IDENT, "name"},
		{TOKEN_STRING_TYPE, "STRING"},
		{TOKEN_COMMA, ","},
		{TOKEN_IDENT, "age"},
		{TOKEN_INT64, "INT64"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestInsert(t *testing.T) {
	input := `INSERT INTO users VALUES (1, 'Alice', 30)`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_INSERT, "INSERT"},
		{TOKEN_INTO, "INTO"},
		{TOKEN_IDENT, "users"},
		{TOKEN_VALUES, "VALUES"},
		{TOKEN_LPAREN, "("},
		{TOKEN_INT, "1"},
		{TOKEN_COMMA, ","},
		{TOKEN_STRING, "Alice"},
		{TOKEN_COMMA, ","},
		{TOKEN_INT, "30"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestSelectWithAggregate(t *testing.T) {
	input := `SELECT COUNT(*), SUM(age), AVG(age) FROM users`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_SELECT, "SELECT"},
		{TOKEN_COUNT, "COUNT"},
		{TOKEN_LPAREN, "("},
		{TOKEN_ASTERISK, "*"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_COMMA, ","},
		{TOKEN_SUM, "SUM"},
		{TOKEN_LPAREN, "("},
		{TOKEN_IDENT, "age"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_COMMA, ","},
		{TOKEN_AVG, "AVG"},
		{TOKEN_LPAREN, "("},
		{TOKEN_IDENT, "age"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "users"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestOperators(t *testing.T) {
	input := `= != <> < > <= >= + - * /`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_EQ, "="},
		{TOKEN_NEQ, "!="},
		{TOKEN_NEQ, "<>"},
		{TOKEN_LT, "<"},
		{TOKEN_GT, ">"},
		{TOKEN_LTE, "<="},
		{TOKEN_GTE, ">="},
		{TOKEN_PLUS, "+"},
		{TOKEN_MINUS, "-"},
		{TOKEN_ASTERISK, "*"},
		{TOKEN_SLASH, "/"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestNumbers(t *testing.T) {
	input := `123 45.67 -100 -3.14 1e10 1.5e-3`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_INT, "123"},
		{TOKEN_FLOAT, "45.67"},
		{TOKEN_INT, "-100"},
		{TOKEN_FLOAT, "-3.14"},
		{TOKEN_FLOAT, "1e10"},
		{TOKEN_FLOAT, "1.5e-3"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`'hello'`, "hello"},
		{`'hello world'`, "hello world"},
		{`''`, ""},
		{`'it''s'`, "it's"}, // escaped single quote
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.NextToken()

		if tok.Type != TOKEN_STRING {
			t.Errorf("expected TOKEN_STRING, got %v", tok.Type)
		}

		if tok.Literal != tt.expected {
			t.Errorf("expected %q, got %q", tt.expected, tok.Literal)
		}
	}
}

func TestKeywords(t *testing.T) {
	input := `SELECT FROM WHERE INSERT INTO VALUES CREATE TABLE
	          ORDER BY ASC DESC LIMIT AND OR NOT NULL TRUE FALSE
	          COUNT SUM AVG MIN MAX INT64 FLOAT64 STRING BOOL`

	expectedTypes := []TokenType{
		TOKEN_SELECT, TOKEN_FROM, TOKEN_WHERE, TOKEN_INSERT, TOKEN_INTO,
		TOKEN_VALUES, TOKEN_CREATE, TOKEN_TABLE, TOKEN_ORDER, TOKEN_BY,
		TOKEN_ASC, TOKEN_DESC, TOKEN_LIMIT, TOKEN_AND, TOKEN_OR,
		TOKEN_NOT, TOKEN_NULL, TOKEN_TRUE, TOKEN_FALSE, TOKEN_COUNT,
		TOKEN_SUM, TOKEN_AVG, TOKEN_MIN, TOKEN_MAX, TOKEN_INT64,
		TOKEN_FLOAT64, TOKEN_STRING_TYPE, TOKEN_BOOL, TOKEN_EOF,
	}

	l := New(input)

	for i, expected := range expectedTypes {
		tok := l.NextToken()
		if tok.Type != expected {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v (literal=%q)",
				i, expected, tok.Type, tok.Literal)
		}
	}
}

func TestCaseInsensitiveKeywords(t *testing.T) {
	input := `select SELECT Select sElEcT`

	l := New(input)

	for i := 0; i < 4; i++ {
		tok := l.NextToken()
		if tok.Type != TOKEN_SELECT {
			t.Fatalf("tests[%d] - expected TOKEN_SELECT, got %v", i, tok.Type)
		}
	}
}

func TestComplexQuery(t *testing.T) {
	input := `SELECT name, age FROM users WHERE age >= 18 AND age <= 65 ORDER BY age DESC LIMIT 10`

	l := New(input)
	tokens := l.Tokenize()

	// Just verify it tokenizes without error and has reasonable token count
	if len(tokens) < 15 {
		t.Errorf("expected at least 15 tokens, got %d", len(tokens))
	}

	if tokens[len(tokens)-1].Type != TOKEN_EOF {
		t.Error("last token should be EOF")
	}
}

func TestComments(t *testing.T) {
	input := `SELECT * FROM users -- this is a comment
	WHERE age > 25`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_SELECT, "SELECT"},
		{TOKEN_ASTERISK, "*"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "users"},
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "age"},
		{TOKEN_GT, ">"},
		{TOKEN_INT, "25"},
		{TOKEN_EOF, ""},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.Type)
		}
	}
}

func TestTokenize(t *testing.T) {
	input := `SELECT * FROM users`
	l := New(input)
	tokens := l.Tokenize()

	if len(tokens) != 5 { // SELECT, *, FROM, users, EOF
		t.Errorf("expected 5 tokens, got %d", len(tokens))
	}
}
