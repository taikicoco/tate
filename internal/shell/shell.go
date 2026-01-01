// Package shell implements the interactive database shell.
package shell

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/taikicoco/tate/internal/executor"
	"github.com/taikicoco/tate/internal/parser"
	"github.com/taikicoco/tate/internal/storage"
)

const Prompt = "tate> "

// Shell implements the interactive database shell.
type Shell struct {
	catalog  *storage.Catalog
	executor *executor.Executor
	dataDir  string
	in       io.Reader
	out      io.Writer
}

// New creates a new Shell instance.
func New(catalog *storage.Catalog, exec *executor.Executor, dataDir string) *Shell {
	return &Shell{
		catalog:  catalog,
		executor: exec,
		dataDir:  dataDir,
		in:       os.Stdin,
		out:      os.Stdout,
	}
}

// Run starts the shell.
func (s *Shell) Run() error {
	s.printBanner()

	scanner := bufio.NewScanner(s.in)

	for {
		fmt.Fprint(s.out, Prompt)
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		if s.handleCommand(input) {
			continue
		}

		s.executeSQL(input)
	}

	return scanner.Err()
}

func (s *Shell) printBanner() {
	banner := `
  _____      _
 |_   _|__ _| |_ ___
   | |/ _' | __/ _ \
   | | (_| | ||  __/
   |_|\__,_|\__\___|

Tate - A minimal columnar database
`
	fmt.Fprint(s.out, banner)
	fmt.Fprintf(s.out, "Data directory: %s\n", s.dataDir)
	fmt.Fprintln(s.out, "Type 'help' for available commands, 'exit' to quit.")
	fmt.Fprintln(s.out)
}

func (s *Shell) handleCommand(input string) bool {
	lower := strings.ToLower(input)

	switch {
	case lower == "exit" || lower == "quit" || lower == "\\q":
		fmt.Fprintln(s.out, "Bye!")
		os.Exit(0)
		return true

	case lower == "help" || lower == "\\h":
		s.printHelp()
		return true

	case lower == "tables" || lower == "\\dt":
		s.listTables()
		return true

	case lower == "clear" || lower == "\\c":
		fmt.Fprint(s.out, "\033[H\033[2J")
		return true

	case strings.HasPrefix(lower, "describe ") || strings.HasPrefix(lower, "\\d "):
		var tableName string
		if strings.HasPrefix(lower, "describe ") {
			tableName = strings.TrimSpace(input[9:])
		} else {
			tableName = strings.TrimSpace(input[3:])
		}
		s.describeTable(tableName)
		return true
	}

	return false
}

func (s *Shell) printHelp() {
	help := `
Available Commands:
  help, \h           - Show this help message
  exit, \q           - Exit the program
  tables, \dt        - List all tables
  describe <table>   - Show table schema
  clear, \c          - Clear the screen

SQL Commands:
  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...)
  INSERT INTO table_name VALUES (val1, val2, ...)
  INSERT INTO table_name (col1, col2) VALUES (val1, val2)
  SELECT col1, col2 FROM table_name
  SELECT * FROM table_name
  DROP TABLE table_name

Supported Data Types:
  INT64    - 64-bit integer
  FLOAT64  - 64-bit floating point
  STRING   - Variable-length string
  BOOL     - Boolean (TRUE/FALSE)

Examples:
  CREATE TABLE users (id INT64, name STRING, active BOOL);
  INSERT INTO users VALUES (1, 'Alice', TRUE);
  INSERT INTO users VALUES (2, 'Bob', FALSE);
  SELECT * FROM users;
  SELECT name FROM users;
`
	fmt.Fprintln(s.out, help)
}

func (s *Shell) listTables() {
	tables := s.catalog.ListTables()
	if len(tables) == 0 {
		fmt.Fprintln(s.out, "No tables found.")
		return
	}

	fmt.Fprintln(s.out, "Tables:")
	fmt.Fprintln(s.out, "--------")
	for _, name := range tables {
		schema, ok := s.catalog.GetTable(name)
		if ok && schema != nil {
			fmt.Fprintf(s.out, "  %s (%d columns)\n", name, len(schema.Columns))
		} else {
			fmt.Fprintf(s.out, "  %s\n", name)
		}
	}
	fmt.Fprintln(s.out)
}

func (s *Shell) describeTable(tableName string) {
	schema, ok := s.catalog.GetTable(tableName)
	if !ok || schema == nil {
		fmt.Fprintf(s.out, "Table '%s' not found.\n", tableName)
		return
	}

	fmt.Fprintf(s.out, "\nTable: %s\n", schema.Name)
	fmt.Fprintln(s.out, strings.Repeat("-", 50))
	fmt.Fprintf(s.out, "%-20s %-15s %s\n", "Column", "Type", "Properties")
	fmt.Fprintln(s.out, strings.Repeat("-", 50))

	for _, col := range schema.Columns {
		props := []string{}
		if !col.Nullable {
			props = append(props, "NOT NULL")
		}
		fmt.Fprintf(s.out, "%-20s %-15s %s\n", col.Name, col.Type.String(), strings.Join(props, ", "))
	}
	fmt.Fprintln(s.out)
}

func (s *Shell) executeSQL(sql string) {
	start := time.Now()

	l := parser.NewLexer(sql)
	p := parser.NewParser(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		fmt.Fprintln(s.out, "Parse error:")
		for _, err := range p.Errors() {
			fmt.Fprintf(s.out, "  %s\n", err)
		}
		return
	}

	if stmt == nil {
		fmt.Fprintln(s.out, "Error: Unknown statement type")
		return
	}

	result, err := s.executor.Execute(stmt)
	if err != nil {
		fmt.Fprintf(s.out, "Execution error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	if result.Message != "" {
		fmt.Fprintln(s.out, result.Message)
	}

	if result.RowCount() > 0 || len(result.Columns) > 0 {
		fmt.Fprintln(s.out, result.String())
	}

	fmt.Fprintf(s.out, "(%d row(s) in %.3f ms)\n\n", result.RowCount(), float64(elapsed.Microseconds())/1000)
}
