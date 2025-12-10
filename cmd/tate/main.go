// Package main provides the entry point for the Tate columnar database.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/taikicoco/tate/internal/executor"
	"github.com/taikicoco/tate/internal/parser"
	"github.com/taikicoco/tate/internal/storage"
)

const (
	version = "0.2.0"
	prompt  = "tate> "
)

var (
	dataDir string
	cat     *storage.Catalog
	exec    *executor.Executor
)

func main() {
	flag.StringVar(&dataDir, "data", "", "Data directory (default: ~/.tate)")
	flag.Parse()

	if dataDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting home directory: %v\n", err)
			os.Exit(1)
		}
		dataDir = filepath.Join(homeDir, ".tate")
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating data directory: %v\n", err)
		os.Exit(1)
	}

	var err error
	cat, err = storage.NewCatalog(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing catalog: %v\n", err)
		os.Exit(1)
	}

	exec = executor.New(cat, dataDir)

	printBanner()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(prompt)
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		if handleCommand(input) {
			continue
		}

		executeSQL(input)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

func printBanner() {
	banner := `
  _____      _
 |_   _|__ _| |_ ___
   | |/ _' | __/ _ \
   | | (_| | ||  __/
   |_|\__,_|\__\___|

`
	fmt.Print(banner)
	fmt.Printf("Tate Columnar Database v%s\n", version)
	fmt.Println("A learning project for understanding column-oriented databases.")
	fmt.Println()
	fmt.Printf("Data directory: %s\n", dataDir)
	fmt.Println("Type 'help' for available commands, 'exit' to quit.")
	fmt.Println()
}

func handleCommand(input string) bool {
	lower := strings.ToLower(input)

	switch {
	case lower == "exit" || lower == "quit" || lower == "\\q":
		fmt.Println("Bye!")
		os.Exit(0)
		return true

	case lower == "help" || lower == "\\h":
		printHelp()
		return true

	case lower == "tables" || lower == "\\dt":
		listTables()
		return true

	case lower == "version" || lower == "\\v":
		fmt.Printf("Tate Columnar Database v%s\n", version)
		return true

	case lower == "clear" || lower == "\\c":
		fmt.Print("\033[H\033[2J")
		return true

	case strings.HasPrefix(lower, "describe ") || strings.HasPrefix(lower, "\\d "):
		var tableName string
		if strings.HasPrefix(lower, "describe ") {
			tableName = strings.TrimSpace(input[9:])
		} else {
			tableName = strings.TrimSpace(input[3:])
		}
		describeTable(tableName)
		return true
	}

	return false
}

func printHelp() {
	help := `
Available Commands:
  help, \h           - Show this help message
  exit, \q           - Exit the program
  tables, \dt        - List all tables
  describe <table>   - Show table schema
  version, \v        - Show version information
  clear, \c          - Clear the screen

SQL Commands:
  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...)
  INSERT INTO table_name VALUES (val1, val2, ...)
  INSERT INTO table_name (col1, col2) VALUES (val1, val2)
  SELECT col1, col2 FROM table_name [WHERE condition]
  SELECT COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col) FROM table_name
  SELECT DISTINCT col FROM table_name
  SELECT * FROM table_name ORDER BY col [ASC|DESC]
  SELECT * FROM table_name LIMIT n
  DROP TABLE table_name

Supported Data Types:
  INT64      - 64-bit integer
  FLOAT64    - 64-bit floating point
  STRING     - Variable-length string
  BOOL       - Boolean (TRUE/FALSE)
  TIMESTAMP  - Date and time

Operators:
  Comparison: =, !=, <>, <, <=, >, >=
  Logical:    AND, OR, NOT
  Arithmetic: +, -, *, /

Examples:
  CREATE TABLE users (id INT64, name STRING, age INT64);
  INSERT INTO users VALUES (1, 'Alice', 30);
  INSERT INTO users VALUES (2, 'Bob', 25);
  SELECT * FROM users;
  SELECT name, age FROM users WHERE age > 25;
  SELECT COUNT(*), AVG(age) FROM users;
  SELECT * FROM users ORDER BY age DESC LIMIT 10;
`
	fmt.Println(help)
}

func listTables() {
	tables := cat.ListTables()
	if len(tables) == 0 {
		fmt.Println("No tables found.")
		return
	}

	fmt.Println("Tables:")
	fmt.Println("--------")
	for _, name := range tables {
		schema, ok := cat.GetTable(name)
		if ok && schema != nil {
			fmt.Printf("  %s (%d columns)\n", name, len(schema.Columns))
		} else {
			fmt.Printf("  %s\n", name)
		}
	}
	fmt.Println()
}

func describeTable(tableName string) {
	schema, ok := cat.GetTable(tableName)
	if !ok || schema == nil {
		fmt.Printf("Table '%s' not found.\n", tableName)
		return
	}

	fmt.Printf("\nTable: %s\n", schema.Name)
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("%-20s %-15s %s\n", "Column", "Type", "Properties")
	fmt.Println(strings.Repeat("-", 50))

	for _, col := range schema.Columns {
		props := []string{}
		if !col.Nullable {
			props = append(props, "NOT NULL")
		}
		fmt.Printf("%-20s %-15s %s\n", col.Name, col.Type.String(), strings.Join(props, ", "))
	}
	fmt.Println()
}

func executeSQL(sql string) {
	start := time.Now()

	l := parser.NewLexer(sql)
	p := parser.NewParser(l)
	stmt := p.Parse()

	if len(p.Errors()) > 0 {
		fmt.Println("Parse error:")
		for _, err := range p.Errors() {
			fmt.Printf("  %s\n", err)
		}
		return
	}

	if stmt == nil {
		fmt.Println("Error: Unknown statement type")
		return
	}

	result, err := exec.Execute(stmt)
	if err != nil {
		fmt.Printf("Execution error: %v\n", err)
		return
	}

	elapsed := time.Since(start)

	if result.Message != "" {
		fmt.Println(result.Message)
	}

	if result.RowCount() > 0 || len(result.Columns) > 0 {
		fmt.Println(result.String())
	}

	fmt.Printf("(%d row(s) in %.3f ms)\n\n", result.RowCount(), float64(elapsed.Microseconds())/1000)
}
