# Tate: Go言語による列指向データベース 実装計画書

> プロジェクト名: **tate**（縦 = 列指向の意）

---

## 目次

1. [プロジェクト概要](#1-プロジェクト概要)
2. [アーキテクチャ設計](#2-アーキテクチャ設計)
3. [実装フェーズ](#3-実装フェーズ)
4. [Phase 1: 基盤構築](#4-phase-1-基盤構築)
5. [Phase 2: ストレージエンジン](#5-phase-2-ストレージエンジン)
6. [Phase 3: SQLパーサー](#6-phase-3-sqlパーサー)
7. [Phase 4: クエリ実行エンジン](#7-phase-4-クエリ実行エンジン)
8. [Phase 5: インデックス](#8-phase-5-インデックス)
9. [Phase 6: 圧縮・エンコーディング](#9-phase-6-圧縮エンコーディング)
10. [Phase 7: 高度な機能](#10-phase-7-高度な機能)
11. [テスト戦略](#11-テスト戦略)
12. [開発環境セットアップ](#12-開発環境セットアップ)

---

## 1. プロジェクト概要

### 1.1 目標

学習目的で、Go言語を使用して列指向データベースを一から実装する。

**達成目標:**
- 列指向ストレージの基本概念の理解
- データベース内部構造（パーサー、実行エンジン、ストレージ）の理解
- Go言語での実践的なシステムプログラミング経験

### 1.2 スコープ

**実装する機能:**
- 基本的なSQL（SELECT, INSERT, CREATE TABLE）
- 列指向ストレージ
- 基本的なインデックス（ビットマップ、Zone Map）
- データ圧縮（RLE、辞書エンコーディング）
- 集計クエリ（COUNT, SUM, AVG, MIN, MAX）
- REPL（対話的なコマンドライン）

**実装しない機能（スコープ外）:**
- UPDATE/DELETE（初期バージョンでは追記のみ）
- JOIN（単一テーブルクエリのみ）
- トランザクション（ACID完全準拠）
- 分散処理
- ネットワークプロトコル（PostgreSQL互換など）

### 1.3 技術スタック

| 項目 | 技術 |
|------|------|
| 言語 | Go 1.21+ |
| 依存ライブラリ | 標準ライブラリ中心、最小限の外部依存 |
| テスト | Go標準 testing パッケージ |
| ビルド | Go Modules |

---

## 2. アーキテクチャ設計

### 2.1 システム全体図

```
                    ┌──────────────────┐
                    │      REPL        │
                    │   (main.go)      │
                    └────────┬─────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────┐
│                    フロントエンド層                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐     │
│  │  Lexer   │ → │  Parser  │ → │  Semantic        │     │
│  │          │    │          │    │  Analyzer        │     │
│  └──────────┘    └──────────┘    └──────────────────┘     │
└────────────────────────────────────────────────────────────┘
                             │
                             ▼ AST
┌────────────────────────────────────────────────────────────┐
│                    プランナー層                             │
│  ┌──────────────────┐    ┌──────────────────┐              │
│  │  Logical Plan   │ → │  Physical Plan   │              │
│  │  Generator      │    │  Generator       │              │
│  └──────────────────┘    └──────────────────┘              │
└────────────────────────────────────────────────────────────┘
                             │
                             ▼ Physical Plan
┌────────────────────────────────────────────────────────────┐
│                    実行エンジン層                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                    Executor                          │  │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────────┐   │  │
│  │  │   Scan     │ │  Filter    │ │  Aggregate     │   │  │
│  │  │  Operator  │ │  Operator  │ │  Operator      │   │  │
│  │  └────────────┘ └────────────┘ └────────────────┘   │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────┐
│                    ストレージ層                             │
│  ┌────────────┐ ┌────────────┐ ┌────────────────────────┐  │
│  │  Column    │ │  Index     │ │  Catalog               │  │
│  │  Store     │ │  Manager   │ │  (Metadata)            │  │
│  └────────────┘ └────────────┘ └────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────┐
│                    ファイルシステム                         │
│         data/                                              │
│         ├── catalog.json                                   │
│         └── tables/                                        │
│             └── users/                                     │
│                 ├── _meta.json                             │
│                 ├── col_id.dat                             │
│                 ├── col_name.dat                           │
│                 └── col_age.dat                            │
└────────────────────────────────────────────────────────────┘
```

### 2.2 ディレクトリ構造

```
tate/
├── cmd/
│   └── tate/
│       └── main.go              # エントリポイント、REPL
│
├── internal/
│   ├── ast/                     # AST (抽象構文木)
│   │   └── ast.go
│   │
│   ├── lexer/                   # 字句解析
│   │   ├── lexer.go
│   │   ├── token.go
│   │   └── lexer_test.go
│   │
│   ├── parser/                  # 構文解析
│   │   ├── parser.go
│   │   └── parser_test.go
│   │
│   ├── analyzer/                # 意味解析
│   │   ├── analyzer.go
│   │   └── analyzer_test.go
│   │
│   ├── planner/                 # 実行計画
│   │   ├── logical_plan.go
│   │   ├── physical_plan.go
│   │   ├── planner.go
│   │   └── planner_test.go
│   │
│   ├── executor/                # 実行エンジン
│   │   ├── executor.go
│   │   ├── operators.go         # Scan, Filter, Aggregate
│   │   ├── result.go
│   │   └── executor_test.go
│   │
│   ├── storage/                 # ストレージエンジン
│   │   ├── column.go            # カラムストア
│   │   ├── column_test.go
│   │   ├── table.go             # テーブル管理
│   │   ├── table_test.go
│   │   ├── page.go              # ページ管理
│   │   └── file.go              # ファイルI/O
│   │
│   ├── encoding/                # エンコーディング
│   │   ├── rle.go               # Run-Length Encoding
│   │   ├── rle_test.go
│   │   ├── dictionary.go        # 辞書エンコーディング
│   │   ├── dictionary_test.go
│   │   ├── delta.go             # 差分エンコーディング
│   │   └── delta_test.go
│   │
│   ├── compression/             # 圧縮
│   │   ├── compress.go
│   │   └── compress_test.go
│   │
│   ├── index/                   # インデックス
│   │   ├── bitmap.go            # ビットマップインデックス
│   │   ├── bitmap_test.go
│   │   ├── zonemap.go           # Zone Map (Min-Max)
│   │   └── zonemap_test.go
│   │
│   ├── catalog/                 # メタデータ管理
│   │   ├── catalog.go
│   │   ├── schema.go
│   │   └── catalog_test.go
│   │
│   └── types/                   # データ型
│       ├── types.go
│       ├── value.go
│       └── types_test.go
│
├── pkg/                         # 外部公開パッケージ（将来用）
│
├── test/                        # 統合テスト
│   └── integration_test.go
│
├── data/                        # データディレクトリ（.gitignore）
│
├── docs/                        # ドキュメント
│   ├── 01_columnar_database_guide.md
│   └── 02_implementation_plan.md
│
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

### 2.3 データ型定義

```go
// internal/types/types.go

package types

type DataType uint8

const (
    TypeNull DataType = iota
    TypeBool
    TypeInt64
    TypeFloat64
    TypeString
    TypeTimestamp
)

func (t DataType) String() string {
    switch t {
    case TypeNull:
        return "NULL"
    case TypeBool:
        return "BOOL"
    case TypeInt64:
        return "INT64"
    case TypeFloat64:
        return "FLOAT64"
    case TypeString:
        return "STRING"
    case TypeTimestamp:
        return "TIMESTAMP"
    default:
        return "UNKNOWN"
    }
}

func (t DataType) Size() int {
    switch t {
    case TypeBool:
        return 1
    case TypeInt64, TypeFloat64, TypeTimestamp:
        return 8
    case TypeString:
        return -1 // 可変長
    default:
        return 0
    }
}
```

---

## 3. 実装フェーズ

### 3.1 フェーズ概要

```
Phase 1: 基盤構築
    │
    ▼
Phase 2: ストレージエンジン
    │
    ▼
Phase 3: SQLパーサー
    │
    ▼
Phase 4: クエリ実行エンジン
    │
    ▼
Phase 5: インデックス
    │
    ▼
Phase 6: 圧縮・エンコーディング
    │
    ▼
Phase 7: 高度な機能
```

### 3.2 マイルストーン

| Phase | 内容 | 完了条件 |
|-------|------|----------|
| Phase 1 | 基盤構築 | プロジェクト構造、型システム、基本REPL |
| Phase 2 | ストレージ | カラムファイル読み書き、テーブル管理 |
| Phase 3 | パーサー | SELECT/INSERT/CREATE TABLE のパース |
| Phase 4 | 実行エンジン | 基本クエリ実行、集計関数 |
| Phase 5 | インデックス | ビットマップ、Zone Map |
| Phase 6 | 圧縮 | RLE、辞書エンコーディング |
| Phase 7 | 高度機能 | WHERE句、ORDER BY、LIMIT |

---

## 4. Phase 1: 基盤構築

### 4.1 目標

- プロジェクト構造の確立
- 基本的な型システムの実装
- シンプルなREPLの作成

### 4.2 実装タスク

#### Task 1.1: プロジェクト初期化

```bash
# コマンド
cd tate
go mod init github.com/taikicoco/tate
```

#### Task 1.2: 基本型の実装

```go
// internal/types/types.go

package types

import (
    "encoding/binary"
    "fmt"
    "time"
)

// DataType はカラムのデータ型を表す
type DataType uint8

const (
    TypeNull DataType = iota
    TypeBool
    TypeInt64
    TypeFloat64
    TypeString
    TypeTimestamp
)

// Value は任意のカラム値を表す
type Value struct {
    Type    DataType
    IsNull  bool
    data    interface{}
}

// NewNullValue はNULL値を作成
func NewNullValue() Value {
    return Value{Type: TypeNull, IsNull: true}
}

// NewInt64Value はInt64値を作成
func NewInt64Value(v int64) Value {
    return Value{Type: TypeInt64, data: v}
}

// NewFloat64Value はFloat64値を作成
func NewFloat64Value(v float64) Value {
    return Value{Type: TypeFloat64, data: v}
}

// NewStringValue はString値を作成
func NewStringValue(v string) Value {
    return Value{Type: TypeString, data: v}
}

// NewBoolValue はBool値を作成
func NewBoolValue(v bool) Value {
    return Value{Type: TypeBool, data: v}
}

// NewTimestampValue はTimestamp値を作成
func NewTimestampValue(v time.Time) Value {
    return Value{Type: TypeTimestamp, data: v}
}

// AsInt64 は値をInt64として取得
func (v Value) AsInt64() (int64, bool) {
    if v.Type != TypeInt64 || v.IsNull {
        return 0, false
    }
    return v.data.(int64), true
}

// AsFloat64 は値をFloat64として取得
func (v Value) AsFloat64() (float64, bool) {
    if v.Type != TypeFloat64 || v.IsNull {
        return 0, false
    }
    return v.data.(float64), true
}

// AsString は値をStringとして取得
func (v Value) AsString() (string, bool) {
    if v.Type != TypeString || v.IsNull {
        return "", false
    }
    return v.data.(string), true
}

// AsBool は値をBoolとして取得
func (v Value) AsBool() (bool, bool) {
    if v.Type != TypeBool || v.IsNull {
        return false, false
    }
    return v.data.(bool), true
}

// String は値の文字列表現を返す
func (v Value) String() string {
    if v.IsNull {
        return "NULL"
    }
    switch v.Type {
    case TypeInt64:
        return fmt.Sprintf("%d", v.data.(int64))
    case TypeFloat64:
        return fmt.Sprintf("%f", v.data.(float64))
    case TypeString:
        return v.data.(string)
    case TypeBool:
        return fmt.Sprintf("%t", v.data.(bool))
    case TypeTimestamp:
        return v.data.(time.Time).Format(time.RFC3339)
    default:
        return "UNKNOWN"
    }
}
```

#### Task 1.3: カタログスキーマ定義

```go
// internal/catalog/schema.go

package catalog

import "github.com/taikicoco/tate/internal/types"

// ColumnDef はカラム定義を表す
type ColumnDef struct {
    Name     string         `json:"name"`
    Type     types.DataType `json:"type"`
    Nullable bool           `json:"nullable"`
    Position int            `json:"position"`
}

// TableSchema はテーブルスキーマを表す
type TableSchema struct {
    Name    string      `json:"name"`
    Columns []ColumnDef `json:"columns"`
}

// GetColumn は名前でカラム定義を取得
func (s *TableSchema) GetColumn(name string) (*ColumnDef, bool) {
    for i := range s.Columns {
        if s.Columns[i].Name == name {
            return &s.Columns[i], true
        }
    }
    return nil, false
}

// GetColumnIndex はカラムのインデックスを取得
func (s *TableSchema) GetColumnIndex(name string) int {
    for i, col := range s.Columns {
        if col.Name == name {
            return i
        }
    }
    return -1
}
```

#### Task 1.4: 基本REPL

```go
// cmd/tate/main.go

package main

import (
    "bufio"
    "fmt"
    "os"
    "strings"
)

const (
    version = "0.1.0"
    prompt  = "tate> "
)

func main() {
    fmt.Printf("Tate Columnar Database v%s\n", version)
    fmt.Println("Type 'help' for available commands, 'exit' to quit.")
    fmt.Println()

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

        switch strings.ToLower(input) {
        case "exit", "quit", "\\q":
            fmt.Println("Bye!")
            return
        case "help", "\\h":
            printHelp()
        default:
            // TODO: SQL実行
            fmt.Printf("Executing: %s\n", input)
            fmt.Println("(SQL execution not implemented yet)")
        }
    }
}

func printHelp() {
    fmt.Println("Available commands:")
    fmt.Println("  help, \\h    - Show this help")
    fmt.Println("  exit, \\q    - Exit the program")
    fmt.Println()
    fmt.Println("SQL commands (coming soon):")
    fmt.Println("  CREATE TABLE table_name (col1 TYPE, ...)")
    fmt.Println("  INSERT INTO table_name VALUES (...)")
    fmt.Println("  SELECT col1, col2 FROM table_name")
}
```

### 4.3 テスト

```go
// internal/types/types_test.go

package types

import (
    "testing"
)

func TestInt64Value(t *testing.T) {
    v := NewInt64Value(42)

    if v.Type != TypeInt64 {
        t.Errorf("expected TypeInt64, got %v", v.Type)
    }

    if v.IsNull {
        t.Error("expected non-null value")
    }

    val, ok := v.AsInt64()
    if !ok {
        t.Error("AsInt64 should return true")
    }
    if val != 42 {
        t.Errorf("expected 42, got %d", val)
    }
}

func TestNullValue(t *testing.T) {
    v := NewNullValue()

    if !v.IsNull {
        t.Error("expected null value")
    }

    if v.String() != "NULL" {
        t.Errorf("expected 'NULL', got %s", v.String())
    }
}

func TestStringValue(t *testing.T) {
    v := NewStringValue("hello")

    str, ok := v.AsString()
    if !ok || str != "hello" {
        t.Errorf("expected 'hello', got %s", str)
    }
}
```

---

## 5. Phase 2: ストレージエンジン

### 5.1 目標

- カラムデータの読み書き
- テーブルの作成・管理
- メタデータの永続化

### 5.2 実装タスク

#### Task 2.1: カラムストア

```go
// internal/storage/column.go

package storage

import (
    "encoding/binary"
    "fmt"
    "io"
    "os"

    "github.com/taikicoco/tate/internal/types"
)

const (
    MagicNumber   = "TCOL"
    FormatVersion = 1
)

// ColumnHeader はカラムファイルのヘッダー
type ColumnHeader struct {
    Magic          [4]byte
    Version        uint16
    DataType       types.DataType
    Compression    uint8
    RowCount       uint64
    NullBitmapSize uint64
    DataSize       uint64
}

// ColumnFile はカラムファイルを管理する
type ColumnFile struct {
    Header   ColumnHeader
    NullMask []byte
    Data     []byte
    path     string
}

// NewColumnFile は新しいカラムファイルを作成
func NewColumnFile(path string, dataType types.DataType) *ColumnFile {
    header := ColumnHeader{
        Version:  FormatVersion,
        DataType: dataType,
    }
    copy(header.Magic[:], MagicNumber)

    return &ColumnFile{
        Header: header,
        path:   path,
    }
}

// AppendInt64 はInt64値を追加
func (cf *ColumnFile) AppendInt64(value int64, isNull bool) {
    cf.appendNullBit(isNull)

    if !isNull {
        buf := make([]byte, 8)
        binary.LittleEndian.PutUint64(buf, uint64(value))
        cf.Data = append(cf.Data, buf...)
    } else {
        // NULL の場合もプレースホルダーを追加（位置合わせのため）
        cf.Data = append(cf.Data, make([]byte, 8)...)
    }

    cf.Header.RowCount++
}

// AppendFloat64 はFloat64値を追加
func (cf *ColumnFile) AppendFloat64(value float64, isNull bool) {
    cf.appendNullBit(isNull)

    buf := make([]byte, 8)
    if !isNull {
        binary.LittleEndian.PutUint64(buf, uint64(value))
    }
    cf.Data = append(cf.Data, buf...)
    cf.Header.RowCount++
}

// AppendString はString値を追加
func (cf *ColumnFile) AppendString(value string, isNull bool) {
    cf.appendNullBit(isNull)

    // 文字列は長さ + データ形式
    var strBytes []byte
    if !isNull {
        strBytes = []byte(value)
    }

    lenBuf := make([]byte, 4)
    binary.LittleEndian.PutUint32(lenBuf, uint32(len(strBytes)))
    cf.Data = append(cf.Data, lenBuf...)
    cf.Data = append(cf.Data, strBytes...)
    cf.Header.RowCount++
}

// appendNullBit はNULLビットマップにビットを追加
func (cf *ColumnFile) appendNullBit(isNull bool) {
    byteIndex := cf.Header.RowCount / 8
    bitIndex := cf.Header.RowCount % 8

    // 必要に応じてバイトを追加
    for uint64(len(cf.NullMask)) <= byteIndex {
        cf.NullMask = append(cf.NullMask, 0)
    }

    if isNull {
        cf.NullMask[byteIndex] |= (1 << bitIndex)
    }
}

// IsNull は指定行がNULLかどうかを返す
func (cf *ColumnFile) IsNull(rowIndex uint64) bool {
    if rowIndex >= cf.Header.RowCount {
        return true
    }
    byteIndex := rowIndex / 8
    bitIndex := rowIndex % 8
    return (cf.NullMask[byteIndex] & (1 << bitIndex)) != 0
}

// GetInt64 は指定行のInt64値を取得
func (cf *ColumnFile) GetInt64(rowIndex uint64) (int64, bool) {
    if cf.IsNull(rowIndex) {
        return 0, false
    }
    offset := rowIndex * 8
    return int64(binary.LittleEndian.Uint64(cf.Data[offset:])), true
}

// Save はカラムファイルを保存
func (cf *ColumnFile) Save() error {
    file, err := os.Create(cf.path)
    if err != nil {
        return err
    }
    defer file.Close()

    // ヘッダー更新
    cf.Header.NullBitmapSize = uint64(len(cf.NullMask))
    cf.Header.DataSize = uint64(len(cf.Data))

    // ヘッダー書き込み
    if err := binary.Write(file, binary.LittleEndian, cf.Header); err != nil {
        return err
    }

    // NULLビットマップ書き込み
    if _, err := file.Write(cf.NullMask); err != nil {
        return err
    }

    // データ書き込み
    if _, err := file.Write(cf.Data); err != nil {
        return err
    }

    return nil
}

// Load はカラムファイルを読み込み
func LoadColumnFile(path string) (*ColumnFile, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    cf := &ColumnFile{path: path}

    // ヘッダー読み込み
    if err := binary.Read(file, binary.LittleEndian, &cf.Header); err != nil {
        return nil, err
    }

    // マジックナンバー検証
    if string(cf.Header.Magic[:]) != MagicNumber {
        return nil, fmt.Errorf("invalid column file format")
    }

    // NULLビットマップ読み込み
    cf.NullMask = make([]byte, cf.Header.NullBitmapSize)
    if _, err := io.ReadFull(file, cf.NullMask); err != nil {
        return nil, err
    }

    // データ読み込み
    cf.Data = make([]byte, cf.Header.DataSize)
    if _, err := io.ReadFull(file, cf.Data); err != nil {
        return nil, err
    }

    return cf, nil
}
```

#### Task 2.2: テーブル管理

```go
// internal/storage/table.go

package storage

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"

    "github.com/taikicoco/tate/internal/catalog"
    "github.com/taikicoco/tate/internal/types"
)

// Table はテーブルを表す
type Table struct {
    Schema  catalog.TableSchema
    Columns map[string]*ColumnFile
    dataDir string
}

// CreateTable は新しいテーブルを作成
func CreateTable(dataDir string, schema catalog.TableSchema) (*Table, error) {
    tableDir := filepath.Join(dataDir, "tables", schema.Name)

    // ディレクトリ作成
    if err := os.MkdirAll(tableDir, 0755); err != nil {
        return nil, err
    }

    t := &Table{
        Schema:  schema,
        Columns: make(map[string]*ColumnFile),
        dataDir: tableDir,
    }

    // 各カラムのファイルを初期化
    for _, col := range schema.Columns {
        colPath := filepath.Join(tableDir, fmt.Sprintf("col_%s.dat", col.Name))
        t.Columns[col.Name] = NewColumnFile(colPath, col.Type)
    }

    // メタデータ保存
    if err := t.saveMetadata(); err != nil {
        return nil, err
    }

    return t, nil
}

// LoadTable は既存のテーブルを読み込み
func LoadTable(dataDir string, tableName string) (*Table, error) {
    tableDir := filepath.Join(dataDir, "tables", tableName)

    // メタデータ読み込み
    metaPath := filepath.Join(tableDir, "_meta.json")
    metaData, err := os.ReadFile(metaPath)
    if err != nil {
        return nil, err
    }

    var schema catalog.TableSchema
    if err := json.Unmarshal(metaData, &schema); err != nil {
        return nil, err
    }

    t := &Table{
        Schema:  schema,
        Columns: make(map[string]*ColumnFile),
        dataDir: tableDir,
    }

    // 各カラムファイル読み込み
    for _, col := range schema.Columns {
        colPath := filepath.Join(tableDir, fmt.Sprintf("col_%s.dat", col.Name))
        cf, err := LoadColumnFile(colPath)
        if err != nil {
            // ファイルが存在しない場合は新規作成
            if os.IsNotExist(err) {
                t.Columns[col.Name] = NewColumnFile(colPath, col.Type)
                continue
            }
            return nil, err
        }
        t.Columns[col.Name] = cf
    }

    return t, nil
}

// Insert は行を挿入
func (t *Table) Insert(values map[string]types.Value) error {
    for _, col := range t.Schema.Columns {
        cf := t.Columns[col.Name]
        val, exists := values[col.Name]

        if !exists {
            // 値が指定されていない場合はNULL
            val = types.NewNullValue()
        }

        switch col.Type {
        case types.TypeInt64:
            v, ok := val.AsInt64()
            cf.AppendInt64(v, !ok || val.IsNull)
        case types.TypeFloat64:
            v, ok := val.AsFloat64()
            cf.AppendFloat64(v, !ok || val.IsNull)
        case types.TypeString:
            v, ok := val.AsString()
            cf.AppendString(v, !ok || val.IsNull)
        default:
            return fmt.Errorf("unsupported type: %v", col.Type)
        }
    }
    return nil
}

// Save はテーブルを保存
func (t *Table) Save() error {
    for _, cf := range t.Columns {
        if err := cf.Save(); err != nil {
            return err
        }
    }
    return t.saveMetadata()
}

// RowCount は行数を返す
func (t *Table) RowCount() uint64 {
    if len(t.Columns) == 0 {
        return 0
    }
    // 最初のカラムの行数を返す（全カラム同じはず）
    for _, cf := range t.Columns {
        return cf.Header.RowCount
    }
    return 0
}

func (t *Table) saveMetadata() error {
    metaPath := filepath.Join(t.dataDir, "_meta.json")
    data, err := json.MarshalIndent(t.Schema, "", "  ")
    if err != nil {
        return err
    }
    return os.WriteFile(metaPath, data, 0644)
}
```

#### Task 2.3: カタログ

```go
// internal/catalog/catalog.go

package catalog

import (
    "encoding/json"
    "os"
    "path/filepath"
    "sync"
)

// Catalog はデータベースのメタデータを管理
type Catalog struct {
    Tables  map[string]*TableSchema `json:"tables"`
    dataDir string
    mu      sync.RWMutex
}

// NewCatalog は新しいカタログを作成
func NewCatalog(dataDir string) (*Catalog, error) {
    c := &Catalog{
        Tables:  make(map[string]*TableSchema),
        dataDir: dataDir,
    }

    // カタログファイルが存在すれば読み込み
    if err := c.load(); err != nil && !os.IsNotExist(err) {
        return nil, err
    }

    return c, nil
}

// RegisterTable はテーブルを登録
func (c *Catalog) RegisterTable(schema *TableSchema) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.Tables[schema.Name] = schema
    return c.save()
}

// GetTable はテーブルスキーマを取得
func (c *Catalog) GetTable(name string) (*TableSchema, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    schema, exists := c.Tables[name]
    return schema, exists
}

// TableExists はテーブルが存在するか確認
func (c *Catalog) TableExists(name string) bool {
    c.mu.RLock()
    defer c.mu.RUnlock()

    _, exists := c.Tables[name]
    return exists
}

// ListTables は全テーブル名を返す
func (c *Catalog) ListTables() []string {
    c.mu.RLock()
    defer c.mu.RUnlock()

    tables := make([]string, 0, len(c.Tables))
    for name := range c.Tables {
        tables = append(tables, name)
    }
    return tables
}

func (c *Catalog) save() error {
    path := filepath.Join(c.dataDir, "catalog.json")
    data, err := json.MarshalIndent(c, "", "  ")
    if err != nil {
        return err
    }
    return os.WriteFile(path, data, 0644)
}

func (c *Catalog) load() error {
    path := filepath.Join(c.dataDir, "catalog.json")
    data, err := os.ReadFile(path)
    if err != nil {
        return err
    }
    return json.Unmarshal(data, c)
}
```

---

## 6. Phase 3: SQLパーサー

### 6.1 目標

- SQLの字句解析（Lexer）
- 構文解析（Parser）
- AST（抽象構文木）の生成

### 6.2 サポートするSQL構文

```sql
-- CREATE TABLE
CREATE TABLE table_name (
    column1 INT64,
    column2 STRING,
    column3 FLOAT64
);

-- INSERT
INSERT INTO table_name (col1, col2) VALUES (1, 'hello');
INSERT INTO table_name VALUES (1, 'hello', 3.14);

-- SELECT
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name;
SELECT col1, col2 FROM table_name WHERE col1 > 10;
SELECT COUNT(*), SUM(col1), AVG(col1) FROM table_name;
SELECT col1 FROM table_name ORDER BY col1;
SELECT col1 FROM table_name LIMIT 10;
```

### 6.3 実装タスク

#### Task 3.1: トークン定義

```go
// internal/lexer/token.go

package lexer

type TokenType int

const (
    // 特殊トークン
    TOKEN_ILLEGAL TokenType = iota
    TOKEN_EOF
    TOKEN_WS

    // リテラル
    TOKEN_IDENT     // 識別子
    TOKEN_INT       // 整数
    TOKEN_FLOAT     // 浮動小数点
    TOKEN_STRING    // 文字列

    // 演算子
    TOKEN_EQ        // =
    TOKEN_NEQ       // != または <>
    TOKEN_LT        // <
    TOKEN_GT        // >
    TOKEN_LTE       // <=
    TOKEN_GTE       // >=
    TOKEN_PLUS      // +
    TOKEN_MINUS     // -
    TOKEN_ASTERISK  // *
    TOKEN_SLASH     // /

    // 区切り文字
    TOKEN_COMMA     // ,
    TOKEN_SEMICOLON // ;
    TOKEN_LPAREN    // (
    TOKEN_RPAREN    // )

    // キーワード
    TOKEN_SELECT
    TOKEN_FROM
    TOKEN_WHERE
    TOKEN_INSERT
    TOKEN_INTO
    TOKEN_VALUES
    TOKEN_CREATE
    TOKEN_TABLE
    TOKEN_ORDER
    TOKEN_BY
    TOKEN_ASC
    TOKEN_DESC
    TOKEN_LIMIT
    TOKEN_AND
    TOKEN_OR
    TOKEN_NOT
    TOKEN_NULL

    // 集計関数
    TOKEN_COUNT
    TOKEN_SUM
    TOKEN_AVG
    TOKEN_MIN
    TOKEN_MAX

    // データ型
    TOKEN_INT64
    TOKEN_FLOAT64
    TOKEN_STRING_TYPE
    TOKEN_BOOL
    TOKEN_TIMESTAMP
)

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
    "ORDER":     TOKEN_ORDER,
    "BY":        TOKEN_BY,
    "ASC":       TOKEN_ASC,
    "DESC":      TOKEN_DESC,
    "LIMIT":     TOKEN_LIMIT,
    "AND":       TOKEN_AND,
    "OR":        TOKEN_OR,
    "NOT":       TOKEN_NOT,
    "NULL":      TOKEN_NULL,
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

func LookupIdent(ident string) TokenType {
    if tok, ok := keywords[ident]; ok {
        return tok
    }
    return TOKEN_IDENT
}
```

#### Task 3.2: 字句解析器

```go
// internal/lexer/lexer.go

package lexer

import (
    "strings"
    "unicode"
)

type Lexer struct {
    input        string
    position     int  // 現在位置
    readPosition int  // 次の読み取り位置
    ch           byte // 現在の文字
    line         int
    column       int
}

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
        l.readChar() // '.'
        for isDigit(l.ch) {
            l.readChar()
        }
    }

    return l.input[position:l.position], isFloat
}

func (l *Lexer) readString() string {
    l.readChar() // 開始の引用符をスキップ
    position := l.position

    for l.ch != '\'' && l.ch != 0 {
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

// Tokenize は入力を全てトークン化
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
```

#### Task 3.3: AST定義

```go
// internal/ast/ast.go

package ast

import "github.com/taikicoco/tate/internal/types"

// Node はAST のノードインターフェース
type Node interface {
    node()
}

// Statement は文を表す
type Statement interface {
    Node
    statementNode()
}

// Expression は式を表す
type Expression interface {
    Node
    expressionNode()
}

// ================== Statements ==================

// CreateTableStatement は CREATE TABLE 文
type CreateTableStatement struct {
    TableName string
    Columns   []ColumnDefinition
}

func (s *CreateTableStatement) node()          {}
func (s *CreateTableStatement) statementNode() {}

// ColumnDefinition はカラム定義
type ColumnDefinition struct {
    Name     string
    DataType types.DataType
    Nullable bool
}

// InsertStatement は INSERT 文
type InsertStatement struct {
    TableName string
    Columns   []string      // 省略時は nil
    Values    []Expression
}

func (s *InsertStatement) node()          {}
func (s *InsertStatement) statementNode() {}

// SelectStatement は SELECT 文
type SelectStatement struct {
    Columns   []SelectColumn
    TableName string
    Where     Expression
    OrderBy   []OrderByClause
    Limit     *int64
}

func (s *SelectStatement) node()          {}
func (s *SelectStatement) statementNode() {}

// SelectColumn は SELECT 句のカラム
type SelectColumn struct {
    Expression Expression
    Alias      string
    IsWildcard bool
}

// OrderByClause は ORDER BY 句の要素
type OrderByClause struct {
    Column string
    Desc   bool
}

// ================== Expressions ==================

// Identifier は識別子
type Identifier struct {
    Name string
}

func (e *Identifier) node()           {}
func (e *Identifier) expressionNode() {}

// IntegerLiteral は整数リテラル
type IntegerLiteral struct {
    Value int64
}

func (e *IntegerLiteral) node()           {}
func (e *IntegerLiteral) expressionNode() {}

// FloatLiteral は浮動小数点リテラル
type FloatLiteral struct {
    Value float64
}

func (e *FloatLiteral) node()           {}
func (e *FloatLiteral) expressionNode() {}

// StringLiteral は文字列リテラル
type StringLiteral struct {
    Value string
}

func (e *StringLiteral) node()           {}
func (e *StringLiteral) expressionNode() {}

// NullLiteral は NULL リテラル
type NullLiteral struct{}

func (e *NullLiteral) node()           {}
func (e *NullLiteral) expressionNode() {}

// BinaryExpression は二項演算式
type BinaryExpression struct {
    Left     Expression
    Operator string // =, <>, <, >, <=, >=, AND, OR
    Right    Expression
}

func (e *BinaryExpression) node()           {}
func (e *BinaryExpression) expressionNode() {}

// FunctionCall は関数呼び出し
type FunctionCall struct {
    Name      string       // COUNT, SUM, AVG, MIN, MAX
    Arguments []Expression
    Distinct  bool
}

func (e *FunctionCall) node()           {}
func (e *FunctionCall) expressionNode() {}
```

#### Task 3.4: パーサー

```go
// internal/parser/parser.go

package parser

import (
    "fmt"
    "strconv"

    "github.com/taikicoco/tate/internal/ast"
    "github.com/taikicoco/tate/internal/lexer"
    "github.com/taikicoco/tate/internal/types"
)

type Parser struct {
    l         *lexer.Lexer
    curToken  lexer.Token
    peekToken lexer.Token
    errors    []string
}

func New(l *lexer.Lexer) *Parser {
    p := &Parser{l: l, errors: []string{}}
    p.nextToken()
    p.nextToken()
    return p
}

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
    msg := fmt.Sprintf("expected next token to be %v, got %v instead",
        t, p.peekToken.Type)
    p.errors = append(p.errors, msg)
}

// Parse はSQLをパースしてASTを返す
func (p *Parser) Parse() ast.Statement {
    switch p.curToken.Type {
    case lexer.TOKEN_SELECT:
        return p.parseSelectStatement()
    case lexer.TOKEN_INSERT:
        return p.parseInsertStatement()
    case lexer.TOKEN_CREATE:
        return p.parseCreateStatement()
    default:
        p.errors = append(p.errors, fmt.Sprintf("unexpected token: %s", p.curToken.Literal))
        return nil
    }
}

// parseSelectStatement は SELECT 文をパース
func (p *Parser) parseSelectStatement() *ast.SelectStatement {
    stmt := &ast.SelectStatement{}

    p.nextToken() // SELECT の次へ

    // SELECT 句
    stmt.Columns = p.parseSelectColumns()

    // FROM 句
    if !p.expectPeek(lexer.TOKEN_FROM) {
        return nil
    }
    p.nextToken()

    if !p.curTokenIs(lexer.TOKEN_IDENT) {
        p.errors = append(p.errors, "expected table name")
        return nil
    }
    stmt.TableName = p.curToken.Literal

    // WHERE 句（オプション）
    if p.peekTokenIs(lexer.TOKEN_WHERE) {
        p.nextToken() // WHERE
        p.nextToken() // 条件式の開始
        stmt.Where = p.parseExpression()
    }

    // ORDER BY 句（オプション）
    if p.peekTokenIs(lexer.TOKEN_ORDER) {
        p.nextToken() // ORDER
        if !p.expectPeek(lexer.TOKEN_BY) {
            return nil
        }
        stmt.OrderBy = p.parseOrderByClause()
    }

    // LIMIT 句（オプション）
    if p.peekTokenIs(lexer.TOKEN_LIMIT) {
        p.nextToken() // LIMIT
        p.nextToken() // 数値
        if p.curTokenIs(lexer.TOKEN_INT) {
            limit, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
            stmt.Limit = &limit
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
                Expression: p.parseExpression(),
            }
            columns = append(columns, col)
        }

        if !p.peekTokenIs(lexer.TOKEN_COMMA) {
            break
        }
        p.nextToken() // ,
        p.nextToken() // 次のカラム
    }

    return columns
}

func (p *Parser) parseOrderByClause() []ast.OrderByClause {
    var clauses []ast.OrderByClause

    p.nextToken() // BY の次へ

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
        p.nextToken() // ,
        p.nextToken() // 次のカラム
    }

    return clauses
}

// parseInsertStatement は INSERT 文をパース
func (p *Parser) parseInsertStatement() *ast.InsertStatement {
    stmt := &ast.InsertStatement{}

    if !p.expectPeek(lexer.TOKEN_INTO) {
        return nil
    }
    p.nextToken()

    if !p.curTokenIs(lexer.TOKEN_IDENT) {
        p.errors = append(p.errors, "expected table name")
        return nil
    }
    stmt.TableName = p.curToken.Literal

    // カラム名リスト（オプション）
    if p.peekTokenIs(lexer.TOKEN_LPAREN) {
        p.nextToken() // (
        stmt.Columns = p.parseIdentifierList()
        if !p.expectPeek(lexer.TOKEN_RPAREN) {
            return nil
        }
    }

    // VALUES
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

// parseCreateStatement は CREATE 文をパース
func (p *Parser) parseCreateStatement() *ast.CreateTableStatement {
    if !p.expectPeek(lexer.TOKEN_TABLE) {
        return nil
    }
    p.nextToken()

    stmt := &ast.CreateTableStatement{}

    if !p.curTokenIs(lexer.TOKEN_IDENT) {
        p.errors = append(p.errors, "expected table name")
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

func (p *Parser) parseColumnDefinitions() []ast.ColumnDefinition {
    var defs []ast.ColumnDefinition

    p.nextToken() // ( の次へ

    for !p.curTokenIs(lexer.TOKEN_RPAREN) && !p.curTokenIs(lexer.TOKEN_EOF) {
        def := ast.ColumnDefinition{Nullable: true}

        if !p.curTokenIs(lexer.TOKEN_IDENT) {
            break
        }
        def.Name = p.curToken.Literal

        p.nextToken() // 型へ
        def.DataType = p.parseDataType()

        defs = append(defs, def)

        if p.peekTokenIs(lexer.TOKEN_COMMA) {
            p.nextToken() // ,
            p.nextToken() // 次のカラム名
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
        return types.TypeNull
    }
}

func (p *Parser) parseIdentifierList() []string {
    var idents []string

    p.nextToken() // ( の次へ

    for !p.curTokenIs(lexer.TOKEN_RPAREN) && !p.curTokenIs(lexer.TOKEN_EOF) {
        if p.curTokenIs(lexer.TOKEN_IDENT) {
            idents = append(idents, p.curToken.Literal)
        }

        if p.peekTokenIs(lexer.TOKEN_COMMA) {
            p.nextToken()
            p.nextToken()
        } else {
            break
        }
    }

    return idents
}

func (p *Parser) parseExpressionList() []ast.Expression {
    var exprs []ast.Expression

    for !p.curTokenIs(lexer.TOKEN_RPAREN) && !p.curTokenIs(lexer.TOKEN_EOF) {
        exprs = append(exprs, p.parseExpression())

        if p.peekTokenIs(lexer.TOKEN_COMMA) {
            p.nextToken()
            p.nextToken()
        } else {
            break
        }
    }

    return exprs
}

// parseExpression は式をパース
func (p *Parser) parseExpression() ast.Expression {
    left := p.parsePrimaryExpression()

    // 二項演算子をチェック
    for p.isBinaryOperator(p.peekToken.Type) {
        p.nextToken()
        op := p.curToken.Literal
        p.nextToken()
        right := p.parsePrimaryExpression()
        left = &ast.BinaryExpression{
            Left:     left,
            Operator: op,
            Right:    right,
        }
    }

    return left
}

func (p *Parser) parsePrimaryExpression() ast.Expression {
    switch p.curToken.Type {
    case lexer.TOKEN_INT:
        val, _ := strconv.ParseInt(p.curToken.Literal, 10, 64)
        return &ast.IntegerLiteral{Value: val}
    case lexer.TOKEN_FLOAT:
        val, _ := strconv.ParseFloat(p.curToken.Literal, 64)
        return &ast.FloatLiteral{Value: val}
    case lexer.TOKEN_STRING:
        return &ast.StringLiteral{Value: p.curToken.Literal}
    case lexer.TOKEN_NULL:
        return &ast.NullLiteral{}
    case lexer.TOKEN_COUNT, lexer.TOKEN_SUM, lexer.TOKEN_AVG,
        lexer.TOKEN_MIN, lexer.TOKEN_MAX:
        return p.parseFunctionCall()
    case lexer.TOKEN_IDENT:
        return &ast.Identifier{Name: p.curToken.Literal}
    default:
        return nil
    }
}

func (p *Parser) parseFunctionCall() *ast.FunctionCall {
    fn := &ast.FunctionCall{Name: p.curToken.Literal}

    if !p.expectPeek(lexer.TOKEN_LPAREN) {
        return nil
    }

    p.nextToken()

    // COUNT(*) の特別扱い
    if p.curTokenIs(lexer.TOKEN_ASTERISK) {
        fn.Arguments = []ast.Expression{&ast.Identifier{Name: "*"}}
    } else {
        fn.Arguments = p.parseExpressionList()
    }

    if !p.expectPeek(lexer.TOKEN_RPAREN) {
        return nil
    }

    return fn
}

func (p *Parser) isBinaryOperator(t lexer.TokenType) bool {
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
```

---

## 7. Phase 4: クエリ実行エンジン

### 7.1 目標

- 実行計画の生成
- 基本的なクエリの実行
- 集計関数の実装

### 7.2 実装タスク

#### Task 4.1: 実行計画

```go
// internal/planner/plan.go

package planner

import (
    "github.com/taikicoco/tate/internal/ast"
    "github.com/taikicoco/tate/internal/catalog"
)

// PlanNode は実行計画のノード
type PlanNode interface {
    planNode()
}

// ScanNode はテーブルスキャン
type ScanNode struct {
    TableName string
    Columns   []string // 読み込むカラム（空なら全て）
    Schema    *catalog.TableSchema
}

func (n *ScanNode) planNode() {}

// FilterNode はフィルタ処理
type FilterNode struct {
    Input     PlanNode
    Condition ast.Expression
}

func (n *FilterNode) planNode() {}

// ProjectNode は射影（カラム選択）
type ProjectNode struct {
    Input   PlanNode
    Columns []ast.SelectColumn
}

func (n *ProjectNode) planNode() {}

// AggregateNode は集計処理
type AggregateNode struct {
    Input      PlanNode
    Aggregates []AggregateFunc
}

func (n *AggregateNode) planNode() {}

// AggregateFunc は集計関数
type AggregateFunc struct {
    Name   string // COUNT, SUM, AVG, MIN, MAX
    Column string
    Alias  string
}

// SortNode はソート処理
type SortNode struct {
    Input   PlanNode
    OrderBy []ast.OrderByClause
}

func (n *SortNode) planNode() {}

// LimitNode は結果制限
type LimitNode struct {
    Input PlanNode
    Limit int64
}

func (n *LimitNode) planNode() {}
```

#### Task 4.2: プランナー

```go
// internal/planner/planner.go

package planner

import (
    "github.com/taikicoco/tate/internal/ast"
    "github.com/taikicoco/tate/internal/catalog"
)

// Planner は実行計画を生成
type Planner struct {
    catalog *catalog.Catalog
}

func NewPlanner(cat *catalog.Catalog) *Planner {
    return &Planner{catalog: cat}
}

// Plan はASTから実行計画を生成
func (p *Planner) Plan(stmt ast.Statement) (PlanNode, error) {
    switch s := stmt.(type) {
    case *ast.SelectStatement:
        return p.planSelect(s)
    default:
        return nil, nil
    }
}

func (p *Planner) planSelect(stmt *ast.SelectStatement) (PlanNode, error) {
    schema, _ := p.catalog.GetTable(stmt.TableName)

    // 1. スキャンノード
    var plan PlanNode = &ScanNode{
        TableName: stmt.TableName,
        Schema:    schema,
    }

    // 2. フィルタノード（WHERE句がある場合）
    if stmt.Where != nil {
        plan = &FilterNode{
            Input:     plan,
            Condition: stmt.Where,
        }
    }

    // 3. 集計関数があるかチェック
    hasAggregates := p.hasAggregates(stmt.Columns)
    if hasAggregates {
        plan = &AggregateNode{
            Input:      plan,
            Aggregates: p.extractAggregates(stmt.Columns),
        }
    } else {
        // 4. 射影ノード
        plan = &ProjectNode{
            Input:   plan,
            Columns: stmt.Columns,
        }
    }

    // 5. ソートノード
    if len(stmt.OrderBy) > 0 {
        plan = &SortNode{
            Input:   plan,
            OrderBy: stmt.OrderBy,
        }
    }

    // 6. リミットノード
    if stmt.Limit != nil {
        plan = &LimitNode{
            Input: plan,
            Limit: *stmt.Limit,
        }
    }

    return plan, nil
}

func (p *Planner) hasAggregates(columns []ast.SelectColumn) bool {
    for _, col := range columns {
        if _, ok := col.Expression.(*ast.FunctionCall); ok {
            return true
        }
    }
    return false
}

func (p *Planner) extractAggregates(columns []ast.SelectColumn) []AggregateFunc {
    var aggs []AggregateFunc

    for _, col := range columns {
        if fn, ok := col.Expression.(*ast.FunctionCall); ok {
            agg := AggregateFunc{Name: fn.Name}
            if len(fn.Arguments) > 0 {
                if ident, ok := fn.Arguments[0].(*ast.Identifier); ok {
                    agg.Column = ident.Name
                }
            }
            aggs = append(aggs, agg)
        }
    }

    return aggs
}
```

#### Task 4.3: エグゼキュータ

```go
// internal/executor/executor.go

package executor

import (
    "fmt"
    "sort"

    "github.com/taikicoco/tate/internal/ast"
    "github.com/taikicoco/tate/internal/planner"
    "github.com/taikicoco/tate/internal/storage"
    "github.com/taikicoco/tate/internal/types"
)

// Result はクエリ結果
type Result struct {
    Columns []string
    Rows    [][]types.Value
}

// Executor はクエリを実行
type Executor struct {
    tables map[string]*storage.Table
}

func NewExecutor(tables map[string]*storage.Table) *Executor {
    return &Executor{tables: tables}
}

// Execute は実行計画を実行
func (e *Executor) Execute(plan planner.PlanNode) (*Result, error) {
    return e.executePlan(plan)
}

func (e *Executor) executePlan(plan planner.PlanNode) (*Result, error) {
    switch p := plan.(type) {
    case *planner.ScanNode:
        return e.executeScan(p)
    case *planner.FilterNode:
        return e.executeFilter(p)
    case *planner.ProjectNode:
        return e.executeProject(p)
    case *planner.AggregateNode:
        return e.executeAggregate(p)
    case *planner.SortNode:
        return e.executeSort(p)
    case *planner.LimitNode:
        return e.executeLimit(p)
    default:
        return nil, fmt.Errorf("unknown plan node type")
    }
}

func (e *Executor) executeScan(node *planner.ScanNode) (*Result, error) {
    table, exists := e.tables[node.TableName]
    if !exists {
        return nil, fmt.Errorf("table not found: %s", node.TableName)
    }

    result := &Result{}

    // カラム名を設定
    for _, col := range node.Schema.Columns {
        result.Columns = append(result.Columns, col.Name)
    }

    // 全行を読み込み
    rowCount := table.RowCount()
    for i := uint64(0); i < rowCount; i++ {
        row := make([]types.Value, len(node.Schema.Columns))

        for j, col := range node.Schema.Columns {
            cf := table.Columns[col.Name]
            switch col.Type {
            case types.TypeInt64:
                if v, ok := cf.GetInt64(i); ok {
                    row[j] = types.NewInt64Value(v)
                } else {
                    row[j] = types.NewNullValue()
                }
            // 他の型も同様に実装
            }
        }

        result.Rows = append(result.Rows, row)
    }

    return result, nil
}

func (e *Executor) executeFilter(node *planner.FilterNode) (*Result, error) {
    input, err := e.executePlan(node.Input)
    if err != nil {
        return nil, err
    }

    result := &Result{Columns: input.Columns}

    for _, row := range input.Rows {
        if e.evaluateCondition(node.Condition, input.Columns, row) {
            result.Rows = append(result.Rows, row)
        }
    }

    return result, nil
}

func (e *Executor) evaluateCondition(expr ast.Expression, columns []string, row []types.Value) bool {
    switch e := expr.(type) {
    case *ast.BinaryExpression:
        leftVal := e.evaluateExpr(e.Left, columns, row)
        rightVal := e.evaluateExpr(e.Right, columns, row)
        return e.compareValues(leftVal, rightVal, e.Operator)
    }
    return true
}

func (e *Executor) evaluateExpr(expr ast.Expression, columns []string, row []types.Value) types.Value {
    switch ex := expr.(type) {
    case *ast.Identifier:
        for i, col := range columns {
            if col == ex.Name {
                return row[i]
            }
        }
    case *ast.IntegerLiteral:
        return types.NewInt64Value(ex.Value)
    case *ast.StringLiteral:
        return types.NewStringValue(ex.Value)
    }
    return types.NewNullValue()
}

func (e *Executor) compareValues(left, right types.Value, op string) bool {
    // Int64同士の比較
    if left.Type == types.TypeInt64 && right.Type == types.TypeInt64 {
        l, _ := left.AsInt64()
        r, _ := right.AsInt64()
        switch op {
        case "=":
            return l == r
        case "<>", "!=":
            return l != r
        case "<":
            return l < r
        case ">":
            return l > r
        case "<=":
            return l <= r
        case ">=":
            return l >= r
        }
    }
    return false
}

func (e *Executor) executeProject(node *planner.ProjectNode) (*Result, error) {
    input, err := e.executePlan(node.Input)
    if err != nil {
        return nil, err
    }

    // ワイルドカードの場合はそのまま返す
    for _, col := range node.Columns {
        if col.IsWildcard {
            return input, nil
        }
    }

    result := &Result{}

    // 選択するカラムのインデックスを特定
    var indices []int
    for _, col := range node.Columns {
        if ident, ok := col.Expression.(*ast.Identifier); ok {
            for i, c := range input.Columns {
                if c == ident.Name {
                    indices = append(indices, i)
                    result.Columns = append(result.Columns, c)
                    break
                }
            }
        }
    }

    // 行を射影
    for _, row := range input.Rows {
        newRow := make([]types.Value, len(indices))
        for i, idx := range indices {
            newRow[i] = row[idx]
        }
        result.Rows = append(result.Rows, newRow)
    }

    return result, nil
}

func (e *Executor) executeAggregate(node *planner.AggregateNode) (*Result, error) {
    input, err := e.executePlan(node.Input)
    if err != nil {
        return nil, err
    }

    result := &Result{}
    resultRow := make([]types.Value, len(node.Aggregates))

    for i, agg := range node.Aggregates {
        result.Columns = append(result.Columns, fmt.Sprintf("%s(%s)", agg.Name, agg.Column))

        switch agg.Name {
        case "COUNT":
            resultRow[i] = types.NewInt64Value(int64(len(input.Rows)))
        case "SUM":
            sum := e.calculateSum(input, agg.Column)
            resultRow[i] = types.NewFloat64Value(sum)
        case "AVG":
            sum := e.calculateSum(input, agg.Column)
            avg := sum / float64(len(input.Rows))
            resultRow[i] = types.NewFloat64Value(avg)
        case "MIN":
            min := e.calculateMin(input, agg.Column)
            resultRow[i] = min
        case "MAX":
            max := e.calculateMax(input, agg.Column)
            resultRow[i] = max
        }
    }

    result.Rows = [][]types.Value{resultRow}
    return result, nil
}

func (e *Executor) calculateSum(input *Result, column string) float64 {
    colIdx := -1
    for i, c := range input.Columns {
        if c == column {
            colIdx = i
            break
        }
    }
    if colIdx == -1 {
        return 0
    }

    var sum float64
    for _, row := range input.Rows {
        if v, ok := row[colIdx].AsInt64(); ok {
            sum += float64(v)
        } else if v, ok := row[colIdx].AsFloat64(); ok {
            sum += v
        }
    }
    return sum
}

func (e *Executor) calculateMin(input *Result, column string) types.Value {
    // 実装省略（同様のロジック）
    return types.NewNullValue()
}

func (e *Executor) calculateMax(input *Result, column string) types.Value {
    // 実装省略（同様のロジック）
    return types.NewNullValue()
}

func (e *Executor) executeSort(node *planner.SortNode) (*Result, error) {
    input, err := e.executePlan(node.Input)
    if err != nil {
        return nil, err
    }

    // ソート対象のカラムインデックスを特定
    orderByIndices := make([]int, len(node.OrderBy))
    for i, ob := range node.OrderBy {
        for j, col := range input.Columns {
            if col == ob.Column {
                orderByIndices[i] = j
                break
            }
        }
    }

    // ソート実行
    sort.Slice(input.Rows, func(i, j int) bool {
        for k, idx := range orderByIndices {
            cmp := e.compareValuesForSort(input.Rows[i][idx], input.Rows[j][idx])
            if cmp != 0 {
                if node.OrderBy[k].Desc {
                    return cmp > 0
                }
                return cmp < 0
            }
        }
        return false
    })

    return input, nil
}

func (e *Executor) compareValuesForSort(a, b types.Value) int {
    if a.IsNull && b.IsNull {
        return 0
    }
    if a.IsNull {
        return -1
    }
    if b.IsNull {
        return 1
    }

    switch a.Type {
    case types.TypeInt64:
        av, _ := a.AsInt64()
        bv, _ := b.AsInt64()
        if av < bv {
            return -1
        } else if av > bv {
            return 1
        }
        return 0
    }
    return 0
}

func (e *Executor) executeLimit(node *planner.LimitNode) (*Result, error) {
    input, err := e.executePlan(node.Input)
    if err != nil {
        return nil, err
    }

    if int64(len(input.Rows)) > node.Limit {
        input.Rows = input.Rows[:node.Limit]
    }

    return input, nil
}
```

---

## 8. Phase 5: インデックス

### 8.1 目標

- ビットマップインデックスの実装
- Zone Map（Min-Max インデックス）の実装

### 8.2 実装タスク

#### Task 5.1: ビットマップインデックス

```go
// internal/index/bitmap.go

package index

// BitmapIndex は低カーディナリティカラム用のビットマップインデックス
type BitmapIndex struct {
    ColumnName string
    Bitmaps    map[interface{}]*Bitmap
}

// Bitmap はビットの集合
type Bitmap struct {
    bits   []uint64
    length int
}

func NewBitmap() *Bitmap {
    return &Bitmap{bits: make([]uint64, 0)}
}

func (b *Bitmap) Set(pos int) {
    wordIdx := pos / 64
    bitIdx := pos % 64

    // 必要に応じて拡張
    for len(b.bits) <= wordIdx {
        b.bits = append(b.bits, 0)
    }

    b.bits[wordIdx] |= (1 << bitIdx)
    if pos >= b.length {
        b.length = pos + 1
    }
}

func (b *Bitmap) Get(pos int) bool {
    wordIdx := pos / 64
    if wordIdx >= len(b.bits) {
        return false
    }
    bitIdx := pos % 64
    return (b.bits[wordIdx] & (1 << bitIdx)) != 0
}

// And はビットマップのAND演算
func (b *Bitmap) And(other *Bitmap) *Bitmap {
    result := NewBitmap()
    minLen := len(b.bits)
    if len(other.bits) < minLen {
        minLen = len(other.bits)
    }

    result.bits = make([]uint64, minLen)
    for i := 0; i < minLen; i++ {
        result.bits[i] = b.bits[i] & other.bits[i]
    }
    return result
}

// Or はビットマップのOR演算
func (b *Bitmap) Or(other *Bitmap) *Bitmap {
    result := NewBitmap()
    maxLen := len(b.bits)
    if len(other.bits) > maxLen {
        maxLen = len(other.bits)
    }

    result.bits = make([]uint64, maxLen)
    for i := 0; i < maxLen; i++ {
        if i < len(b.bits) {
            result.bits[i] = b.bits[i]
        }
        if i < len(other.bits) {
            result.bits[i] |= other.bits[i]
        }
    }
    return result
}

// Count は立っているビットの数を返す
func (b *Bitmap) Count() int {
    count := 0
    for _, word := range b.bits {
        count += popcount(word)
    }
    return count
}

// Positions は立っているビットの位置を返す
func (b *Bitmap) Positions() []int {
    var positions []int
    for i := 0; i < b.length; i++ {
        if b.Get(i) {
            positions = append(positions, i)
        }
    }
    return positions
}

func popcount(x uint64) int {
    count := 0
    for x != 0 {
        count++
        x &= x - 1
    }
    return count
}

// NewBitmapIndex は新しいビットマップインデックスを作成
func NewBitmapIndex(columnName string) *BitmapIndex {
    return &BitmapIndex{
        ColumnName: columnName,
        Bitmaps:    make(map[interface{}]*Bitmap),
    }
}

// Add は値と行位置を追加
func (bi *BitmapIndex) Add(value interface{}, rowPos int) {
    if bi.Bitmaps[value] == nil {
        bi.Bitmaps[value] = NewBitmap()
    }
    bi.Bitmaps[value].Set(rowPos)
}

// Lookup は値に対応する行位置を返す
func (bi *BitmapIndex) Lookup(value interface{}) *Bitmap {
    return bi.Bitmaps[value]
}
```

#### Task 5.2: Zone Map

```go
// internal/index/zonemap.go

package index

import "github.com/taikicoco/tate/internal/types"

// ZoneMap はRow Group単位のMin-Maxインデックス
type ZoneMap struct {
    ColumnName string
    Zones      []Zone
}

// Zone は単一のゾーン（Row Group）の統計情報
type Zone struct {
    RowGroupID int
    MinValue   types.Value
    MaxValue   types.Value
    RowCount   int
    NullCount  int
}

func NewZoneMap(columnName string) *ZoneMap {
    return &ZoneMap{
        ColumnName: columnName,
        Zones:      make([]Zone, 0),
    }
}

// AddZone はゾーンを追加
func (zm *ZoneMap) AddZone(zone Zone) {
    zm.Zones = append(zm.Zones, zone)
}

// CanSkip は条件に基づいてゾーンをスキップできるか判定
func (zm *ZoneMap) CanSkip(zoneID int, op string, value types.Value) bool {
    if zoneID >= len(zm.Zones) {
        return false
    }

    zone := zm.Zones[zoneID]

    switch op {
    case "=":
        // 値がmin-max範囲外ならスキップ可能
        if compareValues(value, zone.MinValue) < 0 ||
            compareValues(value, zone.MaxValue) > 0 {
            return true
        }
    case "<":
        // 全ての値が検索値以上ならスキップ可能
        if compareValues(zone.MinValue, value) >= 0 {
            return true
        }
    case ">":
        // 全ての値が検索値以下ならスキップ可能
        if compareValues(zone.MaxValue, value) <= 0 {
            return true
        }
    case "<=":
        if compareValues(zone.MinValue, value) > 0 {
            return true
        }
    case ">=":
        if compareValues(zone.MaxValue, value) < 0 {
            return true
        }
    }

    return false
}

// GetCandidateZones は条件を満たす可能性のあるゾーンIDを返す
func (zm *ZoneMap) GetCandidateZones(op string, value types.Value) []int {
    var candidates []int
    for i := range zm.Zones {
        if !zm.CanSkip(i, op, value) {
            candidates = append(candidates, i)
        }
    }
    return candidates
}

func compareValues(a, b types.Value) int {
    if a.IsNull && b.IsNull {
        return 0
    }
    if a.IsNull {
        return -1
    }
    if b.IsNull {
        return 1
    }

    switch a.Type {
    case types.TypeInt64:
        av, _ := a.AsInt64()
        bv, _ := b.AsInt64()
        if av < bv {
            return -1
        } else if av > bv {
            return 1
        }
        return 0
    }
    return 0
}
```

---

## 9. Phase 6: 圧縮・エンコーディング

### 9.1 目標

- RLE（Run-Length Encoding）の実装
- 辞書エンコーディングの実装
- 差分エンコーディングの実装

### 9.2 実装タスク

#### Task 6.1: RLE

```go
// internal/encoding/rle.go

package encoding

import (
    "encoding/binary"
    "io"
)

// RLEEncoder はRun-Length Encodingを行う
type RLEEncoder struct {
    runs []Run
}

// Run は連続する値とその回数
type Run struct {
    Value interface{}
    Count int
}

func NewRLEEncoder() *RLEEncoder {
    return &RLEEncoder{}
}

// EncodeInt64 はInt64配列をRLEエンコード
func (e *RLEEncoder) EncodeInt64(data []int64) []Run {
    if len(data) == 0 {
        return nil
    }

    var runs []Run
    current := Run{Value: data[0], Count: 1}

    for i := 1; i < len(data); i++ {
        if data[i] == current.Value.(int64) {
            current.Count++
        } else {
            runs = append(runs, current)
            current = Run{Value: data[i], Count: 1}
        }
    }
    runs = append(runs, current)

    e.runs = runs
    return runs
}

// DecodeInt64 はRLEデコードしてInt64配列を返す
func (e *RLEEncoder) DecodeInt64(runs []Run) []int64 {
    var result []int64

    for _, run := range runs {
        val := run.Value.(int64)
        for i := 0; i < run.Count; i++ {
            result = append(result, val)
        }
    }

    return result
}

// WriteInt64 はエンコード済みデータをバイナリで書き込み
func (e *RLEEncoder) WriteInt64(w io.Writer, runs []Run) error {
    // ラン数を書き込み
    if err := binary.Write(w, binary.LittleEndian, uint32(len(runs))); err != nil {
        return err
    }

    for _, run := range runs {
        // 値を書き込み
        if err := binary.Write(w, binary.LittleEndian, run.Value.(int64)); err != nil {
            return err
        }
        // カウントを書き込み
        if err := binary.Write(w, binary.LittleEndian, uint32(run.Count)); err != nil {
            return err
        }
    }

    return nil
}

// ReadInt64 はバイナリからRLEデータを読み込み
func (e *RLEEncoder) ReadInt64(r io.Reader) ([]Run, error) {
    var runCount uint32
    if err := binary.Read(r, binary.LittleEndian, &runCount); err != nil {
        return nil, err
    }

    runs := make([]Run, runCount)
    for i := uint32(0); i < runCount; i++ {
        var val int64
        var count uint32

        if err := binary.Read(r, binary.LittleEndian, &val); err != nil {
            return nil, err
        }
        if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
            return nil, err
        }

        runs[i] = Run{Value: val, Count: int(count)}
    }

    return runs, nil
}

// CompressionRatio は圧縮率を計算
func (e *RLEEncoder) CompressionRatio(originalCount int) float64 {
    if len(e.runs) == 0 {
        return 0
    }
    // 元のサイズ vs エンコード後のサイズ（ラン数 * (値 + カウント)）
    originalSize := originalCount * 8 // int64 = 8 bytes
    encodedSize := len(e.runs) * 12   // int64 + uint32 = 12 bytes
    return float64(originalSize) / float64(encodedSize)
}
```

#### Task 6.2: 辞書エンコーディング

```go
// internal/encoding/dictionary.go

package encoding

import (
    "encoding/binary"
    "io"
)

// DictionaryEncoder は辞書エンコーディングを行う
type DictionaryEncoder struct {
    dict    map[string]int
    reverse []string
}

func NewDictionaryEncoder() *DictionaryEncoder {
    return &DictionaryEncoder{
        dict:    make(map[string]int),
        reverse: make([]string, 0),
    }
}

// Encode は文字列配列を辞書エンコード
func (e *DictionaryEncoder) Encode(data []string) []int {
    encoded := make([]int, len(data))

    for i, val := range data {
        if idx, exists := e.dict[val]; exists {
            encoded[i] = idx
        } else {
            idx := len(e.reverse)
            e.dict[val] = idx
            e.reverse = append(e.reverse, val)
            encoded[i] = idx
        }
    }

    return encoded
}

// Decode は辞書インデックスから文字列を復元
func (e *DictionaryEncoder) Decode(indices []int) []string {
    result := make([]string, len(indices))
    for i, idx := range indices {
        if idx < len(e.reverse) {
            result[i] = e.reverse[idx]
        }
    }
    return result
}

// GetValue はインデックスから値を取得
func (e *DictionaryEncoder) GetValue(idx int) (string, bool) {
    if idx < 0 || idx >= len(e.reverse) {
        return "", false
    }
    return e.reverse[idx], true
}

// Dictionary は辞書を返す
func (e *DictionaryEncoder) Dictionary() []string {
    return e.reverse
}

// Write は辞書とエンコード済みデータを書き込み
func (e *DictionaryEncoder) Write(w io.Writer, indices []int) error {
    // 辞書サイズを書き込み
    if err := binary.Write(w, binary.LittleEndian, uint32(len(e.reverse))); err != nil {
        return err
    }

    // 辞書を書き込み
    for _, val := range e.reverse {
        valBytes := []byte(val)
        if err := binary.Write(w, binary.LittleEndian, uint32(len(valBytes))); err != nil {
            return err
        }
        if _, err := w.Write(valBytes); err != nil {
            return err
        }
    }

    // インデックス配列を書き込み
    if err := binary.Write(w, binary.LittleEndian, uint32(len(indices))); err != nil {
        return err
    }
    for _, idx := range indices {
        if err := binary.Write(w, binary.LittleEndian, uint32(idx)); err != nil {
            return err
        }
    }

    return nil
}

// Read は辞書とインデックスを読み込み
func (e *DictionaryEncoder) Read(r io.Reader) ([]int, error) {
    // 辞書サイズを読み込み
    var dictSize uint32
    if err := binary.Read(r, binary.LittleEndian, &dictSize); err != nil {
        return nil, err
    }

    // 辞書を読み込み
    e.reverse = make([]string, dictSize)
    e.dict = make(map[string]int)
    for i := uint32(0); i < dictSize; i++ {
        var strLen uint32
        if err := binary.Read(r, binary.LittleEndian, &strLen); err != nil {
            return nil, err
        }
        strBytes := make([]byte, strLen)
        if _, err := io.ReadFull(r, strBytes); err != nil {
            return nil, err
        }
        val := string(strBytes)
        e.reverse[i] = val
        e.dict[val] = int(i)
    }

    // インデックス配列を読み込み
    var indexCount uint32
    if err := binary.Read(r, binary.LittleEndian, &indexCount); err != nil {
        return nil, err
    }

    indices := make([]int, indexCount)
    for i := uint32(0); i < indexCount; i++ {
        var idx uint32
        if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
            return nil, err
        }
        indices[i] = int(idx)
    }

    return indices, nil
}

// CompressionRatio は圧縮率を計算
func (e *DictionaryEncoder) CompressionRatio(originalData []string) float64 {
    var originalSize int
    for _, s := range originalData {
        originalSize += len(s)
    }

    var dictSize int
    for _, s := range e.reverse {
        dictSize += len(s) + 4 // 文字列 + 長さ
    }
    encodedSize := dictSize + len(originalData)*4 // 辞書 + インデックス

    return float64(originalSize) / float64(encodedSize)
}
```

#### Task 6.3: 差分エンコーディング

```go
// internal/encoding/delta.go

package encoding

import (
    "encoding/binary"
    "io"
)

// DeltaEncoder は差分エンコーディングを行う
type DeltaEncoder struct{}

func NewDeltaEncoder() *DeltaEncoder {
    return &DeltaEncoder{}
}

// Encode はInt64配列を差分エンコード
func (e *DeltaEncoder) Encode(data []int64) (base int64, deltas []int64) {
    if len(data) == 0 {
        return 0, nil
    }

    base = data[0]
    deltas = make([]int64, len(data))

    for i, val := range data {
        deltas[i] = val - base
    }

    return base, deltas
}

// Decode は差分デコードしてInt64配列を返す
func (e *DeltaEncoder) Decode(base int64, deltas []int64) []int64 {
    result := make([]int64, len(deltas))

    for i, delta := range deltas {
        result[i] = base + delta
    }

    return result
}

// EncodeDeltaOfDelta はさらに差分の差分を取る（時系列データに効果的）
func (e *DeltaEncoder) EncodeDeltaOfDelta(data []int64) (first int64, second int64, deltas []int64) {
    if len(data) < 2 {
        return 0, 0, nil
    }

    first = data[0]
    second = data[1] - data[0]
    deltas = make([]int64, len(data)-2)

    prev := second
    for i := 2; i < len(data); i++ {
        delta := data[i] - data[i-1]
        deltas[i-2] = delta - prev
        prev = delta
    }

    return first, second, deltas
}

// DecodeDeltaOfDelta は差分の差分をデコード
func (e *DeltaEncoder) DecodeDeltaOfDelta(first, second int64, deltas []int64) []int64 {
    result := make([]int64, len(deltas)+2)
    result[0] = first
    result[1] = first + second

    prevDelta := second
    for i, dod := range deltas {
        delta := prevDelta + dod
        result[i+2] = result[i+1] + delta
        prevDelta = delta
    }

    return result
}

// Write は差分エンコード済みデータを書き込み
func (e *DeltaEncoder) Write(w io.Writer, base int64, deltas []int64) error {
    if err := binary.Write(w, binary.LittleEndian, base); err != nil {
        return err
    }

    if err := binary.Write(w, binary.LittleEndian, uint32(len(deltas))); err != nil {
        return err
    }

    for _, delta := range deltas {
        if err := binary.Write(w, binary.LittleEndian, delta); err != nil {
            return err
        }
    }

    return nil
}

// Read は差分エンコード済みデータを読み込み
func (e *DeltaEncoder) Read(r io.Reader) (int64, []int64, error) {
    var base int64
    if err := binary.Read(r, binary.LittleEndian, &base); err != nil {
        return 0, nil, err
    }

    var count uint32
    if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
        return 0, nil, err
    }

    deltas := make([]int64, count)
    for i := uint32(0); i < count; i++ {
        if err := binary.Read(r, binary.LittleEndian, &deltas[i]); err != nil {
            return 0, nil, err
        }
    }

    return base, deltas, nil
}
```

---

## 10. Phase 7: 高度な機能

### 10.1 目標

- WHERE句の完全な実装
- ORDER BY、LIMIT の最適化
- EXPLAIN機能
- 統計情報の収集

### 10.2 追加機能（発展課題）

- GROUP BY 句
- HAVING 句
- DISTINCT
- OFFSET
- 複合インデックス
- Bloom Filter

---

## 11. テスト戦略

### 11.1 テストレベル

| レベル | 対象 | ツール |
|--------|------|--------|
| 単体テスト | 個々の関数・メソッド | Go testing |
| 統合テスト | コンポーネント間の連携 | Go testing |
| E2Eテスト | SQLから結果まで | カスタムテストフレームワーク |

### 11.2 テストケース例

```go
// test/integration_test.go

func TestCreateTableAndInsert(t *testing.T) {
    // テスト用一時ディレクトリ
    tmpDir := t.TempDir()

    // データベース初期化
    db, err := NewDatabase(tmpDir)
    require.NoError(t, err)

    // CREATE TABLE
    _, err = db.Execute("CREATE TABLE users (id INT64, name STRING)")
    require.NoError(t, err)

    // INSERT
    _, err = db.Execute("INSERT INTO users VALUES (1, 'Alice')")
    require.NoError(t, err)

    _, err = db.Execute("INSERT INTO users VALUES (2, 'Bob')")
    require.NoError(t, err)

    // SELECT
    result, err := db.Execute("SELECT * FROM users")
    require.NoError(t, err)

    assert.Equal(t, 2, len(result.Rows))
}

func TestAggregation(t *testing.T) {
    // ...
    result, err := db.Execute("SELECT COUNT(*), SUM(age), AVG(age) FROM users")
    require.NoError(t, err)

    // 検証
    assert.Equal(t, int64(3), result.Rows[0][0].AsInt64())
    // ...
}
```

### 11.3 ベンチマーク

```go
func BenchmarkColumnScan(b *testing.B) {
    // 100万行のデータを準備
    // ...

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = db.Execute("SELECT SUM(value) FROM large_table")
    }
}

func BenchmarkWithoutIndex(b *testing.B) {
    // インデックスなしのスキャン
}

func BenchmarkWithBitmapIndex(b *testing.B) {
    // ビットマップインデックス使用
}
```

---

## 12. 開発環境セットアップ

### 12.1 必要なもの

- Go 1.21+
- Git
- エディタ（VSCode推奨）

### 12.2 初期セットアップ

```bash
# リポジトリ作成
mkdir tate
cd tate
git init

# Go modules 初期化
go mod init github.com/taikicoco/tate

# ディレクトリ構造作成
mkdir -p cmd/tate
mkdir -p internal/{ast,lexer,parser,analyzer,planner,executor,storage,encoding,compression,index,catalog,types}
mkdir -p pkg
mkdir -p test
mkdir -p docs
mkdir -p data

# 最初のファイル作成
touch cmd/tate/main.go
touch internal/types/types.go
```

### 12.3 Makefile

```makefile
.PHONY: build run test clean

build:
	go build -o bin/tate ./cmd/tate

run: build
	./bin/tate

test:
	go test -v ./...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/
	rm -rf data/

lint:
	golangci-lint run

fmt:
	go fmt ./...
```

### 12.4 .gitignore

```
# Binaries
bin/
*.exe

# Data
data/

# IDE
.idea/
.vscode/
*.swp

# Test
coverage.out

# OS
.DS_Store
```

---

## まとめ

この実装計画に従って進めることで、以下を学べます：

1. **データベースの基本構造**: パーサー、プランナー、エグゼキュータ
2. **列指向ストレージ**: カラムファイル、圧縮、インデックス
3. **Go言語のシステムプログラミング**: ファイルI/O、バイナリ処理、並行処理

**重要なポイント:**
- 各Phaseを順番に実装し、動作確認してから次に進む
- テストを先に書く（TDD）ことで設計が明確になる
- 最初はシンプルに、徐々に機能を追加

頑張ってください！
