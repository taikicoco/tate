---
title: "Goでゼロから作る列指向データベースTate"
emoji: "📊"
type: "tech"
topics: ["go", "database", "columnar", "olap"]
published: false
---

## はじめに

データベースには大きく分けて**行指向**と**列指向**の2つのストレージ形式があります。MySQL や PostgreSQL などの一般的な RDBMS は行指向ですが、BigQuery、ClickHouse、Redshift といった分析用途のデータベースは列指向を採用しています。

本記事では、Go 言語で列指向データベース「**Tate**（縦）」を実装した経験をもとに、列指向データベースの仕組みと実装方法を解説します。

https://github.com/taikicoco/tate

### この記事で得られるもの

- 列指向データベースの基礎概念と動作原理
- Go言語での列指向ストレージの実装方法
- 圧縮アルゴリズム（RLE、辞書、Delta）の仕組み
- ビットマップインデックスと Zone Map の実装
- SQL パーサーの設計パターン

## なぜ列指向なのか？

### 行指向 vs 列指向

まず、行指向と列指向の違いを理解しましょう。

```sql
-- 例: ユーザーテーブル
CREATE TABLE users (
    id INT64,
    name STRING,
    age INT64,
    city STRING
);
```

#### 行指向ストレージ（Row-Oriented）

```
ディスク上のレイアウト:
[1, "Alice", 30, "Tokyo"] → [2, "Bob", 25, "Osaka"] → [3, "Charlie", 35, "Tokyo"]
```

- 1行のデータがまとまって格納される
- `SELECT * FROM users WHERE id = 1` のような**単一行の取得が高速**
- OLTP（トランザクション処理）に最適

#### 列指向ストレージ（Column-Oriented）

```
ディスク上のレイアウト:
id列:   [1, 2, 3, ...]
name列: ["Alice", "Bob", "Charlie", ...]
age列:  [30, 25, 35, ...]
city列: ["Tokyo", "Osaka", "Tokyo", ...]
```

- 同じ列のデータが連続して格納される
- `SELECT AVG(age) FROM users` のような**集計クエリが高速**
- OLAP（分析処理）に最適

### 列指向の3つの利点

#### 1. 必要な列だけ読み込める（カラムプルーニング）

```sql
SELECT city FROM users WHERE age > 30;
```

行指向の場合、すべての列（id, name, age, city）を読み込む必要がありますが、列指向では `age` と `city` の2列だけを読めば OK です。

#### 2. 高い圧縮率

同じ型のデータが連続するため、圧縮効率が非常に高くなります。

```
# city列の例
["Tokyo", "Tokyo", "Tokyo", "Osaka", "Tokyo", ...]

# 辞書エンコーディング後
辞書: {0: "Tokyo", 1: "Osaka"}
データ: [0, 0, 0, 1, 0, ...]  # さらに圧縮可能！
```

#### 3. SIMD 最適化が可能

連続したメモリ領域に同じ型のデータが並ぶため、CPU の SIMD 命令を使った並列処理が可能です。

## アーキテクチャ設計

Tate は以下の7つのコンポーネントで構成されています。

```
tate/
├── cmd/tate/           # REPLエントリポイント
├── internal/
│   ├── types/          # データ型定義
│   ├── catalog/        # メタデータ管理
│   ├── lexer/          # SQL字句解析
│   ├── parser/         # SQL構文解析
│   ├── ast/            # 抽象構文木
│   ├── executor/       # クエリ実行エンジン
│   ├── storage/        # 列指向ストレージエンジン
│   ├── encoding/       # 圧縮・エンコーディング
│   └── index/          # インデックス（Bitmap, ZoneMap）
```

### 全体アーキテクチャ図

```
┌─────────────────────────────────────────────────────────┐
│                       クライアント                        │
│                    (REPL / SQL文)                       │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                   フロントエンド層                        │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐          │
│  │  Lexer   │ -> │  Parser  │ -> │   AST    │          │
│  │ (字句解析)│    │ (構文解析)│    │          │          │
│  └──────────┘    └──────────┘    └──────────┘          │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    実行エンジン層                         │
│  ┌─────────────────────────────────────────────┐        │
│  │             Executor                        │        │
│  │  - WHERE条件評価                             │        │
│  │  - 集約関数処理                              │        │
│  │  - ORDER BY / LIMIT                         │        │
│  └─────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                   ストレージ層                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Column   │  │  Index   │  │ Catalog  │              │
│  │  Store   │  │ (Bitmap, │  │(メタデータ)│              │
│  │          │  │ ZoneMap) │  │          │              │
│  └──────────┘  └──────────┘  └──────────┘              │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    物理ストレージ                         │
│         ┌─────────────────────────────┐                 │
│         │   Column Files (.dat)       │                 │
│         │   ├── col_id.dat            │                 │
│         │   ├── col_name.dat          │                 │
│         │   ├── col_age.dat           │                 │
│         │   └── col_city.dat          │                 │
│         └─────────────────────────────┘                 │
└─────────────────────────────────────────────────────────┘
```

### データフロー

```
SQL文: "SELECT age FROM users WHERE age > 30"
  │
  ▼
┌─────────────────────────────────────┐
│ [Lexer] 字句解析                     │
│ → [SELECT, age, FROM, users, ...]   │
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│ [Parser] 構文解析                    │
│ → SelectStatement{                  │
│     Columns: ["age"],               │
│     TableName: "users",             │
│     Where: age > 30                 │
│   }                                 │
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│ [Executor] 実行計画作成              │
│ 1. カラムプルーニング: age列のみ読む │
│ 2. WHERE age > 30 でフィルタ        │
└─────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────┐
│ [Storage] 列データの読み書き         │
│ - col_age.dat から age列を読み込み  │
│ - Zone Mapでスキャン範囲を限定      │
└─────────────────────────────────────┘
  │
  ▼
結果: [35, 40, 32, ...]
```

## 実装の詳細

### 1. 列指向ストレージエンジン

#### カラムファイル構造

各列は独立したファイルとして保存されます。

```
┌─────────────────────────────────────────────────────┐
│              Column File Format                     │
├─────────────────────────────────────────────────────┤
│  Header (固定長)                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │ Magic: "TCOL"                                 │  │
│  │ Version: 1                                    │  │
│  │ DataType: INT64                               │  │
│  │ Compression: RLE                              │  │
│  │ RowCount: 1000000                             │  │
│  │ NullBitmapSize: 125000 bytes                  │  │
│  │ DataSize: 800000 bytes (圧縮後)               │  │
│  └───────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────┤
│  NULL Bitmap                                        │
│  ┌───────────────────────────────────────────────┐  │
│  │ [0,0,0,1,0,0,0,0,0,0,1,0,...]                 │  │
│  │  ↑ビット単位でNULLを管理                       │  │
│  └───────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────┤
│  Data Section (圧縮済み)                            │
│  ┌───────────────────────────────────────────────┐  │
│  │ RLE: [(100, 5000), (200, 3000), (300, 2000)]  │  │
│  │      値   回数    値   回数    値   回数       │  │
│  └───────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────┤
│  Footer (統計情報)                                  │
│  ┌───────────────────────────────────────────────┐  │
│  │ MinValue: 100                                 │  │
│  │ MaxValue: 300                                 │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

```go
type ColumnHeader struct {
    Magic          [4]byte         // "TCOL"
    Version        uint16
    DataType       types.DataType
    Compression    CompressionType
    RowCount       uint64
    NullBitmapSize uint64
    DataSize       uint64
}

type ColumnFile struct {
    Header   ColumnHeader
    NullMask []byte  // NULL値の位置をビットマップで管理
    Data     []byte  // 実データ（圧縮済み）

    // 統計情報（Zone Map用）
    minValue types.Value
    maxValue types.Value
}
```

#### NULL 値の管理

NULL 値はビットマップで効率的に管理します。1行あたり1ビットで記録するため、1000万行でも約1.2MB で済みます。

```go
func (cf *ColumnFile) appendNullBit(isNull bool) {
    byteIndex := cf.Header.RowCount / 8
    bitIndex := cf.Header.RowCount % 8

    // ビットマップを拡張
    for uint64(len(cf.NullMask)) <= byteIndex {
        cf.NullMask = append(cf.NullMask, 0)
    }

    if isNull {
        cf.NullMask[byteIndex] |= (1 << bitIndex)
    }
}
```

#### カラムプルーニングの実装

必要な列だけを読み込むことで、I/O を大幅に削減できます。

```go
// 列単位でスキャン
func (t *Table) ScanColumns(columnNames []string,
    callback func(rowIndex uint64, values []types.Value) bool) error {

    // 必要な列のColumnFileだけを取得
    columns := make([]*ColumnFile, len(columnNames))
    for i, name := range columnNames {
        columns[i] = t.Columns[name]
    }

    // 必要な列だけスキャン
    for i := uint64(0); i < t.RowCount(); i++ {
        values := make([]types.Value, len(columnNames))
        for j, cf := range columns {
            values[j] = cf.GetValue(i)
        }
        if !callback(i, values) {
            break
        }
    }
    return nil
}
```

### 2. 圧縮・エンコーディング

#### Run-Length Encoding (RLE)

連続する同じ値を「値 × 回数」で表現します。

```go
type Run struct {
    Value int64
    Count int
}

// RLE エンコード
func (e *RLEEncoder) Encode(data []int64) []Run {
    runs := []Run{}
    current := Run{Value: data[0], Count: 1}

    for i := 1; i < len(data); i++ {
        if data[i] == current.Value {
            current.Count++
        } else {
            runs = append(runs, current)
            current = Run{Value: data[i], Count: 1}
        }
    }
    return append(runs, current)
}
```

**効果:**
```
# 例: status列（ソート済み）
元データ: [1, 1, 1, 1, 1, 2, 2, 2, 3, 3, ...]
RLE後:    [(1, 5), (2, 3), (3, 2), ...]
圧縮率:   83% 削減
```

#### Dictionary Encoding（辞書エンコーディング）

低カーディナリティ（値の種類が少ない）列に効果的です。

```go
type DictionaryEncoder struct {
    dict    map[string]int  // 値 → インデックス
    reverse []string        // インデックス → 値
}
```

**効果:**
```
# 例: 都道府県列（47種類）
元データ: ["Tokyo", "Osaka", "Tokyo", "Tokyo", "Kyoto", ...]
辞書:     {0: "Tokyo", 1: "Osaka", 2: "Kyoto"}
エンコード: [0, 1, 0, 0, 2, ...]

# 100万行の場合
元サイズ: 100万行 × 平均10文字 = 10MB
圧縮後:   辞書(数百バイト) + 100万行 × 4バイト = 4MB
圧縮率:   60% 削減
```

#### Delta Encoding（差分エンコーディング）

時系列データやIDのような連続する値に効果的です。

```go
func (e *DeltaEncoder) Encode(data []int64) (int64, []int64) {
    base := data[0]
    deltas := make([]int64, len(data))

    for i, val := range data {
        deltas[i] = val - base
    }

    return base, deltas
}
```

**効果:**
```
# 例: timestamp列
元データ: [1609459200, 1609459201, 1609459202, ...]  # 各8バイト
基準値:   1609459200
差分:     [0, 1, 2, ...]  # 小さな値 → ビット幅削減可能

# さらに bit packing で 1バイトに圧縮可能
圧縮率: 87.5% 削減
```

### 3. インデックス構造

#### Bitmap Index（ビットマップインデックス）

低カーディナリティ列（性別、ステータスなど）に最適です。

**仕組み:**
```
status列のデータ:
Row 0: "active"
Row 1: "active"
Row 2: "inactive"
Row 3: "active"
Row 4: "inactive"

ビットマップインデックス:
┌────────────────────────────────────┐
│ "active" のビットマップ             │
│ [1, 1, 0, 1, 0]                    │
│  ↑  ↑     ↑                        │
│  Row0,1,3 がactive                 │
└────────────────────────────────────┘
┌────────────────────────────────────┐
│ "inactive" のビットマップ           │
│ [0, 0, 1, 0, 1]                    │
│        ↑     ↑                      │
│        Row2,4 がinactive            │
└────────────────────────────────────┘

クエリ: WHERE status = 'active' AND age > 30
┌────────────────────────────────────┐
│ status='active' のビットマップ      │
│ [1, 1, 0, 1, 0]                    │
└────────────────────────────────────┘
              AND
┌────────────────────────────────────┐
│ age > 30 のビットマップ             │
│ [1, 0, 1, 1, 0]                    │
└────────────────────────────────────┘
              ‖
┌────────────────────────────────────┐
│ 結果のビットマップ                  │
│ [1, 0, 0, 1, 0]                    │
│  ↑        ↑                         │
│  Row0,3 が条件に一致                │
└────────────────────────────────────┘
```

```go
type Bitmap struct {
    bits   []uint64  // ビット配列（64ビット単位）
    length int
}

func (b *Bitmap) Set(pos int) {
    wordIdx := pos / 64
    bitIdx := pos % 64
    b.bits[wordIdx] |= (1 << bitIdx)
}

// ビットマップの AND 演算で複数条件を高速処理
func (b *Bitmap) And(other *Bitmap) *Bitmap {
    result := &Bitmap{bits: make([]uint64, len(b.bits))}
    for i := 0; i < len(b.bits); i++ {
        result.bits[i] = b.bits[i] & other.bits[i]
    }
    return result
}
```

**使用例:**
```go
// WHERE status = 'active' AND age > 30
activeBitmap := index.Lookup("active")     // [1,1,0,1,0,...]
ageFilterBitmap := ageIndex.Lookup(">30")  // [1,0,1,1,0,...]
result := activeBitmap.And(ageFilterBitmap) // [1,0,0,1,0,...]
```

複数条件の AND/OR を高速なビット演算で処理できます。

#### Zone Map（Min-Max Index）

各行グループの最小値・最大値を記録し、不要なスキャンをスキップします。

**仕組み:**
```
age列（100万行を10万行ずつグループ化）

┌─────────────────────────────────────────────────────┐
│ Zone 0 (Row 0-99,999)                               │
│ ┌─────────────────────────────────────────────────┐ │
│ │ Min: 18,  Max: 25,  RowCount: 100,000          │ │
│ └─────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────┐
│ Zone 1 (Row 100,000-199,999)                        │
│ ┌─────────────────────────────────────────────────┐ │
│ │ Min: 26,  Max: 35,  RowCount: 100,000          │ │
│ └─────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────┐
│ Zone 2 (Row 200,000-299,999)                        │
│ ┌─────────────────────────────────────────────────┐ │
│ │ Min: 36,  Max: 45,  RowCount: 100,000          │ │
│ └─────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
...

クエリ: WHERE age = 30

Zone 0: min=18, max=25  → 30 は範囲外 → ✅ スキップ
Zone 1: min=26, max=35  → 30 は範囲内 → 🔍 スキャン
Zone 2: min=36, max=45  → 30 は範囲外 → ✅ スキップ
...

結果: 9つのZoneをスキップ、1つだけスキャン
      → スキャン削減率: 90%
```

```go
type Zone struct {
    RowGroupID int
    MinValue   types.Value
    MaxValue   types.Value
    RowCount   int
}

func (zm *ZoneMap) CanSkip(zoneID int, op string, value types.Value) bool {
    zone := zm.Zones[zoneID]

    switch op {
    case "=":
        // 値が範囲外なら読み飛ばせる
        return value.Compare(zone.MinValue) < 0 ||
               value.Compare(zone.MaxValue) > 0
    case ">":
        // 全ての値が検索値以下ならスキップ
        return zone.MaxValue.Compare(value) <= 0
    // ...
    }
    return false
}
```


### 4. SQL パーサー

#### 字句解析（Lexer）

SQL文をトークンに分割します。

**処理フロー:**
```
入力SQL: "SELECT age FROM users WHERE age > 30"

字句解析:
┌──────────────────────────────────────────────────────┐
│ "SELECT age FROM users WHERE age > 30"               │
└──────────────────────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────┐
│ トークン列:                                           │
│ [SELECT] [age] [FROM] [users] [WHERE] [age] [>] [30]│
│   ↑      ↑      ↑      ↑       ↑      ↑    ↑    ↑   │
│ Keyword Ident Keyword Ident  Keyword Ident Op  Int  │
└──────────────────────────────────────────────────────┘
```

```go
type Token struct {
    Type    TokenType  // SELECT, FROM, WHERE, INT, STRING など
    Literal string
    Line    int
}

func (l *Lexer) NextToken() Token {
    l.skipWhitespace()

    switch l.ch {
    case '(':
        return Token{Type: TOKEN_LPAREN, Literal: "("}
    case ',':
        return Token{Type: TOKEN_COMMA, Literal: ","}
    default:
        if isLetter(l.ch) {
            literal := l.readIdentifier()
            tokenType := lookupKeyword(literal)  // SELECT, FROM など
            return Token{Type: tokenType, Literal: literal}
        }
    }
}
```

#### 構文解析（Parser）

Pratt Parsing（演算子優先順位解析）を使用して、トークン列を AST に変換します。

**処理フロー:**
```
トークン列:
[SELECT] [age] [FROM] [users] [WHERE] [age] [>] [30]
              │
              ▼
┌─────────────────────────────────────────────────────┐
│              AST (抽象構文木)                        │
│                                                     │
│         SelectStatement                             │
│              │                                      │
│      ┌───────┴────────┐                            │
│      │                │                            │
│   Columns          Where                           │
│      │                │                            │
│   ["age"]      BinaryExpression                    │
│                      │                             │
│                 ┌────┴────┐                        │
│                 │         │                        │
│              Identifier  IntLiteral                │
│                "age"       30                      │
│                           │                        │
│                      Operator: ">"                 │
└─────────────────────────────────────────────────────┘
```

```go
// 演算子の優先順位
const (
    LOWEST = iota
    OR_PREC      // OR
    AND_PREC     // AND
    EQUALS       // = !=
    LESSGREATER  // < > <= >=
    SUM          // + -
    PRODUCT      // * /
)

func (p *Parser) parseExpression(precedence int) ast.Expression {
    left := p.parsePrefixExpression()

    // 優先順位に基づいて中置演算子を処理
    for precedence < p.peekPrecedence() {
        p.nextToken()
        left = p.parseInfixExpression(left)
    }

    return left
}
```

**演算子優先順位の例:**
```
SQL: age > 20 AND name = 'Alice'

演算子優先順位:
  OR     (低)
  AND    ↓
  =, !=  ↓
  <, >   ↓
  +, -   ↓
  *, /   (高)

パース結果のAST:
        AND
       /   \
      >     =
     / \   / \
   age 20 name 'Alice'
```

### 5. クエリ実行エンジン

#### SELECT 文の実行フロー

```go
func (e *Executor) executeSelect(stmt *ast.SelectStatement) (*Result, error) {
    // 1. カラムプルーニング: 必要な列だけを特定
    selectColumns := extractColumns(stmt.Columns)

    // 2. テーブルスキャン + WHERE フィルタ
    filteredRows := [][]types.Value{}
    table.Scan(func(rowIndex uint64, row []types.Value) bool {
        if stmt.Where != nil {
            match := evaluateCondition(stmt.Where, row)
            if !match {
                return true  // スキップ
            }
        }
        filteredRows = append(filteredRows, row)
        return true
    })

    // 3. ORDER BY でソート
    if stmt.OrderBy != nil {
        sort.Slice(filteredRows, ...)
    }

    // 4. LIMIT / OFFSET を適用
    return applyLimitOffset(filteredRows, stmt.Limit, stmt.Offset)
}
```

#### 集約関数の実装

```go
func (e *Executor) executeAggregateSelect(...) (*Result, error) {
    // 集約状態を初期化
    state := &aggregateState{count: 0, sum: 0}

    // テーブルをスキャンして集約
    table.Scan(func(rowIndex uint64, row []types.Value) bool {
        val := row[colIndex]

        state.count++
        state.sum += val.ToFloat64()

        if val.Compare(state.min) < 0 {
            state.min = val
        }
        if val.Compare(state.max) > 0 {
            state.max = val
        }

        return true
    })

    // 結果を計算
    return buildAggregateResult(state)
}
```

## 実際に動かしてみる

### インストール

```bash
git clone https://github.com/taikicoco/tate.git
cd tate
make build
./bin/tate
```

### 使用例

```sql
tate> CREATE TABLE employees (
    id INT64,
    name STRING,
    age INT64,
    department STRING,
    salary FLOAT64
);
Table "employees" created successfully

tate> INSERT INTO employees VALUES (1, 'Alice', 30, 'Engineering', 95000.0);
1 row inserted

tate> INSERT INTO employees VALUES (2, 'Bob', 25, 'Sales', 65000.0);
1 row inserted

tate> SELECT * FROM employees WHERE age > 25;
+----+-------+-----+-------------+--------+
| id | name  | age | department  | salary |
+----+-------+-----+-------------+--------+
| 1  | Alice | 30  | Engineering | 95000  |
+----+-------+-----+-------------+--------+
(1 row in 0.123 ms)

tate> SELECT department, COUNT(*), AVG(salary) FROM employees;
+-------------+----------+-------------+
| department  | COUNT(*) | AVG(salary) |
+-------------+----------+-------------+
| Engineering | 1        | 95000       |
| Sales       | 1        | 65000       |
+-------------+----------+-------------+
(2 rows in 0.089 ms)
```

### ディスク上のファイル構造

```bash
~/.tate/
└── tables/
    └── employees/
        ├── _meta.json          # テーブルメタデータ
        ├── col_id.dat          # id列
        ├── col_name.dat        # name列
        ├── col_age.dat         # age列
        ├── col_department.dat  # department列
        └── col_salary.dat      # salary列
```

各カラムが独立したファイルとして保存されています。

## パフォーマンス特性

### 列指向が速いケース ✅

```sql
-- 集約クエリ（必要な列だけ読める）
SELECT AVG(age), MAX(salary) FROM employees WHERE department = 'Engineering';

-- 高い圧縮率による I/O 削減
-- Zone Map によるスキャンスキップ
```

### 列指向が遅いケース ⚠️

```sql
-- 単一行の完全な取得（すべての列ファイルを読む必要がある）
SELECT * FROM employees WHERE id = 1;

-- 頻繁な INSERT/UPDATE（各列ファイルに書き込みが必要）
INSERT INTO employees VALUES (...);
```

## 学んだこと・今後の改善点

### 学んだこと

1. **列指向の本質**
   データを列単位で保存するという単純なアイデアが、なぜ分析クエリで圧倒的に速いのかを実装を通じて深く理解できました

2. **圧縮の重要性**
   同じ型のデータが連続するため、RLE や辞書エンコーディングが非常に効果的に働きます

3. **インデックスの違い**
   行指向の B-Tree ではなく、Bitmap や Zone Map が列指向に適している理由を実感しました

4. **SQLパーサーの実装**
   Pratt Parsing による演算子優先順位の処理は、再帰下降パーサーよりもエレガントで実装しやすかったです

### 今後の改善点

1. **ベクトル化実行エンジン**
   現在は1行ずつ処理していますが、バッチ処理で SIMD を活用したい

2. **並列クエリ実行**
   ゴルーチンを使った列ごとの並列スキャン

3. **トランザクションサポート**
   MVCC を使った並行制御

4. **より高度な圧縮**
   Snappy、LZ4 などの汎用圧縮、Bit packing による整数圧縮

5. **クエリオプティマイザー**
   コストベースの実行計画選択、Zone Map を活用した述語プッシュダウン

## まとめ

列指向データベースは、**データを列単位で格納する**という単純なアイデアから、以下のような大きなメリットを生み出します：

- ✅ 必要な列だけ読み込める（カラムプルーニング）
- ✅ 高い圧縮率（同じ型のデータが連続）
- ✅ Zone Map によるスキャンスキップ
- ✅ Bitmap Index による高速フィルタリング

一方で、単一行アクセスや頻繁な更新には向いていません。このトレードオフを理解して、**OLTP には行指向**、**OLAP には列指向**を選択することが重要です。

Go 言語での実装を通じて、列指向データベースの仕組みを深く理解できました。実装は約2000行程度とコンパクトで、学習用途に最適です。

興味がある方は、ぜひコードを読んでみてください！

https://github.com/taikicoco/tate

## 参考資料

- [The Design and Implementation of Modern Column-Oriented Database Systems](https://www.cs.umd.edu/~abadi/papers/abadi-column-stores.pdf) - 列指向DBの学術論文（PDF）
- [Apache Parquet Documentation](https://parquet.apache.org/docs/) - 列指向ファイルフォーマット
- [DuckDB](https://duckdb.org/) - 組み込み型 OLAP データベース
- [ClickHouse](https://clickhouse.com/) - 高性能列指向データベース
- [Writing a SQL database from scratch in Go](https://notes.eatonphil.com/database-basics.html) - Go でのDB実装チュートリアル
