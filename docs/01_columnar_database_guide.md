# 列指向データベース（Columnar Database）完全ガイド

> Go言語で列指向DBを自作するための基礎知識

---

## 目次

1. [はじめに：なぜ列指向DBなのか](#1-はじめに)
2. [行指向 vs 列指向：根本的な違い](#2-行指向-vs-列指向)
3. [列指向DBのアーキテクチャ](#3-アーキテクチャ)
4. [ストレージ設計](#4-ストレージ設計)
5. [圧縮技術](#5-圧縮技術)
6. [インデックス構造](#6-インデックス構造)
7. [クエリ実行エンジン](#7-クエリ実行エンジン)
8. [トランザクション処理](#8-トランザクション処理)
9. [実装における主要コンポーネント](#9-主要コンポーネント)
10. [参考文献・リソース](#10-参考文献)

---

## 1. はじめに

### 1.1 列指向DBとは

列指向データベース（Column-Oriented Database / Columnar Database）は、データを**列（カラム）単位**で格納するデータベースです。

従来のRDBMS（MySQL、PostgreSQL等）が採用する**行指向**とは対照的なアプローチであり、**OLAP（Online Analytical Processing）** ワークロードに最適化されています。

### 1.2 代表的な列指向DB

| データベース | 特徴 |
|------------|------|
| **ClickHouse** | 高速な分析クエリ、リアルタイム処理 |
| **Apache Parquet** | 列指向ファイルフォーマット |
| **Apache Arrow** | インメモリ列指向データ形式 |
| **Amazon Redshift** | クラウドデータウェアハウス |
| **Google BigQuery** | サーバーレス分析 |
| **Vertica** | 大規模分析用途 |
| **DuckDB** | 組み込み型OLAP |

### 1.3 ユースケース

- **データウェアハウス**: 大量の履歴データを分析
- **ビジネスインテリジェンス（BI）**: レポート・ダッシュボード
- **ログ分析**: 大量のログデータの集計
- **時系列データ分析**: IoTセンサーデータ、メトリクス
- **機械学習の特徴量計算**: 大規模データセットからの特徴抽出

---

## 2. 行指向 vs 列指向

### 2.1 データ格納方式の違い

以下のテーブルを例に説明します：

```
users テーブル
+----+--------+-----+-----------+
| id | name   | age | city      |
+----+--------+-----+-----------+
| 1  | 田中   | 30  | 東京      |
| 2  | 佐藤   | 25  | 大阪      |
| 3  | 鈴木   | 35  | 名古屋    |
+----+--------+-----+-----------+
```

#### 行指向ストレージ（Row-Oriented）

```
ディスク上のレイアウト:
[1, 田中, 30, 東京] → [2, 佐藤, 25, 大阪] → [3, 鈴木, 35, 名古屋]
```

- 1行のデータが連続して格納される
- **INSERT/UPDATE/DELETE** が高速
- 特定の行を取得するのが効率的
- **OLTP（トランザクション処理）** に最適

#### 列指向ストレージ（Column-Oriented）

```
ディスク上のレイアウト:
id列:   [1, 2, 3]
name列: [田中, 佐藤, 鈴木]
age列:  [30, 25, 35]
city列: [東京, 大阪, 名古屋]
```

- 同じ列のデータが連続して格納される
- **集計クエリ（SUM, AVG, COUNT）** が高速
- 必要な列だけ読み込める（**カラムプルーニング**）
- **OLAP（分析処理）** に最適

### 2.2 クエリパフォーマンスの比較

#### クエリ例：「全ユーザーの平均年齢を求める」

```sql
SELECT AVG(age) FROM users;
```

**行指向の場合:**
```
読み込みデータ: [1, 田中, 30, 東京], [2, 佐藤, 25, 大阪], [3, 鈴木, 35, 名古屋]
→ 全カラムを読み込む必要がある（無駄なI/O）
```

**列指向の場合:**
```
読み込みデータ: [30, 25, 35]
→ age列のみ読み込み（効率的）
```

### 2.3 メリット・デメリット比較

| 観点 | 行指向 | 列指向 |
|------|--------|--------|
| **書き込み性能** | ◎ 高速 | △ 複数ファイルに分散 |
| **単一行の読み取り** | ◎ 1回のI/O | △ 複数列を結合 |
| **集計クエリ** | △ 全列読み込み | ◎ 必要な列のみ |
| **圧縮効率** | △ 異種データ混在 | ◎ 同種データで高圧縮 |
| **スキャン性能** | △ | ◎ |
| **主な用途** | OLTP | OLAP |

---

## 3. アーキテクチャ

### 3.1 全体構成図

```
┌─────────────────────────────────────────────────────────────┐
│                      クライアント                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    フロントエンド層                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Parser    │→ │  Analyzer   │→ │  Planner    │         │
│  │ (構文解析)   │  │ (意味解析)   │  │ (実行計画)  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    実行エンジン層                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Executor   │  │ Vectorized  │  │  Aggregator │         │
│  │ (実行器)    │  │  Engine     │  │ (集約処理)  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    ストレージ層                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Column    │  │   Index     │  │  Metadata   │         │
│  │   Store     │  │   Manager   │  │  Catalog    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      物理ストレージ                          │
│         ┌────────────────────────────────────┐              │
│         │   Column Files (.col)              │              │
│         │   ├── column_id.col                │              │
│         │   ├── column_name.col              │              │
│         │   ├── column_age.col               │              │
│         │   └── column_city.col              │              │
│         └────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 主要コンポーネント

#### 3.2.1 フロントエンド層

| コンポーネント | 役割 |
|--------------|------|
| **Lexer（字句解析器）** | SQL文字列をトークンに分割 |
| **Parser（構文解析器）** | トークン列をAST（抽象構文木）に変換 |
| **Analyzer（意味解析器）** | テーブル/カラムの存在確認、型チェック |
| **Planner（実行計画立案）** | 最適な実行計画を生成 |
| **Optimizer（最適化器）** | コストベースで実行計画を最適化 |

#### 3.2.2 実行エンジン層

| コンポーネント | 役割 |
|--------------|------|
| **Executor（実行器）** | 実行計画に従ってクエリを実行 |
| **Vectorized Engine** | ベクトル化処理で高速化 |
| **Aggregator** | GROUP BY、SUM、AVG等の集計処理 |

#### 3.2.3 ストレージ層

| コンポーネント | 役割 |
|--------------|------|
| **Column Store** | 列データの読み書き |
| **Index Manager** | インデックスの管理 |
| **Metadata Catalog** | テーブル定義、スキーマ情報の管理 |
| **Buffer Manager** | メモリバッファの管理 |

---

## 4. ストレージ設計

### 4.1 カラムファイル構造

各カラムは独立したファイルとして格納されます：

```
table_users/
├── _metadata.json       # テーブルメタデータ
├── column_id.col        # id列データ
├── column_name.col      # name列データ
├── column_age.col       # age列データ
└── column_city.col      # city列データ
```

### 4.2 カラムファイルフォーマット

```
┌──────────────────────────────────────────┐
│           Column File Format             │
├──────────────────────────────────────────┤
│  Header (固定長)                          │
│  ├── Magic Number (4 bytes): "TCOL"      │
│  ├── Version (2 bytes)                   │
│  ├── Data Type (2 bytes)                 │
│  ├── Compression Type (2 bytes)          │
│  ├── Row Count (8 bytes)                 │
│  ├── Null Bitmap Offset (8 bytes)        │
│  └── Data Offset (8 bytes)               │
├──────────────────────────────────────────┤
│  Null Bitmap                             │
│  └── ビットマップで NULL 位置を管理         │
├──────────────────────────────────────────┤
│  Data Section                            │
│  └── 圧縮された列データ                    │
├──────────────────────────────────────────┤
│  Footer                                  │
│  ├── Min Value                           │
│  ├── Max Value                           │
│  ├── Checksum                            │
│  └── Statistics                          │
└──────────────────────────────────────────┘
```

### 4.3 Row Group（行グループ）

大量のデータを効率的に処理するため、行をグループ化します：

```
┌─────────────────────────────────────────────────────┐
│                    Table                            │
├─────────────────────────────────────────────────────┤
│  Row Group 0 (rows 0 - 65535)                       │
│  ├── Column Chunk: id                               │
│  ├── Column Chunk: name                             │
│  ├── Column Chunk: age                              │
│  └── Column Chunk: city                             │
├─────────────────────────────────────────────────────┤
│  Row Group 1 (rows 65536 - 131071)                  │
│  ├── Column Chunk: id                               │
│  ├── Column Chunk: name                             │
│  ├── Column Chunk: age                              │
│  └── Column Chunk: city                             │
├─────────────────────────────────────────────────────┤
│  ...                                                │
└─────────────────────────────────────────────────────┘
```

**Row Groupのメリット:**
- 並列処理が容易
- メモリ効率が良い
- 行グループ単位でスキップ可能

### 4.4 Virtual ID（仮想ID）

列指向DBでは、各値に物理的なIDを付与せず、**位置（オフセット）** で行を特定します：

```
Column: age
Index:   0    1    2    3    4
Value: [30] [25] [35] [28] [32]

# インデックス2の行 = 全カラムの位置2のデータ
row[2] = {
  id:   column_id[2],
  name: column_name[2],
  age:  column_age[2],   // = 35
  city: column_city[2]
}
```

---

## 5. 圧縮技術

### 5.1 なぜ列指向は圧縮に強いのか

同じカラムのデータは同じ型・似た値を持つため、非常に高い圧縮率を実現できます。

```
行指向のデータ:
[1, "田中", 30, "東京", 2, "佐藤", 25, "大阪", ...]
→ 異種データが混在、圧縮しにくい

列指向のデータ:
age列: [30, 25, 35, 28, 32, 31, 29, 27, ...]
→ 全て整数、パターンあり、高圧縮可能
```

### 5.2 主要な圧縮・エンコーディング手法

#### 5.2.1 Run-Length Encoding (RLE)

連続する同じ値を「値 × 回数」で表現：

```
元データ: [A, A, A, A, B, B, C, C, C, C, C]
RLE後:    [(A, 4), (B, 2), (C, 5)]

# ソート済みの列で特に効果的
status列: [active, active, active, ...(10000回), inactive, inactive, ...]
RLE後:    [(active, 10000), (inactive, 5000), ...]
```

**Go実装イメージ:**
```go
type RLEValue struct {
    Value interface{}
    Count int
}

func RLEEncode(data []interface{}) []RLEValue {
    if len(data) == 0 {
        return nil
    }

    var result []RLEValue
    current := RLEValue{Value: data[0], Count: 1}

    for i := 1; i < len(data); i++ {
        if data[i] == current.Value {
            current.Count++
        } else {
            result = append(result, current)
            current = RLEValue{Value: data[i], Count: 1}
        }
    }
    result = append(result, current)
    return result
}
```

#### 5.2.2 Dictionary Encoding（辞書エンコーディング）

頻出する値を短いコードに置換：

```
元データ: ["東京", "大阪", "東京", "名古屋", "東京", "大阪"]

辞書:
  0 → "東京"
  1 → "大阪"
  2 → "名古屋"

エンコード後: [0, 1, 0, 2, 0, 1]
```

**Go実装イメージ:**
```go
type DictionaryEncoder struct {
    dict    map[string]int
    reverse []string
}

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
```

#### 5.2.3 Delta Encoding（差分エンコーディング）

連続する値の差分を格納（時系列データに効果的）：

```
タイムスタンプ列:
元データ: [1000, 1001, 1002, 1005, 1006, 1010]
基準値:   1000
差分:     [0, 1, 2, 5, 6, 10]

# さらにビット幅を削減可能
```

**Go実装イメージ:**
```go
func DeltaEncode(data []int64) (base int64, deltas []int64) {
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
```

#### 5.2.4 Bit Packing（ビットパッキング）

値の範囲に応じて必要最小限のビット数で格納：

```
age列の値: [25, 30, 28, 35, 22]  # 範囲: 0-127

通常: 各値 64bit = 320 bits
Bit Packing: 各値 7bit = 35 bits (約91%削減)
```

### 5.3 圧縮アルゴリズム

エンコーディング後、さらに汎用圧縮を適用：

| アルゴリズム | 特徴 |
|------------|------|
| **Snappy** | 高速、中程度の圧縮率 |
| **LZ4** | 非常に高速、低圧縮率 |
| **Zstd** | 高圧縮率、やや低速 |
| **Gzip** | 高圧縮率、低速 |

---

## 6. インデックス構造

### 6.1 列指向DBでのインデックス

列指向DBでは、行指向DBとは異なるインデックス戦略を採用します。

### 6.2 Bitmap Index（ビットマップインデックス）

低カーディナリティ（値の種類が少ない）カラムに最適：

```
status列: [active, active, inactive, active, inactive]

ビットマップ:
  active:   [1, 1, 0, 1, 0]
  inactive: [0, 0, 1, 0, 1]

# クエリ: WHERE status = 'active' AND city = 'Tokyo'
# → ビットマップ同士のAND演算で高速フィルタリング
```

**Go実装イメージ:**
```go
type BitmapIndex struct {
    bitmaps map[interface{}]*roaring.Bitmap // roaringビットマップ使用
}

func (idx *BitmapIndex) Add(value interface{}, rowID uint32) {
    if idx.bitmaps[value] == nil {
        idx.bitmaps[value] = roaring.New()
    }
    idx.bitmaps[value].Add(rowID)
}

func (idx *BitmapIndex) Query(value interface{}) *roaring.Bitmap {
    return idx.bitmaps[value]
}
```

### 6.3 Min-Max Index（Zone Map）

各Row Groupの最小値・最大値を記録：

```
Row Group 0: age列 min=20, max=35
Row Group 1: age列 min=36, max=50
Row Group 2: age列 min=18, max=30

クエリ: WHERE age = 45
→ Row Group 1のみスキャン（0と2はスキップ）
```

### 6.4 Bloom Filter

値の存在を高速に判定（偽陽性あり、偽陰性なし）：

```
# Bloom Filterに "Tokyo" を追加
hash1("Tokyo") → ビット位置 5 をセット
hash2("Tokyo") → ビット位置 12 をセット
hash3("Tokyo") → ビット位置 7 をセット

# "Tokyo" の存在確認
→ ビット位置 5, 12, 7 が全てセットされていれば「存在する可能性あり」
```

### 6.5 B-Tree / B+Tree

範囲検索が必要な場合に使用：

```
        [30]
       /    \
    [20]    [40, 50]
   /  \     /  |  \
[10,15] [25] [35] [45] [55,60]
```

**B+Treeの特徴:**
- リーフノードのみがデータへのポインタを持つ
- リーフノード同士がリンクされている（範囲スキャンに有利）
- 列指向DBではプライマリインデックスとして使用されることがある

---

## 7. クエリ実行エンジン

### 7.1 ベクトル化実行（Vectorized Execution）

一度に複数の値を処理してCPUキャッシュ効率を最大化：

```
# 従来の1行ずつ処理（Volcano Model）
for each row:
    process(row)

# ベクトル化処理
for each batch (1024行):
    process_batch(batch)  # SIMD命令活用可能
```

**Go実装イメージ:**
```go
const BatchSize = 1024

type Vector struct {
    Data     []int64
    NullMask []bool
    Length   int
}

// ベクトル化された加算
func VectorAdd(a, b *Vector) *Vector {
    result := &Vector{
        Data:   make([]int64, a.Length),
        Length: a.Length,
    }

    for i := 0; i < a.Length; i++ {
        result.Data[i] = a.Data[i] + b.Data[i]
    }
    return result
}
```

### 7.2 Late Materialization（遅延実体化）

必要になるまで行の再構築を遅延：

```sql
SELECT name FROM users WHERE age > 30;

# Early Materialization（従来）
1. 全行を読み込み
2. age > 30 でフィルタ
3. name を抽出

# Late Materialization
1. age列のみ読み込み
2. age > 30 の位置リスト取得: [0, 2, 5, ...]
3. 位置リストを使って name列から該当値のみ読み込み
```

### 7.3 カラムプルーニング（Column Pruning）

クエリに必要な列のみを読み込み：

```sql
SELECT name, city FROM users WHERE age > 30;

# 読み込む列: name, city, age
# 読み込まない列: id, その他全て
```

### 7.4 述語プッシュダウン（Predicate Pushdown）

フィルタ条件をストレージ層に押し下げ：

```sql
SELECT * FROM users WHERE age > 30;

# フィルタをストレージ層で実行
# → 条件を満たす行のみがエグゼキュータに渡される
```

---

## 8. トランザクション処理

### 8.1 列指向DBとトランザクション

多くの列指向DBは **OLAP** に特化しているため、完全なACID準拠ではないことが多いです。

しかし、学習目的で基本的なトランザクション機能を実装することは有益です。

### 8.2 シンプルなトランザクション実装

```go
type Transaction struct {
    ID        uint64
    Status    TxStatus
    WriteSet  map[string][]byte  // 変更データ
    ReadSet   map[string]uint64  // 読み取ったバージョン
    StartTime time.Time
}

type TxStatus int

const (
    TxActive TxStatus = iota
    TxCommitted
    TxAborted
)
```

### 8.3 MVCC（Multi-Version Concurrency Control）

読み取りと書き込みが互いにブロックしない並行制御：

```
バージョン管理:
Row 0: [v1: age=30, v2: age=31, v3: age=32]

Tx1 (開始時刻: T1): v1 を読む (age=30)
Tx2 (開始時刻: T2): v2 を書く (age=31)
Tx1: まだ age=30 を見る（スナップショット分離）
```

---

## 9. 主要コンポーネント

### 9.1 実装すべきコンポーネント一覧

```
tate/
├── cmd/
│   └── tate/
│       └── main.go           # エントリポイント
├── internal/
│   ├── parser/               # SQL パーサー
│   │   ├── lexer.go         # 字句解析
│   │   ├── parser.go        # 構文解析
│   │   └── ast.go           # AST定義
│   ├── planner/             # クエリプランナー
│   │   ├── planner.go       # 実行計画生成
│   │   └── optimizer.go     # 最適化
│   ├── executor/            # 実行エンジン
│   │   ├── executor.go      # 実行器
│   │   └── aggregate.go     # 集計処理
│   ├── storage/             # ストレージエンジン
│   │   ├── column.go        # カラムストア
│   │   ├── page.go          # ページ管理
│   │   └── buffer.go        # バッファ管理
│   ├── encoding/            # エンコーディング
│   │   ├── rle.go           # RLE
│   │   ├── dictionary.go    # 辞書エンコーディング
│   │   └── delta.go         # 差分エンコーディング
│   ├── compression/         # 圧縮
│   │   └── compress.go      # 圧縮/解凍
│   ├── index/               # インデックス
│   │   ├── bitmap.go        # ビットマップインデックス
│   │   └── zonemap.go       # Zone Map
│   ├── catalog/             # メタデータ管理
│   │   └── catalog.go       # カタログ
│   └── types/               # データ型
│       └── types.go         # 型定義
├── pkg/
│   └── protocol/            # クライアントプロトコル
└── test/                    # テスト
```

### 9.2 データ型

```go
type DataType int

const (
    TypeInt64 DataType = iota
    TypeFloat64
    TypeString
    TypeBool
    TypeTimestamp
    TypeNull
)

type ColumnValue struct {
    Type    DataType
    IntVal  int64
    FloatVal float64
    StrVal  string
    BoolVal bool
    IsNull  bool
}
```

### 9.3 カラム構造

```go
type Column struct {
    Name       string
    Type       DataType
    Nullable   bool
    Compressed bool

    // データ
    Data       []byte      // 圧縮/エンコード済みデータ
    NullBitmap []byte      // NULLビットマップ

    // 統計情報
    RowCount   int64
    MinValue   ColumnValue
    MaxValue   ColumnValue
    DistinctCount int64
}
```

---

## 10. 参考文献

### 書籍
- **Database Design and Implementation** (Edward Sciore) - 自作RDBMS の定番書籍

### 日本語リソース
- [自作RDBMSやろうぜ！](https://ryogrid.github.io/dbms-jisaku/) - 日本語での自作RDBMS実装ガイド
- [Goで自作RDBMS](https://blog.abekoh.dev/posts/simple-db) - Go言語での実装例
- [列指向データベース管理システム - Wikipedia](https://ja.wikipedia.org/wiki/列指向データベース管理システム)
- [カラム型データベースとは](https://www.publickey1.jp/blog/11/post_175.html) - 仕組みの解説

### 英語リソース
- [Writing a SQL database from scratch in Go](https://notes.eatonphil.com/database-basics.html) - Go言語でのDB実装チュートリアル
- [kelindar/column](https://github.com/kelindar/column) - Go言語の列指向ストレージライブラリ
- [FrostDB](https://github.com/polarsignals/frostdb) - Go言語の組み込み列指向DB
- [The Design and Implementation of Modern Column-Oriented Database Systems](https://www.nowpublishers.com/article/DownloadSummary/DBS-024) - 学術論文

### 関連プロジェクト
- Apache Arrow - インメモリ列指向フォーマット
- Apache Parquet - 列指向ファイルフォーマット
- ClickHouse - 高性能列指向DB
- DuckDB - 組み込みOLAP DB

---

## 次のステップ

この資料を読んだ後は、以下の順序で進めることをお勧めします：

1. **実装計画書を読む** - 詳細な実装ステップ
2. **Phase 1から開始** - 基本的なカラムストア実装
3. **段階的に機能追加** - パーサー、エグゼキュータ、インデックスなど

Happy Coding! 🚀
