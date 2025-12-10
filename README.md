# Tate - Go 列指向データベース

> **tate** (縦) = 列

学習目的でGoで実装した列指向データベースです。

## 概要

このプロジェクトは列指向データベースの仕組みを学ぶためのものです。

### 主な機能

- **列指向ストレージ**: 列単位でデータを格納
- **圧縮**: RLEエンコーディング、辞書エンコーディング、デルタエンコーディング
- **インデックス**: ビットマップインデックス、Zone Map
- **SQLサポート**: SELECT, INSERT, CREATE TABLE, DROP TABLE

## クイックスタート

```bash
# ビルド
make build

# 実行
make run

# または直接実行
./bin/tate

# カスタムデータディレクトリを指定
./bin/tate -data /path/to/data
```

## サポートするSQL

```sql
-- テーブル作成
CREATE TABLE users (
    id INT64,
    name STRING,
    age INT64
);

-- データ挿入
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO users (name, age) VALUES ('Charlie', 35);

-- 基本クエリ
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25;

-- 集約関数
SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users;

-- ソートとリミット
SELECT * FROM users ORDER BY age DESC LIMIT 10;

-- DISTINCT
SELECT DISTINCT name FROM users;

-- テーブル削除
DROP TABLE users;
```

## サポートするデータ型

| 型 | 説明 |
|------|------|
| INT64 | 64ビット整数 |
| FLOAT64 | 64ビット浮動小数点数 |
| STRING | 可変長文字列 |
| BOOL | ブール値 (TRUE/FALSE) |
| TIMESTAMP | 日時 |

## REPLコマンド

| コマンド | 説明 |
|---------|------|
| help, \h | ヘルプを表示 |
| exit, \q | 終了 |
| tables, \dt | テーブル一覧 |
| describe <table>, \d <table> | テーブルスキーマを表示 |
| version, \v | バージョン情報 |
| clear, \c | 画面クリア |

## プロジェクト構造

```
tate/
├── cmd/tate/          # メインエントリポイント（REPL）
├── internal/
│   ├── ast/           # 抽象構文木
│   ├── lexer/         # 字句解析器
│   ├── parser/        # 構文解析器
│   ├── executor/      # クエリ実行エンジン
│   ├── storage/       # ストレージエンジン
│   ├── encoding/      # エンコーディング（RLE、辞書、デルタ）
│   ├── index/         # インデックス
│   ├── catalog/       # メタデータ管理
│   └── types/         # 型定義
└── docs/              # ドキュメント
```

## ドキュメント

- [列指向データベース ガイド](docs/01_columnar_database_guide.md) - 列指向DBの基礎知識
- [実装計画](docs/02_implementation_plan.md) - 詳細な実装ステップ

## 開発

```bash
# テスト実行
make test

# ベンチマーク
make bench

# フォーマット
make fmt

# 静的解析
make lint
```

## 実装フェーズ

| Phase | 内容 | 状態 |
|-------|------|------|
| Phase 1 | カタログ（メタデータ管理） | 完了 |
| Phase 2 | ストレージエンジン | 完了 |
| Phase 3 | SQLパーサー | 完了 |
| Phase 4 | クエリ実行エンジン | 完了 |
| Phase 5 | インデックス | 完了 |
| Phase 6 | 圧縮・エンコーディング | 完了 |
| Phase 7 | 統合とREPL完成 | 完了 |

## 列指向DBの特徴

### 行指向 vs 列指向

**行指向（OLTP向け）**
```
Row 1: [id=1, name="Alice", age=30]
Row 2: [id=2, name="Bob", age=25]
```

**列指向（OLAP向け）**
```
id列:   [1, 2, 3, ...]
name列: ["Alice", "Bob", "Charlie", ...]
age列:  [30, 25, 35, ...]
```

### メリット

1. **高い圧縮率**: 同じ型のデータが連続するため圧縮効率が良い
2. **集計クエリの高速化**: 必要な列だけを読み込める
3. **SIMD最適化**: 列データへのベクトル演算が可能

### デメリット

1. **単一行アクセスが遅い**: 複数の列ファイルを読む必要がある
2. **書き込みオーバーヘッド**: 各列ファイルに分散して書く必要がある

