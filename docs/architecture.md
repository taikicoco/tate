# Tate アーキテクチャ

Tate は最小構成の3層アーキテクチャで実装されたカラムナー（列指向）データベースです。

## ディレクトリ構成

```
tate/
├── cmd/tate/main.go          # エントリーポイント + REPL
└── internal/
    ├── parser/               # 第1層: SQL解析
    │   ├── lexer.go         # 字句解析
    │   ├── ast.go           # AST定義
    │   └── parser.go        # 構文解析
    ├── executor/             # 第2層: クエリ実行
    │   └── executor.go      # 実行エンジン
    └── storage/              # 第3層: ストレージ
        ├── types.go         # データ型
        ├── catalog.go       # メタデータ管理
        └── table.go         # テーブル・カラム
```

## 3層アーキテクチャ

```
SQL文字列
    │
    ▼
┌──────────────┐
│   Parser     │  SQL → AST
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Executor   │  AST → 結果
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Storage    │  データ読み書き
└──────────────┘
```

## 各層の責務

### Parser（構文解析）

SQL文字列を抽象構文木（AST）に変換する。

- **Lexer**: 文字列をトークン列に分割
- **Parser**: トークン列をASTに変換（Pratt Parser）

```
"SELECT * FROM users WHERE age > 20"
                ↓
SelectStatement {
    Columns: [Wildcard]
    TableName: "users"
    Where: BinaryExpr{age > 20}
}
```

### Executor（実行エンジン）

ASTを解釈してStorageに対する操作を実行する。

- CREATE TABLE / DROP TABLE
- INSERT INTO
- SELECT（WHERE, ORDER BY, LIMIT, 集計関数）

### Storage（ストレージ）

データの永続化を担当する。

- **列指向ストレージ**: 各カラムを別ファイルに保存
- **NULLビットマップ**: NULL値を効率的に管理
- **カタログ**: テーブルスキーマのメタデータ管理

## 依存関係

```
parser   → 依存なし
storage  → 依存なし
executor → parser, storage
```

循環依存なし。各層が独立してテスト可能。

## ファイルフォーマット

### カラムファイル (.dat)

```
+----------------+
| Magic (4B)     |  "TCOL"
| Version (2B)   |
| DataType (1B)  |
| RowCount (8B)  |
| NullMaskSize   |
| NullMask       |  ビットマップ
| DataSize       |
| Data           |  実データ
+----------------+
```

### カタログ (catalog.json)

```json
{
  "tables": {
    "users": {
      "name": "users",
      "columns": [
        {"name": "id", "type": 2, "nullable": true},
        {"name": "name", "type": 4, "nullable": true}
      ]
    }
  }
}
```
