# Tate チュートリアル

## ビルドと起動

```bash
# ビルド
go build -o bin/tate ./cmd/tate

# 起動
./bin/tate

# データディレクトリを指定して起動
./bin/tate -data /path/to/data
```

## 基本操作

### テーブル作成

```sql
CREATE TABLE users (
    id INT64,
    name STRING,
    age INT64,
    active BOOL
);
```

サポートされるデータ型:
- `INT64` - 64ビット整数
- `FLOAT64` - 64ビット浮動小数点
- `STRING` - 可変長文字列
- `BOOL` - 真偽値
- `TIMESTAMP` - 日時

### データ挿入

```sql
-- 全カラム指定
INSERT INTO users VALUES (1, 'Alice', 30, TRUE);

-- カラム指定
INSERT INTO users (id, name) VALUES (2, 'Bob');
```

### データ検索

```sql
-- 全件取得
SELECT * FROM users;

-- カラム指定
SELECT name, age FROM users;

-- 条件指定
SELECT * FROM users WHERE age > 25;

-- 複合条件
SELECT * FROM users WHERE age > 25 AND active = TRUE;

-- ソート
SELECT * FROM users ORDER BY age DESC;

-- 件数制限
SELECT * FROM users LIMIT 10;

-- 重複排除
SELECT DISTINCT name FROM users;
```

### 集計関数

```sql
SELECT COUNT(*) FROM users;
SELECT SUM(age) FROM users;
SELECT AVG(age) FROM users;
SELECT MIN(age), MAX(age) FROM users;
```

### テーブル削除

```sql
DROP TABLE users;
```

## コマンド

| コマンド | 説明 |
|---------|------|
| `help` | ヘルプ表示 |
| `tables` | テーブル一覧 |
| `describe <table>` | スキーマ表示 |
| `exit` | 終了 |

## 演算子

### 比較演算子
`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`

### 論理演算子
`AND`, `OR`, `NOT`

### 算術演算子
`+`, `-`, `*`, `/`
