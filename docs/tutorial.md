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
    active BOOL
);
```

サポートされるデータ型:
- `INT64` - 64ビット整数
- `FLOAT64` - 64ビット浮動小数点
- `STRING` - 可変長文字列
- `BOOL` - 真偽値

### データ挿入

```sql
-- 全カラム指定
INSERT INTO users VALUES (1, 'Alice', TRUE);

-- カラム指定
INSERT INTO users (id, name) VALUES (2, 'Bob');
```

### データ検索

```sql
-- 全件取得
SELECT * FROM users;

-- カラム指定
SELECT name FROM users;
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
