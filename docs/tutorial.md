# Tate 列指向データベース チュートリアル

Tate を使った実践的な操作手順を、初心者でも理解できるように段階的に解説します。

---

## 目次

1. [セットアップ](#1-セットアップ)
2. [基本操作](#2-基本操作)
3. [実践例: Eコマースデータ分析](#3-実践例-eコマースデータ分析)
4. [実践例: ログ分析](#4-実践例-ログ分析)
5. [パフォーマンステスト](#5-パフォーマンステスト)
6. [トラブルシューティング](#6-トラブルシューティング)

---

## 1. セットアップ

### 1.1 ビルドと起動

```bash
# リポジトリのクローン
git clone https://github.com/taikicoco/tate.git
cd tate

# ビルド
make build

# 実行
./bin/tate
```

起動すると、以下のようなバナーが表示されます:

```
  _____      _
 |_   _|__ _| |_ ___
   | |/ _' | __/ _ \
   | | (_| | ||  __/
   |_|\__,_|\__\___|

Tate Columnar Database v0.1.0
A learning project for understanding column-oriented databases.

Data directory: /Users/your-name/.tate
Type 'help' for available commands, 'exit' to quit.

tate>
```

### 1.2 ヘルプの確認

```sql
tate> help
```

または

```sql
tate> \h
```

利用可能なコマンド一覧が表示されます:

```
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
  SELECT col1, col2 FROM table_name [WHERE condition]
  DROP TABLE table_name

Supported Data Types:
  INT64      - 64-bit integer
  FLOAT64    - 64-bit floating point
  STRING     - Variable-length string
  BOOL       - Boolean (TRUE/FALSE)
  TIMESTAMP  - Date and time
```

---

## 2. 基本操作

### 2.1 テーブルの作成

#### 例1: シンプルなユーザーテーブル

```sql
tate> CREATE TABLE users (
    id INT64,
    name STRING,
    age INT64,
    email STRING
);
```

**出力:**
```
Table "users" created successfully
(0 rows in 0.023 ms)
```

#### 例2: より複雑な商品テーブル

```sql
tate> CREATE TABLE products (
    product_id INT64,
    name STRING,
    price FLOAT64,
    in_stock BOOL,
    category STRING,
    created_at TIMESTAMP
);
```

### 2.2 テーブル一覧の確認

```sql
tate> tables
```

または

```sql
tate> \dt
```

**出力:**
```
Tables:
--------
  users (4 columns)
  products (6 columns)
```

### 2.3 テーブルスキーマの確認

```sql
tate> describe users
```

または

```sql
tate> \d users
```

**出力:**
```
Table: users
--------------------------------------------------
Column               Type            Properties
--------------------------------------------------
id                   INT64
name                 STRING
age                  INT64
email                STRING
```

### 2.4 データの挿入

#### 基本的な INSERT

```sql
tate> INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com');
```

**出力:**
```
1 row inserted
(1 rows in 0.012 ms)
```

#### 複数行の挿入

```sql
tate> INSERT INTO users VALUES (2, 'Bob', 25, 'bob@example.com');
tate> INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com');
tate> INSERT INTO users VALUES (4, 'Diana', 28, 'diana@example.com');
tate> INSERT INTO users VALUES (5, 'Eve', 32, 'eve@example.com');
```

#### カラムを指定した挿入

```sql
tate> INSERT INTO users (id, name, age) VALUES (6, 'Frank', 29);
```

この場合、`email` カラムには NULL が入ります。

### 2.5 データの取得

#### 全データの取得

```sql
tate> SELECT * FROM users;
```

**出力:**
```
+----+---------+-----+---------------------+
| id | name    | age | email               |
+----+---------+-----+---------------------+
| 1  | Alice   | 30  | alice@example.com   |
| 2  | Bob     | 25  | bob@example.com     |
| 3  | Charlie | 35  | charlie@example.com |
| 4  | Diana   | 28  | diana@example.com   |
| 5  | Eve     | 32  | eve@example.com     |
| 6  | Frank   | 29  | NULL                |
+----+---------+-----+---------------------+
(6 rows in 0.145 ms)
```

#### 特定のカラムのみ取得

```sql
tate> SELECT name, age FROM users;
```

**出力:**
```
+---------+-----+
| name    | age |
+---------+-----+
| Alice   | 30  |
| Bob     | 25  |
| Charlie | 35  |
| Diana   | 28  |
| Eve     | 32  |
| Frank   | 29  |
+---------+-----+
(6 rows in 0.089 ms)
```

**メモ**: 列指向DBなので、`name` と `age` の2列だけを読み込みます。他の列（`id`, `email`）は読み込まれません。

#### WHERE 条件でフィルタ

```sql
tate> SELECT * FROM users WHERE age > 30;
```

**出力:**
```
+----+---------+-----+---------------------+
| id | name    | age | email               |
+----+---------+-----+---------------------+
| 3  | Charlie | 35  | charlie@example.com |
| 5  | Eve     | 32  | eve@example.com     |
+----+---------+-----+---------------------+
(2 rows in 0.112 ms)
```

#### 複数条件のフィルタ

```sql
tate> SELECT name, age FROM users WHERE age > 25 AND age < 32;
```

**出力:**
```
+-------+-----+
| name  | age |
+-------+-----+
| Alice | 30  |
| Diana | 28  |
| Frank | 29  |
+-------+-----+
(3 rows in 0.098 ms)
```

### 2.6 集約関数

#### COUNT

```sql
tate> SELECT COUNT(*) FROM users;
```

**出力:**
```
+----------+
| COUNT(*) |
+----------+
| 6        |
+----------+
(1 row in 0.067 ms)
```

#### AVG（平均）

```sql
tate> SELECT AVG(age) FROM users;
```

**出力:**
```
+----------+
| AVG(age) |
+----------+
| 29.83    |
+----------+
(1 row in 0.072 ms)
```

#### MIN / MAX

```sql
tate> SELECT MIN(age), MAX(age) FROM users;
```

**出力:**
```
+----------+----------+
| MIN(age) | MAX(age) |
+----------+----------+
| 25       | 35       |
+----------+----------+
(1 row in 0.068 ms)
```

#### 複数の集約関数を組み合わせ

```sql
tate> SELECT COUNT(*), AVG(age), MIN(age), MAX(age) FROM users;
```

**出力:**
```
+----------+----------+----------+----------+
| COUNT(*) | AVG(age) | MIN(age) | MAX(age) |
+----------+----------+----------+----------+
| 6        | 29.83    | 25       | 35       |
+----------+----------+----------+----------+
(1 row in 0.075 ms)
```

### 2.7 ソートとリミット

#### ORDER BY（昇順）

```sql
tate> SELECT * FROM users ORDER BY age ASC;
```

**出力:**
```
+----+---------+-----+---------------------+
| id | name    | age | email               |
+----+---------+-----+---------------------+
| 2  | Bob     | 25  | bob@example.com     |
| 4  | Diana   | 28  | diana@example.com   |
| 6  | Frank   | 29  | NULL                |
| 1  | Alice   | 30  | alice@example.com   |
| 5  | Eve     | 32  | eve@example.com     |
| 3  | Charlie | 35  | charlie@example.com |
+----+---------+-----+---------------------+
(6 rows in 0.123 ms)
```

#### ORDER BY（降順）

```sql
tate> SELECT * FROM users ORDER BY age DESC;
```

#### LIMIT

```sql
tate> SELECT * FROM users ORDER BY age DESC LIMIT 3;
```

**出力:**
```
+----+---------+-----+---------------------+
| id | name    | age | email               |
+----+---------+-----+---------------------+
| 3  | Charlie | 35  | charlie@example.com |
| 5  | Eve     | 32  | eve@example.com     |
| 1  | Alice   | 30  | alice@example.com   |
+----+---------+-----+---------------------+
(3 rows in 0.098 ms)
```

### 2.8 DISTINCT

```sql
-- まずテストデータを追加
tate> INSERT INTO users VALUES (7, 'Alice', 40, 'alice2@example.com');

-- 重複を除外
tate> SELECT DISTINCT name FROM users;
```

**出力:**
```
+---------+
| name    |
+---------+
| Alice   |
| Bob     |
| Charlie |
| Diana   |
| Eve     |
| Frank   |
+---------+
(6 rows in 0.087 ms)
```

### 2.9 テーブルの削除

```sql
tate> DROP TABLE users;
```

**出力:**
```
Table "users" dropped successfully
(0 rows in 0.034 ms)
```

---

## 3. 実践例: Eコマースデータ分析

実際のEコマースサイトを想定したデータ分析のシナリオです。

### 3.1 テーブル設計

```sql
-- 商品テーブル
tate> CREATE TABLE products (
    product_id INT64,
    name STRING,
    category STRING,
    price FLOAT64,
    stock_quantity INT64
);

-- 注文テーブル
tate> CREATE TABLE orders (
    order_id INT64,
    product_id INT64,
    customer_id INT64,
    quantity INT64,
    total_price FLOAT64,
    order_date STRING,
    status STRING
);

-- 顧客テーブル
tate> CREATE TABLE customers (
    customer_id INT64,
    name STRING,
    email STRING,
    city STRING,
    registration_date STRING
);
```

### 3.2 サンプルデータの投入

#### 商品データ

```sql
tate> INSERT INTO products VALUES (1, 'MacBook Pro', 'Electronics', 199800.0, 50);
tate> INSERT INTO products VALUES (2, 'iPhone 15', 'Electronics', 124800.0, 200);
tate> INSERT INTO products VALUES (3, 'AirPods Pro', 'Electronics', 39800.0, 500);
tate> INSERT INTO products VALUES (4, 'Magic Keyboard', 'Accessories', 19800.0, 150);
tate> INSERT INTO products VALUES (5, 'Apple Watch', 'Electronics', 59800.0, 100);
tate> INSERT INTO products VALUES (6, 'iPad Air', 'Electronics', 84800.0, 80);
tate> INSERT INTO products VALUES (7, 'USB-C Cable', 'Accessories', 2980.0, 1000);
tate> INSERT INTO products VALUES (8, 'Mac Mini', 'Electronics', 84800.0, 30);
```

#### 顧客データ

```sql
tate> INSERT INTO customers VALUES (1, '田中太郎', 'tanaka@example.com', 'Tokyo', '2023-01-15');
tate> INSERT INTO customers VALUES (2, '佐藤花子', 'sato@example.com', 'Osaka', '2023-02-20');
tate> INSERT INTO customers VALUES (3, '鈴木一郎', 'suzuki@example.com', 'Tokyo', '2023-03-10');
tate> INSERT INTO customers VALUES (4, '高橋美咲', 'takahashi@example.com', 'Nagoya', '2023-04-05');
tate> INSERT INTO customers VALUES (5, '伊藤健太', 'ito@example.com', 'Fukuoka', '2023-05-12');
```

#### 注文データ

```sql
tate> INSERT INTO orders VALUES (1, 1, 1, 1, 199800.0, '2024-01-10', 'completed');
tate> INSERT INTO orders VALUES (2, 2, 1, 2, 249600.0, '2024-01-11', 'completed');
tate> INSERT INTO orders VALUES (3, 3, 2, 1, 39800.0, '2024-01-12', 'completed');
tate> INSERT INTO orders VALUES (4, 4, 2, 1, 19800.0, '2024-01-13', 'completed');
tate> INSERT INTO orders VALUES (5, 5, 3, 1, 59800.0, '2024-01-14', 'shipped');
tate> INSERT INTO orders VALUES (6, 2, 3, 1, 124800.0, '2024-01-15', 'shipped');
tate> INSERT INTO orders VALUES (7, 6, 4, 1, 84800.0, '2024-01-16', 'processing');
tate> INSERT INTO orders VALUES (8, 7, 4, 5, 14900.0, '2024-01-17', 'completed');
tate> INSERT INTO orders VALUES (9, 8, 5, 1, 84800.0, '2024-01-18', 'completed');
tate> INSERT INTO orders VALUES (10, 3, 5, 2, 79600.0, '2024-01-19', 'completed');
```

### 3.3 データ分析クエリ

#### 分析1: 売上合計

```sql
tate> SELECT COUNT(*) AS total_orders, SUM(total_price) AS total_revenue
      FROM orders;
```

**出力:**
```
+--------------+---------------+
| total_orders | total_revenue |
+--------------+---------------+
| 10           | 958700.0      |
+--------------+---------------+
(1 row in 0.089 ms)
```

**解説**: 列指向DBなので、`total_price` 列だけを読み込みます。他の列（`order_id`, `product_id` など）は読み込まれません。

#### 分析2: 注文ステータス別の集計

```sql
tate> SELECT status, COUNT(*) AS order_count, SUM(total_price) AS revenue
      FROM orders;
```

**出力:**
```
+------------+-------------+----------+
| status     | order_count | revenue  |
+------------+-------------+----------+
| completed  | 7           | 689500.0 |
| shipped    | 2           | 184600.0 |
| processing | 1           | 84800.0  |
+------------+-------------+----------+
(3 rows in 0.095 ms)
```

#### 分析3: 完了した注文のみの統計

```sql
tate> SELECT COUNT(*) AS completed_orders,
             AVG(total_price) AS avg_order_value,
             MIN(total_price) AS min_order,
             MAX(total_price) AS max_order
      FROM orders
      WHERE status = 'completed';
```

**出力:**
```
+------------------+-----------------+-----------+-----------+
| completed_orders | avg_order_value | min_order | max_order |
+------------------+-----------------+-----------+-----------+
| 7                | 98500.0         | 14900.0   | 249600.0  |
+------------------+-----------------+-----------+-----------+
(1 row in 0.102 ms)
```

#### 分析4: 高額商品の分析

```sql
tate> SELECT * FROM products WHERE price > 50000.0 ORDER BY price DESC;
```

**出力:**
```
+------------+-------------+-------------+----------+----------------+
| product_id | name        | category    | price    | stock_quantity |
+------------+-------------+-------------+----------+----------------+
| 1          | MacBook Pro | Electronics | 199800.0 | 50             |
| 2          | iPhone 15   | Electronics | 124800.0 | 200            |
| 6          | iPad Air    | Electronics | 84800.0  | 80             |
| 8          | Mac Mini    | Electronics | 84800.0  | 30             |
| 5          | Apple Watch | Electronics | 59800.0  | 100            |
+------------+-------------+-------------+----------+----------------+
(5 rows in 0.134 ms)
```

#### 分析5: 在庫が少ない商品

```sql
tate> SELECT name, stock_quantity
      FROM products
      WHERE stock_quantity < 100
      ORDER BY stock_quantity ASC;
```

**出力:**
```
+-------------+----------------+
| name        | stock_quantity |
+-------------+----------------+
| Mac Mini    | 30             |
| MacBook Pro | 50             |
| iPad Air    | 80             |
+-------------+----------------+
(3 rows in 0.098 ms)
```

#### 分析6: 東京の顧客

```sql
tate> SELECT name, email FROM customers WHERE city = 'Tokyo';
```

**出力:**
```
+------------+---------------------+
| name       | email               |
+------------+---------------------+
| 田中太郎   | tanaka@example.com  |
| 鈴木一郎   | suzuki@example.com  |
+------------+---------------------+
(2 rows in 0.087 ms)
```

---

## 4. 実践例: ログ分析

Webサーバーのアクセスログを分析するシナリオです。

### 4.1 ログテーブルの作成

```sql
tate> CREATE TABLE access_logs (
    log_id INT64,
    timestamp STRING,
    ip_address STRING,
    method STRING,
    path STRING,
    status_code INT64,
    response_time INT64,
    user_agent STRING
);
```

### 4.2 ログデータの投入

```sql
tate> INSERT INTO access_logs VALUES (1, '2024-12-11 10:00:01', '192.168.1.100', 'GET', '/api/users', 200, 45, 'Mozilla/5.0');
tate> INSERT INTO access_logs VALUES (2, '2024-12-11 10:00:05', '192.168.1.101', 'GET', '/api/products', 200, 89, 'Chrome/120.0');
tate> INSERT INTO access_logs VALUES (3, '2024-12-11 10:00:12', '192.168.1.102', 'POST', '/api/orders', 201, 234, 'Mozilla/5.0');
tate> INSERT INTO access_logs VALUES (4, '2024-12-11 10:00:18', '192.168.1.100', 'GET', '/api/users', 200, 38, 'Mozilla/5.0');
tate> INSERT INTO access_logs VALUES (5, '2024-12-11 10:00:25', '192.168.1.103', 'GET', '/api/products', 404, 12, 'Chrome/120.0');
tate> INSERT INTO access_logs VALUES (6, '2024-12-11 10:00:30', '192.168.1.104', 'GET', '/api/products', 200, 67, 'Safari/17.0');
tate> INSERT INTO access_logs VALUES (7, '2024-12-11 10:00:45', '192.168.1.105', 'DELETE', '/api/users/5', 403, 8, 'Postman/10.0');
tate> INSERT INTO access_logs VALUES (8, '2024-12-11 10:01:02', '192.168.1.100', 'POST', '/api/orders', 500, 1234, 'Mozilla/5.0');
tate> INSERT INTO access_logs VALUES (9, '2024-12-11 10:01:15', '192.168.1.106', 'GET', '/api/products', 200, 56, 'Chrome/120.0');
tate> INSERT INTO access_logs VALUES (10, '2024-12-11 10:01:30', '192.168.1.107', 'GET', '/api/users', 200, 42, 'Safari/17.0');
```

### 4.3 ログ分析クエリ

#### 分析1: エラーログの抽出

```sql
tate> SELECT * FROM access_logs WHERE status_code >= 400;
```

**出力:**
```
+--------+---------------------+---------------+--------+---------------+-------------+---------------+-------------+
| log_id | timestamp           | ip_address    | method | path          | status_code | response_time | user_agent  |
+--------+---------------------+---------------+--------+---------------+-------------+---------------+-------------+
| 5      | 2024-12-11 10:00:25 | 192.168.1.103 | GET    | /api/products | 404         | 12            | Chrome/120  |
| 7      | 2024-12-11 10:00:45 | 192.168.1.105 | DELETE | /api/users/5  | 403         | 8             | Postman/10  |
| 8      | 2024-12-11 10:01:02 | 192.168.1.100 | POST   | /api/orders   | 500         | 1234          | Mozilla/5.0 |
+--------+---------------------+---------------+--------+---------------+-------------+---------------+-------------+
(3 rows in 0.112 ms)
```

#### 分析2: 平均レスポンスタイム

```sql
tate> SELECT AVG(response_time) AS avg_response_time FROM access_logs;
```

**出力:**
```
+-------------------+
| avg_response_time |
+-------------------+
| 178.3             |
+-------------------+
(1 row in 0.067 ms)
```

**解説**: `response_time` 列だけを読み込むため、非常に高速です。

#### 分析3: 遅いリクエストの検出

```sql
tate> SELECT path, status_code, response_time
      FROM access_logs
      WHERE response_time > 100
      ORDER BY response_time DESC;
```

**出力:**
```
+-------------+-------------+---------------+
| path        | status_code | response_time |
+-------------+-------------+---------------+
| /api/orders | 500         | 1234          |
| /api/orders | 201         | 234           |
+-------------+-------------+---------------+
(2 rows in 0.098 ms)
```

#### 分析4: HTTPメソッド別の統計

```sql
tate> SELECT method, COUNT(*) AS request_count FROM access_logs;
```

**出力:**
```
+--------+---------------+
| method | request_count |
+--------+---------------+
| GET    | 7             |
| POST   | 2             |
| DELETE | 1             |
+--------+---------------+
(3 rows in 0.089 ms)
```

#### 分析5: ステータスコード別の統計

```sql
tate> SELECT status_code, COUNT(*) AS count FROM access_logs;
```

**出力:**
```
+-------------+-------+
| status_code | count |
+-------------+-------+
| 200         | 6     |
| 201         | 1     |
| 403         | 1     |
| 404         | 1     |
| 500         | 1     |
+-------------+-------+
(5 rows in 0.092 ms)
```

#### 分析6: 特定IPアドレスのアクティビティ

```sql
tate> SELECT timestamp, method, path, status_code
      FROM access_logs
      WHERE ip_address = '192.168.1.100';
```

**出力:**
```
+---------------------+--------+-------------+-------------+
| timestamp           | method | path        | status_code |
+---------------------+--------+-------------+-------------+
| 2024-12-11 10:00:01 | GET    | /api/users  | 200         |
| 2024-12-11 10:00:18 | GET    | /api/users  | 200         |
| 2024-12-11 10:01:02 | POST   | /api/orders | 500         |
+---------------------+--------+-------------+-------------+
(3 rows in 0.105 ms)
```

---

## 5. パフォーマンステスト

列指向DBの特性を実感できるテストです。

### 5.1 大量データの投入

```sql
-- センサーデータテーブル
tate> CREATE TABLE sensor_data (
    sensor_id INT64,
    timestamp STRING,
    temperature FLOAT64,
    humidity FLOAT64,
    pressure FLOAT64,
    location STRING
);

-- データを大量投入（実際には1000行以上推奨）
tate> INSERT INTO sensor_data VALUES (1, '2024-12-11 00:00:00', 22.5, 45.2, 1013.25, 'Tokyo');
tate> INSERT INTO sensor_data VALUES (2, '2024-12-11 00:00:01', 22.6, 45.3, 1013.26, 'Tokyo');
tate> INSERT INTO sensor_data VALUES (3, '2024-12-11 00:00:02', 22.7, 45.1, 1013.27, 'Tokyo');
-- ... (continue inserting data)
```

### 5.2 カラムプルーニングの効果を確認

#### ケース1: 1列だけ取得（高速）

```sql
tate> SELECT temperature FROM sensor_data;
```

**期待される動作**: `temperature` 列のファイルだけを読み込むため高速

#### ケース2: 全列取得（やや遅い）

```sql
tate> SELECT * FROM sensor_data;
```

**期待される動作**: 全てのカラムファイルを読み込むため、ケース1より遅い

#### ケース3: 集約関数（高速）

```sql
tate> SELECT AVG(temperature), AVG(humidity) FROM sensor_data;
```

**期待される動作**: 2列だけを読み込むため高速

### 5.3 実行時間の比較

クエリ実行後に表示される実行時間を確認:

```
(1000 rows in 0.234 ms)  ← この部分
```

列指向DBの特性:
- ✅ **速い**: 必要な列だけの取得、集約関数
- ⚠️ **遅い**: 全列の取得、単一行の取得

---

## 6. トラブルシューティング

### 6.1 よくあるエラーと解決方法

#### エラー1: テーブルが既に存在する

**エラーメッセージ:**
```
Execution error: table "users" already exists
```

**解決方法:**
```sql
-- 既存のテーブルを削除
tate> DROP TABLE users;

-- または別の名前でテーブルを作成
tate> CREATE TABLE users_v2 (...);
```

#### エラー2: テーブルが存在しない

**エラーメッセージ:**
```
Execution error: table "products" does not exist
```

**解決方法:**
```sql
-- テーブル一覧を確認
tate> tables

-- 正しいテーブル名でクエリ実行
tate> SELECT * FROM <correct_table_name>;
```

#### エラー3: カラム数の不一致

**エラーメッセージ:**
```
Execution error: column count mismatch: expected 4, got 3
```

**解決方法:**
```sql
-- スキーマを確認
tate> describe users

-- 全てのカラムに値を指定
tate> INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com');

-- または、カラムを指定して挿入
tate> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
```

#### エラー4: 構文エラー

**エラーメッセージ:**
```
Parse error: line 1: expected FROM, got FORM instead
```

**解決方法:**
- SQL文のスペルを確認
- キーワードの順序を確認（SELECT → FROM → WHERE の順）

### 6.2 データの確認とデバッグ

#### 現在のテーブル一覧

```sql
tate> tables
```

#### テーブルの詳細

```sql
tate> describe <table_name>
```

#### データの件数確認

```sql
tate> SELECT COUNT(*) FROM <table_name>;
```

#### サンプルデータの確認

```sql
tate> SELECT * FROM <table_name> LIMIT 5;
```

### 6.3 データディレクトリの確認

データは以下のディレクトリに保存されます:

```bash
# デフォルトの保存先
~/.tate/

# ディレクトリ構造
~/.tate/
├── catalog.json
└── tables/
    ├── users/
    │   ├── _meta.json
    │   ├── col_id.dat
    │   ├── col_name.dat
    │   ├── col_age.dat
    │   └── col_email.dat
    └── products/
        └── ...
```

### 6.4 データのクリーンアップ

全てのデータを削除して最初からやり直したい場合:

```bash
# Tateを終了
tate> exit

# データディレクトリを削除
rm -rf ~/.tate

# Tateを再起動
./bin/tate
```

---

## まとめ

このチュートリアルでは、以下の操作を学びました:

1. ✅ **基本操作**: CREATE TABLE, INSERT, SELECT, DROP TABLE
2. ✅ **データ取得**: WHERE, ORDER BY, LIMIT, DISTINCT
3. ✅ **集約関数**: COUNT, AVG, MIN, MAX, SUM
4. ✅ **実践例**: Eコマース分析、ログ分析
5. ✅ **パフォーマンス**: カラムプルーニングの効果

### 列指向DBの特性を実感するポイント

- 📊 **集約クエリが高速**: `SELECT AVG(age) FROM users` は age列だけ読む
- 🔍 **カラムプルーニング**: `SELECT name FROM users` は name列だけ読む
- ⚡ **分析に最適**: 大量データの統計処理に強い

### 次のステップ

- より大量のデータでテスト（数万〜数十万行）
- 複雑なクエリの組み合わせ
- パフォーマンスの測定と比較

Happy Querying! 🚀
