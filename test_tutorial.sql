-- Tutorial Test Script
-- Section 2.1: Create Table
CREATE TABLE users (
    id INT64,
    name STRING,
    age INT64,
    email STRING
);

-- Section 2.3: Describe Table
-- (manual command: describe users)

-- Section 2.4: Insert Data
INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 25, 'bob@example.com');
INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com');
INSERT INTO users VALUES (4, 'Diana', 28, 'diana@example.com');
INSERT INTO users VALUES (5, 'Eve', 32, 'eve@example.com');
INSERT INTO users VALUES (6, 'Frank', 29, 'frank@example.com');

-- Section 2.5: Select All
SELECT * FROM users;

-- Section 2.5: Select Specific Columns
SELECT name, age FROM users;

-- Section 2.5: WHERE Filter
SELECT * FROM users WHERE age > 30;

-- Section 2.5: Multiple Conditions
SELECT name, age FROM users WHERE age > 25 AND age < 32;

-- Section 2.6: COUNT
SELECT COUNT(*) FROM users;

-- Section 2.6: AVG
SELECT AVG(age) FROM users;

-- Section 2.6: MIN/MAX
SELECT MIN(age), MAX(age) FROM users;

-- Section 2.6: Multiple Aggregates
SELECT COUNT(*), AVG(age), MIN(age), MAX(age) FROM users;

-- Section 2.7: ORDER BY ASC
SELECT * FROM users ORDER BY age ASC;

-- Section 2.7: ORDER BY DESC
SELECT * FROM users ORDER BY age DESC;

-- Section 2.7: LIMIT
SELECT * FROM users ORDER BY age DESC LIMIT 3;

-- Section 2.8: DISTINCT
INSERT INTO users VALUES (7, 'Alice', 40, 'alice2@example.com');
SELECT DISTINCT name FROM users;

-- Section 3: E-commerce Example
CREATE TABLE products (
    product_id INT64,
    name STRING,
    category STRING,
    price FLOAT64,
    stock_quantity INT64
);

INSERT INTO products VALUES (1, 'MacBook Pro', 'Electronics', 199800.0, 50);
INSERT INTO products VALUES (2, 'iPhone 15', 'Electronics', 124800.0, 200);
INSERT INTO products VALUES (3, 'AirPods Pro', 'Electronics', 39800.0, 500);

SELECT * FROM products;
SELECT AVG(price) FROM products;
SELECT * FROM products WHERE price > 50000.0 ORDER BY price DESC;
