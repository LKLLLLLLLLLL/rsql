# The sql features supported in RSQL
RSQL supports basic ACID features and a subset of SQL features. 

## 1. Data Definition Language (DDL)
### 1.1 CREATE TABLE
Supported:
- `CREATE TABLE`
- Column definitions
- Column constraints:
  - `PRIMARY KEY`
  - `NOT NULL`
  - `UNIQUE`
  - `INDEX`
- Data types:
  - `INTEGER`: 64-bit signed integer
  - `FLOAT`: double-precision floating point
  - `VARCHAR(n)`
  - `CHAR(n)`
  - `BOOLEAN`
- `IF NOT EXISTS` clause

e.g.
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY, // auto indexed
    name VARCHAR(100) NOT NULL,
    age INTEGER,
    email VARCHAR(100) UNIQUE, // auto indexed
    is_active BOOLEAN,
    INDEX (age)
);
```

### 1.2 CREATE INDEX
Supported:
- `CREATE INDEX`
- `UNIQUE` constraint
- Single-column indexes
- `IF NOT EXISTS` clause

Not Supported:
- Multi-column indexes
- Indexes on VARCHAR columns

e.g.
```sql
CREATE INDEX idx_age ON users(age);
CREATE UNIQUE INDEX idx_email ON users(email) IF NOT EXISTS;
```

### 1.3 ALTER TABLE
Supported:
- `ALTER TABLE RENAME TO`
- `IF EXISTS` clause
- `ALTER TABLE RENAME COLUMN TO`: Only support renaming unindexed columns

e.g.
```sql
ALTER TABLE old_table_name IF EXISTS RENAME TO new_table_name;
```

### 1.4 DROP TABLE
Supported:
- `DROP TABLE`
- `IF EXISTS` clause

e.g.
```sql
DROP TABLE IF EXISTS users;
```

## 2. Data Manipulation Language (DML)
### 2.1 INSERT
Supported:
- `INSERT VALUES`
Not Supported:
- Insert from subquery

e.g.
```sql
INSERT INTO users (id, name, age, email, is_active) VALUES (1, 'Alice', 30, 'alice@example.com', true);
```

### 2.2 SELECT
Supported:
- basic `SELECT` statements
- `WHERE` clause with complex conditions:
  - Simple comparisons: `=`, `<`, `>`, `<=`, `>=`, `!=`
  - Pattern matching: `LIKE`, `ILIKE` (case-insensitive)
  - Range matching: `BETWEEN <low> AND <high>` (optimized with B-Tree index)
- `JOIN` operations (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN)
- `GROUP BY` clause and aggregation functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
- `FROM` clause with one subquery
- `ORDER BY` clause
- Renaming columns using `AS` // TO BE SUPPORTED IN FUTURE

e.g.
```sql
SELECT name, age FROM users WHERE age > 25;
SELECT name FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE age BETWEEN 20 AND 30;
SELECT u.name, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id;
SELECT age, COUNT(*) FROM users GROUP BY age;
```

### 2.3 UPDATE
Supported:
- `UPDATE` statements with `SET` clause
- `WHERE` clause with all supported conditions (comparisons, `LIKE`, `BETWEEN`, etc.)

e.g.
```sql
UPDATE users SET is_active = false WHERE last_login < 1600000000;
UPDATE users SET bio = 'Secret' WHERE name LIKE 'Private%';
```

### 2.4 DELETE
Supported:
- `DELETE` statements
- `WHERE` clause with all supported conditions

e.g.
```sql
DELETE FROM users WHERE is_active = false;
DELETE FROM users WHERE age BETWEEN 0 AND 18;
```

## 3. Transaction Control Language (TCL)
### 3.1 BEGIN TRANSACTION
Supported:
- `BEGIN TRANSACTION`
e.g.
```sql
BEGIN TRANSACTION;
```

### 3.2 COMMIT
Supported:
- `COMMIT`
e.g.
```sql
COMMIT;
```

### 3.3 ROLLBACK
Supported:
- `ROLLBACK`
e.g.
```sql
ROLLBACK;
```

## 4. Data Control Language (DCL)
### 4.1 CREATE USER
Supported:
- `CREATE USER`
- `IF NOT EXISTS` clause
- `PASSWORD` specification

Created users have **no permissions** by default and cannot read or write any tables until granted.

e.g.
```sql
CREATE USER alice PASSWORD 'securepassword' IF NOT EXISTS;
```

### 4.2 DROP USER
Supported:
- `DROP USER`
- `IF EXISTS` clause

e.g.
```sql
DROP USER IF EXISTS alice;
```

### 4.3 GRANT
Supported:
- `GRANT READ TO user`: Grant global read access.
- `GRANT WRITE TO user`: Grant global write access (includes read).
- `GRANT READ ON table TO user`: Grant table-specific read access.
- `GRANT WRITE ON table TO user`: Grant table-specific write access.

e.g.
```sql
GRANT WRITE TO alice;
GRANT READ ON orders TO guest_user;
```

### 4.4 REVOKE
Supported:
- `REVOKE READ FROM user`: Revoke all global permissions.
- `REVOKE WRITE FROM user`: Revoke all global permissions.
- `REVOKE READ ON table FROM user`: Revoke table-specific access.
- `REVOKE WRITE ON table FROM user`: Revoke table-specific access.

e.g.
```sql
REVOKE WRITE FROM alice;
REVOKE READ ON orders FROM guest_user;
```
