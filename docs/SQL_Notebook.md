# SQL Feature Support Notebook

## 1. Transaction

Supported:
- `BEGIN`
- `COMMIT`
- `ROLLBACK`

Limitations:
- No nested transactions

---

## 2. DDL

### 2.1 CREATE

Supported:
- `CREATE TABLE`
- Column definitions (`ColumnDef`)
- Column constraints:
  - `PRIMARY KEY`
  - `NOT NULL`
  - `UNIQUE`
- `CREATE INDEX`
- `CREATE UNIQUE INDEX`

Unsupported:
- None

---

### 2.2 ALTER TABLE

Supported:
- `RENAME TABLE`

Unsupported:
- `ADD COLUMN`
- `DROP COLUMN`
- `ALTER COLUMN TYPE`

---

### 2.3 DROP

Supported:
- `DROP TABLE`
- `DROP TABLE IF EXISTS`

---

## 3. DML

### 3.1 INSERT

Supported:
- `INSERT VALUES`
- `INSERT SELECT`

| SQL Type | DataItem Variant | Storage Method |
|----------|----------------|----------------|
| INT      | Integer(i64)   | Fixed 8 bytes  |
| CHAR(n)  | Char(String, i32) | Fixed n bytes (with padding) |
| VARCHAR(n) | VarChar(String) | Length prefix + content |
| FLOAT    | Float(f64)     | Fixed 8 bytes  |
| BOOLEAN  | Bool(bool)     | Fixed size     |
| NULL     | Null           | Marker only    |

---

### 3.2 DELETE

Supported:
- `DELETE`
- `DELETE WHERE`

---

### 3.3 UPDATE

Supported:
- `UPDATE`
- `UPDATE WHERE`

---

## 4. SELECT (Single Table)

Supported:
- `SELECT *`
- Projection (`SELECT col1, col2`)
- `WHERE`

---

### 4.1 WHERE Conditions

### a) Comparison Operators

Used in **WHERE clauses**, **JOIN conditions**, and **constraint enforcement**. 

| Operator | Description                     | Applicable Types        | Notes |
|----------|---------------------------------|------------------------|-------|
| `=`      | Equality                        | INT, FLOAT, CHAR, VARCHAR, BOOLEAN | Null comparison returns false (unless `IS NULL`) |
| `!=` | Not equal                     | INT, FLOAT, CHAR, VARCHAR, BOOLEAN | Null comparison returns false |
| `>`      | Greater than                    | INT, FLOAT |
| `<`      | Less than                       | INT, FLOAT |
| `>=`     | Greater than or equal           | INT, FLOAT |
| `<=`     | Less than or equal              | INT, FLOAT |

### b) Arithmetic Operators

Arithmetic operators are supported for numeric types:

| Operator | Description        | Applicable Types |
|----------|------------------|----------------|
| `+`      | Addition          | INT, FLOAT     |
| `-`      | Subtraction       | INT, FLOAT     |
| `*`      | Multiplication    | INT, FLOAT     |
| `/`      | Division          | INT, FLOAT     |

### c) Logical Operators

Logical operators are used in boolean expressions:

| Operator | Description        | Notes |
|----------|------------------|-------|
| `AND`    | Logical AND        | Short-circuit evaluation |
| `OR`     | Logical OR         | Short-circuit evaluation |
| `NOT`    | Logical NOT        | Unary operator |

Unsupported:
- `LIKE`
- `ILIKE`

---

### 4.2 GROUP BY & Aggregate

Supported:
- `GROUP BY`
- `COUNT`
- `SUM`
- `AVG`
- `MIN`
- `MAX`

---

## 5. JOIN (Multi-Table)

Supported:
- `INNER JOIN`
- `LEFT JOIN`
- `RIGHT JOIN`
- `FULL JOIN`
- `CROSS JOIN`
- `FROM A, B`

Unsupported:
- `NATURAL JOIN`

---

## 6. Subquery

Supported:
- Scalar subquery

Unsupported:
- `IN`
- `NOT IN`
- `EXISTS`
---

## 7. DCL

Supported:
- `CREATE USER`
- `DROP USER`

Unsupported:
- `GRANT`
- `REVOKE`

---

## 8. Notes

- Unsupported features are rejected during logical plan construction.
- All supported features are mapped to explicit logical plan nodes.