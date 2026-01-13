# SQL Language Implementation Functions

## 1. Data Definition Language (DDL)

### a) CREATE TABLE

The `CREATE TABLE` statement defines the schema of a table, including:

- Table name
- Column names
- Data types
- Constraints

#### Supported Data Types and Implementation

- INT (Integer)
- CHAR(n) (Fixed-length Character)
- VARCHAR(n) (Variable-length Character)
- FLOAT
- BOOLEAN
- NULL 

#### Constraints

- **PRIMARY KEY:** Recorded in table metadata; enforced by uniqueness checks during insertion and update.  
- **NOT NULL:** Prevents insertion or update of `NULL` values.  
- **UNIQUE (optional):** Ensures column values are not duplicated.

### b) ALTER TABLE

- **ADD column:** Appends a new column definition to metadata; existing records are filled with default values or `NULL`.  
- **DROP column:** Marks the column as invalid in metadata; physical data removal may be deferred (logical deletion).  
- **MODIFY / RENAME column:** Renaming affects metadata only; type modification may require rewriting table data.

### c) DROP TABLE

---

## 2. Data Manipulation Language (DML)

### a) INSERT INTO

| SQL Type | DataItem Variant | Storage Method |
|----------|----------------|----------------|
| INT      | Integer(i64)   | Fixed 8 bytes  |
| CHAR(n)  | Char(String, i32) | Fixed n bytes (with padding) |
| VARCHAR(n) | VarChar(String) | Length prefix + content |
| FLOAT    | Float(f64)     | Fixed 8 bytes  |
| BOOLEAN  | Bool(bool)     | Fixed size     |
| NULL     | Null           | Marker only    |

### b) DELETE FROM

- Records are identified using the `WHERE` clause.  

### c) UPDATE

- Target records are located using the `WHERE` clause.  

### d) SELECT

#### i. Selection

- **Keyword:** `WHERE`  

- **Example:**  
  ```sql
  SELECT * FROM users WHERE age > 18;
  ```

---

## 3. Operations and Expressions

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

- e.g., `age > 18 AND salary < 10000`.