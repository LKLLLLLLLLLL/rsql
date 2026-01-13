# SQL Language Implementation Functions

This document describes the SQL language functions implemented in the system, including supported SQL statements, data types, and their underlying storage and execution mechanisms.  
All descriptions are standardized according to the following **fixed data item representation**, which serves as the authoritative implementation basis.

## Data Item Representation

Each attribute value stored in a table record is represented using the following data structure:

```rust
// Data item representation in one block in table.
enum DataItem {
    Integer(i64),
    Float(f64),
    Char(String, i32),   // Fixed length CHAR(n)
    VarChar(String),    // Variable length VARCHAR(n)
    Bool(bool),
    Null,
}
```
# SQL System Implementation Specification

This document provides a detailed description of the SQL language features implemented in the system, including supported SQL statements, data types, storage formats, and execution mechanisms. All descriptions follow a **fixed data item representation**, which serves as the authoritative basis for implementation.

---

## Design Clarifications

- SQL `INT` type is implemented as a 64-bit signed integer (`i64`).
- Fixed-length character fields store both the actual value and the declared length `n`.

---

## 1. Data Definition Language (DDL)

### a) CREATE TABLE

The `CREATE TABLE` statement defines the schema of a table, including:

- Table name
- Column names
- Data types
- Constraints

#### Supported Data Types and Implementation

**INT (Integer)**  
- **Logical Meaning:** Stores integer values.  
- **Implementation:** Mapped to `DataItem::Integer(i64)`.  
- **Storage Format:** 8 bytes (64-bit), stored in binary using two’s complement.  
- **Notes:** Chosen for simplicity and consistency with internal numeric representation.

**CHAR(n) (Fixed-length Character)**  
- **Logical Meaning:** Stores strings of fixed length `n`.  
- **Implementation:** Mapped to `DataItem::Char(String, i32)`. The `String` stores the actual content, `i32` stores the declared fixed length `n`.  
- **Storage Format:** Occupies exactly `n` bytes. If `String.len() < n`, padding is applied (spaces or `\0`). If `String.len() > n`, insertion fails or truncation occurs (policy-dependent).

**VARCHAR(n) (Variable-length Character)**  
- **Logical Meaning:** Stores strings with a maximum length `n`.  
- **Implementation:** Mapped to `DataItem::VarChar(String)`.  
- **Storage Format:** Length prefix (1–2 bytes) indicating the actual string length, followed by the string content.  
- **Notes:** Maximum length `n` is validated against schema metadata.

**FLOAT**  
- **Logical Meaning:** Stores floating-point numbers.  
- **Implementation:** Mapped to `DataItem::Float(f64)`.  
- **Storage Format:** 8-byte IEEE 754 double-precision floating point.

**BOOLEAN**  
- **Logical Meaning:** Stores logical values.  
- **Implementation:** Mapped to `DataItem::Bool(bool)`.  
- **Storage Format:** Typically 1 byte or platform-dependent.

**NULL**  
- **Logical Meaning:** Represents missing or undefined values.  
- **Implementation:** Mapped to `DataItem::Null`.

#### Constraints

- **PRIMARY KEY:** Recorded in table metadata; enforced by uniqueness checks during insertion and update.  
- **NOT NULL:** Prevents insertion or update of `NULL` values.  
- **UNIQUE (optional):** Ensures column values are not duplicated.

---

### b) ALTER TABLE

The `ALTER TABLE` statement modifies an existing table schema:

- **ADD column:** Appends a new column definition to metadata; existing records are filled with default values or `NULL`.  
- **DROP column:** Marks the column as invalid in metadata; physical data removal may be deferred (logical deletion).  
- **MODIFY / RENAME column:** Renaming affects metadata only; type modification may require rewriting table data.

---

### c) DROP TABLE

The `DROP TABLE` statement removes an entire table:

- Deletes table metadata.  
- Releases or marks associated data storage as reusable.  
- The table and its records become inaccessible.

---

## 2. Data Manipulation Language (DML)

### a) INSERT INTO

The `INSERT INTO` statement adds new records to a table:

- Records are constructed as ordered collections of `DataItem`.  
- Each value is converted according to the column’s SQL type.

| SQL Type | DataItem Variant | Storage Method |
|----------|----------------|----------------|
| INT      | Integer(i64)   | Fixed 8 bytes  |
| CHAR(n)  | Char(String, i32) | Fixed n bytes (with padding) |
| VARCHAR(n) | VarChar(String) | Length prefix + content |
| FLOAT    | Float(f64)     | Fixed 8 bytes  |
| BOOLEAN  | Bool(bool)     | Fixed size     |
| NULL     | Null           | Marker only    |

Before insertion, the system checks:

- Column count consistency.  
- Data type compatibility.  
- Constraint enforcement (`PRIMARY KEY`, `NOT NULL`, `UNIQUE`).

---

### b) DELETE FROM

The `DELETE FROM` statement removes records that satisfy a condition:

- Records are identified using the `WHERE` clause.  
- Deletion strategies:  
  - Logical deletion using a delete flag.  
  - Physical deletion by rewriting data files.

---

### c) UPDATE

The `UPDATE` statement modifies existing records:

- Target records are located using the `WHERE` clause.  
- Specified columns are updated with new `DataItem` values.  
- Special considerations:  
  - Variable-length fields may change record size.  
  - Records may be updated in place or rewritten.

---

### d) SELECT

The `SELECT` statement retrieves data from one or more tables and implements relational query operations.

#### i. Selection

- **Purpose:** Filters rows based on conditions.  
- **Keyword:** `WHERE`  
- **Implementation:** Sequentially scans records and evaluates condition expressions on `DataItem` values.  
- **Example:**  
  ```sql
  SELECT * FROM users WHERE age > 18;