# Examples

## 1. create table
```sql
create table students (
	id INTEGER PRIMARY KEY,
	name CHAR(50) NOT NULL UNIQUE,
	age INTEGER,
	is_active BOOL,
	email VARCHAR(100),
	INDEX (age)
);
create table scores (
	id INTEGER PRIMARY KEY,
	score INTEGER NOT NULL,
	class VARCHAR(100) NOT NULL
);
```

## 2. rename table
```sql
ALTER TABLE students RENAME TO users;
```

## 3. rename column 
```sql
ALTER TABLE users RENAME COLUMN is_active TO active;
```

