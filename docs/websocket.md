# RSQL WebSocket Interface Documentation

## Overview

RSQL provides a WebSocket-based SQL query interface that supports real-time bidirectional communication. Clients send SQL query requests through WebSocket connections, and the server returns structured results.

## Connection Information

### Basic Connection
- Protocol: WebSocket (ws:// or wss://)
- Path: /ws
- Authentication Method: URL Query Parameters

### Connection URL Format
```
ws://<host>:<port>/ws?username=<username>&password=<password>
```

### Connection Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| username | string | Yes | Username, needs URL encoding |
| password | string | Yes | Password, needs URL encoding |

## Message Format

### Client Request Format

```json
{
  "username": "string",
  "userid": 123,
  "request_content": "SQL query statement"
}
```

Field descriptions:

- `username`: Current operating user's name
- `userid`: Current operating user's ID
- `request_content`: SQL query statement, supports batch queries (separated by semicolons)

### Server Response Format

```json
{
  "rayon_response": {
    "response_content": [],  // Original execution result array
    "uniform_result": [      // Standardized result array
      {
        "result_type": "string",
        "data": {}
      }
    ],
    "error": "string",       // Error message (prompt message on success)
    "execution_time": 123    // Execution time (milliseconds)
  },
  "timestamp": 1234567890,   // Unix timestamp (seconds)
  "success": true,           // Whether successful
  "connection_id": 456       // Connection ID
}
```

## Result Type Specifications

### 1. Query Results (query)
- `result_type`: "query"

```json
{
  "result_type": "query",
  "data": {
    "columns": ["column1", "column2", ...],
    "rows": [
      ["value1", "value2", ...],
      ...
    ],
    "row_count": 10,
    "column_count": 2
  }
}
```

### 2. Data Modification Results (mutation)
- `result_type`: "mutation"

```json
{
  "result_type": "mutation",
  "data": {
    "message": "Rows affected: 5",
    "affected_rows": 5
  }
}
```

### 3. Transaction Control Results

#### 3.1 Begin Transaction
- `result_type`: "transaction_begin"

```json
{
  "result_type": "transaction_begin",
  "data": {
    "message": "Transaction started successfully"
  }
}
```

#### 3.2 Commit Transaction
- `result_type`: "transaction_commit"

```json
{
  "result_type": "transaction_commit",
  "data": {
    "message": "Transaction committed successfully"
  }
}
```

#### 3.3 Rollback Transaction
- `result_type`: "transaction_rollback"

```json
{
  "result_type": "transaction_rollback",
  "data": {
    "message": "Transaction rolled back successfully"
  }
}
```

### 4. DDL Operation Results (ddl)
- `result_type`: "ddl"

```json
{
  "result_type": "ddl",
  "data": {
    "message": "Table created successfully"
  }
}
```

### 5. DCL Operation Results (dcl)
- `result_type`: "dcl"

```json
{
  "result_type": "dcl",
  "data": {
    "message": "User created successfully"
  }
}
```

## Supported SQL Features

### Data Definition Language (DDL)
- CREATE TABLE - Supports column definitions, primary keys, NOT NULL, UNIQUE, and index constraints
- CREATE INDEX - Supports single-column indexes (does not support multi-column or VARCHAR column indexes)
- ALTER TABLE RENAME TO - Table renaming
- DROP TABLE - Delete table

### Data Manipulation Language (DML)
- INSERT VALUES - Insert data (does not support subquery insertion)
- SELECT - Query data, supports WHERE, JOIN, GROUP BY, aggregate functions
- UPDATE - Update data
- DELETE - Delete data

### Transaction Control Language (TCL)
- BEGIN TRANSACTION - Start transaction
- COMMIT - Commit transaction
- ROLLBACK - Rollback transaction

### Data Control Language (DCL)
- CREATE USER - Create user
- DROP USER - Delete user

## Connection Lifecycle

### 1. Connection Establishment
- Client connects to server via WebSocket
- Server validates username and password
- Server returns welcome message containing connection ID

### 2. Session Period
- Client sends SQL query requests
- Server executes queries and returns results
- Server automatically performs checkpoint operations every minute

### 3. Connection Closure
- Client disconnects
- Server automatically rolls back uncommitted transactions
- Server cleans up connection-related resources

## Error Handling

### Authentication Errors
- Error Code: 401
- Reason: Incorrect username or password
- Handling: Connection rejected

### SQL Syntax Errors
- Error Code: success field in response is false
- Reason: SQL statement syntax error or semantic error
- Handling: Check the error field in response for details

### Server Internal Errors
- Error Code: success field in response is false
- Reason: Server internal processing error
- Handling: Check server logs, retry operation

### Connection Timeout
- Error Code: WebSocket connection disconnected
- Reason: Network issues or server restart
- Handling: Re-establish connection

## Notes

### 1. Concurrency Limitations
- Queries on the same connection are executed serially
- Different connections can execute queries in parallel
- Use connection_id to distinguish different connections

### 2. Transaction Management
- Each connection independently manages transactions
- Uncommitted transactions are automatically rolled back when connection closes
- Supports explicit transaction control (BEGIN/COMMIT/ROLLBACK)

### 3. Automatic Checkpoints
- Server automatically performs checkpoint operations every minute
- Checkpoint operations do not affect normal queries
- Notification message sent after checkpoint success

### 4. Data Type Mapping

| RSQL Data Type | JSON Representation |
|----------------|---------------------|
| INTEGER | number |
| FLOAT | number |
| VARCHAR | string |
| CHAR | string |
| BOOLEAN | boolean |
| NULL | null |

### 5. Security
- Password transmitted via URL parameters, HTTPS(wss://) recommended
- Supports user authentication and authorization
- Input parameters require URL encoding

## Heartbeat Mechanism
- Client does not need to send heartbeat
- Server periodically sends checkpoint notifications
- Connection idle timeout determined by server configuration

## Example Flow

### Successful Connection Flow
1. Client: `ws://localhost:8080/ws?username=alice&password=123456`
2. Server: Validates user, returns welcome message
3. Client: Sends SQL query request
4. Server: Executes query, returns result
5. Client: Disconnects
6. Server: Cleans up resources, rolls back uncommitted transactions

### Batch Query Example

```json
{
  "username": "admin",
  "userid": 1,
  "request_content": "BEGIN TRANSACTION; INSERT INTO users VALUES (1, 'Alice'); COMMIT;"
}
```

## Testing Recommendations

### Basic Testing
- Connection authentication testing
- DDL operation testing (CREATE TABLE/DROP TABLE)
- DML operation testing (INSERT/SELECT/UPDATE/DELETE)
- Transaction operation testing
- Error handling testing

### Performance Testing
- Concurrent connection testing
- Large data volume query testing
- Long-term connection testing
- Automatic checkpoint impact testing

### Compatibility Testing
- Different client implementation testing
- Network interruption reconnection testing
- Abnormal data format testing
- Timeout retry testing