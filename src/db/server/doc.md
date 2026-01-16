# Server Module Documentation

## Overview
The server module provides a dual-protocol (HTTP/WebSocket) query processing system that handles client requests through a multi-threaded architecture combining Actix-web for I/O and Rayon for CPU-intensive tasks. It supports both single-query HTTP transactions and stateful WebSocket connections with transaction management.

## Module Structure

### mod.rs
Exports all sub-modules (`server`, `types`, `thread_pool`, `websocket_actor`) and provides access for external code.

### server.rs
**Actix-web HTTP/WebSocket server implementation:**

- **AppState**: Shared state with thread pool and atomic counters
- **handle_http_query()**: HTTP request handler for single-query transactions with concurrency tracking
- **handle_ws_query()**: WebSocket connection handler that creates dedicated actors for each connection
- **start_server()**: Server initialization and startup

### thread_pool.rs
**Rayon thread pool manager:**

- **WorkingThreadPool**: Configurable thread pool structure
- **parse_and_execute_query()**: Distributes SQL queries via oneshot channels
- **rollback()**: Asynchronous rollback operation for transaction management  
- **rollback_sync()**: Synchronous rollback for connection cleanup
- Auto-scales based on CPU cores when THREAD_MAXNUM=0

### types.rs
**Data structures for communication:**

- **HttpQueryRequest**: Client HTTP request structure
- **HttpQueryResponse**: Server HTTP response structure  
- **RayonQueryRequest**: Query structure sent to rayon thread pool
- **RayonQueryResponse**: Response structure from rayon thread pool
- **WebsocketResponse**: WebSocket message structure with connection ID

### websocket_actor.rs
**WebSocket connection and transaction management:**

- **WebsocketActor**: One actor per WebSocket connection, managing connection lifecycle
- **started()**: Called on connection establishment, sends connection ID to client
- **stopped()**: Called on connection closure, triggers rollback for pending transactions
- **StreamHandler**: Handles incoming WebSocket messages (queries, close events)
- **SendTextMessage**: Internal message type for sending responses back to client