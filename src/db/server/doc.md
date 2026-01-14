# Server Module Documentation

## Overview
The server module provides an HTTP-based query processing system that handles client requests through a multi-threaded architecture combining Actix-web for I/O and Rayon for CPU-intensive tasks.

## Module Structure

### mod.rs
Exports all sub-modules (`server`, `types`, `thread_pool`) and provides access for external code.

### server.rs
**Actix-web HTTP server implementation:**
- `AppState`: Shared state with thread pool and atomic counter
- `handle_query()`: Request handler with concurrency tracking
- `start_server()`: Server initialization and startup

### thread_pool.rs
**Rayon thread pool manager:**
- `WorkingThreadPool`: Configurable thread pool structure
- `parse_and_execute_query()`: Distributes queries via oneshot channels
- Auto-scales based on CPU cores when THREAD_MAXNUM=0

### types.rs
**Data structures for HTTP communication:**
- `HttpQueryRequest`: Client request structure
- `HttpQueryResponse`: Server response structure

## Key Components

### Thread Architecture
- **Actix-web Workers**: Multiple threads for async I/O (default = CPU cores)
- **Rayon Thread Pool**: Global pool for CPU-bound tasks with work-stealing
- **Channel Communication**: Oneshot channels for inter-thread data transfer

### Request Flow
1. HTTP request → Actix-web worker thread
2. Create oneshot channel → Pass sender to Rayon thread
3. Rayon executes query → Sends result via channel
4. Actix-web awaits result → Returns HTTP response

### Concurrency Management
- **Atomic Counters**: `AtomicU64` for thread-safe query tracking
- **Shared State**: `Arc`-based `AppState` shared across workers
- **Async/Await**: Non-blocking operations for high concurrency