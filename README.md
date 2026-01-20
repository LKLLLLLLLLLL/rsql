# rsql
Rsql is a simple relational database system written in Rust, supporting basic ACID properties and a subset of SQL.

More features please refer to the [documentation](./docs/sql_supports.md).

## Quick Start
1. build dashboard
    ```bash
    cd client
    npm install
    npm run build
    ```
2. build rsql
    ```bash
    cargo build --release
    ```
3. run rsql
    ```bash
    cargo run --release
    ```
4. now you can open the dashboard in your browser at `http://localhost:4456` or use websocket api to `ws://localhost:4456/ws`(for detailed informantion, see [websocket documentation](./docs/websocket.md)). 

## Development
### Build
```bash
cargo build
```

### Check
```bash
cargo check
```

### Test
```bash
cargo test
```

### Performance Test
```bash
cargo bench
```
