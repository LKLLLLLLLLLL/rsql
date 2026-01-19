use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const WS_URL: &str = "ws://localhost:4456/ws?username=root&password=password";

// 用于生成全局唯一的 ID
static GLOBAL_ID: AtomicU64 = AtomicU64::new(100000);

async fn execute_query_short(sql: String) {
    let (mut ws_stream, _) = connect_async(WS_URL).await.expect("Failed to connect");
    let payload = json!({
        "username": "root",
        "userid": 1,
        "request_content": sql
    });
    ws_stream.send(Message::Text(payload.to_string())).await.unwrap();
    let _ = ws_stream.next().await;
}

async fn setup_db() {
    execute_query_short("DROP TABLE IF EXISTS test_main; DROP TABLE IF EXISTS test_orders;".to_string()).await;
    
    // main table
    execute_query_short("CREATE TABLE test_main (id INTEGER PRIMARY KEY, val VARCHAR(255), category INTEGER, INDEX(category));".to_string()).await;
    // related table (for JOIN tests)
    execute_query_short("CREATE TABLE test_orders (oid INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT);".to_string()).await;

    // reduce data volume: pre-fill 20 records
    for i in 1..=20 {
        execute_query_short(format!(
            "INSERT INTO test_main(id, val, category) VALUES ({}, 'val_{}', {}); INSERT INTO test_orders(oid, user_id, amount) VALUES ({}, {}, {}.5);", 
            i, i, i % 5, i, i, i
        )).await;
    }
}

fn bench_db_suites(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(setup_db());

    // --- Group 1: Basic Operations (SELECT/UPDATE/INSERT) ---
    let mut g1 = c.benchmark_group("Basic-Operations");
    g1.measurement_time(Duration::from_secs(5)); // Shorten individual test time

    g1.bench_function("point_select_indexed", |b| {
        b.to_async(&rt).iter(|| async {
            execute_query_short("SELECT * FROM test_main WHERE id = 25;".to_string()).await;
        });
    });

    g1.bench_function("range_scan_simple", |b| {
        b.to_async(&rt).iter(|| async {
            // test simple range scan
            execute_query_short("SELECT * FROM test_main WHERE id > 20 AND id < 30;".to_string()).await;
        });
    });

    g1.bench_function("update_single_row", |b| {
        b.to_async(&rt).iter(|| async {
            execute_query_short("UPDATE test_main SET val = 'updated' WHERE id = 10;".to_string()).await;
        });
    });

    g1.bench_function("transaction_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let id = GLOBAL_ID.fetch_add(1, Ordering::SeqCst);
            let sql = format!("BEGIN TRANSACTION; INSERT INTO test_main(id, val, category) VALUES ({}, 'tx', 1); COMMIT;", id);
            execute_query_short(sql).await;
        });
    });
    g1.finish();

    // --- Group 2: Complex Queries (JOIN/Subqueries/Aggregations) ---
    let mut g2 = c.benchmark_group("Analytical-Queries");
    g2.measurement_time(Duration::from_secs(5));

    g2.bench_function("inner_join_simple", |b| {
        b.to_async(&rt).iter(|| async {
            execute_query_short("SELECT t.val, o.amount FROM test_main t INNER JOIN test_orders o ON t.id = o.user_id WHERE t.id = 10;".to_string()).await;
        });
    });

    g2.bench_function("subquery_simple", |b| {
        b.to_async(&rt).iter(|| async {
            // simple subquery
            execute_query_short("SELECT * FROM (SELECT id, val FROM test_main WHERE category = 1) WHERE id < 20;".to_string()).await;
        });
    });

    g2.bench_function("group_by_aggregation", |b| {
        b.to_async(&rt).iter(|| async {
            execute_query_short("SELECT category, COUNT(*), MAX(id), AVG(id) FROM test_main GROUP BY category;".to_string()).await;
        });
    });
    g2.finish();

    // --- Group 3: System and Permissions (DCL/DDL) ---
    let mut g3 = c.benchmark_group("System-Operations");
    g3.measurement_time(Duration::from_secs(5));

    g3.bench_function("dcl_create_user", |b| {
        b.to_async(&rt).iter(|| async {
            // Note: Due to short connections requiring auth each time, frequent operations on the user table may affect stability. Here we simulate the creation logic.
            let user_id = GLOBAL_ID.fetch_add(1, Ordering::SeqCst);
            execute_query_short(format!("CREATE USER user_{} PASSWORD 'pass_{}';", user_id, user_id)).await;
        });
    });
    g3.finish();
}

criterion_group!(benches, bench_db_suites);
criterion_main!(benches);