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
    execute_query_short("DROP TABLE IF EXISTS test_main;".to_string()).await;
    execute_query_short("DROP TABLE IF EXISTS test_orders;".to_string()).await;
    
    // 主表
    execute_query_short("CREATE TABLE test_main (id INTEGER PRIMARY KEY, val VARCHAR(255), category INTEGER, INDEX(category));".to_string()).await;
    // 关联表 (用于 JOIN 测试)
    execute_query_short("CREATE TABLE test_orders (oid INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT);".to_string()).await;

    // 预填充部分数据用于查询测试
    for i in 1..=100 {
        execute_query_short(format!("INSERT INTO test_main VALUES ({}, 'val_{}', {});", i, i, i % 5)).await;
        execute_query_short(format!("INSERT INTO test_orders VALUES ({}, {}, {}.5);", i, i, i)).await;
    }
}

fn bench_db_suites(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(setup_db());

    // --- 分组 1: 基础 DML (点查/插入) ---
    let mut g1 = c.benchmark_group("Basic-Operations");
    g1.measurement_time(Duration::from_secs(15));

    g1.bench_function("point_select_indexed", |b| {
        b.to_async(&rt).iter(|| async {
            execute_query_short("SELECT * FROM test_main WHERE id = 50;".to_string()).await;
        });
    });

    g1.bench_function("transaction_insert", |b| {
        b.to_async(&rt).iter(|| async {
            let id = GLOBAL_ID.fetch_add(1, Ordering::SeqCst);
            // 测试显式事务开销
            let sql = format!("BEGIN TRANSACTION; INSERT INTO test_main VALUES ({}, 'tx', 1); COMMIT;", id);
            execute_query_short(sql).await;
        });
    });
    g1.finish();

    // --- 分组 2: 复杂查询 (JOIN/聚合) ---
    let mut g2 = c.benchmark_group("Analytical-Queries");
    g2.measurement_time(Duration::from_secs(15));

    g2.bench_function("inner_join_simple", |b| {
        b.to_async(&rt).iter(|| async {
            // 简单两表关联
            execute_query_short("SELECT t.val, o.amount FROM test_main t INNER JOIN test_orders o ON t.id = o.user_id WHERE t.id = 10;".to_string()).await;
        });
    });

    g2.bench_function("group_by_aggregation", |b| {
        b.to_async(&rt).iter(|| async {
            // 测试聚合函数性能
            execute_query_short("SELECT category, COUNT(*), AVG(id) FROM test_main GROUP BY category;".to_string()).await;
        });
    });
    g2.finish();
}

criterion_group!(benches, bench_db_suites);
criterion_main!(benches);