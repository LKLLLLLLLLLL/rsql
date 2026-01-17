use actix_web::{web, App, HttpServer, HttpRequest, HttpResponse, Responder, Error};
use actix_web_actors::ws;
use actix_files::Files;
use serde::Deserialize;
use tracing::info;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH, Instant};

use crate::config::WEB_PORT;
use crate::server::websocket_actor::WebsocketActor;
use crate::server::thread_pool::WorkingThreadPool;
use crate::server::types::{HttpQueryRequest, HttpQueryResponse, RayonQueryResponse};
use crate::storage::wal::WAL;

// WebSocket 连接查询参数
#[derive(Deserialize)]
struct WsQueryParams {
    username: Option<String>,
    password: Option<String>,
}

// 应用全局状态，存储数据库连接池和查询计数器
struct AppState {
    working_thread_pool: Arc<WorkingThreadPool>,
    working_query: Arc<AtomicU64>,
}

/// 处理WebSocket连接
/// 当浏览器连接到 ws://localhost:4456/ws?username=xxx&password=yyy 时调用
async fn handle_ws_query(
    request: HttpRequest,
    query: web::Query<WsQueryParams>,
    stream: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    info!("WebSocket connection requested from: {:?}", request.peer_addr());

    // 从URL查询参数中提取username和password
    let username = query.username.clone().unwrap_or_else(|| "guest".to_string());
    let password = query.password.clone().unwrap_or_else(|| "".to_string());
    
    info!("WebSocket connection attempt - username: {}", username);
    
    // 生成唯一的连接ID（使用时间戳）
    let connection_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    ws::start(
        WebsocketActor::new(
            state.working_thread_pool.clone(),
            state.working_query.clone(),
            connection_id,
            username,
            password,
        ),
        &request,
        stream,
    )
}

/// 处理HTTP API请求（POST /api/query）
/// 前端发送SQL查询请求时调用
async fn handle_http_query(
    query_request: web::Json<HttpQueryRequest>,
    state: web::Data<AppState>,
) -> impl Responder {
    info!("Received HTTP query request: {:?}", query_request);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let start = Instant::now();

    // 增加当前处理中的查询计数
    let current = state.working_query.fetch_add(1, Ordering::SeqCst) + 1;
    info!("Current working query count: {}", current);

    let request = query_request.rayon_request.clone();
    
    // 执行查询
    let result = state.working_thread_pool.parse_and_execute_query(request, 0).await;

    // 减少查询计数
    let current_after = state.working_query.fetch_sub(1, Ordering::SeqCst) - 1;
    info!("Query execution completed. current working query count: {}", current_after);

    let exec_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(result) => {
            let response = HttpQueryResponse {
                rayon_response: RayonQueryResponse {
                    response_content: result,
                    error: String::new(),
                    execution_time: exec_ms,
                },
                timestamp: now,
                success: true,
            };
            HttpResponse::Ok().json(response)
        }
        Err(e) => {
            let response = HttpQueryResponse {
                rayon_response: RayonQueryResponse {
                    response_content: String::new(),
                    error: e.to_string(),
                    execution_time: exec_ms,
                },
                timestamp: now,
                success: false,
            };
            HttpResponse::InternalServerError().json(response)
        }
    }
}

/// SPA路由处理器
/// 当用户访问不存在的路径（如 /database、/users）时，
/// 返回 index.html，让Vue Router接管路由
async fn spa_fallback() -> impl Responder {
    // 返回 index.html 的内容，让前端Vue Router处理路由
    let index_html = std::fs::read_to_string("./client/dist/index.html")
        .unwrap_or_else(|_| {
            r#"<!DOCTYPE html>
<html>
<head><title>RSQL</title></head>
<body>
    <div id="app"></div>
    <p>无法加载应用。请确保已运行 <code>npm run build</code> 编译前端代码。</p>
</body>
</html>"#.to_string()
        });
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(index_html)
}

pub async fn start_server() -> std::io::Result<()> {
    info!("Starting Web Server...");
    
    // 检查Vue编译输出文件夹是否存在
    if !Path::new("./client/dist").exists() {
        tracing::warn!("./client/dist 文件夹不存在!");
        tracing::warn!("请先运行: cd client && npm run build");
    }

    // ========== 关键：初始化数据库（WAL Recovery）==========
    // 必须在任何数据库操作之前调用，否则会panic
    info!("Initializing database: performing WAL recovery...");
    let _wal = WAL::global();
    if let Err(e) = WAL::recovery(
        &mut |_table_id, _page_id, _data| Ok(()),
        &mut |_table_id, _page_id, _offset, _len, _data| Ok(()),
        &mut |_table_id| Ok(0),
        &mut |_table_id| Ok(()),
        &mut |_table_id| Ok(0),
    ) {
        tracing::error!("WAL recovery failed: {:?}", e);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "WAL recovery failed"));
    }
    info!("WAL recovery completed successfully");
    
    // 初始化数据库连接池
    let working_thread_pool = Arc::new(WorkingThreadPool::new());
    working_thread_pool.show_info();
    
    let state = web::Data::new(AppState {
        working_thread_pool,
        working_query: Arc::new(AtomicU64::new(0)),
    });

    // 创建应用配置（闭包）
    let app_factory = move || {
        App::new()
            .app_data(state.clone())
            // ========== 后端API路由 ==========
            // WebSocket路由：接收前端SQL查询
            .route("/ws", web::get().to(handle_ws_query))
            // HTTP API路由：用于单次查询请求
            .route("/api/query", web::post().to(handle_http_query))
            
            // ========== 静态文件服务 ==========
            // 提供Vue编译后的所有静态文件（JS、CSS、图片等）
            .service(
                Files::new("/", "./client/dist")
                    .index_file("index.html")
                    .use_last_modified(true)
            )
            
            // ========== SPA路由回退 ==========
            // 当用户访问 /database、/users 等路由时
            // 返回 index.html，由Vue Router处理
            .default_service(web::get().to(spa_fallback))
    };

    // 创建主服务器（4456 端口，向后兼容）
    let server1 = HttpServer::new(app_factory.clone())
        .bind(("127.0.0.1", WEB_PORT))?
        .run();
    
    info!("Web Server listening on 127.0.0.1:{}", WEB_PORT);

    // 创建第二个服务器（4455 端口，同事使用）
    let server2 = HttpServer::new(app_factory)
        .bind(("127.0.0.1", 4455))?
        .run();
    
    info!("Web Server listening on 127.0.0.1:4455");
    
    // 同时运行两个服务器
    tokio::select! {
        result1 = server1 => result1,
        result2 = server2 => result2,
    }
}
