use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use tracing::info;
use crate::config::{PORT};
use super::thread_pool::WorkingThreadPool;
use super::types::{HttpQueryRequest, HttpQueryResponse};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH, Instant};

struct AppState{
    working_thread_pool: WorkingThreadPool,
    working_query: AtomicU64
}

async fn handle_query(
    query_request: web::Json<HttpQueryRequest>,
    state: web::Data<AppState>
)-> impl Responder {

    info!("Received query request: {:?}", query_request);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let start = Instant::now();

    let current = state.working_query.fetch_add(1, Ordering::SeqCst) + 1;
    info!("Current working query count: {}", current);

    let result = state.working_thread_pool.parse_and_execute_query(query_request.into_inner()).await;

    let current_after = state.working_query.fetch_sub(1, Ordering::SeqCst) - 1;
    info!("Query execution completed. current working query count: {}", current_after);

    let exec_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(result)=>{
            let response = HttpQueryResponse{
                response_content: result,
                error: String::new(),
                execution_time: exec_ms,
                timestamp: now,
                success: true,
            };
            HttpResponse::Ok().json(response)
        }
        Err(e)=>{
            let response = HttpQueryResponse {
                response_content: String::new(),
                error: e.to_string(),
                execution_time: exec_ms,
                timestamp: now,
                success: false,
            };
            HttpResponse::InternalServerError().json(response)
        }
    }
}

pub async fn start_server() -> std::io::Result<()> {
    info!("Starting server on port {}", PORT);
    let working_thread_pool = WorkingThreadPool::new();
    let state = web::Data::new(AppState{
        working_thread_pool,
        working_query: AtomicU64::new(0)
    });
    state.working_thread_pool.show_info();
    HttpServer::new( move || {
        App::new()
            .app_data(state.clone())
            .route("/query", web::post().to(handle_query))
    })
    .bind(("127.0.0.1",PORT))?
    .run()
    .await
}