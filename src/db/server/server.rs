use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use tracing::info;

use crate::config::{PORT};
use super::websocket_actor::WebsocketActor;
use super::thread_pool::WorkingThreadPool;
use super::types::{HttpQueryRequest, HttpQueryResponse, RayonQueryResponse};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Instant};

//global state for the server, single instance for the entire server
struct AppState{
    working_thread_pool: Arc<WorkingThreadPool>,
    working_query: Arc<AtomicU64>,
}

//handle websocket connection
async fn handle_ws_query(
    request: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>
)-> Result<HttpResponse, actix_web::Error> {
    info!("WebSocket connection requested from: {:?}", request.peer_addr());

    ws::start(
        WebsocketActor::new(
            state.working_thread_pool.clone(),
            state.working_query.clone(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        ),
        &request,
        stream,
    )
}

//handle http query
async fn handle_http_query(
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

    let request = query_request.rayon_request.clone();

    let result = state.working_thread_pool.parse_and_execute_query(request,0).await;

    let current_after = state.working_query.fetch_sub(1, Ordering::SeqCst) - 1;
    info!("Query execution completed. current working query count: {}", current_after);

    let exec_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(result)=>{
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
        Err(e)=>{
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

pub async fn start_server() -> std::io::Result<()> {
    info!("Starting server on port {}", PORT);
    let working_thread_pool = Arc::new(WorkingThreadPool::new());
    let state = web::Data::new(AppState{
        working_thread_pool,
        working_query: Arc::new(AtomicU64::new(0)),
    });
    state.working_thread_pool.show_info();
    HttpServer::new( move || {
        App::new()
            .app_data(state.clone())
            .route("/query", web::post().to(handle_http_query))
            .route("/ws",web::get().to(handle_ws_query))
    })
    .bind(("127.0.0.1",PORT))?
    .run()
    .await
}