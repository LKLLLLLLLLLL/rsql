use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use actix_web::middleware::NormalizePath;
use actix_web_actors::ws;
use tokio::time::interval;
use tracing::{info, error};
use rust_embed::RustEmbed;

use crate::config::{PORT, BACKUP_INTERVAL_SECS, CHECKPOINT_INTERVAL_SECS};
use crate::server::conncetion_user_map::ConnectionUserMap;
use super::sqlserver_actor::SQLWebsocketActor;
use super::thread_pool::WorkingThreadPool;

use std::sync::atomic::{AtomicU64};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

#[derive(RustEmbed)]
#[folder = "client/dist"] 
struct Assets;

async fn handle_static_files(path: web::Path<String>) -> HttpResponse {
    let path = if path.is_empty() {
        "index.html".to_string()
    } else {
        path.into_inner()
    };
    
    match Assets::get(&path) {
        Some(content) => {
            let body = content.data.into_owned();
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            
            HttpResponse::Ok()
                .content_type(mime.as_ref())
                .body(body)
        }
        None => {
            match Assets::get("index.html") {
                Some(content) => {
                    let body = content.data.into_owned();
                    HttpResponse::Ok()
                        .content_type("text/html")
                        .body(body)
                }
                None => HttpResponse::NotFound().body("404 Not Found"),
            }
        }
    }
}

async fn index() -> HttpResponse {
    match Assets::get("index.html") {
        Some(content) => {
            let body = content.data.into_owned();
            HttpResponse::Ok()
                .content_type("text/html")
                .body(body)
        }
        None => HttpResponse::NotFound().body("index.html not found"),
    }
}

struct AppState{
    working_thread_pool: Arc<WorkingThreadPool>,
    working_query: Arc<AtomicU64>,
}

fn url_decode(encoded: &str) -> String {
    let mut result = String::new();
    let mut chars = encoded.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '%' => {
                let hex1 = chars.next();
                let hex2 = chars.peek().cloned();
                
                if let (Some(h1), Some(h2)) = (hex1, hex2) {
                    let hex_str = format!("{}{}", h1, h2);
                    if let Ok(byte_val) = u8::from_str_radix(&hex_str, 16) {
                        result.push(byte_val as char);
                        chars.next();
                    } else {
                        result.push('%');
                        result.push(h1);
                        if let Some(h2) = chars.peek().cloned() {
                            result.push(h2);
                        }
                    }
                } else {
                    result.push('%');
                    if let Some(h1) = hex1 {
                        result.push(h1);
                    }
                }
            }
            '+' => result.push(' '),
            _ => result.push(ch),
        }
    }

    result
}

//handle websocket connection
async fn handle_ws_query(
    request: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>
)-> Result<HttpResponse, actix_web::Error> {
    info!("WebSocket connection requested from: {:?}", request.peer_addr());

    let query_params = request.query_string();
    let mut username = String::new();
    let mut password = String::new();

    for param in query_params.split('&'){
        if let Some((key,value)) = param.split_once('='){
            match key{
                "username"=> username = url_decode(value),
                "password"=> password = url_decode(value),
                _=>{}
            }
        }
    }
    info!("Received username: {}, password: {}", username, password);

    let thread_pool = state.working_thread_pool.clone();
    match thread_pool.validate(0, &username, &password).await {// use connection_id 0 to validate user
        Ok(true) => {
            let connection_id = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
            let username_tmp = username.clone();
            ConnectionUserMap::global().insert_connection(connection_id, username_tmp);
            ws::start(
                SQLWebsocketActor::new(
                    state.working_thread_pool.clone(),
                    state.working_query.clone(),
                    connection_id,
                    true,
                    username,
                ),
                &request,
                stream,
            )
        }
        Ok(false) => {
            info!("Invalid username or password");
            Err(actix_web::error::ErrorUnauthorized("Invalid username or password"))
        }
        Err(_) => {
            info!("Internal server error");
            Err(actix_web::error::ErrorInternalServerError("Internal server error"))
        }
    }
}

fn start_scheduled_tasks(thread_pool: Arc<WorkingThreadPool>) {
    let backup_pool = thread_pool.clone();
    actix::spawn(async move {
        let mut interval = interval(Duration::from_secs(BACKUP_INTERVAL_SECS));
        
        info!("Scheduled backup task started, interval: {} seconds", BACKUP_INTERVAL_SECS);
        
        interval.tick().await;
        
        loop {
            interval.tick().await;
            
            match backup_pool.make_backup().await {
                Ok(msg) => info!("Scheduled backup successful: {}", msg),
                Err(e) => error!("Scheduled backup failed: {:?}", e),
            }
        }
    });
    
    let checkpoint_pool = thread_pool.clone();
    actix::spawn(async move {
        let mut interval = interval(Duration::from_secs(CHECKPOINT_INTERVAL_SECS));
        
        info!("Scheduled checkpoint task started, interval: {} seconds", CHECKPOINT_INTERVAL_SECS);
        
        loop {
            interval.tick().await;
            
            match checkpoint_pool.make_checkpoint().await {
                Ok(msg) => info!("Scheduled checkpoint successful: {}", msg),
                Err(e) => error!("Scheduled checkpoint failed: {:?}", e),
            }
        }
    });
}

pub async fn start_server() -> std::io::Result<()> {
    info!("Starting server on port {}", PORT);
    let working_thread_pool = Arc::new(WorkingThreadPool::new());
    let state = web::Data::new(AppState{
        working_thread_pool,
        working_query: Arc::new(AtomicU64::new(0)),
    });
    let working_thread_pool_schedule = state.working_thread_pool.clone();
    start_scheduled_tasks(working_thread_pool_schedule);
    state.working_thread_pool.show_info();
    HttpServer::new( move || {
        App::new()
            .app_data(state.clone())
            .route("/ws",web::get().to(handle_ws_query))
            .route("/", web::get().to(index))
            .route("/{path:.*}", web::get().to(handle_static_files))
            .wrap(NormalizePath::trim())
    })
    .bind(("127.0.0.1",PORT))?
    .run()
    .await
}