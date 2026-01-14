use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use crate::config::{PORT, THREAD_MAXNUM};
use super::types::{HttpQueryRequest, HttpQueryResponse};
use std::time::{SystemTime, UNIX_EPOCH};

async fn handle_query(query_request: web::Json<HttpQueryRequest>)-> impl Responder {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    //execute 

    if true {//query execute failed
        let response = HttpQueryResponse {
            response_content: String::new(),
            error: "default execution failure".to_string(),
            execution_time: 0,
            timestamp: now,
            success: false,
        };
        HttpResponse::Ok().json(response)
    }
    else{//query execute success
        let response = HttpQueryResponse {
            response_content: String::new(),
            error: "default execution failure".to_string(),
            execution_time: 0,
            timestamp: now,
            success: false,
        };
        HttpResponse::Ok().json(response)
    }
}

pub async fn start_server() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .route("/query", web::post().to(handle_query))
    })
    .bind(("127.0.0.1",PORT))?
    .run()
    .await
}