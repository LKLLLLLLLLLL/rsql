use super::thread_pool::WorkingThreadPool;
use super::types::{RayonQueryRequest, WebsocketResponse, RayonQueryResponse};

use actix_web_actors::ws;
use actix::{Actor, StreamHandler, AsyncContext};
use serde_json;
use tracing::{info, error};
use futures::executor;

use std::time::{SystemTime, UNIX_EPOCH, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// one SQLWebsocketActor corresponds to one websocket connection and multiple transactions
pub struct SQLWebsocketActor{
    working_thread_pool: Arc<WorkingThreadPool>,
    working_query: Arc<AtomicU64>,
    current_connection_id: u64,
    authenticated: bool,
    username: String,
}

impl Actor for SQLWebsocketActor{
    type Context = ws::WebsocketContext<Self>;

    //start the websocket connection
    fn started(&mut self, ctx: &mut Self::Context) {
        info!("WebSocket connection established, connection_id: {}, username: {}, authenticated: {}", 
              self.current_connection_id, self.username, self.authenticated);
        
        // send confimation message to the front-end
        let welcome_msg = WebsocketResponse {
            rayon_response: RayonQueryResponse {
                response_content: Vec::new(),
                error: String::from("Websocket Connection Established"),
                execution_time: 0,
            },
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            success: true,
            connection_id: self.current_connection_id,
        };
        
        if let Ok(json_msg) = serde_json::to_string(&welcome_msg) {
            ctx.text(json_msg);
        }

        let thread_pool = self.working_thread_pool.clone();
        let connection_id = self.current_connection_id;
        let addr = ctx.address().clone();
        
        ctx.run_interval(std::time::Duration::from_secs(60), move |_act, _ctx| {
            let thread_pool = thread_pool.clone();
            let addr = addr.clone();
            
            actix::spawn(async move {
                match thread_pool.make_checkpoint(connection_id).await {
                    Ok(msg) => {
                        info!("Checkpoint successful: {}", msg);
                        let checkpoint_msg = WebsocketResponse {
                            rayon_response: RayonQueryResponse {
                                response_content: Vec::new(),
                                error: String::from("Checkpoint Success"),
                                execution_time: 0,
                            },
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            success: true,
                            connection_id,
                        };
                        
                        if let Ok(json_msg) = serde_json::to_string(&checkpoint_msg) {
                            addr.do_send(SendTextMessage { json: json_msg });
                        }
                    }
                    Err(e) => error!("Checkpoint failed: {:?}", e),
                }
            });
        });
    }

    //stop the websocket connection
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("WebSocket connection closed, connection_id: {}, username: {}", self.current_connection_id, self.username);

        let thread_pool = self.working_thread_pool.clone();
        let connection_id = self.current_connection_id;
        
        let result = executor::block_on(async {
            thread_pool.rollback(connection_id).await
        });
        
        match result {
            Ok(msg) => info!("Rollback completed for connection {}: {}", connection_id, msg),
            Err(e) => error!("Rollback failed for connection {}: {:?}", connection_id, e),
        }
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for SQLWebsocketActor {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                match serde_json::from_str::<RayonQueryRequest>(&text) {
                    Ok(query_request) => {
                        info!("Received query request on connection {} from user: {}", 
                              self.current_connection_id, query_request.username);
                        
                        let pool = self.working_thread_pool.clone();
                        let connection_id = self.current_connection_id;
                        let query_counter = self.working_query.clone();
                        
                        let addr = ctx.address();
                        
                        actix::spawn(async move {
                            let start = Instant::now();
                            
                            let current = query_counter.fetch_add(1, Ordering::SeqCst) + 1;
                            info!("Current working query count: {}", current);
                            
                            let result = pool.parse_and_execute_query(query_request, connection_id).await;
                            
                            let current_after = query_counter.fetch_sub(1, Ordering::SeqCst) - 1;
                            info!("Query execution completed. Current working query count: {}", 
                                  current_after);
                            
                            let exec_ms = start.elapsed().as_millis() as u64;
                            
                            let response = match result {
                                Ok(content) => WebsocketResponse {
                                    rayon_response: RayonQueryResponse {
                                        response_content: content,
                                        error: String::from("Query Success"),
                                        execution_time: exec_ms,
                                    },
                                    timestamp: SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    success: true,
                                    connection_id,
                                },
                                Err(e) => WebsocketResponse {
                                    rayon_response: RayonQueryResponse {
                                        response_content: Vec::new(),
                                        error: e.to_string(),
                                        execution_time: exec_ms,
                                    },
                                    timestamp: SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    success: false,
                                    connection_id,
                                },
                            };

                            //send back
                            if let Ok(json_response) = serde_json::to_string(&response) {
                                addr.do_send(SendTextMessage { json: json_response });
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to parse message as RayonQueryRequest on connection {}: {}", 
                               self.current_connection_id, e);
                        
                        let error_response = WebsocketResponse {
                            rayon_response: RayonQueryResponse {
                                response_content: Vec::new(),
                                error: format!("Invalid request format: {}", e),
                                execution_time: 0,
                            },
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            success: false,
                            connection_id: self.current_connection_id,
                        };
                        
                        if let Ok(json_msg) = serde_json::to_string(&error_response) {
                            ctx.text(json_msg);
                        }
                    }
                }
            }
            Ok(ws::Message::Close(reason)) => {
                info!("WebSocket connection {} closing: {:?}", 
                      self.current_connection_id, reason);
                
                if let Some(reason) = reason {
                    ctx.close(Some(reason));
                } else {
                    ctx.close(None);
                }
                
            }
            Ok(ws::Message::Ping(msg)) => {
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {

            }
            Ok(ws::Message::Binary(_)) => {

            }
            Ok(ws::Message::Continuation(_)) => {

            }
            Ok(ws::Message::Nop) => {

            }
            Err(e) => {
                error!("WebSocket protocol error: {:?}", e);
                ctx.close(Some(ws::CloseReason {
                    code: ws::CloseCode::Error,
                    description: Some("Protocol error".to_string()),
                }));
            }
        }
    }
}

impl SQLWebsocketActor{
    pub fn new(
        working_thread_pool: Arc<WorkingThreadPool>,
        working_query: Arc<AtomicU64>,
        current_connection_id: u64,
        authenticated: bool,
        username: String,
    ) -> Self{
        Self{
            working_thread_pool,
            working_query,
            current_connection_id,
            authenticated,
            username
        }
    }
}

//send result back to the front-end
#[derive(actix::Message)]
#[rtype(result = "()")]
struct SendTextMessage {
    pub json: String,
}

impl actix::Handler<SendTextMessage> for SQLWebsocketActor {
    type Result = ();
    
    fn handle(&mut self, msg: SendTextMessage, ctx: &mut Self::Context) -> Self::Result {
        ctx.text(msg.json);
    }
}