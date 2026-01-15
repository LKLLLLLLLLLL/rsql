use super::thread_pool::WorkingThreadPool;
use super::transaction::TransactionManager;
use super::types::{WebsocketRequestType, RayonQueryRequest, WebsocketResponse, RayonQueryResponse};

use actix_web_actors::ws;
use actix::{Actor, StreamHandler, AsyncContext};
use serde_json;
use tracing::{info, error};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// one WebsocketActor corresponds to one websocket connection and one transaction
#[derive(Clone)]
pub struct WebsocketActor{
    working_thread_pool: Arc<WorkingThreadPool>,
    working_query: Arc<AtomicU64>,
    transaction_manager: Arc<TransactionManager>,
    current_transaction_id: Option<u64>,
}

impl Actor for WebsocketActor{
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("WebSocket connection established");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(tx_id) = self.current_transaction_id {
            info!("Connection closed, auto-rolling back transaction: {}", tx_id);
            let tx_manager = self.transaction_manager.clone();
            
            actix::spawn(async move {
                tx_manager
                    .update_transaction(tx_id, |tx| {
                        tx.rollback();
                    })
                    .await;
                tx_manager.delete_transaction(tx_id).await;
            });
        }
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebsocketActor{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context){
        match msg{
            Ok(ws::Message::Text(text))=>{
                info!("Received WebSocket message: {}", text);
                match serde_json::from_str::<WebsocketRequestType>(&text){//parse the json string to WebsocketRequestType enum
                    Ok(request) => {//match the enum variant
                        match request {
                            WebsocketRequestType::TransactionBegin => {
                                self.handle_transaction_begin(ctx);
                            }
                            WebsocketRequestType::Query(rayon_request) => {
                                self.handle_query(rayon_request, ctx);
                            }
                            WebsocketRequestType::TransactionCommit => {
                                self.handle_transaction_commit(ctx);
                            }
                            WebsocketRequestType::TransactionRollback => {
                                self.handle_transaction_rollback(ctx);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse JSON: {}", e);
                        ctx.text(serde_json::json!({"error": e.to_string(), "success": false}).to_string());
                    }
                }
            }
            Ok(ws::Message::Close(reason)) => {
                info!("Client requested close");
                ctx.close(reason);
            }
            Ok(ws::Message::Ping(msg)) => {
                ctx.pong(&msg)
            }
            Ok(ws::Message::Pong(_)) => (),
            _ =>{}
        }
    }
}

impl WebsocketActor{
    pub fn new(
        working_thread_pool: Arc<WorkingThreadPool>,
        working_query: Arc<AtomicU64>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self{
        Self{
            working_thread_pool,
            working_query,
            transaction_manager,
            current_transaction_id: None,
        }
    }

    fn handle_transaction_begin(&mut self, ctx: &mut ws::WebsocketContext<Self>){
        let tx_manager = self.transaction_manager.clone();
        let addr = ctx.address();

        actix::spawn(async move {
            let tx = tx_manager.create_transaction().await;
            let tx_id = tx.transaction_id;

            let start = Instant::now();
            
            // send transaction id to itself
            let _ = addr.send(SetTransactionId(tx_id)).await;

            let response = WebsocketResponse {
                rayon_response: RayonQueryResponse {
                    response_content: "Transaction committed successfully".to_string(),
                    error: String::new(),
                    execution_time: start.elapsed().as_millis() as u64,
                },
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                success: true,
                tx_id,
            };
            
            let json_response = serde_json::json!(response);
            let _ = addr.send(SendResponse(json_response.to_string())).await;
        });
    }

    fn handle_transaction_commit(&mut self, ctx: &mut ws::WebsocketContext<Self>){
        if let Some(tx_id) = self.current_transaction_id {
            let tx_manager = self.transaction_manager.clone();
            let addr = ctx.address();

            actix::spawn(async move {
                let start = Instant::now();
                
                if let Some(mut transaction) = tx_manager.get_transaction(tx_id).await {

                    transaction.commit();
                    
                    tx_manager.update_transaction(tx_id, |tx| {
                        tx.commit();
                    }).await;
                    
                    tx_manager.delete_transaction(tx_id).await;

                    let response = WebsocketResponse {
                        rayon_response: RayonQueryResponse {
                            response_content: "Transaction committed successfully".to_string(),
                            error: String::new(),
                            execution_time: start.elapsed().as_millis() as u64,
                        },
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        success: true,
                        tx_id,
                    };
                    let json_response = serde_json::json!(response);
                    let _ = addr.send(SendResponse(json_response.to_string())).await;
                    let _ = addr.send(ClearTransactionId).await;
                }
            });

            self.current_transaction_id = None;
        } else {
            error!("No active transaction to commit");
            ctx.text(serde_json::json!({"error": "No active transaction", "success": false}).to_string());
        }
    }

    fn handle_transaction_rollback(&mut self, ctx: &mut ws::WebsocketContext<Self>){
        if let Some(tx_id) = self.current_transaction_id {
            let tx_manager = self.transaction_manager.clone();
            let addr = ctx.address();

            actix::spawn(async move {
                let start = Instant::now();
                
                tx_manager.update_transaction(tx_id, |tx| {
                    tx.rollback();
                }).await;
                
                tx_manager.delete_transaction(tx_id).await;

                let response = WebsocketResponse {
                    rayon_response: RayonQueryResponse {
                        response_content: "Transaction rolled back".to_string(),
                        error: String::new(),
                        execution_time: start.elapsed().as_millis() as u64,
                    },
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    success: true,
                    tx_id,
                };
                let json_response = serde_json::json!(response);
                let _ = addr.send(SendResponse(json_response.to_string())).await;
                let _ = addr.send(ClearTransactionId).await;
            });

            self.current_transaction_id = None;
        }
    }

    fn handle_query(&mut self, rayon_request: RayonQueryRequest, ctx: &mut ws::WebsocketContext<Self>){
        if self.current_transaction_id.is_none() {
            let tx_manager = self.transaction_manager.clone();
            let addr = ctx.address();

            actix::spawn(async move {
                let tx = tx_manager.create_transaction().await;
                let tx_id = tx.transaction_id;
                let _ = addr.send(SetTransactionId(tx_id)).await;
            });
        }

        let start = Instant::now();
        let tx_id = self.current_transaction_id;
        let thread_pool = self.working_thread_pool.clone();
        let counter = self.working_query.clone();
        let tx_manager = self.transaction_manager.clone();
        let addr = ctx.address();

        counter.fetch_add(1, Ordering::SeqCst);
        let current = counter.load(Ordering::SeqCst);
        info!("Current working query count: {}", current);

        actix::spawn(async move {
            if let Some(tx_id) = tx_id {
                tx_manager
                    .update_transaction(tx_id, |tx| {
                        tx.add_operation_to_history(rayon_request.request_content.clone());
                    })
                    .await;

                let exec_ms = start.elapsed().as_millis() as u64;

                match thread_pool.parse_and_execute_query(rayon_request).await {
                    Ok(result) => {
                        let response = WebsocketResponse {
                            rayon_response: RayonQueryResponse {
                                response_content: result,
                                error: String::new(),
                                execution_time: exec_ms,
                            },
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            success: true,
                            tx_id,
                        };
                        let json_response = serde_json::json!(response);
                        let _ = addr.send(SendResponse(json_response.to_string())).await;
                    }
                    Err(e) => {
                        error!("Query failed: {}", e);
                        let response = WebsocketResponse {
                            rayon_response: RayonQueryResponse {
                                response_content: String::new(),
                                error: e.to_string(),
                                execution_time: exec_ms,
                            },
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            success: false,
                            tx_id,
                        };
                        
                        tx_manager.update_transaction(tx_id, |tx| {
                            tx.fail();
                        }).await;

                        let json_response = serde_json::json!(response);
                        let _ = addr.send(SendResponse(json_response.to_string())).await;
                    }
                }
            }
            counter.fetch_sub(1, Ordering::SeqCst);
            let current_after = counter.load(Ordering::SeqCst);
            info!("Query execution completed. Current working query count: {}", current_after);
        });
    }
}

//use actix::Message to define a message type that can be sent to the actor
//give response to the client
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct SendResponse(pub String);

impl actix::Handler<SendResponse> for WebsocketActor {
    type Result = ();

    fn handle(&mut self, msg: SendResponse, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

//give id to the transaction
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct SetTransactionId(pub u64);

impl actix::Handler<SetTransactionId> for WebsocketActor {
    type Result = ();

    fn handle(&mut self, msg: SetTransactionId, _ctx: &mut Self::Context) {
        self.current_transaction_id = Some(msg.0);
        info!("Transaction ID set: {}", msg.0);
    }
}

//clear the transaction id
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ClearTransactionId;

impl actix::Handler<ClearTransactionId> for WebsocketActor {
    type Result = ();

    fn handle(&mut self, _msg: ClearTransactionId, _ctx: &mut Self::Context) {
        self.current_transaction_id = None;
        info!("Transaction ID cleared");
    }
}