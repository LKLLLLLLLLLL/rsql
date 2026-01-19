use serde::{Serialize, Deserialize};
use crate::execution::result::ExecutionResult;
use serde_json::Value;

/* http request/response structure is reserved for compatibility of single-query transaction */
// http request structure received from the front-end
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct HttpQueryRequest {
//     pub rayon_request: RayonQueryRequest,
// }

// // http response structure to be sent to the front-end
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct HttpQueryResponse {
//     pub rayon_response: RayonQueryResponse,
//     pub timestamp: u64,
//     pub success: bool,
// }

/* rayon request/response structure */
// request sent to the rayon thread pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RayonQueryRequest {
    pub username: String,
    pub userid: u64,
    pub request_content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniformedResult {
    pub result_type: String,      
    pub data: Value,              
}
// response sent from the rayon thread pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RayonQueryResponse {
    pub response_content: Vec<ExecutionResult>,  
    pub uniform_result: Vec<UniformedResult>,   
    pub error: String,
    pub execution_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketResponse {
    pub rayon_response: RayonQueryResponse,
    pub timestamp: u64,
    pub success: bool,
    pub connection_id: u64,
}