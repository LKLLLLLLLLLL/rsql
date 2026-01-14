use serde::{Serialize,Deserialize};
//http request structure received from the front-end
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpQueryRequest{
    pub username: String,
    pub userid: u64,
    pub request_content: String
}
// http response structure to be sent to the front-end
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpQueryResponse{
    pub response_content: String,
    pub error: String,
    pub execution_time: u64,
    pub timestamp: u64,
    pub success: bool
}