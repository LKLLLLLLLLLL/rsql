use serde::{Serialize,Deserialize};
/* http request/response structure is reserved for compatibility of single-query transaction */
//http request structure received from the front-end
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpQueryRequest{
    pub rayon_request: RayonQueryRequest,
}
// http response structure to be sent to the front-end
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpQueryResponse{
    pub rayon_response: RayonQueryResponse,
    pub timestamp: u64,
    pub success: bool
}

/* rayon request/response structure */
// request sent to the rayon thread pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RayonQueryRequest{
    pub username: String,
    pub userid: u64,
    pub request_content: String
}
#[derive(Debug, Clone, Serialize, Deserialize)]
// response sent from the rayon thread pool
pub struct RayonQueryResponse{
    pub response_content: String,
    pub error: String,
    pub execution_time: u64
}

/* websocket request/response structure used for transmitting data between the server and client */
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command")]//use command to differentiate between different types of requests
#[serde(rename_all = "lowercase")]//use lowercase to convert the enum variant to lowercase
pub enum WebsocketRequestType{
    #[serde(rename = "begin")]
    TransactionBegin,
    #[serde(rename = "commit")]
    TransactionCommit,
    #[serde(rename = "rollback")]
    TransactionRollback,
    #[serde(rename = "query")]
    Query(RayonQueryRequest)//for example, {"command":"query","rayon_request":{"username":"user1","userid":1,"request_content":"select * from users"}}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketResponse {
    pub rayon_response: RayonQueryResponse,
    pub timestamp: u64,
    pub success: bool,
    pub tx_id: u64
}
