use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Clone)]
pub enum TransactionState{
    EXECUTING,
    SUCCESS,
    FAILURE,
    ROLLBACK
}

// manage requests in one transaction
#[derive(Debug, Clone)]
pub struct Transaction{
    pub state: TransactionState,
    pub transaction_id: u64,
    pub operation_history: Vec<String>
}

impl Transaction{
    pub fn new() -> Self{
        Self{
            state: TransactionState::EXECUTING,
            transaction_id: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            operation_history: Vec::new()
        }
    }

    pub fn add_operation_to_history(&mut self, operation: String){
        self.operation_history.push(operation);
    }

    pub fn commit(&mut self){
        self.state = TransactionState::SUCCESS;
    }

    pub fn fail(&mut self){
        self.state = TransactionState::FAILURE;
    }

    pub fn rollback(&mut self){
        self.state = TransactionState::ROLLBACK;
    }
}

// manage all the transactions 
pub struct TransactionManager{
    pub transactions: Arc<RwLock<HashMap<u64, Transaction>>>
}

impl TransactionManager{
    pub fn new() -> Self{
        Self{
            transactions: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub async fn create_transaction(&self) -> Transaction{
        let transaction = Transaction::new();
        let temp = transaction.clone();
        self.transactions.write().await.insert(transaction.transaction_id, transaction);
        info!("Transaction created: {}", temp.transaction_id);
        temp
    }

    pub async fn delete_transaction(&self, transaction_id: u64){
        self.transactions.write().await.remove(&transaction_id);
        info!("Transaction deleted: {}", transaction_id);
    }

    pub async fn get_transaction(&self, transaction_id: u64) -> Option<Transaction>{
        self.transactions.read().await.get(&transaction_id).cloned()
    }

    pub async fn update_transaction<F>(&self, transaction_id: u64, f: F) -> bool
    where
        F: FnOnce(&mut Transaction),
    {
        let mut txs = self.transactions.write().await;
        if let Some(tx) = txs.get_mut(&transaction_id) {
            f(tx);
            return true;
        }
        false
    }

    pub async fn show_info(&self){
        let txs = self.transactions.read().await;
        for (transaction_id, transaction) in txs.iter(){
            println!("transaction id: {}", transaction_id);
            println!("transaction state: {:?}",transaction.state);
            println!("transaction operation history: {:?}", transaction.operation_history);
        }
    }
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            transactions: self.transactions.clone(),
        }
    }
}