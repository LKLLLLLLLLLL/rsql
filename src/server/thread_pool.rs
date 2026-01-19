use rayon::ThreadPoolBuilder;
use num_cpus;
use tracing::{error, info};
use futures::channel::oneshot;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use crate::execution::executor;
use crate::execution::result::ExecutionResult;
use crate::{config::THREAD_MAXNUM};
use crate::common::{ RsqlResult};
use super::types::{ RayonQueryRequest };
use crate::execution::execute;

pub struct WorkingThreadPool{
    thread_pool: rayon::ThreadPool,
    max_thread_num: usize,
    serialize_lock: Arc<Mutex<HashMap<u64, Arc<Mutex<bool>>>>>
}

impl WorkingThreadPool{
    pub fn new() -> Self{
        if THREAD_MAXNUM == 0 {
            let detected = num_cpus::get();
            return Self{
                thread_pool: ThreadPoolBuilder::new()
                   .num_threads(detected)
                   .build()
                   .unwrap(),
                max_thread_num: detected,
                serialize_lock: Arc::new(Mutex::new(HashMap::new()))
            }
        }
        Self{
            thread_pool: ThreadPoolBuilder::new()
                .num_threads(THREAD_MAXNUM)
                .build()
                .unwrap(),
            max_thread_num: THREAD_MAXNUM,
            serialize_lock: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub async fn validate(&self, connection_id: u64, username: &str, password: &str) -> RsqlResult<bool> {
        let (sender, receiver) = oneshot::channel::<RsqlResult<bool>>();

        let serialize_lock = self.serialize_lock.clone();
        let username = username.to_string();
        let password = password.to_string();

        self.thread_pool.spawn(move || {
            let conn_mutex = {
                let mut map_guard = serialize_lock.lock().unwrap();
                map_guard.entry(connection_id).or_insert_with(|| {
                    Arc::new(Mutex::new(true))
                }).clone()
            };

            let _conn_guard = conn_mutex.lock().unwrap();

            match executor::validate_user(&username, &password){
                Ok(valid) => {
                    sender.send(Ok(valid)).unwrap();
                }
                Err(e) => {
                    sender.send(Err(e)).unwrap();
                }
            }
                
        });
        
        let result = receiver.await.unwrap();
        match result {
            Ok(valid) => Ok(valid),
            Err(e) => Err(e)
        }
    }

    pub async fn parse_and_execute_query(&self, query: RayonQueryRequest, connection_id: u64) -> RsqlResult<Vec<ExecutionResult>> {
        let (sender, receiver) = oneshot::channel::<RsqlResult<Vec<ExecutionResult>>>();

        let serialize_lock = self.serialize_lock.clone();

        self.thread_pool.spawn(move ||{

            let conn_mutex = {
                let mut map_guard = serialize_lock.lock().unwrap();
                map_guard.entry(connection_id).or_insert_with(|| {
                    Arc::new(Mutex::new(true))
                }).clone()
            };

            let _conn_guard = conn_mutex.lock().unwrap();

            let result = execute(&query.request_content,connection_id);

            match result {
                Ok(results) => {
                    sender.send(Ok(results));
                }
                Err(e) => {
                    sender.send(Err(e));
                }
            }
        });
        let result = receiver.await.unwrap();
        match result {
            Ok(result) => Ok(result),
            Err(e) => Err(e)
        }
    }

    pub async fn rollback(&self, connection_id: u64) -> RsqlResult<String> {
        let (sender, receiver) = oneshot::channel::<RsqlResult<String>>();

        let serialize_lock = self.serialize_lock.clone();

        self.thread_pool.spawn(move ||{
            let conn_mutex = {
                let mut map_guard = serialize_lock.lock().unwrap();
                map_guard.entry(connection_id).or_insert_with(|| {
                    Arc::new(Mutex::new(true))
                }).clone()
            };

            let _conn_guard = conn_mutex.lock().unwrap();

            match executor::disconnect_callback(connection_id){
                Ok(_) => {
                    sender.send(Ok(format!("rollbacking transaction for connection id: {}", connection_id))).unwrap();
                }
                Err(e) => {
                    sender.send(Err(e)).unwrap();
                }
            }
        });
        let result = receiver.await.unwrap();
        match result {
            Ok(result) => Ok(result),
            Err(e) => Err(e)
        }
    }

    pub async fn make_checkpoint(&self, connection_id: u64) -> RsqlResult<String> {

        let (sender, receiver) = oneshot::channel::<RsqlResult<String>>();

        let serialize_lock = self.serialize_lock.clone();

        self.thread_pool.spawn(move ||{
            let conn_mutex = {
                let mut map_guard = serialize_lock.lock().unwrap();
                map_guard.entry(connection_id).or_insert_with(|| {
                    Arc::new(Mutex::new(true))
                }).clone()
            };

            let _conn_guard = conn_mutex.lock().unwrap();

            match executor::checkpoint(){
                Ok(_) => {
                    sender.send(Ok(format!("checkpointing for connection id: {}", connection_id))).unwrap();
                }
                Err(e) => {
                    sender.send(Err(e)).unwrap();
                }
            }
        });
        let result = receiver.await.unwrap();
        match result {
            Ok(result) => Ok(result),
            Err(e) => Err(e)
        }

    }

    pub fn show_info(&self){
        info!("max thread num: {}", self.max_thread_num);
        info!("thread pool:{:?}",self.thread_pool)
    }
}