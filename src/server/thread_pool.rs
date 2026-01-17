use rayon::ThreadPoolBuilder;
use num_cpus;
use tracing::{error, info};
use futures::channel::oneshot;

use crate::{config::THREAD_MAXNUM};
use crate::common::{ RsqlResult};
use super::types::{ RayonQueryRequest };
use crate::execution::executor;

pub struct WorkingThreadPool{
    thread_pool: rayon::ThreadPool,
    max_thread_num: usize,
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
            }
        }
        Self{
            thread_pool: ThreadPoolBuilder::new()
                .num_threads(THREAD_MAXNUM)
                .build()
                .unwrap(),
            max_thread_num: THREAD_MAXNUM,
        }
    }

    pub async fn parse_and_execute_query(&self, query: RayonQueryRequest, connection_id: u64) -> RsqlResult<String> {
        let (sender, receiver) = oneshot::channel::<RsqlResult<String>>();
        self.thread_pool.spawn(move ||{
            if let Err(err) = executor::execute(&query.request_content,connection_id){
                error!("execute query failed: {:?}", err);
            };
            let result = Ok(query.request_content);
            sender.send(result).unwrap();
        });
        let result = receiver.await.unwrap();
        match result {
            Ok(result) => Ok(result),
            Err(e) => Err(e)
        }
    }

    pub async fn rollback(&self, connection_id: u64) -> RsqlResult<String> {
        self.thread_pool.spawn(move ||{

        });
        let result = format!("rollbacking transaction for connection id: {}", connection_id);
        Ok(result)
    }

    pub fn rollback_sync(&self, connection_id: u64) -> RsqlResult<String> {
        let result = format!("rollbacking transaction (sync) for connection id: {}", connection_id);
        self.thread_pool.spawn(move ||{

        });
        // match result {
        //     Ok(result)=> Ok(result),
        //     Err(e)=> Err(e)
        // }
        Ok(result)
    }

    pub fn show_info(&self){
        info!("max thread num: {}", self.max_thread_num);
        info!("thread pool:{:?}",self.thread_pool)
    }
}