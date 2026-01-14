use rayon::ThreadPoolBuilder;
// use crate::{config::THREAD_MAXNUM};
use super::types::{HttpQueryRequest};
use super::super::executor;
use num_cpus;
use tracing::{error};
use crate::db::errors::{RsqlResult,RsqlError};
use futures::channel::oneshot;
pub struct WorkingThreadPool{
    thread_pool: rayon::ThreadPool,
    max_thread_num: usize,
}

impl WorkingThreadPool{
    pub fn new() -> Self{
        let detected = num_cpus::get();
        Self{
            thread_pool: ThreadPoolBuilder::new()
                .num_threads(detected)
                .build()
                .unwrap(),
            max_thread_num: detected,
        }
    }

    pub async fn parse_and_execute_query(&self, query: HttpQueryRequest) -> RsqlResult<String> {
        let (sender, receiver) = oneshot::channel::<RsqlResult<String>>();
        self.thread_pool.spawn(move ||{
            if let Err(err) = executor::execute(&query.request_content){
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
    pub fn show_info(&self){
        println!("max thread num: {}", self.max_thread_num);
        println!("thread pool:{:?}",self.thread_pool)
    }
}