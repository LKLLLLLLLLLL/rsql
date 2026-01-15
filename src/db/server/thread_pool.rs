use rayon::ThreadPoolBuilder;
use num_cpus;
use tracing::{error};
use futures::channel::oneshot;

use crate::{config::THREAD_MAXNUM};
use crate::db::errors::{ RsqlResult};
use super::types::{ RayonQueryRequest };
use super::super::executor;

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

    pub async fn parse_and_execute_query(&self, query: RayonQueryRequest) -> RsqlResult<String> {
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