use serde::{Deserialize, Serialize};
use crate::common::{RsqlResult, RsqlError};
use crate::common::data_item::{DataItem};
use crate::catalog::table_schema::{ColType};
use std::collections::HashMap;
use crate::storage::table::{Table};

pub enum MiddleResult {
    Query {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>, // query result
    },
    Mutation(String), // update, delete, insert
    TableObj(TableObject), // table object after scan
    TableWithFilter {
        table_obj: TableObject,
        rows: Vec<Vec<DataItem>>, // temp query result after filter
    },
    TempTable {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>,
        table_name: Option<String>,
    }, // used for join and subquery
    AggrTable {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>,
        aggr_cols: Vec<String>, // aggregate columns
    },
}

impl MiddleResult {
    pub fn to_exec_result(&self) -> RsqlResult<ExecutionResult> {
        match self {
            MiddleResult::Query{cols, rows} => Ok(ExecutionResult::Query{cols: cols.0.clone(), rows: rows.clone()}),
            MiddleResult::Mutation(msg) => Ok(ExecutionResult::Mutation(msg.clone())),
            _ => Err(RsqlError::ExecutionError(format!("unexpected middle result")))
        }
    }
}

pub struct TableObject {
    pub table_obj: Table,
    pub map: HashMap<String, usize>, // col_name -> col_index
    pub cols: (Vec<String>, Vec<ColType>), // (cols_name, cols_type)
    pub indexed_cols: Vec<String>, // indexed columns
    pub pk_col: (String, ColType), // primary key column name
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecutionResult {
    TnxBeginSuccess,
    CommitSuccess,
    RollbackSuccess,
    Ddl(String), // create, drop
    Dcl(String), // users 
    Query {
        cols: Vec<String>,
        rows: Vec<Vec<DataItem>>, // query result
    },
    Mutation(String), // update, delete, insert
}