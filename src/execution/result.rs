use crate::common::RsqlResult;
use crate::sql::{Plan, plan::{PlanItem}};
use crate::common::data_item::{DataItem};
use super::{dml_interpreter::execute_dml_plan_node, ddl_interpreter::execute_ddl_plan_node, dcl_interpreter::execute_dcl_plan_node};
use tracing::info;
use crate::catalog::table_schema::{TableSchema, ColType, TableColumn};
use crate::transaction::TnxManager;
use std::collections::HashMap;
use crate::storage::table::{Table};

pub enum ExecutionResult {
    Query {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>, // query result
    },
    Mutation, // update, delete, insert
    Ddl, // create, drop
    Dcl,
    TableObj(TableObject), // table object after scan
    TableWithFilter {
        table_obj: TableObject,
        rows: Vec<Vec<DataItem>>, // temp query result after filter
    },
    TempTable {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>,
        table_name: Option<String>,
    } // used for join, group by, aggregate and subquery
}
pub struct TableObject {
    pub table_obj: Table,
    pub map: HashMap<String, usize>, // col_name -> col_index
    pub cols: (Vec<String>, Vec<ColType>), // (cols_name, cols_type)
    pub indexed_cols: Vec<String>, // indexed columns
    pub pk_col: (String, ColType), // primary key column name
}