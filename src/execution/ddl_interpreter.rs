use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode, JoinType};
use crate::common::data_item::{DataItem};
use crate::catalog::table_schema::{TableSchema, ColType, TableColumn};
use crate::storage::table::{Table};
use super::result::{ExecutionResult::{self, Ddl, TableObj, TableWithFilter, TempTable}, TableObject};
use super::expr_interpreter::{handle_on_expr, handle_table_obj_filter_expr, handle_temp_table_filter_expr, handle_insert_expr};
use tracing::info;
use std::collections::HashMap;
use sqlparser::ast::{Expr};

/// table and index relevant sql statements
pub fn execute_ddl_plan_node(node: &PlanNode, tnx_id: u64) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::CreateTable { table_name, columns} => {
            todo!()
        },
        PlanNode::AlterTable {table_name, operation} => {
            todo!()
        },
        PlanNode::DropTable { table_name, if_exists} => {
            todo!()
        },
        PlanNode::CreateIndex {
            index_name,
            table_name,
            columns,
            unique,
        } => {
            todo!()
        },
        _ => {
            panic!("Unsupported DDL operation")
        }
    }
}