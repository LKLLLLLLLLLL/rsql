use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode, JoinType};
use crate::common::data_item::{DataItem};
use crate::catalog::table_schema::{TableSchema, ColType, TableColumn};
use crate::storage::table::{Table};
use super::result::{ExecutionResult::{self, Dcl, TableObj, TableWithFilter, TempTable}, TableObject};
use super::expr_interpreter::{handle_on_expr, handle_table_obj_filter_expr, handle_temp_table_filter_expr, handle_insert_expr};
use tracing::info;
use sqlparser::ast::{Expr};

/// user relevent sql statements
pub fn execute_dcl_plan_node(node: &PlanNode, tnx_id: u64) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::CreateUser {user_name} => {
            todo!()
        },
        PlanNode::DropUser {user_name, if_exists} => {
            todo!()
        },
        _ => {
            panic!("Unsupported DCL operation")
        }
    }
}