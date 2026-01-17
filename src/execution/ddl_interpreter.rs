use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode};
use super::result::{ExecutionResult::{self, Ddl}};
use tracing::info;

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