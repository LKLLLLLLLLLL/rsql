use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode};
use super::result::{ExecutionResult::{self, Dcl}};
use tracing::info;

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