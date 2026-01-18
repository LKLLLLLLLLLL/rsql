use crate::catalog::SysCatalog;
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
            // check if user exists
            let all_users = SysCatalog::global().get_all_users(tnx_id)?;
            if !all_users.contains(user_name) {
                if *if_exists {
                    info!("User {} does not exist, skipping drop user.", user_name);
                    return Ok(Dcl(format!("User {} does not exist, skipping drop user.", user_name)));
                } else {
                    return Err(RsqlError::ExecutionError(format!("User {} does not exist.", user_name)));
                }
            }
            SysCatalog::global().unregister_user(tnx_id, user_name)?;
            Ok(Dcl(format!("User {} dropped successfully.", user_name)))
        },
        _ => {
            panic!("Unsupported DCL operation")
        }
    }
}