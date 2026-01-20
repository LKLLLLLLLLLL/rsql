use crate::catalog::SysCatalog;
use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode};
use crate::config::DEFAULT_PASSWORD;
use super::result::{ExecutionResult::{self, Dcl}};
use tracing::info;

/// user relevent sql statements
pub fn execute_dcl_plan_node(node: &PlanNode, tnx_id: u64) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::CreateUser {user_name, password, if_not_exists} => {
            // check if user exists
            let all_users = SysCatalog::global().get_all_users(tnx_id)?;
            if all_users.contains(user_name) {
                if *if_not_exists {
                    return Ok(Dcl(format!("User {} already exists, skipping create user.", user_name)));
                } else {
                    return Err(RsqlError::ExecutionError(format!("User {} already exists.", user_name)));
                }
            }
            let password = match password {
                Some(pw) => pw.clone(),
                None => DEFAULT_PASSWORD.to_string(),
            };
            SysCatalog::global().register_user(tnx_id, user_name, &password)?;
            Ok(Dcl(format!("User {} created successfully.", user_name)))
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
        // only support write permission for now
        PlanNode::Grant { privilege, user_name } => {
            if privilege.to_uppercase() != "WRITE" {
                return Err(RsqlError::ExecutionError(format!("Unsupported privilege: {}", privilege)));
            }
            // check if user exists
            let all_users = SysCatalog::global().get_all_users(tnx_id)?;
            if !all_users.contains(user_name) {
                return Err(RsqlError::ExecutionError(format!("User {} does not exist.", user_name)));
            }
            // check if user already has write permission
            let has_permission = SysCatalog::global().check_user_write_permission(tnx_id, user_name)?;
            if has_permission {
                return Ok(Dcl(format!("User {} already has write permission.", user_name)));
            };
            // grant write permission
            SysCatalog::global().set_user_permission(tnx_id, user_name, true)?;
            Ok(Dcl(format!("Granted write permission to user {}.", user_name)))
        },
        // only support write permission for now
        PlanNode::Revoke { privilege, user_name } => {
            if privilege.to_uppercase() != "WRITE" {
                return Err(RsqlError::ExecutionError(format!("Unsupported privilege: {}", privilege)));
            }
            // check if user exists
            let all_users = SysCatalog::global().get_all_users(tnx_id)?;
            if !all_users.contains(user_name) {
                return Err(RsqlError::ExecutionError(format!("User {} does not exist.", user_name)));
            }
            // check if user has write permission
            let has_permission = SysCatalog::global().check_user_write_permission(tnx_id, user_name)?;
            if !has_permission {
                return Ok(Dcl(format!("User {} does not have write permission.", user_name)));
            };
            // revoke write permission
            SysCatalog::global().set_user_permission(tnx_id, user_name, false)?;
            Ok(Dcl(format!("Revoked write permission from user {}.", user_name)))
        },
        _ => {
            panic!("Unsupported DCL operation")
        }
    }
}