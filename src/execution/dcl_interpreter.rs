use crate::catalog::SysCatalog;
use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode};
use crate::config::DEFAULT_PASSWORD;
use crate::server::conncetion_user_map::ConnectionUserMap;
use super::result::{ExecutionResult::{self, Dcl}};
use tracing::info;

/// user relevent sql statements
pub fn execute_dcl_plan_node(node: &PlanNode, tnx_id: u64, connection_id: u64) -> RsqlResult<ExecutionResult> {
    let username = ConnectionUserMap::global()
        .get_username(connection_id)
        .ok_or(RsqlError::ExecutionError("Failed to get username from connection ID".to_string()))?;
    match node {
        PlanNode::CreateUser {user_name, password, if_not_exists} => {
            // verify permision
            let has_permission = SysCatalog::global().check_user_write_permission(tnx_id, &username)?;
            if !has_permission {
                return Err(RsqlError::ExecutionError(format!("User {} does not have permission to create user.", username)));
            }
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
            // verify permision
            let has_permission = SysCatalog::global().check_user_write_permission(tnx_id, &username)?;
            if !has_permission {
                return Err(RsqlError::ExecutionError(format!("User {} does not have permission to drop user.", username)));
            }
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
        PlanNode::Grant { privilege, table_name, user_name } => {
            // verify permision
            let has_permission = SysCatalog::global().check_user_write_permission(tnx_id, &username)?;
            if !has_permission {
                return Err(RsqlError::ExecutionError(format!("User {} does not have permission to grant permission.", username)));
            }
            let priv_code = match privilege.to_uppercase().as_str() {
                "WRITE" | "W" => "W",
                "READ" | "R"  => "R",
                _ => return Err(RsqlError::ExecutionError(format!("Unsupported privilege: {}", privilege))),
            };
            // check if user exists
            let all_users = SysCatalog::global().get_all_users(tnx_id)?;
            if !all_users.contains(user_name) {
                return Err(RsqlError::ExecutionError(format!("User {} does not exist.", user_name)));
            }

            match table_name {
                Some(table) => {
                    SysCatalog::global().set_user_table_privilege(tnx_id, user_name, table, Some(priv_code))?;
                    Ok(Dcl(format!("Granted {} permission to user {} on table {}.", privilege, user_name, table)))
                },
                None => {
                    SysCatalog::global().set_user_permission(tnx_id, user_name, Some(priv_code))?;
                    Ok(Dcl(format!("Granted global {} permission to user {}.", privilege, user_name)))
                }
            }
        },
        // only support write permission for now
        PlanNode::Revoke { privilege: _, table_name, user_name } => {
            // verify permision
            let has_permission = SysCatalog::global().check_user_write_permission(tnx_id, &username)?;
            if !has_permission {
                return Err(RsqlError::ExecutionError(format!("User {} does not have permission to revoke permission.", username)));
            }
            // check if user exists
            let all_users = SysCatalog::global().get_all_users(tnx_id)?;
            if !all_users.contains(user_name) {
                return Err(RsqlError::ExecutionError(format!("User {} does not exist.", user_name)));
            }

            match table_name {
                Some(table) => {
                    SysCatalog::global().set_user_table_privilege(tnx_id, user_name, table, None)?;
                    Ok(Dcl(format!("Revoked permission from user {} on table {}.", user_name, table)))
                },
                None => {
                    SysCatalog::global().set_user_permission(tnx_id, user_name, None)?;
                    Ok(Dcl(format!("Revoked all global permissions from user {}.", user_name)))
                }
            }
        },
        _ => {
            panic!("Unsupported DCL operation")
        }
    }
}