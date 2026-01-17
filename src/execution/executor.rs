use crate::common::RsqlResult;
use crate::sql::{Plan, plan::{PlanItem}};
use crate::common::data_item::{DataItem};
use super::{dml_interpreter::execute_dml_plan_node, ddl_interpreter::execute_ddl_plan_node, dcl_interpreter::execute_dcl_plan_node};
use tracing::info;
use crate::catalog::table_schema::{TableSchema, ColType, TableColumn};
use crate::transaction::TnxManager;
use std::collections::HashMap;
use crate::storage::table::{Table};

pub fn execute(sql: &str, connection_id: u64) -> RsqlResult<()> {
    info!("Executing SQL: {}", sql);
    
    info!("Parsing SQL...");
    let plan = Plan::build_plan(sql)?;
    for item in plan.items.iter() {
        match item {
            PlanItem::Begin => {
                info!("Begin transaction");
                // TnxManager::global().begin_transaction(connection_id);
            },
            PlanItem::Commit => {
                info!("Commit transaction");
            },
            PlanItem::Rollback => {
                info!("Rollback transaction");
            },
            PlanItem::DCL(plan_node) => {
                execute_dcl_plan_node(plan_node, connection_id)?;
            },
            PlanItem::DDL(plan_node) => {
                execute_ddl_plan_node(plan_node, connection_id)?;
            },
            PlanItem::DML(plan_node) => {
                execute_dml_plan_node(plan_node, connection_id)?;
            },
        }
    }
    info!("SQL executed successfully.");
    Ok(())
}