use crate::common::RsqlResult;
use crate::sql::{Plan, plan::{PlanItem}};
use super::handler::execute_plan_node;
use tracing::info;

pub fn execute(sql: &str, connection_id: u64) -> RsqlResult<()> {
    info!("Executing SQL: {}", sql);
    
    info!("Parsing SQL...");
    let plan = Plan::build_plan(sql)?;
    for item in plan.items.iter() {
        match item {
            PlanItem::Begin => {
                info!("Begin transaction");
            },
            PlanItem::Commit => {
                info!("Commit transaction");
            },
            PlanItem::Rollback => {
                info!("Rollback transaction");
            },
            PlanItem::DDL(plan_node) | PlanItem::DML(plan_node) | PlanItem::DCL(plan_node) => {
                info!("Executing statement: {:?}", plan_node);
                execute_plan_node(plan_node, connection_id)?;
            },
        }
    }
    info!("SQL executed successfully.");
    Ok(())
}