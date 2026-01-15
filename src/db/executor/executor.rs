use super::super::errors::RsqlResult;
use super::super::sql_parser::{Plan, plan};
use super::handler::execute_plan_node;
use tracing::info;

pub fn execute(sql: &str) -> RsqlResult<()> {
    info!("Executing SQL: {}", sql);
    
    info!("Parsing SQL...");
    let plan = Plan::build_plan(sql)?;
    
    info!("Opening transaction...");
    for (idx, tnx) in plan.tnxs.iter().enumerate() {
        info!("Processing transaction: {}", idx);
        
        // execute plan_nodes in the transaction
        for stmt in &tnx.stmts {
            info!("Executing statement: {:?}", stmt);
            execute_plan_node(stmt)?;
        }
        
        // 根据事务状态决定提交或回滚
        match &tnx.commit_stat {
            plan::TnxState::Commit => {
                info!("Committing transaction {}", idx);
            }
            plan::TnxState::Rollback => {
                info!("Rolling back transaction {}", idx);
            }
        }
    }

    info!("SQL executed successfully.");
    Ok(())
}