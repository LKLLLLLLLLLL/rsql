/// Pretty printing utilities for logical plans.
/// This module provides methods to display logical query plans in a tree-like format. For easy obervation and testing.

use crate::parser::logical_plan::LogicalPlan;
use sqlparser::ast::Expr;

fn fmt_exprs(exprs: &[Expr]) -> String {
    exprs
        .iter()
        .map(|e| format!("{}", e))
        .collect::<Vec<_>>()
        .join(", ")
}

impl LogicalPlan {
    pub fn pretty_print(&self) {
        self.pretty_print_inner("", true);
    }

    fn pretty_print_inner(&self, prefix: &str, is_last: bool) {
        let branch = if is_last { "└── " } else { "├── " };
        println!("{}{}{}", prefix, branch, self.node_label());

        let new_prefix = if is_last {
            format!("{}    ", prefix)
        } else {
            format!("{}│   ", prefix)
        };

        for (i, child) in self.children().iter().enumerate() {
            let last = i + 1 == self.children().len();
            child.pretty_print_inner(&new_prefix, last);
        }
    }
}

impl LogicalPlan {
    fn node_label(&self) -> String {
        match self {
            LogicalPlan::TableScan { table } => {
                format!("TableScan [{}]", table)
            }
            LogicalPlan::Filter { predicate, .. } => {
                format!("Filter [{}]", predicate)
            }
            LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                ..
            } => {
                format!(
                    "Aggregate [group_by: {}, aggr: {}]",
                    fmt_exprs(group_by),
                    fmt_exprs(aggr_exprs)
                )
            }
            LogicalPlan::Projection { exprs, .. } => {
                format!("Projection [{}]", fmt_exprs(exprs))
            }
            LogicalPlan::Join { join_type, on, .. } => {
                format!("Join [{:?}, on: {}]", join_type, on.as_ref().map_or("None".to_string(), |e| format!("{}", e)))
            }
            LogicalPlan::CreateTable { table_name, columns } => {
                format!("CreateTable [{}] cols={}", table_name, columns.iter().map(|c| c.name.to_string()).collect::<Vec<_>>().join(", "))
            }
            LogicalPlan::AlterTable { table_name, operation } => {
                format!("AlterTable [{}] op={:?}", table_name, operation)
            }
            LogicalPlan::DropTable { table_name } => {
                format!("DropTable [{}]", table_name)
            }
            LogicalPlan::Insert { table_name, columns, values } => {
                format!("Insert [{}] cols={:?} rows={}", table_name, columns, values.len())
            }
            LogicalPlan::Delete { table_name, predicate } => {
                format!("Delete [{}] where={:?}", table_name, predicate)
            }
            LogicalPlan::Update { table_name, assignments, predicate } => {
                format!("Update [{}] assigns={} where={:?}", table_name, assignments.len(), predicate)
            }
        }
    }
}

impl LogicalPlan {
    fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::TableScan { .. } => vec![],
            LogicalPlan::Filter { input, .. } => vec![input],
            LogicalPlan::Aggregate { input, .. } => vec![input],
            LogicalPlan::Projection { input, .. } => vec![input],
            LogicalPlan::Join { left, right, .. } => vec![left, right],
            LogicalPlan::CreateTable { .. } => vec![],
            LogicalPlan::AlterTable { .. } => vec![],
            LogicalPlan::DropTable { .. } => vec![],
            LogicalPlan::Insert { .. } => vec![],
            LogicalPlan::Delete { .. } => vec![],
            LogicalPlan::Update { .. } => vec![],
        }
    }
}

