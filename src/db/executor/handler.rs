use super::super::errors::{RsqlResult, RsqlError};
use super::super::sql_parser::plan::{PlanNode, JoinType};
use crate::db::data_item::{DataItem};
use self::ExecutionResult::{Query, Mutation, Ddl, Table_Obj, Table_With_Filter};
use tracing::info;
use sqlparser::ast::{Expr, BinaryOperator, Value as SqlValue};
use std::collections::HashMap;


#[derive(Debug)]
pub enum ExecutionResult {
    Query {
        cols: Vec<String>,
        rows: Vec<Vec<DataItem>>, // query result
    },
    Mutation {
        affected_rows: u64,  // affected row_number after mutation
    },
    Ddl {
        success: bool, // ddl execution status
    },
    Table_Obj {
        table: Table,
    }, 
    Table_With_Filter {
        table: Table,
        rows: Vec<Vec<DataItem>>, // partial query result
    },
}

pub fn execute_plan_node(node: &PlanNode) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::TableScan { table } => {
            info!("Implement TableScan execution");
            Ok(Table_Obj{table: Table::new(table)})
        }
        PlanNode::Filter { predicate, input } => {
            info!("Implement Filter execution");
            if let Table_Obj {table} = execute_plan_node(input)? {
                // todo: handle predicate
                
            }else {
                Err(RsqlError::ExecutionError(format!("Filter input must be a Table")))
            }
        }
        PlanNode::Projection { exprs, input } => {
            info!("Implement Projection execution");
            if let RowIndexes(table_name, row_indexes) = execute_plan_node(input)? {
                if let Some((cols, rows)) = Table::get_query(&table_name, &row_indexes, vec!["*".to_string()]) {
                    Ok(Query {
                        cols,
                        rows,
                    })
                }else {
                    Ok(Query {
                        cols: vec![],
                        rows: vec![],
                    })
                }
            }else {
                Err(RsqlError::ExecutionError(format!("Projection input must be a RowIndexes")))
            }
        }
        PlanNode::Join { left, right, join_type, on } => {
            info!("Implement Join execution");
            let left_table_name = match execute_plan_node(left)? {
                TableName(name) => name,
                _ => return Err(RsqlError::ExecutionError(format!("Join left input must be a TableName"))),
            };
            let right_table_name = match execute_plan_node(right)? {
                TableName(name) => name,
                _ => return Err(RsqlError::ExecutionError(format!("Join right input must be a TableName"))),
            };
            let join_result = handle_join(&left_table_name, &right_table_name, join_type, on);
        }
        PlanNode::Aggregate { group_by, aggr_exprs, input } => {
            // 实现聚合
            todo!("Implement Aggregate execution")
        }
        PlanNode::Subquery { subquery, alias } => {
            // 实现子查询
            todo!("Implement Subquery execution")
        }
        PlanNode::Insert { table_name, columns, values, input } => {
            // 实现插入
            todo!("Implement Insert execution")
        }
        PlanNode::Delete { input } => {
            // 实现删除
            todo!("Implement Delete execution")
        }
        PlanNode::Update { input, assignments } => {
            // 实现更新
            todo!("Implement Update execution")
        }
        PlanNode::CreateTable { table_name, columns } => {
            // 实现创建表
            todo!("Implement CreateTable execution")
        }
        PlanNode::DropTable { table_name, if_exists } => {
            // 实现删除表
            todo!("Implement DropTable execution")
        }
        PlanNode::Apply { input, subquery, apply_type } => {
            todo!("Implement Apply execution")
        }
        PlanNode::AlterTable { table_name, operation } => {
            todo!("Implement AlterTable execution")
        }
    }
}

fn handle_join (left: &str, right: &str, join_type: &JoinType, on: Option<&Expr>) -> Option<(Vec<String>, Vec<Vec<DataItem>>)> {
    
}

#[derive(Debug)]
struct Table { }

impl Table {
    pub fn get_indexed_cols(&self) -> Vec<String> {
        vec![]
    }
    pub fn get_all_rows(&self) -> impl Iterator<Item = RsqlResult<Vec<DataItem>>> {
        std::iter::empty()
    }
    pub fn get_row(&self, primary_key: DataItem) -> RsqlResult<Vec<DataItem>> {
        Ok(vec![])
    }
    pub fn get_rows_by_index(&self, begin: DataItem, end: DataItem) -> impl Iterator<Item = RsqlResult<Vec<DataItem>>> {
        std::iter::empty()
    }
    pub fn new(table_name: &str) -> Self {
        Self {}
    }
}