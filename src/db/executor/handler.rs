use super::super::errors::{RsqlResult, RsqlError};
use super::super::sql_parser::plan::{PlanNode, JoinType};
use crate::db::data_item::{DataItem};
use crate::db::table::{Table, TableSchema, ColType, TableColumn};
use self::ExecutionResult::{Query, Mutation, Ddl, TableObj, TableWithFilter, TempTable};
use tracing::info;
use std::collections::HashMap;
use sqlparser::ast::Expr;

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
    TableObj(TableObject), // table object after scan
    TableWithFilter {
        table_obj: TableObject,
        rows: Vec<Vec<DataItem>>, // temp query result after filter
    },
    TempTable {
        cols: Vec<String>,
        rows: Vec<Vec<DataItem>>,
        table_name: Option<String>,
    } // used for join, group by, aggregate and subquery
}

pub struct TableObject {
    table_obj: Table,
    map: HashMap<String, usize>, // col_name -> col_index
    cols: Vec<String>,    
}

pub fn execute_plan_node(node: &PlanNode) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::TableScan { table } => {
            info!("Implement TableScan execution");
            let table_column = TableColumn {
                name: "a".to_string(),
                data_type: ColType::Integer,
                pk: true,
                nullable: false,
                index: true,
                unique: false, // TODO: not implemented
            };
            let table_obj = Table::from(0, TableSchema {
                columns: vec![table_column],
            })?;
            let table_schema = table_obj.get_schema();
            let mut map = HashMap::new();
            let mut cols = vec![];
            for (idx, col) in table_schema.columns.iter().enumerate() {
                map.insert(col.name.clone(), idx);
                cols.push(col.name.clone());
            }
            let table_object = TableObject {
                table_obj,
                map,
                cols,
            };
            Ok(TableObj(table_object)) // get table object after scan
        }
        PlanNode::Filter { predicate, input } => {
            info!("Implement Filter execution");
            let input_result = execute_plan_node(input)?;
            if let TableObj(table_obj) = input_result {
                //================ todo: handle predicate ====================
                let rows_iter = table_obj.table_obj.get_all_rows()?;
                let mut rows: Vec<Vec<DataItem>> = vec![];
                for row in rows_iter {
                    match row {
                        Ok(r) => rows.push(r),
                        Err(e) => return Err(e),
                    }
                }
                Ok(TableWithFilter { table_obj, rows }) // get temp query result after filter
            }else {
                if let TempTable{cols, rows, table_name} = input_result {
                    //================ todo: handle predicate ====================
                    Ok(TempTable{cols, rows, table_name})
                }else {
                    Err(RsqlError::ExecutionError(format!("Filter input must be a TableObj or TempTable")))
                }
            }
        }
        PlanNode::Projection { exprs, input } => {
            info!("Implement Projection execution");
            let input_result = execute_plan_node(input)?;
            if let TableWithFilter {table_obj, rows} = input_result {
                //================ todo: handle exprs ====================
                Ok(Query{
                    cols: table_obj.cols.clone(),
                    rows,
                }) // get final query result
            }else {
                if let TempTable{cols, rows, table_name} = input_result {
                    Ok(TempTable { cols, rows, table_name })
                }else {
                    Err(RsqlError::ExecutionError(format!("Projection input must be a TableWithFilter or TempTable")))
                } // handle subquery
            }
        }
        PlanNode::Join { left, right, join_type, on } => {
            info!("Implement Join execution");
            if let (TableObj(left_table_obj), TableObj(right_table_obj)) = (execute_plan_node(left)?, execute_plan_node(right)?) {
                let (joined_cols, joined_rows) = handle_join(&left_table_obj, &right_table_obj, join_type, on)?;
                Ok(TempTable { cols: joined_cols, rows: joined_rows, table_name: None })
            }else {
                Err(RsqlError::ExecutionError(format!("Join input must be a TableObj")))
            }
        }
        PlanNode::Aggregate { group_by, aggr_exprs, input } => {
            todo!("Implement Aggregate execution")
        }
        PlanNode::Subquery { subquery, alias } => {
            info!("Implement Subquery execution");
            let subquery_result = execute_plan_node(subquery)?;
            if let Query{cols, rows} = subquery_result {
                Ok(TempTable { cols, rows, table_name: alias.clone() })
            }else {
                if let TempTable {cols, rows, table_name} = subquery_result {
                    Ok(TempTable {cols, rows, table_name: alias.clone()})
                }else {
                    Err(RsqlError::ExecutionError(format!("Subquery input must be a Query or TempTable")))
                }
            }
        }
        PlanNode::Insert { table_name, columns, values, input } => {
            todo!("Implement Insert execution")
        }
        PlanNode::Delete { input } => {
            todo!("Implement Delete execution")
        }
        PlanNode::Update { input, assignments } => {
            todo!("Implement Update execution")
        }
        PlanNode::CreateTable { table_name, columns } => {
            todo!("Implement CreateTable execution")
        }
        PlanNode::DropTable { table_name, if_exists } => {
            todo!("Implement DropTable execution")
        }
        PlanNode::Apply { input, subquery, apply_type } => {
            // it depends
            todo!("Implement Apply execution")
        }
        PlanNode::AlterTable { table_name, operation } => {
            // it depends
            todo!("Implement AlterTable execution")
        }
    }
}

fn handle_join(left_table_obj: &TableObject, right_table_obj: &TableObject, join_type: &JoinType, on: &Option<Expr>) -> RsqlResult<(Vec<String>, Vec<Vec<DataItem>>)> {
    let mut extended_cols = left_table_obj.cols.clone();
    extended_cols.extend(right_table_obj.cols.clone());
    let mut extended_rows: Vec<Vec<DataItem>> = vec![];
    let left_rows_iter = left_table_obj.table_obj.get_all_rows()?;
    for left_row in left_rows_iter {
        let left_row = left_row?;
        let right_rows_iter = right_table_obj.table_obj.get_all_rows()?;
        for right_row in right_rows_iter {
            let right_row = right_row?;
            let mut extended_row = left_row.clone();
            extended_row.extend(right_row.clone());
            extended_rows.push(extended_row);
        }
    } // extend the left table with right table
    match join_type {
        JoinType::Inner => {
            //================ todo: handle on ====================
            Ok((extended_cols, extended_rows))
        },
        JoinType::Left => {
            //================ todo: handle on ====================
            Ok((extended_cols, extended_rows))
        },
        JoinType::Right => {
            //================ todo: handle on ====================
            Ok((extended_cols, extended_rows))
        },
        JoinType::Full => {
            //================ todo: handle on ====================
            Ok((extended_cols, extended_rows))
        },
        JoinType::Cross => {
            Ok((extended_cols, extended_rows))
        },
    }
}