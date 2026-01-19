use crate::catalog::SysCatalog;
use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{PlanNode, JoinType};
use crate::common::data_item::{DataItem};
use crate::catalog::table_schema::{ColType};
use crate::storage::table::{Table};
use crate::transaction::TnxManager;
use super::result::{MiddleResult::{self, Query, Mutation, TableObj, TableWithFilter, TempTable, AggrTable}, TableObject};
use super::expr_interpreter::{handle_on_expr, 
    handle_table_obj_filter_expr, 
    handle_temp_table_filter_expr, 
    handle_insert_expr, 
    handle_update_expr,
    handle_aggr_expr
};
use tracing::info;
use std::collections::HashMap;
use sqlparser::ast::{Expr};

fn get_table_object (table_name: &str, read_only: bool, tnx_id: u64) -> RsqlResult<TableObject> {
    let Some(table_id) = SysCatalog::global().get_table_id(tnx_id, table_name)? else {
        return Err(RsqlError::ExecutionError(format!("Table {} not found", table_name)));
    };
    // 0. acquire read or write lock
    if read_only {
        TnxManager::global().acquire_read_locks(tnx_id, &vec![table_id])?;
    } else {
        TnxManager::global().acquire_write_locks(tnx_id, &vec![table_id])?;
    };
    // 1. get Table
    let table_schema = SysCatalog::global().get_table_schema(tnx_id, table_id)?;
    let table_obj = Table::from(table_id, table_schema.clone(), false)?;
    // 2. construct TableObject
    let mut map = HashMap::new();
    let mut cols_name = vec![];
    let mut cols_type = vec![];
    let mut pk_col_name = String::new();
    let mut pk_col_type = ColType::Integer;
    for (idx, col) in table_schema.get_columns().iter().enumerate() {
        map.insert(col.name.clone(), idx);
        cols_name.push(col.name.clone());
        cols_type.push(col.data_type.clone());
        if col.pk {
            pk_col_name = col.name.clone();
            pk_col_type = col.data_type.clone();
        }
    }
    let indexed_cols = table_obj.get_schema().get_indexed_col();
    let table_object = TableObject {
        table_obj,
        map,
        cols: (cols_name, cols_type),
        indexed_cols,
        pk_col: (pk_col_name, pk_col_type),
    };
    Ok(table_object)
}

pub fn execute_dml_plan_node(node: &PlanNode, tnx_id: u64, read_only: bool) -> RsqlResult<MiddleResult> {
    match node {
        PlanNode::TableScan { table } => {
            info!("Implement TableScan execution");
            let table_object = get_table_object(table, read_only, tnx_id)?;
            Ok(TableObj(table_object)) // get table object after scan
        },
        PlanNode::Filter { predicate, input } => {
            info!("Implement Filter execution");
            let input_result = execute_dml_plan_node(input, tnx_id, read_only)?;
            if let TableObj(table_obj) = input_result {
                let filter_result = handle_table_obj_filter_expr(&table_obj, predicate)?;
                Ok(TableWithFilter { table_obj, rows: filter_result }) // get temp query result after filter
            }else {
                if let TempTable{cols, rows, table_name} = input_result {
                    let filter_result = handle_temp_table_filter_expr(&cols.0, &cols.1, &rows, predicate)?;
                    Ok(TempTable{
                        cols,
                        rows: filter_result,
                        table_name,
                    })
                }else {
                    Err(RsqlError::ExecutionError(format!("Filter input must be a TableObj or TempTable")))
                }
            }
        },
        PlanNode::Projection { exprs, input } => {
            info!("Implement Projection execution");
            let input_result = execute_dml_plan_node(input, tnx_id, true)?;
            if let TableWithFilter {table_obj, rows: input_rows} = input_result {
                // 0. handle * column
                if exprs.len() == 0 {
                    return Ok(Query {
                        cols: (table_obj.cols.0.clone(), table_obj.cols.1.clone()),
                        rows: input_rows,
                    })
                }
                // 1. get projection columns
                let mut cols_name = vec![];
                let mut cols_type = vec![];
                for expr in exprs {
                    match expr {
                        Expr::Identifier(ident) => {
                            let col_idx = table_obj.map.get(&ident.value).unwrap();
                            cols_name.push(ident.value.clone());
                            cols_type.push(table_obj.cols.1[*col_idx].clone());
                        },
                        _ => {
                            return Err(RsqlError::ExecutionError(format!("Projection expr {:?} is not supported", expr)))
                        }
                    }
                }
                // 2. get projection rows
                let mut rows = vec![];
                for row in input_rows.iter() {
                    let mut r = vec![];
                    for col in cols_name.iter() {
                        let col_idx = table_obj.map.get(col).unwrap();
                        r.push(row[*col_idx].clone());
                    }
                    rows.push(r);
                }
                Ok(Query{
                    cols: (cols_name, cols_type),
                    rows,
                }) // get final query result
            }else {
                if let TempTable{cols: input_cols, rows: input_rows, table_name: _} = input_result {
                    // 0. handle * column
                    if exprs.len() == 0 {
                        return Ok(Query {
                            cols: input_cols,
                            rows: input_rows,
                        })
                    }
                    // 1. get projection columns
                    let mut cols_name = vec![];
                    let mut cols_type = vec![];
                    for expr in exprs {
                        match expr {
                            Expr::Identifier(ident) => {
                                let col_idx = input_cols.0.iter().position(|x| x == &ident.value).unwrap();
                                cols_name.push(ident.value.clone());
                                cols_type.push(input_cols.1[col_idx].clone());
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Projection expr {:?} is not supported", expr)))
                            }
                        }
                    }
                    // 2. get projection rows
                    let mut rows = vec![];
                    for row in input_rows.iter() {
                        let mut r = vec![];
                        for col in cols_name.iter() {
                            let col_idx = input_cols.0.iter().position(|x| x == col).unwrap();
                            r.push(row[col_idx].clone());
                        }
                        rows.push(r);
                    }
                    Ok(Query {
                        cols: (cols_name, cols_type),
                        rows,
                    }) // handle subquery
                }else {
                    if let AggrTable{cols: input_cols, rows: input_rows, aggr_cols} = input_result {
                        // 1. get projection columns
                        let mut cols_name = vec![];
                        for expr in exprs {
                            match expr {
                                Expr::Identifier(ident) => {
                                    cols_name.push(ident.value.clone());
                                },
                                _ => (), // skip aggr cols
                            }
                        }
                        cols_name.extend(aggr_cols);
                        // 2. get projection rows
                        let mut rows = vec![];
                        for row in input_rows.iter() {
                            let mut r = vec![];
                            for col in cols_name.iter() {
                                let col_idx = input_cols.0.iter().position(|x| x == col).unwrap();
                                r.push(row[col_idx].clone());
                            }
                            rows.push(r);
                        }
                        Ok(Query {
                            cols: (cols_name, vec![]), // aggr col types are useless
                            rows,
                        }) // get aggr query result
                    }else {
                        Err(RsqlError::ExecutionError(format!("Projection input must be a TableWithFilter, TempTable or AggrTable")))
                    }
                }
            }
        },
        PlanNode::Join { left, right, join_type, on } => {
            info!("Implement Join execution");
            if let (TableObj(left_table_obj), TableObj(right_table_obj)) = (execute_dml_plan_node(left, tnx_id, read_only)?, execute_dml_plan_node(right, tnx_id, read_only)?) {
                let (joined_cols, joined_rows) = handle_join(&left_table_obj, &right_table_obj, join_type, on)?;
                Ok(TempTable { cols: joined_cols, rows: joined_rows, table_name: None })
            }else {
                Err(RsqlError::ExecutionError(format!("Join input must be a TableObj")))
            }
        },
        PlanNode::Aggregate { group_by, aggr_exprs, input } => {
            info!("Implement Aggregate execution");
            let input_result = execute_dml_plan_node(input, tnx_id, read_only)?;
            if let TableObj(table_obj) = input_result {
                let (cols, rows, aggr_cols) = handle_aggr_expr(table_obj, group_by, aggr_exprs)?;
                Ok(AggrTable {cols, rows, aggr_cols})
            }else {
                Err(RsqlError::ExecutionError(format!("Aggregate input must be a TableObj")))
            }
        },
        PlanNode::Subquery { subquery, alias } => {
            info!("Implement Subquery execution");
            let subquery_result = execute_dml_plan_node(subquery, tnx_id, read_only)?;
            if let Query{cols, rows} = subquery_result {
                Ok(TempTable { cols, rows, table_name: alias.clone() })
            }else {
                if let TempTable {cols, rows, table_name: _} = subquery_result {
                    Ok(TempTable {cols, rows, table_name: alias.clone()})
                }else {
                    Err(RsqlError::ExecutionError(format!("Subquery input must be a Query or TempTable")))
                }
            }
        },
        PlanNode::Insert { table_name, columns, values, input: _ } => {
            info!("Implement Insert execution");
            let mut table_object = get_table_object(table_name, false, tnx_id)?;
            if let Some(cols) = columns {
                let mut null_cols = vec![];
                for col_type in table_object.cols.1.iter() {
                    match col_type {
                        ColType::Integer => {
                            null_cols.push(DataItem::NullInt);
                        },
                        ColType::Float => {
                            null_cols.push(DataItem::NullFloat);
                        },
                        ColType::Chars(size) => {
                            null_cols.push(DataItem::NullChars { len: *size as u64 });
                        },
                        ColType::Bool => {
                            null_cols.push(DataItem::NullBool);
                        },
                        ColType::VarChar(_) => {
                            null_cols.push(DataItem::NullVarChar);
                        },
                    }
                }
                let data_item = handle_insert_expr(&table_object, cols, &null_cols, &values[0])?;
                table_object.table_obj.insert_row(data_item, tnx_id)?;
                Ok(Mutation("Insert successful".to_string()))
            }else {
                Err(RsqlError::ExecutionError(format!("Insert columns is None")))
            }
        },
        PlanNode::Delete { input } => {
            info!("Implement Delete execution");
            let input_result = execute_dml_plan_node(input, tnx_id, false)?;
            if let TableWithFilter{mut table_obj, rows} = input_result {
                for row in rows.iter() {
                    let pk_col_idx = table_obj.map.get(&table_obj.pk_col.0).unwrap();
                    table_obj.table_obj.delete_row(&row[*pk_col_idx], tnx_id)?;
                }
                Ok(Mutation("Delete successful".to_string()))
            }else {
                Err(RsqlError::ExecutionError(format!("Delete input must be a TableWithFilter")))
            }
        },
        PlanNode::Update { input, assignments } => {
            info!("Implement Update execution");
            let input_result = execute_dml_plan_node(input, tnx_id, false)?;
            if let TableWithFilter {mut table_obj, rows} = input_result {
                handle_update_expr(&mut table_obj, assignments, &rows, tnx_id)?;
                Ok(Mutation("Update successful".to_string()))
            }else {
                Err(RsqlError::ExecutionError(format!("Update input must be a TableWithFilter")))
            }
        },
        _ => {
            panic!("Unsupported DML operation")
        }
    }
}

fn handle_join(left_table_obj: &TableObject, right_table_obj: &TableObject, join_type: &JoinType, on: &Option<Expr>) -> RsqlResult<((Vec<String>, Vec<ColType>), Vec<Vec<DataItem>>)> {
    let mut extended_cols = left_table_obj.cols.0.clone();
    let mut extended_cols_type = left_table_obj.cols.1.clone();
    extended_cols.extend(right_table_obj.cols.0.clone());
    extended_cols_type.extend(right_table_obj.cols.1.clone());
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
    let mut left_null_row = vec![];
    let mut right_null_row = vec![];
    for col_type in left_table_obj.cols.1.iter() {
        match col_type {
            ColType::Integer => {
                left_null_row.push(DataItem::NullInt);
            },
            ColType::Float => {
                left_null_row.push(DataItem::NullFloat);
            },
            ColType::Chars(size) => {
                left_null_row.push(DataItem::NullChars { len: *size as u64 });
            },
            ColType::Bool => {
                left_null_row.push(DataItem::NullBool);
            },
            ColType::VarChar(_) => {
                left_null_row.push(DataItem::NullVarChar);
            }
        }
    }
    for col_type in right_table_obj.cols.1.iter() {
        match col_type {
            ColType::Integer => {
                right_null_row.push(DataItem::NullInt);
            },
            ColType::Float => {
                right_null_row.push(DataItem::NullFloat);
            },
            ColType::Chars(size) => {
                right_null_row.push(DataItem::NullChars { len: *size as u64 });
            },
            ColType::Bool => {
                right_null_row.push(DataItem::NullBool);
            },
            ColType::VarChar(_) => {
                right_null_row.push(DataItem::NullVarChar);
            }
        }
    }
    let join_result = handle_on_expr(&left_null_row, &right_null_row, &extended_cols, &extended_rows, join_type, on)?;
    Ok(((extended_cols, extended_cols_type), join_result))
}