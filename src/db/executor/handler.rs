use super::super::errors::{RsqlResult, RsqlError};
use super::super::sql_parser::plan::{PlanNode, JoinType};
use crate::db::data_item::{DataItem};
use crate::db::table_schema::{TableSchema, ColType, TableColumn};
use crate::db::storage_engine::table::{Table};
use self::ExecutionResult::{Query, Mutation, Ddl, TableObj, TableWithFilter, TempTable};
use tracing::info;
use std::collections::HashMap;
use sqlparser::ast::{Expr, BinaryOperator, Value::{Number, SingleQuotedString, Boolean}};

pub enum ExecutionResult {
    Query {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>, // query result
    },
    Mutation, // update, delete, insert
    Ddl, // create, drop
    TableObj(TableObject), // table object after scan
    TableWithFilter {
        table_obj: TableObject,
        rows: Vec<Vec<DataItem>>, // temp query result after filter
    },
    TempTable {
        cols: (Vec<String>, Vec<ColType>),
        rows: Vec<Vec<DataItem>>,
        table_name: Option<String>,
    } // used for join, group by, aggregate and subquery
}

pub struct TableObject {
    table_obj: Table,
    map: HashMap<String, usize>, // col_name -> col_index
    cols: (Vec<String>, Vec<ColType>), // (cols_name, cols_type)
    indexed_cols: Vec<String>, // indexed columns
    pk_col: (String, ColType), // primary key column name
}

pub fn execute_plan_node(node: &PlanNode) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::TableScan { table } => {
            info!("Implement TableScan execution");
            // 1. get Table
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
            // 2. construct TableObject
            let mut map = HashMap::new();
            let mut cols_name = vec![];
            let mut cols_type = vec![];
            let mut pk_col_name = String::new();
            let mut pk_col_type = ColType::Integer;
            for (idx, col) in table_schema.columns.iter().enumerate() {
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
            Ok(TableObj(table_object)) // get table object after scan
        }
        PlanNode::Filter { predicate, input } => {
            info!("Implement Filter execution");
            let input_result = execute_plan_node(input)?;
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
        }
        PlanNode::Projection { exprs, input } => {
            info!("Implement Projection execution");
            let input_result = execute_plan_node(input)?;
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
                    })
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



fn parse_number(s: &str) -> RsqlResult<DataItem> {
    // 1. try to parse integer
    if let Ok(int) = s.parse::<i64>() {
        return Ok(DataItem::Integer(int));
    }
    
    // 2. try to parse float
    if let Ok(float) = s.parse::<f64>() {
        return Ok(DataItem::Float(float));
    }

    Err(RsqlError::InvalidInput(format!("Failed to parse number from string: {}", s)))
}

fn handle_table_obj_filter_expr(table_obj: &TableObject, predicate: &Expr) -> RsqlResult<Vec<Vec<DataItem>>> {
    match predicate {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    let left_rows = handle_table_obj_filter_expr(table_obj, left)?;
                    let right_rows = handle_table_obj_filter_expr(table_obj, right)?;
                    let filtered_rows = left_rows
                        .into_iter()
                        .filter(|left_row| {
                            right_rows.iter().any(|right_row| left_row == right_row)
                        })
                        .collect(); // filter rows by AND (find rows that satisfy both left and right)
                    Ok(filtered_rows)
                },
                BinaryOperator::Or => {
                    let left_rows = handle_table_obj_filter_expr(table_obj, left)?;
                    let right_rows = handle_table_obj_filter_expr(table_obj, right)?;
                    let mut filtered_rows = left_rows;
                    for row in right_rows {
                        if !filtered_rows.iter().any(|r| r == &row) {
                            filtered_rows.push(row);
                        }
                    }
                    Ok(filtered_rows)
                },
                BinaryOperator::Eq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Boolean(b) => {
                                    let col = ident.value.clone();
                                    if col == table_obj.pk_col.0 {
                                        let bool_value = DataItem::Bool(*b);
                                        let row = table_obj.table_obj.get_row_by_pk(&bool_value)?;
                                        if let Some(r) = row {
                                            Ok(vec![r])
                                        }else {
                                            Ok(vec![])
                                        }
                                    }else {
                                        let col_idx = table_obj.map.get(&col).unwrap();
                                        let bool_value = DataItem::Bool(*b);
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] == bool_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    if col == table_obj.pk_col.0 {
                                        let number_value = parse_number(n)?;
                                        let row = table_obj.table_obj.get_row_by_pk(&number_value)?;
                                        if let Some(r) = row {
                                            Ok(vec![r])
                                        }else {
                                            Ok(vec![])
                                        }
                                    }else if table_obj.indexed_cols.contains(&col) {
                                        let number_value = parse_number(n)?;
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &number_value, &number_value)?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            rows.push(row);
                                        }
                                        Ok(rows)
                                    }else {
                                        let col_idx = table_obj.map.get(&col).unwrap();
                                        let number_value = parse_number(n)?;
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] == number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                SingleQuotedString(s) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let col_type = table_obj.cols.1[*col_idx].clone();
                                    let string_value = match col_type {
                                        ColType::Chars(size) => DataItem::Chars{len: size as u64, value: s.clone()},
                                        _ => return Err(RsqlError::ExecutionError(format!("Unsupported char type on table {col}")))
                                    };
                                    if col == table_obj.pk_col.0 {
                                        let row = table_obj.table_obj.get_row_by_pk(&string_value)?;
                                        if let Some(r) = row {
                                            Ok(vec![r])
                                        }else {
                                            Ok(vec![])
                                        }
                                    }else if table_obj.indexed_cols.contains(&col) {
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &string_value, &string_value)?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            rows.push(row);
                                        }
                                        Ok(rows)
                                    }else {
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] == string_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = table_obj.map.get(&left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = table_obj.map.get(&right_col).unwrap();
                            let rows_iter = table_obj.table_obj.get_all_rows()?;
                            let mut rows = vec![];
                            for row in rows_iter {
                                let row = row?;
                                if row[*left_col_idx] == row[*right_col_idx] {
                                    rows.push(row);
                                }
                            }
                            Ok(rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::LtEq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let number_value = parse_number(n)?;
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_min = DataItem::Integer(-2147483647);
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &col_min, &number_value)?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            rows.push(row);
                                        }
                                        Ok(rows)
                                    }else {
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] <= number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = table_obj.map.get(&left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = table_obj.map.get(&right_col).unwrap();
                            let rows_iter = table_obj.table_obj.get_all_rows()?;
                            let mut rows = vec![];
                            for row in rows_iter {
                                let row = row?;
                                if row[*left_col_idx] <= row[*right_col_idx] {
                                    rows.push(row);
                                }
                            }
                            Ok(rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::GtEq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) =>{
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let number_value = parse_number(n)?;
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_max = DataItem::Integer(2147483647);
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &number_value, &col_max)?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            rows.push(row);
                                        }
                                        Ok(rows)
                                    }else {
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] >= number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = table_obj.map.get(&left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = table_obj.map.get(&right_col).unwrap();
                            let rows_iter = table_obj.table_obj.get_all_rows()?;
                            let mut rows = vec![];
                            for row in rows_iter {
                                let row = row?;
                                if row[*left_col_idx] >= row[*right_col_idx] {
                                    rows.push(row);
                                }
                            }
                            Ok(rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::Lt => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let number_value = parse_number(n)?;
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_min = DataItem::Integer(-2147483647);
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &col_min, &number_value)?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] < number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }else {
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] < number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = table_obj.map.get(&left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = table_obj.map.get(&right_col).unwrap();
                            let rows_iter = table_obj.table_obj.get_all_rows()?;
                            let mut rows = vec![];
                            for row in rows_iter {
                                let row = row?;
                                if row[*left_col_idx] < row[*right_col_idx] {
                                    rows.push(row);
                                }
                            }
                            Ok(rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::Gt => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let number_value = parse_number(n)?;
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_max = DataItem::Integer(2147483647);
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &number_value, &col_max)?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] > number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }else {
                                        let rows_iter = table_obj.table_obj.get_all_rows()?;
                                        let mut rows = vec![];
                                        for row in rows_iter {
                                            let row = row?;
                                            if row[*col_idx] > number_value {
                                                rows.push(row);
                                            }
                                        }
                                        Ok(rows)
                                    }
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = table_obj.map.get(&left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = table_obj.map.get(&right_col).unwrap();
                            let rows_iter = table_obj.table_obj.get_all_rows()?;
                            let mut rows = vec![];
                            for row in rows_iter {
                                let row = row?;
                                if row[*left_col_idx] > row[*right_col_idx] {
                                    rows.push(row);
                                }
                            }
                            Ok(rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                _ => {
                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                },
            }
        },
        _ => {
            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
        }
    }
}

fn handle_temp_table_filter_expr(cols: &Vec<String>, cols_type: &Vec<ColType>, rows: &Vec<Vec<DataItem>>, predicate: &Expr) -> RsqlResult<Vec<Vec<DataItem>>> {
    match predicate {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    let left_rows = handle_temp_table_filter_expr(cols, cols_type, rows, left)?;
                    let right_rows = handle_temp_table_filter_expr(cols, cols_type, rows, right)?;
                    let filtered_rows = left_rows
                        .into_iter()
                        .filter(|left_row| {
                            right_rows.iter().any(|right_row| left_row == right_row)
                        })
                        .collect(); // filter rows by AND (find rows that satisfy both left and right)
                    Ok(filtered_rows)
                },
                BinaryOperator::Or => {
                    let left_rows = handle_temp_table_filter_expr(cols, cols_type, rows, left)?;
                    let right_rows = handle_temp_table_filter_expr(cols, cols_type, rows, right)?;
                    let mut filtered_rows = left_rows;
                    for row in right_rows {
                        if !filtered_rows.iter().any(|r| r == &row) {
                            filtered_rows.push(row);
                        }
                    }
                    Ok(filtered_rows)
                },
                BinaryOperator::Eq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            let col = ident.value.clone();
                            let col_idx = cols.iter().position(|c| c == &col).unwrap();
                            match &value.value {
                                Boolean(b) => {
                                    let bool_value = DataItem::Bool(*b);
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] == bool_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                Number(n, _) => {
                                    let number_value = parse_number(n)?;
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] == number_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                SingleQuotedString(s) => {
                                    let col_type = cols_type[col_idx].clone();
                                    let string_value = match col_type {
                                        ColType::Chars(size) => DataItem::Chars{len: size as u64, value: s.clone()},
                                        _ => {
                                            return Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                        },
                                    };
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] == string_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = cols.iter().position(|col| col == &left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = cols.iter().position(|col| col == &right_col).unwrap();
                            let mut filtered_rows = vec![];
                            for row in rows.iter() {
                                if row[left_col_idx] == row[right_col_idx] {
                                    filtered_rows.push(row.clone());
                                }
                            }
                            Ok(filtered_rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::LtEq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = cols.iter().position(|c| c == &col).unwrap();
                                    let number_value = parse_number(n)?;
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] <= number_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = cols.iter().position(|col| col == &left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = cols.iter().position(|col| col == &right_col).unwrap();
                            let mut filtered_rows = vec![];
                            for row in rows.iter() {
                                if row[left_col_idx] <= row[right_col_idx] {
                                    filtered_rows.push(row.clone());
                                }
                            }
                            Ok(filtered_rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::GtEq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = cols.iter().position(|c| c == &col).unwrap();
                                    let number_value = parse_number(n)?;
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] >= number_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = cols.iter().position(|col| col == &left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = cols.iter().position(|col| col == &right_col).unwrap();
                            let mut filtered_rows = vec![];
                            for row in rows.iter() {
                                if row[left_col_idx] >= row[right_col_idx] {
                                    filtered_rows.push(row.clone());
                                }
                            }
                            Ok(filtered_rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::Lt => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = cols.iter().position(|c| c == &col).unwrap();
                                    let number_value = parse_number(n)?;
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] < number_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = cols.iter().position(|col| col == &left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = cols.iter().position(|col| col == &right_col).unwrap();
                            let mut filtered_rows = vec![];
                            for row in rows.iter() {
                                if row[left_col_idx] < row[right_col_idx] {
                                    filtered_rows.push(row.clone());
                                }
                            }
                            Ok(filtered_rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                BinaryOperator::Gt => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = cols.iter().position(|c| c == &col).unwrap();
                                    let number_value = parse_number(n)?;
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] > number_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                }
                            }
                        },
                        (Expr::Identifier(left_ident), Expr::Identifier(right_ident)) => {
                            let left_col = left_ident.value.clone();
                            let left_col_idx = cols.iter().position(|col| col == &left_col).unwrap();
                            let right_col = right_ident.value.clone();
                            let right_col_idx = cols.iter().position(|col| col == &right_col).unwrap();
                            let mut filtered_rows = vec![];
                            for row in rows.iter() {
                                if row[left_col_idx] > row[right_col_idx] {
                                    filtered_rows.push(row.clone());
                                }
                            }
                            Ok(filtered_rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        }
                    }
                },
                _ => {
                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                },
            }
        },
        _ => {
            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
        }
    }
}

fn handle_on_expr(left_null_row: &Vec<DataItem>, right_null_row: &Vec<DataItem>, extended_cols: &Vec<String>, extended_rows: &Vec<Vec<DataItem>>, join_type: &JoinType, on: &Option<Expr>) -> RsqlResult<Vec<Vec<DataItem>>> {
    match join_type {
        JoinType::Inner => {
            if let Some(on) = on {
                match on {
                    Expr::BinaryOp { left, op, right } => {
                        match op {
                            BinaryOperator::Eq => {
                                match (&**left, &**right) {
                                    (Expr::CompoundIdentifier(left_ident), Expr::CompoundIdentifier(right_ident)) => {
                                        let left_col = left_ident[1].value.clone();
                                        let left_col_idx = extended_cols.iter().position(|col| col == &left_col).unwrap();
                                        let right_col = right_ident[1].value.clone();
                                        let right_col_idx = extended_cols.iter().rposition(|col| col == &right_col).unwrap();
                                        let mut filtered_rows = vec![];
                                        for row in extended_rows.iter() {
                                            if row[left_col_idx] == row[right_col_idx] {
                                                filtered_rows.push(row.clone());
                                            }
                                        }
                                        Ok(filtered_rows)
                                    },
                                    _ => {
                                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator between two identifiers")))
                                    }
                                }
                            },
                            _ => {
                                Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator")))
                            }
                        }
                    },
                    _ => {
                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression")))
                    }
                }
            }else {
                Err(RsqlError::ExecutionError(format!("Join type Inner must have on clause")))
            }
        },
        JoinType::Left => {
            if let Some(on) = on {
                match on {
                    Expr::BinaryOp { left, op, right } => {
                        match op {
                            BinaryOperator::Eq => {
                                match (&**left, &**right) {
                                    (Expr::CompoundIdentifier(left_ident), Expr::CompoundIdentifier(right_ident)) => {
                                        let left_col = left_ident[1].value.clone();
                                        let left_col_idx = extended_cols.iter().position(|col| col == &left_col).unwrap();
                                        let right_col = right_ident[1].value.clone();
                                        let right_col_idx = extended_cols.iter().rposition(|col| col == &right_col).unwrap();
                                        let mut filtered_rows = vec![];
                                        for row in extended_rows.iter() {
                                            if row[left_col_idx] == row[right_col_idx] {
                                                filtered_rows.push(row.clone());
                                            }else {
                                                let left_table_cols_len = left_null_row.len(); 
                                                let mut new_row = row.clone();
                                                new_row.truncate(left_table_cols_len);  // remove right table cols from the extended cols
                                                new_row.extend(right_null_row.clone()); // append right null row
                                                filtered_rows.push(new_row);
                                            }
                                        }
                                        Ok(filtered_rows)
                                    },
                                    _ => {
                                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator between two identifiers")))
                                    }
                                }
                            },
                            _ => {
                                Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator")))
                            }
                        }
                    },
                    _ => {
                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression")))
                    }
                }
            }else {
                Err(RsqlError::ExecutionError(format!("Join type Left must have on clause")))
            }
        },
        JoinType::Right => {
            if let Some(on) = on {
                match on {
                    Expr::BinaryOp { left, op, right } => {
                        match op {
                            BinaryOperator::Eq => {
                                match (&**left, &**right) {
                                    (Expr::CompoundIdentifier(left_ident), Expr::CompoundIdentifier(right_ident)) => {
                                        let left_col = left_ident[1].value.clone();
                                        let left_col_idx = extended_cols.iter().position(|col| col == &left_col).unwrap();
                                        let right_col = right_ident[1].value.clone();
                                        let right_col_idx = extended_cols.iter().rposition(|col| col == &right_col).unwrap();
                                        let mut filtered_rows = vec![];
                                        for row in extended_rows.iter() {
                                            if row[left_col_idx] == row[right_col_idx] {
                                                filtered_rows.push(row.clone());
                                            }else {
                                                let left_table_cols_len = left_null_row.len();
                                                let mut right_table_row = row.clone();
                                                right_table_row.drain(0..left_table_cols_len); // keep right table cols from the extended cols
                                                let mut new_row = left_null_row.clone(); // create a new row with left null row
                                                new_row.extend(right_table_row); // append right table row
                                                filtered_rows.push(new_row);
                                            }
                                        }
                                        Ok(filtered_rows)
                                    },
                                    _ => {
                                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator between two identifiers")))
                                    }
                                }
                            },
                            _ => {
                                Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator")))
                            }
                        }
                    },
                    _ => {
                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression")))
                    }
                }
            }else {
                Err(RsqlError::ExecutionError(format!("Join type Right must have on clause")))
            }
        },
        JoinType::Full => {
            if let Some(on) = on {
                match on {
                    Expr::BinaryOp { left, op, right } => {
                        match op {
                            BinaryOperator::Eq => {
                                match (&**left, &**right) {
                                    (Expr::CompoundIdentifier(left_ident), Expr::CompoundIdentifier(right_ident)) => {
                                        let left_col = left_ident[1].value.clone();
                                        let left_col_idx = extended_cols.iter().position(|col| col == &left_col).unwrap();
                                        let right_col = right_ident[1].value.clone();
                                        let right_col_idx = extended_cols.iter().rposition(|col| col == &right_col).unwrap();
                                        let mut filtered_rows = vec![];
                                        for row in extended_rows.iter() {
                                            if row[left_col_idx] == row[right_col_idx] {
                                                filtered_rows.push(row.clone());
                                            }else {
                                                let left_table_cols_len = left_null_row.len();
                                                let mut left_join_new_row = row.clone();
                                                // 1. left join
                                                left_join_new_row.truncate(left_table_cols_len);  // remove right table cols from the extended cols
                                                left_join_new_row.extend(right_null_row.clone()); // append right null row
                                                filtered_rows.push(left_join_new_row);
                                                // 2. right join
                                                let mut right_table_row = row.clone();
                                                right_table_row.drain(0..left_table_cols_len); // keep right table cols from the extended cols
                                                let mut right_join_new_row = left_null_row.clone(); // create a new row with left null row
                                                right_join_new_row.extend(right_table_row); // append right table row
                                                filtered_rows.push(right_join_new_row);
                                            }
                                        }
                                        Ok(filtered_rows)
                                    },
                                    _ => {
                                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator between two identifiers")))
                                    }
                                }
                            },
                            _ => {
                                Err(RsqlError::ExecutionError(format!("On clause must be a binary expression with Eq operator")))
                            }
                        }
                    },
                    _ => {
                        Err(RsqlError::ExecutionError(format!("On clause must be a binary expression")))
                    }
                }
            }else {
                Err(RsqlError::ExecutionError(format!("Join type Full must have on clause")))
            }
        },
        JoinType::Cross => {
            Ok(extended_rows.clone())
        },
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