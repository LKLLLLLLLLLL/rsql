use super::super::common::{RsqlResult, RsqlError};
use super::super::sql_parser::plan::{JoinType};
use crate::db::data_item::{DataItem};
use crate::db::table_schema::{ColType};
use super::handler::{TableObject};
use sqlparser::ast::{Expr, BinaryOperator, Value::{Number, SingleQuotedString, Boolean}};

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

pub fn handle_table_obj_filter_expr(table_obj: &TableObject, predicate: &Expr) -> RsqlResult<Vec<Vec<DataItem>>> {
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
                                        let some_number_value = Some(number_value.clone());
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &some_number_value, &some_number_value)?;
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
                                    let some_string_value = Some(string_value.clone());
                                    if col == table_obj.pk_col.0 {
                                        let row = table_obj.table_obj.get_row_by_pk(&string_value)?;
                                        if let Some(r) = row {
                                            Ok(vec![r])
                                        }else {
                                            Ok(vec![])
                                        }
                                    }else if table_obj.indexed_cols.contains(&col) {
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &some_string_value, &some_string_value)?;
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
                                    let some_number_value = Some(number_value.clone());
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_min = None;
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &col_min, &some_number_value)?;
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
                                    let some_number_value = Some(number_value.clone());
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_max = None;
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &some_number_value, &col_max)?;
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
                                    let some_number_value = Some(number_value.clone());
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_min = None;
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &col_min, &some_number_value)?;
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
                                    let some_number_value = Some(number_value.clone());
                                    if table_obj.indexed_cols.contains(&col) {
                                        let col_max = None;
                                        let rows_iter = table_obj.table_obj.get_rows_by_range_indexed_col(&col, &some_number_value, &col_max)?;
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

pub fn handle_temp_table_filter_expr(cols: &Vec<String>, cols_type: &Vec<ColType>, rows: &Vec<Vec<DataItem>>, predicate: &Expr) -> RsqlResult<Vec<Vec<DataItem>>> {
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

pub fn handle_on_expr(left_null_row: &Vec<DataItem>, right_null_row: &Vec<DataItem>, extended_cols: &Vec<String>, extended_rows: &Vec<Vec<DataItem>>, join_type: &JoinType, on: &Option<Expr>) -> RsqlResult<Vec<Vec<DataItem>>> {
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