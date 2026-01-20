use crate::common::{RsqlResult, RsqlError};
use crate::sql::plan::{JoinType};
use crate::common::data_item::{DataItem, VarCharHead};
use crate::catalog::table_schema::{ColType};
use super::result::{TableObject};
use sqlparser::ast::{Expr, 
    BinaryOperator, 
    Value::{Number, SingleQuotedString, Boolean}, 
    FunctionArguments,
    FunctionArg,
    FunctionArgExpr,
    ObjectName,
    ObjectNamePart,
};
// use tracing::info;

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

fn get_func_arg(args: &FunctionArguments) -> RsqlResult<String> {
    let arg = match args {
        FunctionArguments::List(arg_list) => {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) = arg_list.args[0].clone() {
                ident.value
            }else {
                return Err(RsqlError::ExecutionError(format!("Failed to parse function argument: {:?}", args)))
            }
        },
        _ => {
            return Err(RsqlError::ExecutionError(format!("Failed to parse function argument: {:?}", args)))
        }
    };
    Ok(arg)
}

fn get_func_name(func_obj_name: &ObjectName) -> RsqlResult<String> {
    let name = match func_obj_name {
        ObjectName(obj_name_part) => {
            if let ObjectNamePart::Identifier(ident) = obj_name_part[0].clone() {
                ident.value.to_uppercase()
            }else {
                return Err(RsqlError::ExecutionError(format!("Failed to parse function name: {:?}", func_obj_name)))
            }
        }
    };
    Ok(name)
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
                                        ColType::VarChar(_) => DataItem::VarChar {
                                            head: VarCharHead {max_len: s.len() as u64, len: s.len() as u64, page_ptr: None},
                                            value: s.clone(),
                                        },
                                        _ => return Err(RsqlError::ExecutionError(format!("Unsupported char type on column {col}")))
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
                BinaryOperator::NotEq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            match &value.value {
                                Boolean(b) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let bool_value = DataItem::Bool(*b);
                                    let rows_iter = table_obj.table_obj.get_all_rows()?;
                                    let mut rows = vec![];
                                    for row in rows_iter {
                                        let row = row?;
                                        if row[*col_idx] != bool_value {
                                            rows.push(row);
                                        }
                                    }
                                    Ok(rows)
                                },
                                Number(n, _) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let number_value = parse_number(n)?;
                                    let rows_iter = table_obj.table_obj.get_all_rows()?;
                                    let mut rows = vec![];
                                    for row in rows_iter {
                                        let row = row?;
                                        if row[*col_idx] != number_value {
                                            rows.push(row);
                                        }
                                    }
                                    Ok(rows)
                                },
                                SingleQuotedString(s) => {
                                    let col = ident.value.clone();
                                    let col_idx = table_obj.map.get(&col).unwrap();
                                    let col_type = table_obj.cols.1[*col_idx].clone();
                                    let string_value = match col_type {
                                        ColType::Chars(size) => DataItem::Chars{len: size as u64, value: s.clone()},
                                        ColType::VarChar(_) => DataItem::VarChar {
                                            head: VarCharHead {max_len: s.len() as u64, len: s.len() as u64, page_ptr: None},
                                            value: s.clone(),
                                        },
                                        _ => return Err(RsqlError::ExecutionError(format!("Unsupported char type on table {col}")))
                                    };
                                    let rows_iter = table_obj.table_obj.get_all_rows()?;
                                    let mut rows = vec![];
                                    for row in rows_iter {
                                        let row = row?;
                                        if row[*col_idx] != string_value {
                                            rows.push(row);
                                        }
                                    }
                                    Ok(rows)
                                },
                                _ => {
                                    Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                },
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
                                if row[*left_col_idx] != row[*right_col_idx] {
                                    rows.push(row);
                                }
                            }
                            Ok(rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        },
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
                                        ColType::VarChar(_) => DataItem::VarChar {
                                            head: VarCharHead {max_len: s.len() as u64, len: s.len() as u64, page_ptr: None},
                                            value: s.clone(),
                                        },
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
                BinaryOperator::NotEq => {
                    match (&**left, &**right) {
                        (Expr::Identifier(ident), Expr::Value(value)) => {
                            let col = ident.value.clone();
                            let col_idx = cols.iter().position(|c| c == &col).unwrap();
                            match &value.value {
                                Boolean(b) => {
                                    let bool_value = DataItem::Bool(*b);
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] != bool_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                Number(n, _) => {
                                    let number_value = parse_number(n)?;
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] != number_value {
                                            filtered_rows.push(row.clone());
                                        }
                                    }
                                    Ok(filtered_rows)
                                },
                                SingleQuotedString(s) => {
                                    let col_type = cols_type[col_idx].clone();
                                    let string_value = match col_type {
                                        ColType::Chars(size) => DataItem::Chars{len: size as u64, value: s.clone()},
                                        ColType::VarChar(_) => DataItem::VarChar {
                                            head: VarCharHead {max_len: s.len() as u64, len: s.len() as u64, page_ptr: None},
                                            value: s.clone(),
                                        },
                                        _ => {
                                            return Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                                        },
                                    };
                                    let mut filtered_rows = vec![];
                                    for row in rows.iter() {
                                        if row[col_idx] != string_value {
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
                                if row[left_col_idx] != row[right_col_idx] {
                                    filtered_rows.push(row.clone());
                                }
                            }
                            Ok(filtered_rows)
                        },
                        _ => {
                            Err(RsqlError::ExecutionError(format!("Unsupported filter expression: {:?}", predicate)))
                        },
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

pub fn handle_insert_expr(table_object: &TableObject, cols: &Vec<String>, null_cols: &Vec<DataItem>, values: &Vec<Expr>) -> RsqlResult<Vec<DataItem>> {
    let mut data_item = null_cols.clone();
    for (idx, expr) in values.iter().enumerate() {
        match expr {
            Expr::Value(value) => {
                match &value.value {
                    Boolean(b) => {
                        let col_idx = table_object.map.get(&cols[idx]).unwrap();
                        data_item[*col_idx] = DataItem::Bool(*b);
                    },
                    Number(n, _) => {
                        let number_value = parse_number(n)?;
                        let col_idx = table_object.map.get(&cols[idx]).unwrap();
                        data_item[*col_idx] = number_value;
                    },
                    SingleQuotedString(s) => {
                        let col_idx = table_object.map.get(&cols[idx]).unwrap();
                        let col_type = table_object.cols.1[*col_idx].clone();
                        match col_type {
                            ColType::Chars(size) => {
                                data_item[*col_idx] = DataItem::Chars{len: size as u64, value: s.clone()};
                            },
                            ColType::VarChar(_) => {
                                data_item[*col_idx] = DataItem::VarChar {
                                    head: VarCharHead {max_len: s.len() as u64, len: s.len() as u64, page_ptr: None},
                                    value: s.clone(),
                                }
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Unsupported insert value type: {:?}", value.value)))
                            },
                        }
                    },
                    _ => {
                        return Err(RsqlError::ExecutionError(format!("Unsupported insert value type: {:?}", value.value)))
                    },
                }
            },
            _ => {
                return Err(RsqlError::ExecutionError(format!("Insert value must be a constant expression")))
            }
        }
    }
    Ok(data_item)
}

pub fn handle_update_expr(table_object: &mut TableObject, assignments: &Vec<(String, Expr)>, rows: &Vec<Vec<DataItem>>, tnx_id: u64) -> RsqlResult<()> {
    let mut updated_rows = rows.clone(); // clone the rows to update
    for (col_name, expr) in assignments.iter() {
        let tar_col_idx = table_object.map.get(col_name).unwrap();
        match expr {
            Expr::Identifier(ident) => {
                let src_col_name = ident.value.clone();
                let src_col_idx = table_object.map.get(&src_col_name).unwrap(); // get the source column index to assign its value to the target column
                for row in updated_rows.iter_mut() {
                    let src_col_value = row[*src_col_idx].clone();
                    row[*tar_col_idx] = src_col_value;
                }
            },
            Expr::Value(value) => {
                match &value.value {
                    Boolean(b) => {
                        let bool_value = DataItem::Bool(*b);
                        for row in updated_rows.iter_mut() {
                            row[*tar_col_idx] = bool_value.clone();
                        }
                    },
                    Number(n, _) => {
                        let number_value = parse_number(n)?;
                        for row in updated_rows.iter_mut() {
                            row[*tar_col_idx] = number_value.clone();
                        }
                    },
                    SingleQuotedString(s) => {
                        let col_type = table_object.cols.1[*tar_col_idx].clone();
                        let string_value = match col_type {
                            ColType::Chars(size) => DataItem::Chars{len: size as u64, value: s.clone()},
                            ColType::VarChar(_) => DataItem::VarChar {
                                head: VarCharHead {max_len: s.len() as u64, len: s.len() as u64, page_ptr: None},
                                value: s.clone(),
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Unsupported update value type: {:?}", value.value)))
                            },
                        };
                        for row in updated_rows.iter_mut() {
                            row[*tar_col_idx] = string_value.clone();
                        }
                    },
                    _ => {
                        return Err(RsqlError::ExecutionError(format!("Unsupported update value type: {:?}", value.value)))
                    }
                }
            },
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Plus => {
                        match (&**left, &**right) {
                            (Expr::Identifier(ident), Expr::Value(value)) => {
                                match &value.value {
                                    Number(n, _) => {
                                        let src_col_name = ident.value.clone();
                                        let src_col_idx = table_object.map.get(&src_col_name).unwrap();
                                        let number_value = parse_number(n)?;
                                        match number_value {
                                            DataItem::Integer(n_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Integer(n_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Integer(n_left + n_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            DataItem::Float(f_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Float(f_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Float(f_left + f_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            _ => {
                                                return Err(RsqlError::ExecutionError(format!("unsupported operation number type {number_value:?}")))
                                            },
                                        }
                                    },
                                    _ => {
                                        return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                                    },
                                }
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                            },
                        }
                    },
                    BinaryOperator::Minus => {
                        match (&**left, &**right) {
                            (Expr::Identifier(ident), Expr::Value(value)) => {
                                match &value.value {
                                     Number(n, _) => {
                                        let src_col_name = ident.value.clone();
                                        let src_col_idx = table_object.map.get(&src_col_name).unwrap();
                                        let number_value = parse_number(n)?;
                                        match number_value {
                                             DataItem::Integer(n_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Integer(n_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Integer(n_left - n_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            DataItem::Float(f_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Float(f_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Float(f_left - f_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                             _ => {
                                                 return Err(RsqlError::ExecutionError(format!("unsupported operation number type {number_value:?}")))
                                             },
                                        }
                                     },
                                     _ => {
                                         return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                                     },
                                }
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                            },
                        }
                    },
                    BinaryOperator::Multiply => {
                        match (&**left, &**right) {
                            (Expr::Identifier(ident), Expr::Value(value)) => {
                                match &value.value {
                                    Number(n, _) => {
                                        let src_col_name = ident.value.clone();
                                        let src_col_idx = table_object.map.get(&src_col_name).unwrap();
                                        let number_value = parse_number(n)?;
                                        match number_value {
                                            DataItem::Integer(n_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Integer(n_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Integer(n_left * n_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            DataItem::Float(f_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Float(f_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Float(f_left * f_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            _ => {
                                                return Err(RsqlError::ExecutionError(format!("unsupported operation number type {number_value:?}")))
                                            },
                                        }
                                    },
                                    _ => {
                                        return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                                    },
                                }
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                            },
                        }
                    },
                    BinaryOperator::Divide => {
                        match (&**left, &**right) {
                            (Expr::Identifier(ident), Expr::Value(value)) => {
                                match &value.value {
                                    Number(n, _) => {
                                        let src_col_name = ident.value.clone();
                                        let src_col_idx = table_object.map.get(&src_col_name).unwrap();
                                        let number_value = parse_number(n)?;
                                        match number_value {
                                            DataItem::Integer(n_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Integer(n_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Integer(n_left / n_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            DataItem::Float(f_right) => {
                                                for row in updated_rows.iter_mut() {
                                                    let src_col_value = row[*src_col_idx].clone();
                                                    if let DataItem::Float(f_left) = src_col_value {
                                                        row[*tar_col_idx] = DataItem::Float(f_left / f_right);
                                                    }else {
                                                        return Err(RsqlError::ExecutionError(format!("dismatched operation number type")))
                                                    }
                                                }
                                            },
                                            _ => {
                                                return Err(RsqlError::ExecutionError(format!("unsupported operation number type {number_value:?}")))
                                            },
                                        }
                                    },
                                    _ => {
                                        return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                                    },
                                }
                            },
                            _ => {
                                return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
                            },
                        }
                    },
                    _ => {
                        return Err(RsqlError::ExecutionError(format!("Unsupported binary operator: {:?}", op)))
                    }
                }
            },
            _ => {
                return Err(RsqlError::ExecutionError(format!("Unsupported update expression: {:?}", expr)))
            },
        }
    }
    // uniformly update the rows at the end
    let pk_col_idx = table_object.map.get(&table_object.pk_col.0).unwrap();
    for row in updated_rows.iter() {
        let pk_col_value = &row[*pk_col_idx].clone();
        table_object.table_obj.update_row(pk_col_value, row.clone(), tnx_id)?;
    }
    Ok(())
}

pub fn handle_aggr_expr (table_obj: TableObject, group_by: &Vec<Expr>, aggr_exprs: &Vec<Expr>) -> RsqlResult<((Vec<String>, Vec<ColType>), Vec<Vec<DataItem>>, Vec<String>)> {
    let mut cols_name = vec![];
    let mut cols_type = vec![];
    let mut aggr_cols = vec![];
    let mut aggr_rows = vec![];
    let mut group_by_cols_idx = vec![];
    // 1. construct aggr_cols, (cols_name, cols_type), group_by_cols_idx
    for group_by_col_expr in group_by.iter() {
        if let Expr::Identifier(ident) = group_by_col_expr {
            let col_idx = table_obj.map.get(&ident.value).unwrap();
            let col_type = table_obj.cols.1[*col_idx].clone();
            cols_type.push(col_type);
            cols_name.push(ident.value.clone());
            group_by_cols_idx.push(*col_idx);
        }
    }
    for aggr_expr in aggr_exprs.iter() {
        match aggr_expr {
            Expr::Function(func) => {
                let func_name = get_func_name(&func.name)?;
                if func_name == "COUNT" {
                    let col_type = ColType::Integer;
                    let aggr_col_name = "COUNT".to_string();
                    aggr_cols.push(aggr_col_name.clone());
                    cols_name.push(aggr_col_name.clone());
                    cols_type.push(col_type);
                }else if func_name == "AVG" {
                    let col_type = ColType::Float;
                    let func_arg = get_func_arg(&func.args)?;
                    let aggr_col_name = format!("AVG_{}", &func_arg);
                    aggr_cols.push(aggr_col_name.clone());
                    cols_name.push(aggr_col_name.clone());
                    cols_type.push(col_type);
                }else {
                    let func_arg = get_func_arg(&func.args)?;
                    let col_idx = table_obj.map.get(&func_arg).unwrap();
                    let col_type = table_obj.cols.1[*col_idx].clone();
                    let aggr_col_name = format!("{}_{}", &func_name, &func_arg);
                    aggr_cols.push(aggr_col_name.clone());
                    cols_name.push(aggr_col_name.clone());
                    cols_type.push(col_type);
                }
            },
            _ => {
                return Err(RsqlError::ExecutionError(format!("Unsupported aggregate expression: {:?}", aggr_expr)))
            },
        }
    }
    // 2. get all rows from the table
    let mut rows = vec![];
    let rows_iter = table_obj.table_obj.get_all_rows()?;
    for row in rows_iter {
        let row = row?;
        rows.push(row);
    }
    // 3. get all distinct group_by values
    let mut distinct_group_by_values = vec![];
    for row in rows.iter() {
        let mut group_by_values = vec![];
        for group_by_col_expr in group_by.iter() {
            if let Expr::Identifier(ident) = group_by_col_expr {
                let col_idx = table_obj.map.get(&ident.value).unwrap();
                group_by_values.push(row[*col_idx].clone());
            }
        }
        if !distinct_group_by_values.contains(&group_by_values) {
            distinct_group_by_values.push(group_by_values);
        }
    }
    // 4. construct aggr_rows
    for row in distinct_group_by_values.iter() {
        let mut aggr_row = row.clone();
        for aggr_expr in aggr_exprs.iter() {
            match aggr_expr {
                Expr::Function(func) => {
                    let func_name = get_func_name(&func.name)?;
                    match func_name.as_str() {
                        "COUNT" => {
                            let mut count: i64 = 0;
                            for r in rows.iter() {
                                let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                if group_by_row == *row {
                                    count += 1;
                                }
                            }
                            aggr_row.push(DataItem::Integer(count));
                        },
                        "AVG" => {
                            let mut sum: f64 = 0.0;
                            let mut count: i64 = 0;
                            let func_arg = get_func_arg(&func.args)?;
                            let col_idx = table_obj.map.get(&func_arg).unwrap(); // get col_idx from func_arg
                            for r in rows.iter() {
                                let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                if group_by_row == *row {
                                    match r[*col_idx] {
                                        DataItem::Integer(i) => {
                                            sum += i as f64;
                                            count += 1;
                                        },
                                        DataItem::Float(f) => {
                                            sum += f;
                                            count += 1;
                                        },
                                        _ => {
                                            return Err(RsqlError::ExecutionError(format!("unsupported type for AVG: {:?}", r[*col_idx])));
                                        },
                                    }
                                }
                            }
                            aggr_row.push(DataItem::Float(sum / count as f64));
                        },
                        "SUM" => {
                            let func_arg = get_func_arg(&func.args)?;
                            let col_idx = table_obj.map.get(&func_arg).unwrap(); // get col_idx from func_arg
                            let col_type = table_obj.cols.1[*col_idx].clone();
                            match col_type {
                                ColType::Integer => {
                                    let mut sum = 0 as i64;
                                    for r in rows.iter() {
                                        let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                        if group_by_row == *row {
                                            match r[*col_idx] {
                                                DataItem::Integer(i) => {
                                                    sum += i;
                                                },
                                                _ => {
                                                    return Err(RsqlError::ExecutionError(format!("cannot sum other type with integer: {:?}", r[*col_idx])));
                                                },
                                            }
                                        }
                                    }
                                    aggr_row.push(DataItem::Integer(sum));
                                },
                                ColType::Float => {
                                    let mut sum = 0.0 as f64;
                                    for r in rows.iter() {
                                        let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                        if group_by_row == *row {
                                            match r[*col_idx] {
                                                DataItem::Float(f) => {
                                                    sum += f;
                                                },
                                                _ => {
                                                    return Err(RsqlError::ExecutionError(format!("cannot sum other type with float: {:?}", r[*col_idx])));
                                                },
                                            }
                                        }
                                    }
                                    aggr_row.push(DataItem::Float(sum));
                                },
                                _ => {
                                    return Err(RsqlError::ExecutionError(format!("unsupported type for SUM")));
                                }
                            }
                        },
                        "MIN" => {
                            let func_arg = get_func_arg(&func.args)?;
                            let col_idx = table_obj.map.get(&func_arg).unwrap(); // get col_idx from func_arg
                            let col_type = table_obj.cols.1[*col_idx].clone();
                            match col_type {
                                ColType::Integer => {
                                    let mut min = None;
                                    for r in rows.iter() {
                                        let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                        if group_by_row == *row {
                                            match r[*col_idx] {
                                                DataItem::Integer(i) => {
                                                    if min.is_none() || i < min.unwrap() {
                                                        min = Some(i);
                                                    }
                                                },
                                                _ => {
                                                    return Err(RsqlError::ExecutionError(format!("cannot min other type with integer: {:?}", r[*col_idx])));
                                                },
                                            }
                                        }
                                    }
                                    aggr_row.push(DataItem::Integer(min.unwrap()));
                                },
                                ColType::Float => {
                                    let mut min = None;
                                    for r in rows.iter() {
                                        let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                        if group_by_row == *row {
                                            match r[*col_idx] {
                                                DataItem::Float(f) => {
                                                    if min.is_none() || f < min.unwrap() {
                                                        min = Some(f);
                                                    }
                                                },
                                                _ => {
                                                    return Err(RsqlError::ExecutionError(format!("cannot min other type with float: {:?}", r[*col_idx])));
                                                },
                                            }
                                        }
                                    }
                                    aggr_row.push(DataItem::Float(min.unwrap()));
                                },
                                _ => {
                                    return Err(RsqlError::ExecutionError(format!("unsupported type for MIN")));
                                },
                            }
                        },
                        "MAX" => {
                            let func_arg = get_func_arg(&func.args)?;
                            let col_idx = table_obj.map.get(&func_arg).unwrap(); // get col_idx from func_arg
                            let col_type = table_obj.cols.1[*col_idx].clone();
                            match col_type {
                                ColType::Integer => {
                                    let mut max = None;
                                    for r in rows.iter() {
                                        let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                        if group_by_row == *row {
                                            match r[*col_idx] {
                                                DataItem::Integer(i) => {
                                                    if max.is_none() || i > max.unwrap() {
                                                        max = Some(i);
                                                    }
                                                },
                                                _ => {
                                                    return Err(RsqlError::ExecutionError(format!("cannot max other type with integer: {:?}", r[*col_idx])));
                                                },
                                            }
                                        }
                                    }
                                    aggr_row.push(DataItem::Integer(max.unwrap()));
                                },
                                ColType::Float => {
                                    let mut max = None;
                                    for r in rows.iter() {
                                        let group_by_row: Vec<DataItem> = group_by_cols_idx.iter().map(|i| r[*i].clone()).collect();
                                        if group_by_row == *row {
                                            match r[*col_idx] {
                                                DataItem::Float(f) => {
                                                    if max.is_none() || f > max.unwrap() {
                                                        max = Some(f);
                                                    }
                                                },
                                                _ => {
                                                    return Err(RsqlError::ExecutionError(format!("cannot max other type with float: {:?}", r[*col_idx])));
                                                },
                                            }
                                        }
                                    }
                                    aggr_row.push(DataItem::Float(max.unwrap()));
                                },
                                _ => {
                                    return Err(RsqlError::ExecutionError(format!("unsupported type for MAX")));
                                },
                            }
                        },
                        _ => {
                            return Err(RsqlError::ExecutionError(format!("Unsupported aggregate function: {:?}", func_name)))
                        }
                    }
                },
                _ => {
                    return Err(RsqlError::ExecutionError(format!("Unsupported aggregate expression: {:?}", aggr_expr)))
                },
            }
        }
        aggr_rows.push(aggr_row);
    }
    Ok((
        (cols_name, cols_type),
        aggr_rows,
        aggr_cols,
    ))
}