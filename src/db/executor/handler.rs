use super::super::common::{RsqlResult, RsqlError};
use super::super::sql_parser::plan::{PlanNode, JoinType};
use crate::db::data_item::{DataItem};
use crate::db::table_schema::{TableSchema, ColType, TableColumn};
use crate::db::storage_engine::table::{Table};
use self::ExecutionResult::{Query, Mutation, Ddl, TableObj, TableWithFilter, TempTable};
use super::expr_handler::{handle_on_expr, handle_table_obj_filter_expr, handle_temp_table_filter_expr, handle_insert_expr};
use tracing::info;
use std::collections::HashMap;
use sqlparser::ast::{Expr};

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
    pub table_obj: Table,
    pub map: HashMap<String, usize>, // col_name -> col_index
    pub cols: (Vec<String>, Vec<ColType>), // (cols_name, cols_type)
    pub indexed_cols: Vec<String>, // indexed columns
    pub pk_col: (String, ColType), // primary key column name
}

fn get_table_object (table_name: &str) -> RsqlResult<TableObject> {
    // 1. get Table
    let columns = match table_name {
        "users" => vec![
            TableColumn {
                name: "id".to_string(),
                data_type: ColType::Integer,
                pk: true,
                nullable: false,
                index: true,
                unique: true,
            },
            TableColumn {
                name: "name".to_string(),
                data_type: ColType::Chars(50),
                pk: false,
                nullable: false,
                index: false,
                unique: false,
            },
            TableColumn {
                name: "age".to_string(),
                data_type: ColType::Integer,
                pk: false,
                nullable: true,
                index: false,
                unique: false,
            },
        ],
        "products" => vec![
            TableColumn {
                name: "product_id".to_string(),
                data_type: ColType::Integer,
                pk: true,
                nullable: false,
                index: true,
                unique: true,
            },
            TableColumn {
                name: "product_name".to_string(),
                data_type: ColType::Chars(100),
                pk: false,
                nullable: false,
                index: false,
                unique: false,
            },
            TableColumn {
                name: "price".to_string(),
                data_type: ColType::Integer,
                pk: false,
                nullable: false,
                index: false,
                unique: false,
            },
        ],
        _ => vec![
            TableColumn {
                name: "id".to_string(),
                data_type: ColType::Integer,
                pk: true,
                nullable: false,
                index: true,
                unique: false,
            },
        ],
    };
    //Table::from(0, TableSchema::new(columns)?, false)?
    let table_obj = Table::create(114514, TableSchema::new(columns)?, 0, false)?;
    let table_schema = table_obj.get_schema();
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

pub fn execute_plan_node(node: &PlanNode, tnx_id: u64) -> RsqlResult<ExecutionResult> {
    match node {
        PlanNode::TableScan { table } => {
            info!("Implement TableScan execution");
            let table_object = get_table_object(table)?;
            Ok(TableObj(table_object)) // get table object after scan
        }
        PlanNode::Filter { predicate, input } => {
            info!("Implement Filter execution");
            let input_result = execute_plan_node(input, tnx_id)?;
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
            let input_result = execute_plan_node(input, tnx_id)?;
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
            if let (TableObj(left_table_obj), TableObj(right_table_obj)) = (execute_plan_node(left, tnx_id)?, execute_plan_node(right, tnx_id)?) {
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
            let subquery_result = execute_plan_node(subquery, tnx_id)?;
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
        PlanNode::Insert { table_name, columns, values, input: _ } => {
            let mut table_object = get_table_object(table_name)?;
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
                Ok(Mutation)
            }else {
                Err(RsqlError::ExecutionError(format!("Insert columns is None")))
            }
        }
        PlanNode::Delete { input } => {
            let input_result = execute_plan_node(input, tnx_id)?;
            if let TableWithFilter{mut table_obj, rows} = input_result {
                for row in rows.iter() {
                    let pk_col_idx = table_obj.map.get(&table_obj.pk_col.0).unwrap();
                    table_obj.table_obj.delete_row(&row[*pk_col_idx], tnx_id)?;
                }
                Ok(Mutation)
            }else {
                Err(RsqlError::ExecutionError(format!("Delete input must be a TableWithFilter")))
            }
        }
        PlanNode::Update { input, assignments } => {
            todo!("Implement Update execution")
        }
        PlanNode::CreateTable { table_name, columns } => {
            todo!("Implement CreateTable execution")
        }
        PlanNode::DropTable { table_name, if_exists } => {
            todo!("Implement DropTable execution")
        },
        PlanNode::CreateIndex {index_name, table_name, columns, unique} => {
            todo!("Implement CreateIndex execution")
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sql_parser::Plan;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_table_scan() {
        let result = get_table_object("users");
        assert!(result.is_ok());
        let table_obj = result.unwrap();
        assert_eq!(table_obj.cols.0, vec!["id", "name", "age"]);
        assert_eq!(table_obj.pk_col.0, "id");
    }

    #[test]
    #[serial]
    fn test_simple_select_all() {
        // SELECT * FROM users
        let sql = "SELECT * FROM users";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
    }

    #[test]
    #[serial]
    fn test_projection() {
        // SELECT id, name FROM users
        let sql = "SELECT id, name FROM users";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_filter_with_constant() {
        // SELECT * FROM users WHERE age > 18
        let sql = "SELECT * FROM users WHERE age > 18";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_filter_with_column_comparison() {
        // SELECT * FROM users WHERE id = age
        let sql = "SELECT * FROM users WHERE id = age";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_filter_with_string() {
        // SELECT * FROM users WHERE name = 'Alice'
        let sql = "SELECT * FROM users WHERE name = 'Alice'";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_projection_with_filter() {
        // SELECT id, name FROM users WHERE age > 18
        let sql = "SELECT id, name FROM users WHERE age > 18";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_join_two_tables() {
        // SELECT * FROM users JOIN products ON users.id = products.product_id
        let sql = "SELECT * FROM users JOIN products ON users.id = products.product_id";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_insert_with_columns() {
        // INSERT INTO users (id, name, age) VALUES (1, 'Bob', 25)
        let sql = "INSERT INTO users (id, name, age) VALUES (1, 'Bob', 25)";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_delete_with_filter() {
        // DELETE FROM users WHERE age < 18
        let sql = "DELETE FROM users WHERE age < 18";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_complex_filter_with_and() {
        // SELECT * FROM users WHERE age > 18 AND id = 1
        let sql = "SELECT * FROM users WHERE age > 18 AND id = 1";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_complex_filter_with_or() {
        // SELECT * FROM users WHERE age > 30 OR name = 'Charlie'
        let sql = "SELECT * FROM users WHERE age > 30 OR name = 'Charlie'";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_different_comparison_operators() {
        // Test various operators: >, <, >=, <=, =, !=
        let test_cases = vec![
            "SELECT * FROM users WHERE age > 18",
            "SELECT * FROM users WHERE age < 18",
            "SELECT * FROM users WHERE age >= 18",
            "SELECT * FROM users WHERE age <= 18",
            "SELECT * FROM users WHERE age = 18",
            "SELECT * FROM users WHERE age != 18",
        ];

        for sql in test_cases {
            let plan = Plan::build_plan(sql);
            assert!(plan.is_ok(), "Failed to parse: {}", sql);
        }
    }

    #[test]
    #[serial]
    fn test_projection_ordering() {
        // SELECT name, id FROM users (different column order)
        let sql = "SELECT name, id FROM users";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_multiple_table_types() {
        let table_names = vec!["users", "products"];
        for table_name in table_names {
            let result = get_table_object(table_name);
            assert!(result.is_ok(), "Failed to get table: {}", table_name);
            let table_obj = result.unwrap();
            assert!(!table_obj.cols.0.is_empty());
            assert_eq!(table_obj.cols.0.len(), table_obj.cols.1.len());
        }
    }

    #[test]
    #[serial]
    fn test_table_object_structure() {
        let table_obj = get_table_object("users").unwrap();
        
        // Verify column names
        assert!(table_obj.map.contains_key("id"));
        assert!(table_obj.map.contains_key("name"));
        assert!(table_obj.map.contains_key("age"));
        
        // Verify column indices
        assert_eq!(*table_obj.map.get("id").unwrap(), 0);
        assert_eq!(*table_obj.map.get("name").unwrap(), 1);
        assert_eq!(*table_obj.map.get("age").unwrap(), 2);
        
        // Verify primary key
        assert_eq!(table_obj.pk_col.0, "id");
    }

    #[test]
    #[serial]
    fn test_join_with_filter() {
        // SELECT * FROM users JOIN products ON users.id = products.product_id WHERE age > 18
        let sql = "SELECT * FROM users JOIN products ON users.id = products.product_id WHERE age > 18";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }

    #[test]
    #[serial]
    fn test_select_with_projection_and_filter() {
        // SELECT id, name FROM users WHERE age >= 21 AND id = 1
        let sql = "SELECT id, name FROM users WHERE age >= 21 AND id = 1";
        let plan = Plan::build_plan(sql);
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.items.is_empty());
    }
}