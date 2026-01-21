/// Definitions for logical query plans.
/// This module defines the LogicalPlan enum and related structures for representing query execution plans.

// sqlparser crate
use sqlparser::ast::{
    Expr,
    Statement,
    Query,
    SetExpr,
    Select,
    GroupByExpr,
    SelectItem,
    TableWithJoins,
    TableFactor,
    ObjectType,
    AlterTableOperation,
    AlterTableOperation as AstAlterTableOperation,
    ColumnDef,
    RenameTableNameKind,
    Ident,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Span, Location};

// Internal modules
use crate::sql::utils::is_aggregate_expr;
use crate::common::{RsqlResult, RsqlError};
use crate::catalog::table_schema::{TableSchema, TableColumn, ColType};

/// Represents the type of join operation.
#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner, // INNER JOIN
    Left,  // LEFT OUTER JOIN
    Right, // RIGHT OUTER JOIN
    Full,  // FULL OUTER JOIN
    Cross, // CROSS JOIN
}

/// Represents the type of apply operation for subqueries.
#[derive(Debug, Clone, Copy)]
pub enum ApplyType {
    Scalar,  // Scalar subquery
    // In,      // IN subquery
    // NotIn,   // NOT IN subquery
}

// Removed unused: pub type AlterTableOperation = AstAlterTableOperation;

/// Represents executable DDL operations after AST extraction.
#[derive(Debug, Clone)]
pub enum DdlOperation {
    /// CreateTable now uses TableSchema instead of Vec<ColumnDef>
    CreateTable {
        table_name: String,
        schema: TableSchema,
        if_not_exists: bool,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column: String,
        unique: bool,
        if_not_exists: bool,
    },
    RenameTable {
        old_name: String,
        new_name: String,
        if_exists: bool,
    },
    RenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
}

/// Represents a logical query plan.
/// Each variant corresponds to a relational algebra operation or DDL/DML/DCL operation.
#[derive(Debug)]
pub enum PlanNode {
    /// Scans a table for all rows.
    TableScan {
        table: String,
    },
    /// Represents a subquery.
    Subquery {
        subquery: Box<PlanNode>,
        alias: Option<String>,
    },
    /// Applies a subquery to each row from the input.
    Apply {
        input: Box<PlanNode>,
        subquery: Box<PlanNode>,
        apply_type: ApplyType,
    },
    /// Filters rows based on a predicate.
    Filter {
        predicate: Expr,
        input: Box<PlanNode>,
    },
    /// Groups rows and applies aggregate functions.
    Aggregate {
        group_by: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        input: Box<PlanNode>,
    },
    /// Projects specific columns from the input.
    Projection {
        exprs: Vec<Expr>,
        input: Box<PlanNode>,
    },
    /// Sorts rows based on ORDER BY columns.
    /// Store flattened column-level information instead of Expr
    Sort {
        columns: Vec<String>, // column name or simple identifier
        asc: Vec<bool>,       // true = ASC, false = DESC
        input: Box<PlanNode>,
    },
    /// Joins two plans based on a condition.
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        on: Option<Expr>,
    },
    /// DDL operation (fully extracted, AST-free)
    DDL {
        op: DdlOperation,
    },
    /// Inserts data into a table.
    /// If `input` is Some, it represents an INSERT ... SELECT subquery plan.
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
        input: Option<Box<PlanNode>>, // for INSERT ... SELECT subquery
    },
    /// Deletes rows produced by the input plan.
    Delete {
        input: Box<PlanNode>,
    },
    /// Updates rows produced by the input plan.
    Update {
        input: Box<PlanNode>,
        assignments: Vec<(String, Expr)>,
    },
    /// Creates a new user.
    CreateUser {
        user_name: String,
        password: Option<String>,
        if_not_exists: bool,
    },
    /// Drops a user.
    DropUser {
        user_name: String,
        if_exists: bool,
    },
    /// Grants a privilege to a user.
    Grant {
        privilege: String,
        table_name: Option<String>,
        user_name: String,
    },
    /// Revokes a privilege from a user.
    Revoke {
        privilege: String,
        table_name: Option<String>,
        user_name: String,
    },
}

#[derive(Debug)]
pub enum PlanItem {
    DDL(PlanNode),
    DML(PlanNode),
    DCL(PlanNode),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug)]
pub struct Plan {
    pub items: Vec<PlanItem>,
}

impl Plan {
    /// Builds a logical plan from a SQL string.
    /// Flattens all statements into Plan.items, including transaction boundaries.
    pub fn build_plan(sql: &str) -> RsqlResult<Plan> {
        let mut items = Vec::new();

        // Check for DCL CREATE USER or DROP USER before parsing
        let sql_trimmed = sql.trim_start();
        let lower = sql_trimmed.to_ascii_lowercase();
        // Only handle single statement for DCL shortcut
        if lower.starts_with("create user") {
            // Try AST parse first
            let dialect = GenericDialect {};
            match Parser::parse_sql(&dialect, sql) {
                Ok(ast) => {
                    if ast.is_empty() {
                        return Err(RsqlError::ParserError("Empty SQL".to_string()));
                    }
                    for stmt in ast.iter() {
                        use sqlparser::ast::Statement::*;
                        match stmt {
                            CreateUser { .. } => {
                                let node = Self::from_ast(&stmt)?;
                                items.push(PlanItem::DCL(node));
                            }
                            _ => {
                                let node = Self::from_ast(&stmt)?;
                                items.push(PlanItem::DML(node));
                            }
                        }
                    }
                    return Ok(Plan { items });
                }
                Err(_e) => {
                    // Fallback: manual parse for CREATE USER with/without PASSWORD, and IF NOT EXISTS
                    let rest: &str = &sql_trimmed[("create user".len())..].trim_start();
                    let mut user_name = String::new();
                    let mut password: Option<String> = None;
                    let mut if_not_exists = false;
                    // Tokenize rest
                    let tokens: Vec<&str> = rest.split_whitespace().collect();
                    let mut i = 0;
                    // Check for IF NOT EXISTS
                    if tokens.len() >= 3
                        && tokens[0].eq_ignore_ascii_case("if")
                        && tokens[1].eq_ignore_ascii_case("not")
                        && tokens[2].eq_ignore_ascii_case("exists")
                    {
                        if_not_exists = true;
                        i += 3;
                    }
                    // Get user_name
                    if i < tokens.len() {
                        user_name = tokens[i].trim_matches(|c: char| c == ';').to_string();
                        i += 1;
                    }
                    // Reject any unsupported attribute keywords (only PASSWORD is allowed)
                    if i < tokens.len() {
                        let kw = tokens[i].to_ascii_lowercase();
                        if kw != "password"
                            && kw != "if"
                            && kw != "not"
                            && kw != "exists"
                        {
                            return Err(RsqlError::ParserError(format!(
                                "Unsupported CREATE USER option '{}', only PASSWORD is supported",
                                tokens[i]
                            )));
                        }
                    }
                    // Look for PASSWORD keyword and handle = or whitespace
                    let mut password_found = false;
                    while i < tokens.len() {
                        if tokens[i].eq_ignore_ascii_case("password") {
                            password_found = true;
                            i += 1;
                            // Support PASSWORD = 'xxx' and PASSWORD 'xxx'
                            if i < tokens.len() && tokens[i] == "=" {
                                i += 1;
                            }
                            break;
                        }
                        i += 1;
                    }
                    if password_found && i < tokens.len() {
                        // The password may be quoted or not
                        let pw_token = tokens[i];
                        let pw = pw_token.trim_matches(|c: char| c == ';');
                        // Remove surrounding quotes if present
                        let pw_value = if (pw.starts_with('\'') && pw.ends_with('\'')) || (pw.starts_with('"') && pw.ends_with('"')) {
                            if pw.len() >= 2 {
                                pw[1..pw.len()-1].to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            pw.to_string()
                        };
                        password = Some(pw_value);
                    }
                    if user_name.is_empty() {
                        return Err(RsqlError::ParserError("CREATE USER missing user name".to_string()));
                    }
                    items.push(PlanItem::DCL(PlanNode::CreateUser { user_name, password, if_not_exists }));
                    return Ok(Plan { items });
                }
            }
        } else if lower.starts_with("drop user") {
            // Parse: DROP USER [IF EXISTS] <user_name>[;]
            let rest: &str = &sql_trimmed[("drop user".len())..].trim_start();
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            let mut i = 0;
            let mut if_exists = false;
            // Check for IF EXISTS
            if tokens.len() >= 2
                && tokens[0].eq_ignore_ascii_case("if")
                && tokens[1].eq_ignore_ascii_case("exists")
            {
                if_exists = true;
                i += 2;
            }
            // Get user_name
            let user_name = if i < tokens.len() {
                tokens[i].trim_matches(|c: char| c == ';' || c.is_whitespace()).to_string()
            } else {
                "".to_string()
            };
            if user_name.is_empty() {
                return Err(RsqlError::ParserError("DROP USER missing user name".to_string()));
            }
            items.push(PlanItem::DCL(PlanNode::DropUser { user_name, if_exists }));
            return Ok(Plan { items });
        } else if lower.starts_with("grant") {
            // Parse: 
            // GRANT <privilege> TO <user_name>[;]
            // GRANT <privilege> ON <table_name> TO <user_name>[;]
            let rest: &str = &sql_trimmed[("grant".len())..].trim_start();
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            
            if tokens.len() >= 3 && tokens[1].eq_ignore_ascii_case("to") {
                let privilege = tokens[0].to_string();
                let user_name = tokens[2].trim_matches(|c: char| c == ';').to_string();
                items.push(PlanItem::DCL(PlanNode::Grant { privilege, table_name: None, user_name }));
                return Ok(Plan { items });
            } else if tokens.len() >= 5 && tokens[1].eq_ignore_ascii_case("on") && tokens[3].eq_ignore_ascii_case("to") {
                let privilege = tokens[0].to_string();
                let table_name = Some(tokens[2].to_string());
                let user_name = tokens[4].trim_matches(|c: char| c == ';').to_string();
                items.push(PlanItem::DCL(PlanNode::Grant { privilege, table_name, user_name }));
                return Ok(Plan { items });
            } else {
                return Err(RsqlError::ParserError("Invalid GRANT syntax. Expected GRANT <priv> [ON <table>] TO <user>".to_string()));
            }
        } else if lower.starts_with("revoke") {
            // Parse: 
            // REVOKE <privilege> FROM <user_name>[;]
            // REVOKE <privilege> ON <table_name> FROM <user_name>[;]
            let rest: &str = &sql_trimmed[("revoke".len())..].trim_start();
            let tokens: Vec<&str> = rest.split_whitespace().collect();
            
            if tokens.len() >= 3 && tokens[1].eq_ignore_ascii_case("from") {
                let privilege = tokens[0].to_string();
                let user_name = tokens[2].trim_matches(|c: char| c == ';').to_string();
                items.push(PlanItem::DCL(PlanNode::Revoke { privilege, table_name: None, user_name }));
                return Ok(Plan { items });
            } else if tokens.len() >= 5 && tokens[1].eq_ignore_ascii_case("on") && tokens[3].eq_ignore_ascii_case("from") {
                let privilege = tokens[0].to_string();
                let table_name = Some(tokens[2].to_string());
                let user_name = tokens[4].trim_matches(|c: char| c == ';').to_string();
                items.push(PlanItem::DCL(PlanNode::Revoke { privilege, table_name, user_name }));
                return Ok(Plan { items });
            } else {
                return Err(RsqlError::ParserError("Invalid REVOKE syntax. Expected REVOKE <priv> [ON <table>] FROM <user>".to_string()));
            }
        }

        // Otherwise use sqlparser as normal
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| RsqlError::ParserError(format!("{e}")))?;

        if ast.is_empty() {
            return Err(RsqlError::ParserError("Empty SQL".to_string()));
        }

        for stmt in ast.iter() {
            use sqlparser::ast::Statement::*;
            match stmt {
                StartTransaction { .. } => items.push(PlanItem::Begin),
                Commit { .. } => items.push(PlanItem::Commit),
                Rollback { .. } => items.push(PlanItem::Rollback),
                // DDL
                CreateTable { .. }
                | Drop { object_type: ObjectType::Table, .. }
                | AlterTable { .. }
                | CreateIndex { .. } => {
                    let node = Self::from_ast(&stmt)?;
                    items.push(PlanItem::DDL(node));
                }
                // DML
                Insert { .. }
                | Update { .. }
                | Delete { .. }
                | Query(_) => {
                    let node = Self::from_ast(&stmt)?;
                    items.push(PlanItem::DML(node));
                }
                // DCL
                CreateUser { .. }
                | Drop { object_type: ObjectType::User, .. } => {
                    let node = Self::from_ast(&stmt)?;
                    items.push(PlanItem::DCL(node));
                }
                // Fallback for anything else
                _ => {
                    let node = Self::from_ast(&stmt)?;
                    // Default: treat as DML if not matched above
                    items.push(PlanItem::DML(node));
                }
            }
        }

        Ok(Plan { items })
    }

    /// AST -> LogicalPlan
    fn from_ast(stmt: &Statement) -> RsqlResult<PlanNode> {
        match stmt {
            Statement::Query(_) => Self::from_select_ast(stmt),
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. } => Self::from_ddl_ast(stmt),

            Statement::CreateTable { .. }
            | Statement::Drop { .. }
            | Statement::AlterTable { .. }
            | Statement::CreateIndex { .. } => Self::from_ddl_ast(stmt),
            // Transaction statements are handled in build_plan, so treat as error here.
            Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. } => {
                Err(RsqlError::ParserError("Transaction statements are not valid as standalone logical plan nodes".to_string()))
            }
            _ => Err(RsqlError::ParserError("Unsupported statement type".to_string())),
        }
    }

    // ==================== SELECT ====================
    fn from_select_ast(stmt: &Statement) -> RsqlResult<PlanNode> {
        match stmt {
            Statement::Query(query) => Self::build_query(query),
            _ => Err(RsqlError::ParserError("Only SELECT supported".to_string())),
        }
    }

    fn build_query(query: &Query) -> RsqlResult<PlanNode> {
        match &*query.body {
            SetExpr::Select(select) => {
                let mut plan = Self::build_select_plan(select)?;

                // === ORDER BY handling ===
                if let Some(order_by) = &query.order_by {
                    if let sqlparser::ast::OrderByKind::Expressions(items) = &order_by.kind {
                        let mut columns = Vec::new();
                        let mut asc = Vec::new();

                        for ob in items {
                            let col = match &ob.expr {
                                Expr::Identifier(ident) => ident.value.clone(),
                                Expr::CompoundIdentifier(idents) => {
                                    idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
                                }
                                _ => {
                                    return Err(RsqlError::ParserError(
                                        "ORDER BY only supports column identifiers".to_string(),
                                    ));
                                }
                            };

                            columns.push(col);
                            asc.push(ob.options.asc.unwrap_or(true));
                        }

                        plan = PlanNode::Sort {
                            columns,
                            asc,
                            input: Box::new(plan),
                        };
                    }
                }

                Ok(plan)
            }
            _ => Err(RsqlError::ParserError("Only simple SELECT is supported".to_string())),
        }
    }

    fn build_select_plan(select: &Select) -> RsqlResult<PlanNode> {
        let mut plan = Self::build_from(&select.from)?;

        if let Some(selection) = &select.selection {
            let (clean_predicate, sub_info) = Self::extract_subqueries_from_expr(selection)?;
            plan = PlanNode::Filter {
                predicate: clean_predicate,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = PlanNode::Apply {
                    input: Box::new(plan),
                    subquery: Box::new(sub_plan),
                    apply_type,
                };
            }
        }

        if Self::has_grouping_or_aggregate(select) {
            let group_by = match &select.group_by {
                GroupByExpr::Expressions(exprs, _) => exprs.clone(),
                _ => vec![],
            };
            let aggr_exprs = Self::extract_aggr_exprs(&select.projection);
            plan = PlanNode::Aggregate {
                group_by,
                aggr_exprs,
                input: Box::new(plan),
            };
        }

        if let Some(having) = &select.having {
            let (clean_having, sub_info) = Self::extract_subqueries_from_expr(having)?;
            plan = PlanNode::Filter {
                predicate: clean_having,
                input: Box::new(plan),
            };
            if let Some((sub_plan, apply_type)) = sub_info {
                plan = PlanNode::Apply {
                    input: Box::new(plan),
                    subquery: Box::new(sub_plan),
                    apply_type,
                };
            }
        }

        // === Projection handling ===
        // Always build a Projection node, including for SELECT *
        let proj_exprs = Self::extract_projection(&select.projection);

        if proj_exprs.is_empty() {
            return Err(RsqlError::ParserError(
                "SELECT list cannot be empty".to_string(),
            ));
        }

        let (clean_exprs, sub_info) = Self::extract_subqueries_from_exprs(&proj_exprs)?;
        plan = PlanNode::Projection {
            exprs: clean_exprs,
            input: Box::new(plan),
        };
        if let Some((sub_plan, apply_type)) = sub_info {
            plan = PlanNode::Apply {
                input: Box::new(plan),
                subquery: Box::new(sub_plan),
                apply_type,
            };
        }

        Ok(plan)
    }

    // ==================== FROM / TableFactor ====================
    fn build_from(from: &[TableWithJoins]) -> RsqlResult<PlanNode> {
        if from.is_empty() {
            return Err(RsqlError::ParserError("FROM clause is empty".to_string()));
        }

        // Start with the first table
        let mut plan = Self::build_table_factor(&from[0].relation)?;

        // Handle joins inside the first TableWithJoins
        for table_with_join in &from[0].joins {
            let right_plan = Self::build_table_factor(&table_with_join.relation)?;
            let join_type = match table_with_join.join_operator {
                sqlparser::ast::JoinOperator::Inner(_) => JoinType::Inner,
                sqlparser::ast::JoinOperator::LeftOuter(_) => JoinType::Left,
                sqlparser::ast::JoinOperator::RightOuter(_) => JoinType::Right,
                sqlparser::ast::JoinOperator::FullOuter(_) => JoinType::Full,
                sqlparser::ast::JoinOperator::CrossJoin(_) => JoinType::Cross,
                sqlparser::ast::JoinOperator::Join(_) => JoinType::Inner,
                _ => return Err(RsqlError::ParserError("Unsupported join type".to_string())),
            };
            let on_expr = match &table_with_join.join_operator {
                sqlparser::ast::JoinOperator::Inner(constraint)
                | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                | sqlparser::ast::JoinOperator::RightOuter(constraint)
                | sqlparser::ast::JoinOperator::FullOuter(constraint)
                | sqlparser::ast::JoinOperator::Join(constraint)
                | sqlparser::ast::JoinOperator::CrossJoin(constraint) => {
                    match constraint {
                        sqlparser::ast::JoinConstraint::On(expr) => Some(expr.clone()),
                        _ => None,
                    }
                }
                _ => None,
            };
            plan = PlanNode::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type,
                on: on_expr,
            };
        }

        // Handle additional FROM items as implicit CROSS JOINs
        for table in from.iter().skip(1) {
            let right_plan = Self::build_table_factor(&table.relation)?;
            plan = PlanNode::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: JoinType::Cross,
                on: None,
            };
        }

        Ok(plan)
    }

    fn build_table_factor(table_factor: &TableFactor) -> RsqlResult<PlanNode> {
        match table_factor {
            TableFactor::Table { name, .. } => Ok(PlanNode::TableScan { table: name.to_string() }),
            TableFactor::Derived { subquery, alias, .. } => {
                let sub_plan = Self::build_query(subquery)?;
                let alias_name = alias.as_ref().map(|a| a.name.to_string());
                Ok(PlanNode::Subquery { subquery: Box::new(sub_plan), alias: alias_name })
            }
            _ => Err(RsqlError::ParserError("Unsupported table factor".to_string())),
        }
    }

    fn extract_projection(items: &[SelectItem]) -> Vec<Expr> {
        items.iter().filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) => Some(expr.clone()),
            SelectItem::ExprWithAlias { expr, .. } => Some(expr.clone()),
            SelectItem::Wildcard(_) => Some(Expr::Identifier(Ident {
                value: "*".to_string(),
                quote_style: None,
                span: Span {
                    start: Location { line: 0, column: 0 },
                    end: Location { line: 0, column: 0 },
                },
            })),
            _ => None,
        }).collect()
    }

    fn extract_aggr_exprs(items: &[SelectItem]) -> Vec<Expr> {
        items.iter().filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. } => if is_aggregate_expr(expr) { Some(expr.clone()) } else { None },
            _ => None,
        }).collect()
    }

    fn has_grouping_or_aggregate(select: &Select) -> bool {
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty())
            || Self::extract_aggr_exprs(&select.projection).len() > 0
    }

    fn extract_subqueries_from_expr(expr: &Expr) -> RsqlResult<(Expr, Option<(PlanNode, ApplyType)>)> {
        match expr {
            Expr::Subquery(query) => {
                let plan = Self::build_query(query)?;
                Ok((expr.clone(), Some((plan, ApplyType::Scalar))))
            }
            Expr::InSubquery { .. } => {
                Err(RsqlError::ParserError(
                    "IN / NOT IN subqueries are not supported".to_string(),
                ))
            }
            Expr::Exists { .. } => {
                Err(RsqlError::ParserError(
                    "EXISTS subquery is not supported".to_string(),
                ))
            }
            Expr::Like { negated, expr, pattern, escape_char, any } => {
                let (expr_clean, expr_sub) = Self::extract_subqueries_from_expr(expr)?;
                let (pattern_clean, pattern_sub) = Self::extract_subqueries_from_expr(pattern)?;
                Ok((
                    Expr::Like {
                        negated: *negated,
                        expr: Box::new(expr_clean),
                        pattern: Box::new(pattern_clean),
                        escape_char: escape_char.clone(),
                        any: *any,
                    },
                    expr_sub.or(pattern_sub),
                ))
            }
            Expr::ILike { negated, expr, pattern, escape_char, any } => {
                let (expr_clean, expr_sub) = Self::extract_subqueries_from_expr(expr)?;
                let (pattern_clean, pattern_sub) = Self::extract_subqueries_from_expr(pattern)?;
                Ok((
                    Expr::ILike {
                        negated: *negated,
                        expr: Box::new(expr_clean),
                        pattern: Box::new(pattern_clean),
                        escape_char: escape_char.clone(),
                        any: *any,
                    },
                    expr_sub.or(pattern_sub),
                ))
            }
            Expr::Between { expr, negated, low, high } => {
                let (expr_clean, expr_sub) = Self::extract_subqueries_from_expr(expr)?;
                let (low_clean, low_sub) = Self::extract_subqueries_from_expr(low)?;
                let (high_clean, high_sub) = Self::extract_subqueries_from_expr(high)?;
                Ok((
                    Expr::Between {
                        expr: Box::new(expr_clean),
                        negated: *negated,
                        low: Box::new(low_clean),
                        high: Box::new(high_clean),
                    },
                    expr_sub.or(low_sub).or(high_sub),
                ))
            }
            Expr::BinaryOp { left, op, right } => {
                let (left_clean, left_sub) = Self::extract_subqueries_from_expr(left)?;
                let (right_clean, right_sub) = Self::extract_subqueries_from_expr(right)?;
                Ok((
                    Expr::BinaryOp {
                        left: Box::new(left_clean),
                        op: op.clone(),
                        right: Box::new(right_clean),
                    },
                    left_sub.or(right_sub),
                ))
            }
            Expr::Function(func) if is_aggregate_expr(expr) => {
                Ok((expr.clone(), None))
            }
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Value(_)
            | Expr::Nested(_) => Ok((expr.clone(), None)),
            _ => Err(RsqlError::ParserError(format!("Unsupported expression: {}", expr))),
        }
    }

    fn extract_subqueries_from_exprs(exprs: &[Expr]) -> RsqlResult<(Vec<Expr>, Option<(PlanNode, ApplyType)>)> {
        let mut clean_exprs = Vec::new();
        let mut sub = None;

        for expr in exprs {
            let (clean, s) = Self::extract_subqueries_from_expr(expr)?;
            clean_exprs.push(clean);
            if sub.is_none() {
                sub = s;
            }
        }

        Ok((clean_exprs, sub))
    }

    // ==================== DDL / INSERT / UPDATE / DELETE / DCL ====================
    fn from_ddl_ast(stmt: &Statement) -> RsqlResult<PlanNode> {
        match stmt {
            Statement::CreateTable(create) => {
                // Convert Vec<ColumnDef> (AST) to TableSchema.
                // This will validate and extract all necessary column information.
                let schema = columns_ast_to_schema(&create.columns)?;
                Ok(PlanNode::DDL {
                    op: DdlOperation::CreateTable {
                        table_name: create.name.to_string(),
                        schema,
                        if_not_exists: create.if_not_exists,
                    },
                })
            }
            Statement::Drop { object_type, names, if_exists, .. }
                if *object_type == ObjectType::Table && names.len() == 1 =>
            {
                Ok(PlanNode::DDL {
                    op: DdlOperation::DropTable {
                        table_name: names[0].to_string(),
                        if_exists: *if_exists,
                    },
                })
            }
            Statement::CreateIndex(create_index) => {
                let index_name = match &create_index.name {
                    Some(name) => name.to_string(),
                    None => {
                        return Err(RsqlError::ParserError(
                            "CREATE INDEX must have a name".to_string(),
                        ))
                    }
                };
                // Only support single column index for now
                if create_index.columns.len() != 1 {
                    return Err(RsqlError::ParserError(
                        "CREATE INDEX only supports a single column".to_string(),
                    ));
                }
                // NOTE: GenericDialect does NOT support parsing `IF NOT EXISTS` for CREATE INDEX.
                // If this flag is true, it must have come from a dialect that supports it.
                // We keep the field for semantic completeness, but do not attempt fallback parsing here.
                let column = create_index.columns[0].to_string();
                Ok(PlanNode::DDL {
                    op: DdlOperation::CreateIndex {
                        index_name,
                        table_name: create_index.table_name.to_string(),
                        column,
                        unique: create_index.unique,
                        if_not_exists: create_index.if_not_exists,
                    },
                })
            }
            Statement::AlterTable(alter) => {
                if alter.operations.len() != 1 {
                    return Err(RsqlError::ParserError(
                        "Multiple ALTER TABLE operations not supported".to_string(),
                    ));
                }

                match &alter.operations[0] {
                    AstAlterTableOperation::RenameTable { table_name } => {
                        let new_name = match table_name {
                            RenameTableNameKind::To(obj_name) => obj_name.to_string(),
                            _ => return Err(RsqlError::ParserError(
                                "Unsupported RENAME TABLE target".to_string(),
                            )),
                        };
                        Ok(PlanNode::DDL {
                            op: DdlOperation::RenameTable {
                                old_name: alter.name.to_string(),
                                new_name,
                                if_exists: alter.if_exists,
                            },
                        })
                    }
                    AstAlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                        Ok(PlanNode::DDL {
                            op: DdlOperation::RenameColumn {
                                table_name: alter.name.to_string(),
                                old_name: old_column_name.to_string(),
                                new_name: new_column_name.to_string(),
                            },
                        })
                    }
                    _ => Err(RsqlError::ParserError(
                        "Only ALTER TABLE RENAME TABLE is supported".to_string(),
                    )),
                }
            }
            Statement::Insert(insert) => {
                if let Some(source) = &insert.source {
                    match &*source.body {
                        SetExpr::Values(values) => {
                            let rows: Vec<Vec<Expr>> = values.rows.iter()
                                .map(|row: &Vec<Expr>| row.iter().map(|expr: &Expr| expr.clone()).collect::<Vec<Expr>>())
                                .collect::<Vec<Vec<Expr>>>();
                            Ok(PlanNode::Insert {
                                table_name: insert.table.to_string(),
                                columns: if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) },
                                values: rows,
                                input: None,
                            })
                        },
                        SetExpr::Select(select) => {
                            let sub_plan = Self::build_select_plan(select)?;
                            Ok(PlanNode::Insert {
                                table_name: insert.table.to_string(),
                                columns: if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) },
                                values: vec![],
                                input: Some(Box::new(sub_plan)),
                            })
                        }
                        _ => return Err(RsqlError::ParserError("Unsupported INSERT source".to_string())),
                    }
                } else {
                    Ok(PlanNode::Insert {
                        table_name: insert.table.to_string(),
                        columns: if insert.columns.is_empty() { None } else { Some(insert.columns.iter().map(|c| c.to_string()).collect()) },
                        values: vec![],
                        input: None,
                    })
                }
            }
            Statement::Delete(delete) => {
                let table_name = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        if tables.is_empty() {
                            return Err(RsqlError::ParserError("DELETE with no table".to_string()));
                        }
                        match &tables[0].relation {
                            TableFactor::Table { name, .. } => name.to_string(),
                            _ => {
                                return Err(RsqlError::ParserError(
                                    "Unsupported table factor in DELETE".to_string(),
                                ))
                            }
                        }
                    }
                };

                // Base scan
                let mut plan = PlanNode::TableScan { table: table_name };

                // WHERE clause → Filter (+ Apply if needed)
                if let Some(selection) = &delete.selection {
                    let (clean_pred, sub_info) =
                        Self::extract_subqueries_from_expr(selection)?;
                    plan = PlanNode::Filter {
                        predicate: clean_pred,
                        input: Box::new(plan),
                    };
                    if let Some((sub_plan, apply_type)) = sub_info {
                        plan = PlanNode::Apply {
                            input: Box::new(plan),
                            subquery: Box::new(sub_plan),
                            apply_type,
                        };
                    }
                }

                Ok(PlanNode::Delete {
                    input: Box::new(plan),
                })
            }
            Statement::Update(update) => {
                let table_name = update.table.to_string();

                // Base scan
                let mut plan = PlanNode::TableScan { table: table_name };

                // WHERE clause → Filter (+ Apply if needed)
                if let Some(selection) = &update.selection {
                    let (clean_pred, sub_info) =
                        Self::extract_subqueries_from_expr(selection)?;
                    plan = PlanNode::Filter {
                        predicate: clean_pred,
                        input: Box::new(plan),
                    };
                    if let Some((sub_plan, apply_type)) = sub_info {
                        plan = PlanNode::Apply {
                            input: Box::new(plan),
                            subquery: Box::new(sub_plan),
                            apply_type,
                        };
                    }
                }

                let assignments = update
                    .assignments
                    .iter()
                    .map(|a| (format!("{}", a.target), a.value.clone()))
                    .collect();

                Ok(PlanNode::Update {
                    input: Box::new(plan),
                    assignments,
                })
            }
            // --- DCL: CREATE USER, DROP USER ---
            Statement::CreateUser(inner) => {
                Ok(PlanNode::CreateUser {
                    user_name: inner.name.to_string(),
                    password: None,
                    if_not_exists: inner.if_not_exists,
                })
            }
            Statement::Drop { object_type, names, if_exists, .. }
                if *object_type == ObjectType::User && names.len() == 1 =>
            {
                Ok(PlanNode::DropUser {
                    user_name: names[0].to_string(),
                    if_exists: *if_exists,
                })
            }
            Statement::Grant { .. } | Statement::Revoke { .. } => {
                return Err(RsqlError::ParserError(
                    "GRANT/REVOKE should be handled by manual parser".to_string(),
                ));
            }
            _ => Err(RsqlError::ParserError("DDL/DCL not implemented yet".to_string())),
        }
    }

    /// print LogicalPlan in a pretty tree format
    pub fn pretty_print(plan: &PlanNode) {
        fn fmt_exprs(exprs: &[Expr]) -> String {
            exprs.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
        }

        fn fmt_alter_op(op: &AlterTableOperation, _table_name: &str) -> String {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    format!(
                        "ADD COLUMN {} {}",
                        column_def.name,
                        column_def.data_type
                    )
                }
                AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    let cols = column_names
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    if *if_exists {
                        format!("DROP COLUMN IF EXISTS {}", cols)
                    } else {
                        format!("DROP COLUMN {}", cols)
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    format!(
                        "RENAME COLUMN {} TO {}",
                        old_column_name,
                        new_column_name
                    )
                }
                AlterTableOperation::RenameTable { table_name: new_name } => {
                    // Only print the new table name (do not include "TO" or old name here)
                    format!("{}", new_name)
                }
                AlterTableOperation::AlterColumn { column_name, op } => {
                    match op {
                        sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            format!(
                                "ALTER COLUMN {} TYPE {}",
                                column_name,
                                data_type
                            )
                        }
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            format!("ALTER COLUMN {} SET NOT NULL", column_name)
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            format!("ALTER COLUMN {} DROP NOT NULL", column_name)
                        }
                        _ => format!(
                            "ALTER COLUMN {} {:?}",
                            column_name,
                            op
                        ),
                    }
                }
                _ => format!("{:?}", op),
            }
        }

        fn inner(plan: &PlanNode, prefix: &str, is_last: bool) {
            let branch = if is_last { "└── " } else { "├── " };
            println!("{}{}{}", prefix, branch, label(plan));

            let new_prefix = if is_last { format!("{}    ", prefix) } else { format!("{}│   ", prefix) };
            for (i, child) in children(plan).iter().enumerate() {
                inner(child, &new_prefix, i + 1 == children(plan).len());
            }
        }

        fn label(plan: &PlanNode) -> String {
            match plan {
                PlanNode::TableScan { table } => format!("TableScan [{}]", table),
                PlanNode::Subquery { alias, .. } => format!("Subquery{}", alias.as_ref().map(|a| format!(" AS {}", a)).unwrap_or_default()),
                PlanNode::Apply { apply_type, .. } => format!("Apply [{:?}]", apply_type),
                PlanNode::Filter { predicate, .. } => format!("Filter [{}]", predicate),
                PlanNode::Aggregate { group_by, aggr_exprs, .. } => {
                    format!("Aggregate [group_by: {}, aggr: {}]", fmt_exprs(group_by), fmt_exprs(aggr_exprs))
                }
                PlanNode::Projection { exprs, .. } => format!("Projection [{}]", fmt_exprs(exprs)),
                PlanNode::Sort { columns, asc, .. } => {
                    let items = columns
                        .iter()
                        .zip(asc.iter())
                        .map(|(c, a)| {
                            if *a {
                                format!("{} ASC", c)
                            } else {
                                format!("{} DESC", c)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("Sort [{}]", items)
                }
                PlanNode::Join { join_type, on, .. } => format!("Join [{:?}, on: {}]", join_type, on.as_ref().map_or("None".to_string(), |e| format!("{}", e))),
                PlanNode::DDL { op } => match op {
                    DdlOperation::CreateTable { table_name, .. } => {
                        format!("CreateTable [{}]", table_name)
                    }
                    DdlOperation::DropTable { table_name, if_exists } => {
                        if *if_exists {
                            format!("DropTable [{}] IF EXISTS", table_name)
                        } else {
                            format!("DropTable [{}]", table_name)
                        }
                    }
                    DdlOperation::CreateIndex { index_name, table_name, unique, .. } => {
                        let uniq = if *unique { "UNIQUE " } else { "" };
                        format!("CreateIndex [{}{}] on [{}]", uniq, index_name, table_name)
                    }
                    DdlOperation::RenameTable { old_name, new_name, if_exists } => {
                        let _ = if_exists;
                        format!("AlterTable [{}] RENAME TO {}", old_name, new_name)
                    }
                    DdlOperation::RenameColumn { table_name, old_name, new_name } => {
                        format!("AlterTable [{}] RENAME COLUMN {} TO {}", table_name, old_name, new_name)
                    }
                },
                PlanNode::Insert { table_name, columns, values, input } => {
                    if let Some(_) = input {
                        format!("Insert [{}] cols={:?} [Subquery]", table_name, columns)
                    } else {
                        let rows_str = values.iter().map(|row: &Vec<Expr>| {
                            row.iter().map(|e: &Expr| match e {
                                Expr::Subquery(_) => "[Subquery]".to_string(),
                                _ => format!("{}", e)
                            }).collect::<Vec<_>>().join(", ")
                        }).collect::<Vec<_>>().join(" | ");
                        format!("Insert [{}] cols={:?} rows={}", table_name, columns, rows_str)
                    }
                }
                PlanNode::Delete { .. } => "Delete".to_string(),
                PlanNode::Update { assignments, .. } => {
                    format!("Update [assigns={}]", assignments.len())
                }
                PlanNode::CreateUser { user_name, password, if_not_exists } => {
                    format!("CreateUser [{} password={:?} if_not_exists={}]", user_name, password, if_not_exists)
                }
                PlanNode::DropUser { user_name, if_exists } => {
                    format!("DropUser [{} if_exists={}]", user_name, if_exists)
                }
                PlanNode::Grant { privilege, user_name, table_name: _ } => {
                    format!("Grant [{}] TO {}", privilege, user_name)
                }
                PlanNode::Revoke { privilege, user_name, table_name: _ } => {
                    format!("Revoke [{}] FROM {}", privilege, user_name)
                }
            }
        }

        fn children(plan: &PlanNode) -> Vec<&PlanNode> {
            match plan {
                PlanNode::TableScan { .. } => vec![],
                PlanNode::Subquery { subquery, .. } => vec![subquery],
                PlanNode::Apply { input, subquery, .. } => vec![input, subquery],
                PlanNode::Filter { input, .. } => vec![input],
                PlanNode::Aggregate { input, .. } => vec![input],
                PlanNode::Projection { input, .. } => vec![input],
                PlanNode::Sort { input, .. } => vec![input],
                PlanNode::Join { left, right, .. } => vec![left, right],
                PlanNode::Delete { input } => vec![input],
                PlanNode::Update { input, .. } => vec![input],
                PlanNode::Insert { input: Some(sub_plan), .. } => vec![sub_plan],
                _ => vec![],
            }
        }
        inner(plan, "", true);
    }
    
    /// print LogicalPlan in a pretty tree format, with expression field paths
    pub fn pretty_print_pro(plan: &PlanNode) {
        fn fmt_exprs(exprs: &[Expr]) -> String {
            exprs.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
        }

        fn fmt_alter_op(op: &AlterTableOperation, old_table_name: &str) -> String {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    format!(
                        "ADD COLUMN {} {}",
                        column_def.name,
                        column_def.data_type
                    )
                }
                AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    let cols = column_names
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    if *if_exists {
                        format!("DROP COLUMN IF EXISTS {}", cols)
                    } else {
                        format!("DROP COLUMN {}", cols)
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    format!(
                        "RENAME COLUMN {} TO {}",
                        old_column_name,
                        new_column_name
                    )
                }
                AlterTableOperation::RenameTable { table_name: new_name_kind } => {
                    // Print as: RENAME TABLE <old> TO <new>
                    match new_name_kind {
                        RenameTableNameKind::To(obj_name) => {
                            format!("RENAME TABLE {} TO {}", old_table_name, obj_name)
                        }
                        _ => {
                            // fallback
                            format!("RENAME TABLE {} TO {:?}", old_table_name, new_name_kind)
                        }
                    }
                }
                AlterTableOperation::AlterColumn { column_name, op } => {
                    match op {
                        sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            format!(
                                "ALTER COLUMN {} TYPE {}",
                                column_name,
                                data_type
                            )
                        }
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            format!("ALTER COLUMN {} SET NOT NULL", column_name)
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            format!("ALTER COLUMN {} DROP NOT NULL", column_name)
                        }
                        _ => format!(
                            "ALTER COLUMN {} {:?}",
                            column_name,
                            op
                        ),
                    }
                }
                _ => format!("{:?}", op),
            }
        }

        // Print the PlanNode recursively, and for each PlanNode, print its expressions with paths.
        fn inner(plan: &PlanNode, prefix: &str, is_last: bool) {
            let branch = if is_last { "└── " } else { "├── " };
            println!("{}{}{}", prefix, branch, label(plan));
            // Print expressions with paths for this PlanNode.
            let expr_prefix = if is_last { format!("{}    ", prefix) } else { format!("{}│   ", prefix) };
            print_plan_expr_paths(plan, &expr_prefix, "");
            let new_prefix = expr_prefix.clone();
            for (i, child) in children(plan).iter().enumerate() {
                inner(child, &new_prefix, i + 1 == children(plan).len());
            }
        }

        // Print all expression fields of a PlanNode, with their field paths.
        fn print_plan_expr_paths(plan: &PlanNode, prefix: &str, _plan_path: &str) {
            match plan {
                PlanNode::Filter { predicate, .. } => {
                    let path = format!("(PlanNode::Filter.predicate)");
                    print_expr_with_path(predicate, prefix, &path);
                }
                PlanNode::Aggregate { group_by, aggr_exprs, .. } => {
                    for (i, expr) in group_by.iter().enumerate() {
                        let path = format!("(PlanNode::Aggregate.group_by[{}])", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                    for (i, expr) in aggr_exprs.iter().enumerate() {
                        let path = format!("(PlanNode::Aggregate.aggr_exprs[{}])", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                }
                PlanNode::Projection { exprs, .. } => {
                    for (i, expr) in exprs.iter().enumerate() {
                        let path = format!("(PlanNode::Projection.exprs[{}])", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                }
                PlanNode::Sort { columns, asc, .. } => {
                    for i in 0..columns.len() {
                        let col_path = format!("(PlanNode::Sort.columns[{}])", i);
                        println!("{}{} -> {}", prefix, col_path, columns[i]);
                        let dir_path = format!("(PlanNode::Sort.asc[{}])", i);
                        println!("{}{} -> {}", prefix, dir_path, asc[i]);
                    }
                }
                PlanNode::Join { on: Some(expr), .. } => {
                    let path = format!("(PlanNode::Join.on)");
                    print_expr_with_path(expr, prefix, &path);
                }
                PlanNode::Insert { values, .. } => {
                    for (row_idx, row) in values.iter().enumerate() {
                        for (col_idx, expr) in row.iter().enumerate() {
                            let path = format!("(PlanNode::Insert.values[{}][{}])", row_idx, col_idx);
                            print_expr_with_path(expr, prefix, &path);
                        }
                    }
                }
                PlanNode::Update { assignments, .. } => {
                    for (i, (_col, expr)) in assignments.iter().enumerate() {
                        let path = format!("(PlanNode::Update.assignments[{}].1)", i);
                        print_expr_with_path(expr, prefix, &path);
                    }
                }
                PlanNode::DDL { op } => {
                    match op {
                        DdlOperation::CreateTable { table_name: _, schema, if_not_exists } => {
                            // Print if_not_exists path first
                            let path_exists = "(PlanNode::DDL.op[CreateTable].if_not_exists)";
                            println!("{}{} -> {}", prefix, path_exists, if_not_exists);
                            // For each column, print its name and constraints
                            for (i, col) in schema.get_columns().iter().enumerate() {
                                let name_path = format!("(PlanNode::DDL.op[CreateTable].columns[{}].name)", i);
                                println!("{}{} -> {}", prefix, name_path, col.name);

                                let pk_path = format!("(PlanNode::DDL.op[CreateTable].columns[{}].pk)", i);
                                println!("{}{} -> {}", prefix, pk_path, col.pk);

                                let nullable_path = format!("(PlanNode::DDL.op[CreateTable].columns[{}].nullable)", i);
                                println!("{}{} -> {}", prefix, nullable_path, col.nullable);

                                let unique_path = format!("(PlanNode::DDL.op[CreateTable].columns[{}].unique)", i);
                                println!("{}{} -> {}", prefix, unique_path, col.unique);

                                let index_path = format!("(PlanNode::DDL.op[CreateTable].columns[{}].index)", i);
                                println!("{}{} -> {}", prefix, index_path, col.index);
                            }
                        }
                        DdlOperation::DropTable { table_name, if_exists } => {
                            let path_table = "(PlanNode::DDL.op[DropTable].table_name)";
                            println!("{}{} -> {}", prefix, path_table, table_name);
                            let path_exists = "(PlanNode::DDL.op[DropTable].if_exists)";
                            println!("{}{} -> {}", prefix, path_exists, if_exists);
                        }
                        DdlOperation::CreateIndex { index_name, table_name, column, unique, if_not_exists } => {
                            let path_index = "(PlanNode::DDL.op[CreateIndex].index_name)";
                            println!("{}{} -> {}", prefix, path_index, index_name);
                            let path_table = "(PlanNode::DDL.op[CreateIndex].table_name)";
                            println!("{}{} -> {}", prefix, path_table, table_name);
                            let path_column = "(PlanNode::DDL.op[CreateIndex].column)";
                            println!("{}{} -> {}", prefix, path_column, column);
                            let path_unique = "(PlanNode::DDL.op[CreateIndex].unique)";
                            println!("{}{} -> {}", prefix, path_unique, unique);
                            let path_if_not_exists = "(PlanNode::DDL.op[CreateIndex].if_not_exists)";
                            println!("{}{} -> {}", prefix, path_if_not_exists, if_not_exists);
                        }
                        DdlOperation::RenameTable { old_name, new_name, if_exists } => {
                            let path_exists = "(PlanNode::DDL.op[RenameTable].if_exists)";
                            println!("{}{} -> {}", prefix, path_exists, if_exists);
                            let path_old = "(PlanNode::DDL.op[RenameTable].old_name)";
                            let path_new = "(PlanNode::DDL.op[RenameTable].new_name)";
                            println!("{}{} -> {}", prefix, path_old, old_name);
                            println!("{}{} -> {}", prefix, path_new, new_name);
                        }
                        DdlOperation::RenameColumn { table_name, old_name, new_name } => {
                            let path_table = "(PlanNode::DDL.op[RenameColumn].table_name)";
                            println!("{}{} -> {}", prefix, path_table, table_name);
                            let path_old = "(PlanNode::DDL.op[RenameColumn].old_name)";
                            println!("{}{} -> {}", prefix, path_old, old_name);
                            let path_new = "(PlanNode::DDL.op[RenameColumn].new_name)";
                            println!("{}{} -> {}", prefix, path_new, new_name);
                        }
                    }
                }
                // ---- Add pretty print for CreateUser ----
                PlanNode::CreateUser { user_name, password, if_not_exists } => {
                    let path_user = "(PlanNode::CreateUser.user_name)";
                    println!("{}{} -> {}", prefix, path_user, user_name);
                    let path_pw = "(PlanNode::CreateUser.password)";
                    println!("{}{} -> {:?}", prefix, path_pw, password);
                    let path_if_not_exists = "(PlanNode::CreateUser.if_not_exists)";
                    println!("{}{} -> {}", prefix, path_if_not_exists, if_not_exists);
                }
                // ---- Add pretty print for DropUser ----
                PlanNode::DropUser { user_name, if_exists } => {
                    let path_user = "(PlanNode::DropUser.user_name)";
                    println!("{}{} -> {}", prefix, path_user, user_name);
                    let path_if_exists = "(PlanNode::DropUser.if_exists)";
                    println!("{}{} -> {}", prefix, path_if_exists, if_exists);
                }
                PlanNode::Grant { privilege, user_name, table_name } => {
                    let p1 = "(PlanNode::Grant.privilege)";
                    println!("{}{} -> {}", prefix, p1, privilege);
                    if let Some(t) = table_name {
                        let p_table = "(PlanNode::Grant.table_name)";
                        println!("{}{} -> {}", prefix, p_table, t);
                    }
                    let p2 = "(PlanNode::Grant.user_name)";
                    println!("{}{} -> {}", prefix, p2, user_name);
                }
                PlanNode::Revoke { privilege, user_name, table_name } => {
                    let p1 = "(PlanNode::Revoke.privilege)";
                    println!("{}{} -> {}", prefix, p1, privilege);
                    if let Some(t) = table_name {
                        let p_table = "(PlanNode::Revoke.table_name)";
                        println!("{}{} -> {}", prefix, p_table, t);
                    }
                    let p2 = "(PlanNode::Revoke.user_name)";
                    println!("{}{} -> {}", prefix, p2, user_name);
                }
                _ => {}
            }
        }

        // Print an Expr, recursively, with its path (e.g., "(PlanNode::Filter.predicate)").
        fn print_expr_with_path(expr: &Expr, prefix: &str, path: &str) {
            // Print the path and the current expr node kind
            #[allow(unused_variables)]
            let _expr_label = expr_kind_label(expr);
            println!("{}{} -> {}", prefix, path, _expr_label);
            // For nested expressions, print their fields recursively with extended path
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    let left_path = format!("{}.left", path);
                    print_expr_with_path(left, prefix, &left_path);
                    let op_path = format!("{}.op", path);
                    println!("{}{} -> {:?}", prefix, op_path, op);
                    let right_path = format!("{}.right", path);
                    print_expr_with_path(right, prefix, &right_path);
                }
                Expr::UnaryOp { op, expr: inner } => {
                    let op_path = format!("{}.op", path);
                    println!("{}{} -> {:?}", prefix, op_path, op);
                    let expr_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &expr_path);
                }
                Expr::Nested(inner) => {
                    let inner_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &inner_path);
                }
                Expr::Identifier(ident) => {
                    let val_path = format!("{}.ident", path);
                    println!("{}{} -> {:?}", prefix, val_path, ident);
                }
                Expr::Value(val) => {
                    let val_path = format!("{}.value", path);
                    println!("{}{} -> {:?}", prefix, val_path, val.value);
                }
                Expr::Function(func) => {
                    let name_path = format!("{}.name", path);
                    println!("{}{} -> {:?}", prefix, name_path, func.name);
                }
                Expr::Cast { expr: inner, data_type, .. } => {
                    let expr_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &expr_path);
                    let dt_path = format!("{}.data_type", path);
                    println!("{}{} -> {:?}", prefix, dt_path, data_type);
                }
                Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                    // Do not recurse into subqueries for now
                }
                Expr::CompoundIdentifier(idents) => {
                    let id_path = format!("{}.idents", path);
                    println!("{}{} -> {:?}", prefix, id_path, idents);
                }
                Expr::Between { expr: inner, low, high, .. } => {
                    let expr_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &expr_path);
                    let low_path = format!("{}.low", path);
                    print_expr_with_path(low, prefix, &low_path);
                    let high_path = format!("{}.high", path);
                    print_expr_with_path(high, prefix, &high_path);
                }
                Expr::Case {
                    operand,
                    conditions,
                    else_result,
                    ..
                } => {
                    if let Some(opnd) = operand {
                        let opnd_path = format!("{}.operand", path);
                        print_expr_with_path(opnd, prefix, &opnd_path);
                    }
                    for (i, cw) in conditions.iter().enumerate() {
                        let cond_path = format!("{}.conditions[{}].condition", path, i);
                        print_expr_with_path(&cw.condition, prefix, &cond_path);
                        let res_path = format!("{}.conditions[{}].result", path, i);
                        print_expr_with_path(&cw.result, prefix, &res_path);
                    }
                    if let Some(else_res) = else_result {
                        let else_path = format!("{}.else_result", path);
                        print_expr_with_path(else_res, prefix, &else_path);
                    }
                }
                Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                    let inner_path = format!("{}.expr", path);
                    print_expr_with_path(inner, prefix, &inner_path);
                }
                _ => {}
            }
        }

        // Return a short label for an Expr node
        fn expr_kind_label(expr: &Expr) -> String {
            match expr {
                Expr::BinaryOp { .. } => format!("Expr::BinaryOp"),
                Expr::UnaryOp { .. } => format!("Expr::UnaryOp"),
                Expr::Value(val) => format!("Expr::Value({:?})", val.value),
                Expr::Identifier(ident) => format!("Expr::Identifier({})", ident),
                Expr::CompoundIdentifier(idents) => format!("Expr::CompoundIdentifier({:?})", idents),
                Expr::Nested(_) => format!("Expr::Nested"),
                Expr::Function(func) => format!("Expr::Function({})", func.name),
                Expr::Cast { .. } => format!("Expr::Cast"),
                Expr::Subquery(_) => format!("Expr::Subquery"),
                Expr::InSubquery { negated, .. } => format!("Expr::InSubquery(negated={})", negated),
                Expr::Exists { .. } => format!("Expr::Exists"),
                Expr::Between { .. } => format!("Expr::Between"),
                Expr::Case { .. } => format!("Expr::Case"),
                Expr::IsNull(_) => format!("Expr::IsNull"),
                Expr::IsNotNull(_) => format!("Expr::IsNotNull"),
                _ => format!("{:?}", expr),
            }
        }

        fn label(plan: &PlanNode) -> String {
            match plan {
                PlanNode::TableScan { table } => format!("TableScan [{}]", table),
                PlanNode::Subquery { alias, .. } => format!("Subquery{}", alias.as_ref().map(|a| format!(" AS {}", a)).unwrap_or_default()),
                PlanNode::Apply { apply_type, .. } => format!("Apply [{:?}]", apply_type),
                PlanNode::Filter { predicate, .. } => format!("Filter [{}]", predicate),
                PlanNode::Aggregate { group_by, aggr_exprs, .. } => {
                    format!("Aggregate [group_by: {}, aggr: {}]", fmt_exprs(group_by), fmt_exprs(aggr_exprs))
                }
                PlanNode::Projection { exprs, .. } => format!("Projection [{}]", fmt_exprs(exprs)),
                PlanNode::Sort { columns, asc, .. } => {
                    let items = columns
                        .iter()
                        .zip(asc.iter())
                        .map(|(c, a)| {
                            if *a {
                                format!("{} ASC", c)
                            } else {
                                format!("{} DESC", c)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("Sort [{}]", items)
                }
                PlanNode::Join { join_type, on, .. } => format!("Join [{:?}, on: {}]", join_type, on.as_ref().map_or("None".to_string(), |e| format!("{}", e))),
                PlanNode::DDL { op } => match op {
                    DdlOperation::CreateTable { table_name, .. } => {
                        format!("CreateTable [{}]", table_name)
                    }
                    DdlOperation::DropTable { table_name, if_exists } => {
                        if *if_exists {
                            format!("DropTable [{}] IF EXISTS", table_name)
                        } else {
                            format!("DropTable [{}]", table_name)
                        }
                    }
                    DdlOperation::CreateIndex { index_name, table_name, unique, .. } => {
                        let uniq = if *unique { "UNIQUE " } else { "" };
                        format!("CreateIndex [{}{}] on [{}]", uniq, index_name, table_name)
                    }
                    DdlOperation::RenameTable { old_name, new_name, if_exists } => {
                        let _ = if_exists;
                        format!("AlterTable [{}] RENAME TO {}", old_name, new_name)
                    }
                    DdlOperation::RenameColumn { table_name, old_name, new_name } => {
                        format!("AlterTable [{}] RENAME COLUMN {} TO {}", table_name, old_name, new_name)
                    }
                },
                PlanNode::Insert { table_name, columns, values, input } => {
                    if let Some(_) = input {
                        format!("Insert [{}] cols={:?} [Subquery]", table_name, columns)
                    } else {
                        let rows_str = values.iter().map(|row: &Vec<Expr>| {
                            row.iter().map(|e: &Expr| match e {
                                Expr::Subquery(_) => "[Subquery]".to_string(),
                                Expr::Value(v) => format!("{:?}", v.value),
                                _ => format!("{}", e)
                            }).collect::<Vec<_>>().join(", ")
                        }).collect::<Vec<_>>().join(" | ");
                        format!("Insert [{}] cols={:?} rows={}", table_name, columns, rows_str)
                    }
                }
                PlanNode::Delete { .. } => "Delete".to_string(),
                PlanNode::Update { assignments, .. } => {
                    format!("Update [assigns={}]", assignments.len())
                }
                PlanNode::CreateUser { user_name, password, if_not_exists } => {
                    format!("CreateUser [{} password={:?} if_not_exists={}]", user_name, password, if_not_exists)
                }
                PlanNode::DropUser { user_name, if_exists } => {
                    format!("DropUser [{} if_exists={}]", user_name, if_exists)
                }
                PlanNode::Grant { privilege, user_name, table_name: _ } => {
                    format!("Grant [{}] TO {}", privilege, user_name)
                }
                PlanNode::Revoke { privilege, user_name, table_name: _ } => {
                    format!("Revoke [{}] FROM {}", privilege, user_name)
                }
            }
        }

        fn children(plan: &PlanNode) -> Vec<&PlanNode> {
            match plan {
                PlanNode::TableScan { .. } => vec![],
                PlanNode::Subquery { subquery, .. } => vec![subquery],
                PlanNode::Apply { input, subquery, .. } => vec![input, subquery],
                PlanNode::Filter { input, .. } => vec![input],
                PlanNode::Aggregate { input, .. } => vec![input],
                PlanNode::Projection { input, .. } => vec![input],
                PlanNode::Sort { input, .. } => vec![input],
                PlanNode::Join { left, right, .. } => vec![left, right],
                PlanNode::Delete { input } => vec![input],
                PlanNode::Update { input, .. } => vec![input],
                PlanNode::Insert { input: Some(sub_plan), .. } => vec![sub_plan],
                _ => vec![],
            }
        }
        inner(plan, "", true);
    }
}

pub(crate) fn columns_ast_to_schema(
    columns: &[ColumnDef],
) -> crate::common::RsqlResult<TableSchema> {
    let mut table_columns = Vec::new();
    for col in columns.iter() {
        let name = col.name.to_string();

        let data_type = match &col.data_type {
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => ColType::Integer,
            sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Real => ColType::Float,
            sqlparser::ast::DataType::Double { .. } => ColType::Float,

            sqlparser::ast::DataType::Char(opt_len) => {
                let size = match opt_len {
                    Some(sqlparser::ast::CharacterLength::IntegerLength { length, .. }) => *length as usize,
                    Some(sqlparser::ast::CharacterLength::Max) | None => 1,
                };
                ColType::Chars(size)
            }

            sqlparser::ast::DataType::Varchar(opt_len) => {
                let size = match opt_len {
                    Some(sqlparser::ast::CharacterLength::IntegerLength { length, .. }) => *length as usize,
                    Some(sqlparser::ast::CharacterLength::Max) | None => 255,
                };
                ColType::VarChar(size)
            }

            sqlparser::ast::DataType::Bool => ColType::Bool,
        
            _ => return Err(RsqlError::ParserError(format!("Unsupported data type for column {}", name))),
        };

        let mut pk = false;
        let mut nullable = true;
        let mut unique = false;
        let mut index = false;

        for opt in &col.options {
            use sqlparser::ast::ColumnOption;
            match &opt.option {
                ColumnOption::PrimaryKey { .. } => { pk = true; nullable = false; index = true; unique = true; },
                ColumnOption::Unique { .. } => { unique = true; index = true; },
                ColumnOption::NotNull => { nullable = false; },
                ColumnOption::Null => { nullable = true; },
                _ => {}
            }
        }

        table_columns.push(TableColumn { name, data_type, pk, nullable, unique, index });
    }

    TableSchema::new(table_columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_plan_pretty_print_pro() {
        // let sql = "\
        //     UPDATE student
        //     SET    age  = 23,
        //         name = '李六'
        //     WHERE  id = 1002;
        // ";
        // let plan = Plan::build_plan(sql).unwrap();
        // let plan_node= match &plan.items[0] {
        //     PlanItem::DML(pn) => pn,
        //     _ => panic!("Not a DML plan item"),
        // };
        // Plan::pretty_print_pro(plan_node);

        let sql = "\
            Select * from users
        ";
        let plan = Plan::build_plan(sql).unwrap();
        let plan_node= match &plan.items[0] {
            PlanItem::DML(pn) => pn,
            _ => panic!("Not a DML plan item"),
        };
        Plan::pretty_print_pro(plan_node);

        // let sql = "\
        //     SELECT  dept_id,
        //             job_title,
        //             COUNT(*),
        //             SUM(salary),
        //             AVG(salary),
        //             MAX(salary),
        //             MIN(salary),
        //     FROM    employee
        //     GROUP BY dept_id, job_title;
        // ";
        // let plan = Plan::build_plan(sql).unwrap();
        // let plan_node= match &plan.items[0] {
        //     PlanItem::DML(pn) => pn,
        //     _ => panic!("Not a DML plan item"),
        // };
        // Plan::pretty_print_pro(plan_node);
        // assert!(false);
    }
}