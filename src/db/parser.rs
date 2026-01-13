mod parser;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use parser::ast_to_plan::build_logical_plan;
