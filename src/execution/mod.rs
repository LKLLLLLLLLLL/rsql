pub mod executor;
pub mod result;
mod dml_interpreter;
mod ddl_interpreter;
mod dcl_interpreter;
mod expr_interpreter;

pub use executor::execute;