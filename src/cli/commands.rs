//! This module where all the commands are stored.
//!
//! Ferrum command line syntax:
//!
//! - ferrum --help | Command Line Help
//! - ferrum client | Run the CLI app.
//! - ferrum server | Run the server listener (soon).
//!
//! Once the user is inside the REPL, SQL parser takes over. Here is an 
//! initial syntax for SQL queries that Ferrum will support.
//!
//! - USE database;
//! - CREATE database;
//! - SELECT cols* FROM table;
//! - INSERT INTO table VALUES (values)*;
//! - CREATE TABLE table (schema*)
//!
//! Here * means more than one such values separated by a comma.

use sqlparser::ast::{Select, SelectItem, SetExpr, Statement, TableFactor};

use crate::cli::messages::system_message;
use crate::persistence::{Database, Row, Table, TableReader};

/// The executor class that runs the statements.
///
/// Every statement needs an executor to be run. That way, I can easily
/// integrate multi-threading later on.
///
/// The purpose of the executor will be to handle the locking on the said
/// databases and tables, so that other threads do not harm the integrity
/// of the records in the database.
///
/// After all operations are completed, the lock can then be released for
/// other threads to capture and use.
///
/// In theory, this fits well with the application.
pub struct SqlExecutor {
    statement: Statement,
}

/// After a query runs and completes its execution, the result of the query
/// needs to be displayed in some cases on the terminal.
/// 
/// The [`SqlResult`] is a simple struct that stores the displayable 
/// [`TableReader`] and [`Row`] objects inside it and returns them to the main 
/// terminal thread after completion.
pub struct SqlResult {
    table: Option<TableReader>,
    row: Option<Row>,
}

impl SqlExecutor {
    fn _extract_column_names(&self, select: &Select) -> Result<Vec<String>, String> {
        let mut column_names = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    // SELECT col1, col2, col3, ... FROM

                    if let sqlparser::ast::Expr::Identifier(ident) = expr {
                        column_names.push(ident.value.clone());
                    } else {
                        return Err(system_message(
                            "exctr",
                            format!("Invalid column identifier expression '{}'!", expr),
                        ));
                    }
                }
                SelectItem::Wildcard(_) => {
                    // SELECT * FROM

                    column_names.push("*".to_string());
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    // SELECT table.*

                    column_names.push("*".to_string());
                }
                _ => {
                    return Err(system_message(
                        "exctr",
                        "Unable to process this SELECT statement.".to_string(),
                    ));
                }
            }
        }

        Ok(column_names)
    }

    fn _extract_table_name(&self, select: &Select) -> Result<String, String> {
        let table_with_joins = select.from.first().ok_or(system_message(
            "exctr",
            "There is no table name after FROM keyword.".to_string(),
        ))?;

        match &table_with_joins.relation {
            TableFactor::Table { name, .. } => Ok(name
                .0
                .iter()
                .map(|ident| ident.as_ident().unwrap().value.clone())
                .collect::<Vec<_>>()
                .join(".")),
            _ => Err(system_message(
                "exctr",
                "Invalid table name format.".to_string(),
            )),
        }
    }

    pub fn new(statement: Statement) -> SqlExecutor {
        SqlExecutor { statement }
    }

    pub fn execute(&self) -> Result<usize, String> {
        match &self.statement {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => {
                    let column_names = self._extract_column_names(select)?;
                    let table_name = self._extract_table_name(select)?;

                    println!(
                        "{}",
                        system_message(
                            "exctr",
                            format!("Selecting {} in table {}.", column_names.join(", "), table_name),
                        )
                    );

                    // database.get_table()
                    // table.reader().scan()
                    // TODO: parse the col names and check if * or list of cols is required 
                    // from table_name

                    Ok(1)
                }
                _ => Err(system_message(
                    "exctr",
                    "This type of query is not handled by the engine yet!".to_string(),
                )),
            },
            _ => Err(system_message(
                "exctr",
                "This statement is not handled by the engine yet!".to_string(),
            )),
        }
    }
}