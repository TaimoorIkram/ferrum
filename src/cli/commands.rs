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

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock};
use std::vec;

use indexmap::IndexMap;
use sqlparser::ast::{
    ColumnDef, ColumnOption, DataType, ObjectName, Select, SelectItem, SetExpr, Statement,
    TableConstraint, TableFactor,
};

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
///
/// # Issues
/// - The executor will alter need a Session API to get the current database
/// in focus but for now, a ref to a mutable database will be sufficient.
pub struct SqlExecutor {
    statement: Statement,
    database: Arc<RwLock<Database>>,
}

/// After a query runs and completes its execution, the result of the query
/// needs to be displayed in some cases on the terminal.
///
/// The [`SqlResult`] is a simple struct that stores the displayable
/// [`TableReader`] and [`Row`] objects inside it and returns them to the main
/// terminal thread after completion.
///
/// # Improvements
/// - Add separate displayers for CLI to show rows and tables via the readers or
/// use the Display trait more effectively (food for thought).
pub struct SqlResult {
    pub table: Option<TableReader>,
    pub row: Option<Row>,
    pub n_rows_processed: Option<usize>,
}

impl Display for SqlResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.table.as_ref().unwrap())
    }
}

/// The one and only struct for implementing the commands execution.
///
/// # Issues
/// - Executor needs to have some threading architecture, not obvious how it fits
/// in the current, quickly changing design.
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

    fn _extract_column_definition(
        &self,
        column_definition: ColumnDef,
    ) -> Result<Vec<String>, String> {
        //! Extract the column definition from the [`ColumnDef`] object, to make it compatible with the
        //! perisistence api.
        //!
        //! The resultant vector contains the format of information as [`col_name, datatype, pk/fk, fk_col`]
        //! This can then be consumed to run the actual persistence api to create the table column.
        //!
        //! The main intent of this step is to make the features simple to implement and scope of the project
        //! manageable.
        //!
        //! This function assumes the parsing has pointed out any missing compulsory entries already so everything
        //! is not checked using the let-option pattern binding.

        let mut col_def = vec![];

        let col_name = &column_definition.name.value;
        col_def.push(col_name.clone());

        match column_definition.data_type {
            DataType::Int(_) => col_def.push("num".to_string()),
            DataType::Varchar(_) => col_def.push("txt".to_string()),
            _ => return Err(format!("Invalid type for column {}", col_name)),
        }

        for option in column_definition.options {
            match option.option {
                ColumnOption::PrimaryKey(_) => col_def.push("pk".to_string()),
                _ => return Err(format!("Invalid option for column {}", col_name)),
            }
        }

        Ok(col_def)
    }

    fn _extract_constraint_definition(
        &self,
        column_names: Vec<String>,
        constraint_definition: TableConstraint,
    ) -> Result<(String, Vec<String>), String> {
        //! Foreign keys do not appear inside the `option` field in a column definition, like
        //! primary key does, so it needs to be parsed as a constraint.
        //!
        //! First, check that the column map already contains the definition for this column, and
        //! if there is none, then error out.

        let mut column_constraints = vec![];

        match constraint_definition {
            TableConstraint::ForeignKey(fk) => {
                let col_name = fk
                    .columns
                    .get(0)
                    .expect("Expected a column name to this foreign key.")
                    .value
                    .clone();

                if !column_names.contains(&col_name) {
                    return Err(format!("The column {} was not defined.", &col_name));
                }

                column_constraints.push("fk".to_string());

                let ref_table_name = match fk.foreign_table {
                    ObjectName(obj) => obj
                        .get(0)
                        .expect("Expected a table name to the foreign key constraint")
                        .as_ident()
                        .unwrap()
                        .value
                        .clone(),
                };
                let ref_col_name = fk
                    .referred_columns
                    .get(0)
                    .expect("Expected a column name to this foreign key.")
                    .value
                    .clone();

                let fk = vec![ref_table_name, ref_col_name];
                column_constraints.push(fk.join("."));

                Ok((col_name, column_constraints))
            }
            _ => {
                return Err(format!(
                    "Invalid option for column. Check your statement again."
                ));
            }
        }
    }

    pub fn new(statement: Statement, database: &Arc<RwLock<Database>>) -> SqlExecutor {
        SqlExecutor {
            statement,
            database: Arc::clone(database),
        }
    }

    pub fn execute(&self) -> Result<SqlResult, String> {
        match &self.statement {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => {
                    let column_names = self._extract_column_names(select)?;
                    let table_name = self._extract_table_name(select)?;

                    println!(
                        "{}",
                        system_message(
                            "exctr",
                            format!(
                                "Selecting {} in table {}.",
                                column_names.join(", "),
                                table_name
                            ),
                        )
                    );

                    // database.get_table()
                    // table.reader().scan()
                    // TODO: parse the col names and check if * or list of cols is required
                    // from table_name

                    let database = self.database.read().unwrap();
                    if let Some(table) = database.get_table(&table_name) {
                        let table = table.read().unwrap();
                        let reader = table.reader();

                        let result_table;
                        if column_names.contains(&"*".to_string()) {
                            result_table = table.reader();
                        } else {
                            result_table = reader.select(column_names)?;
                        }

                        Ok(SqlResult {
                            table: Some(result_table),
                            row: None,
                            n_rows_processed: Some(table._rows()),
                        })
                    } else {
                        Err(system_message(
                            "system",
                            format!("Table '{}' does not exist!", &table_name),
                        ))
                    }
                }
                _ => Err(system_message(
                    "exctr",
                    "This type of query is not handled by the engine yet!".to_string(),
                )),
            },
            Statement::CreateTable(create_table) => {
                let table_name = create_table
                    .name
                    .0
                    .iter()
                    .map(|item| item.as_ident().unwrap().value.clone())
                    .collect::<Vec<_>>()
                    .join(".");

                let mut col_def_map = IndexMap::new();

                for column_definition in create_table.columns.iter() {
                    let col_name = column_definition.name.value.clone();
                    let col_def = self._extract_column_definition(column_definition.clone());

                    col_def_map.insert(col_name, col_def.unwrap());
                }

                // for every constraint, if any, process the constraint
                // if the constraint is for a column that is not defined, the code errors; wrong statement
                // if the constraint is for a column that is defined, the code processes it, and returns the
                // col name to append to, and the vec to append

                for constraint in create_table.constraints.iter() {
                    let (column_name, column_constraint) = self._extract_constraint_definition(
                        col_def_map
                            .keys()
                            .cloned()
                            .into_iter()
                            .map(|key| key)
                            .collect(),
                        constraint.clone(),
                    )?;

                    let prev_constraint = col_def_map.get_mut(&column_name).unwrap();
                    prev_constraint.extend(column_constraint);
                }

                let mut database = self.database.write().unwrap();
                let column_definitions = col_def_map
                        .values()
                        .into_iter()
                        .map(|def| def.join(" "))
                    .collect();

                database.create_table(table_name, column_definitions)?;

                Ok(SqlResult {
                    table: None,
                    row: None,
                    n_rows_processed: Some(0),
                })
            },
            _ => Err(system_message(
                "exctr",
                "This statement is not handled by the engine yet!".to_string(),
            )),
        }
    }
}
