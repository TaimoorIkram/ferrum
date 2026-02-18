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
    Assignment, BinaryOperator, ColumnDef, ColumnOption, DataType, Expr, Function, LimitClause,
    ObjectName, OrderBy, Select, SelectItem, SetExpr, Statement, TableConstraint, TableFactor,
    TableObject, TableWithJoins, Use, Value, ValueWithSpan,
};

use crate::cli::messages::{highlight_argument, system_message};
use crate::functions::{aggregators, scalars};
use crate::persistence::{Database, Row, TableReader};
use crate::sessions::session::Session;

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
    session: Arc<RwLock<Session>>,
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
    pub n_rows_processed: Option<usize>,
}

impl Display for SqlResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.table.as_ref().unwrap())
    }
}

/// A struct to represent column selection and column aggregators.
///
/// This class clearly distincts a simple column name from a function
/// call with parameters.
///
/// # Issues
/// - For now, there is not separate wildcard column select option. It
/// is worked around by a * named column inside the [`SelectColumn::Column`]
/// variant.
pub enum SelectColumn {
    Column {
        name: String,
        alias: Option<String>, // used in ExprWithAlias parsing
    },
    Function {
        name: String,
        args: Vec<FunctionArg>,
        function_type: FunctionType,
        alias: Option<String>, // used in ExprWithAlias parsing
    },
}

/// A type for representing a single argument to a column
/// aggregator function.
///
/// Works for cases like:
/// - COUNT(*)       - now
/// - COUNT(name)    - now
/// - COUNT(age * 2) - in future
pub enum FunctionArg {
    Wildcard,
    Column(String),
}

/// A type specifier for the type of [`SelectColumn::Function`].
///
/// It can either be a `Scalar` or an `Aggregator`.
///
/// A Scalar is performed on a single row at once while an Aggregator
/// is performed on the data or group as a whole.
///
/// Both are NOT allowed together.
pub enum FunctionType {
    Scalar,
    Aggregator,
}

enum SqlExecutorSelectMode {
    Column,
    Aggregate,
}

impl Display for SelectColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column { name, alias } => {
                let alias_name = {
                    if alias.is_some() {
                        format!(" as {}", alias.as_ref().unwrap().clone())
                    } else {
                        String::new()
                    }
                };

                write!(f, "{}{}", name.clone(), alias_name)
            }
            Self::Function {
                name, args, alias, ..
            } => {
                let mut arg_names = Vec::new();

                for arg in args.iter() {
                    arg_names.push(format!("{}", arg));
                }

                let alias_name = {
                    if alias.is_some() {
                        format!(" as {}", alias.as_ref().unwrap().clone())
                    } else {
                        String::new()
                    }
                };

                write!(f, "{}({}){}", name, arg_names.join(", "), alias_name)
            }
        }
    }
}

impl Display for FunctionArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(name) => write!(f, "{}", name.clone()),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

/// The one and only struct for implementing the commands execution.
///
/// # Issues
/// - Executor needs to have some threading architecture, not obvious how it fits
/// in the current, quickly changing design.
impl SqlExecutor {
    fn _extract_function_argument(
        &self,
        arg: &sqlparser::ast::FunctionArgExpr,
    ) -> Option<FunctionArg> {
        //! Parse a single [`sqlparser::ast::FunctionArgExpr`] and return the argument as
        //! a [`FunctionArg`] object.

        match arg {
            sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                let expr_string = self._parse_expr(expr);
                Some(FunctionArg::Column(expr_string.unwrap()))
            }
            sqlparser::ast::FunctionArgExpr::Wildcard => Some(FunctionArg::Wildcard),
            _ => None,
        }
    }
    fn _extract_function(
        &self,
        func: &Function,
        alias: Option<String>,
    ) -> Result<SelectColumn, String> {
        //! Parses a [`Function`] object to give a Function variant of [`SelectColumn`]
        //! struct.
        //!
        //! # Issues
        //! - Not yet recursive, so cannot handle call within a call.
        //! - Not yet handling subqueries so cannot put a whole query into
        //! the aggregator.

        let func_name = {
            let _fn = func.name.0.first().unwrap();
            let _fni = _fn.as_ident().unwrap();
            _fni.value.clone()
        };

        let func_args = match &func.args {
            sqlparser::ast::FunctionArguments::List(list) => list
                .args
                .iter()
                .filter_map(|item| match item {
                    sqlparser::ast::FunctionArg::Unnamed(arg) => {
                        self._extract_function_argument(arg)
                    }
                    _ => None,
                })
                .collect(),
            _ => return Err("Invalid type of function arguments. Check your query.".to_string()),
        };

        let func_type = if aggregators::is_allowed(&func_name) {
            FunctionType::Aggregator
        } else if scalars::is_allowed(&func_name) {
            FunctionType::Scalar
        } else {
            return Err(system_message(
                "exctr",
                format!(
                    "The function {} is not an allowed aggregator or scalar.",
                    highlight_argument(&func_name)
                ),
            ));
        };

        Ok(SelectColumn::Function {
            name: func_name,
            args: func_args,
            function_type: func_type,
            alias: alias,
        })
    }

    fn _extract_column_names(
        &self,
        select: &Select,
    ) -> Result<(Vec<SelectColumn>, SqlExecutorSelectMode), String> {
        let mut column_names = Vec::new();
        let mut select_mode: Option<SqlExecutorSelectMode> = None;

        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { expr, alias } => {
                    // TODO: SELECT col1 AS alias, ... FROM

                    match expr {
                        sqlparser::ast::Expr::Identifier(ident) => {
                            // Insert a [`SelectColumn::Column`]
                            let column_name = ident.value.clone();

                            if let Some(mode) = &select_mode {
                                match mode {
                                    SqlExecutorSelectMode::Aggregate => {
                                        return Err(system_message(
                                            "exctr",
                                            format!(
                                                "Invalid {}; columns not allowed with aggregators.",
                                                highlight_argument(&column_name)
                                            ),
                                        ));
                                    }
                                    _ => {}
                                }
                            };

                            column_names.push(SelectColumn::Column {
                                name: column_name,
                                alias: Some(alias.value.clone()),
                            });

                            if select_mode.is_none() {
                                select_mode = Some(SqlExecutorSelectMode::Column);
                            }
                        }
                        sqlparser::ast::Expr::Function(func) => {
                            // Insert a [`SelectColumn::Function`]
                            let function =
                                self._extract_function(func, Some(alias.value.clone()))?;

                            if let Some(mode) = &select_mode {
                                match mode {
                                    SqlExecutorSelectMode::Column => match &function {
                                        SelectColumn::Function {
                                            name,
                                            function_type,
                                            ..
                                        } => match function_type {
                                            FunctionType::Aggregator => {
                                                return Err(system_message(
                                                    "exctr",
                                                    format!(
                                                        "Invalid {}; aggregators not allowed with columns.",
                                                        highlight_argument(&name)
                                                    ),
                                                ));
                                            }
                                            _ => {}
                                        },
                                        _ => {}
                                    },
                                    _ => {}
                                }
                            }

                            column_names.push(function);

                            if select_mode.is_none() {
                                select_mode = Some(SqlExecutorSelectMode::Aggregate);
                            }
                        }
                        _ => {
                            return Err(system_message(
                                "exctr",
                                format!("Invalid column identifier expression '{}'!", expr),
                            ));
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    // SELECT col1, col2, col3, ... FROM
                    // SELECT COUNT(*), MAX(age), ... FROM
                    // Could be made better using the _parse_expr after
                    // matching identifier

                    match expr {
                        sqlparser::ast::Expr::Identifier(ident) => {
                            // Insert a [`SelectColumn::Column`]
                            let column_name = ident.value.clone();

                            if let Some(mode) = &select_mode {
                                match mode {
                                    SqlExecutorSelectMode::Aggregate => {
                                        return Err(system_message(
                                            "exctr",
                                            format!(
                                                "Invalid {}; columns not allowed with aggregators.",
                                                highlight_argument(&column_name)
                                            ),
                                        ));
                                    }
                                    _ => {}
                                }
                            };

                            column_names.push(SelectColumn::Column {
                                name: column_name,
                                alias: None,
                            });

                            if select_mode.is_none() {
                                select_mode = Some(SqlExecutorSelectMode::Column);
                            }
                        }
                        sqlparser::ast::Expr::Function(func) => {
                            // Insert a [`SelectColumn::Function`]
                            let function = self._extract_function(func, None)?;

                            if let Some(mode) = &select_mode {
                                match mode {
                                    SqlExecutorSelectMode::Column => match &function {
                                        SelectColumn::Function {
                                            name,
                                            function_type,
                                            ..
                                        } => match function_type {
                                            FunctionType::Aggregator => {
                                                return Err(system_message(
                                                    "exctr",
                                                    format!(
                                                        "Invalid {}; aggregators not allowed with columns.",
                                                        highlight_argument(&name)
                                                    ),
                                                ));
                                            }
                                            _ => {}
                                        },
                                        _ => {}
                                    },
                                    _ => {}
                                }
                            }

                            column_names.push(function);

                            if select_mode.is_none() {
                                select_mode = Some(SqlExecutorSelectMode::Aggregate);
                            }
                        }
                        _ => {
                            return Err(system_message(
                                "exctr",
                                format!("Invalid column identifier expression '{}'!", expr),
                            ));
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // SELECT * FROM

                    column_names.push(SelectColumn::Column {
                        name: "*".to_string(),
                        alias: None,
                    });
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    // SELECT table.*

                    column_names.push(SelectColumn::Column {
                        name: "*".to_string(),
                        alias: None,
                    });
                }
            }
        }

        Ok((
            column_names,
            select_mode.unwrap_or(SqlExecutorSelectMode::Column),
        ))
    }

    fn _extract_table_name(&self, table_with_joins: &TableWithJoins) -> Result<String, String> {
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

    fn _parse_expr(&self, expr: &Expr) -> Result<String, String> {
        match expr {
            Expr::Value(value) => self._parse_value(&value),
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::UnaryOp { op, expr } => {
                let value = self._parse_expr(expr)?;
                let prefix = match op {
                    sqlparser::ast::UnaryOperator::Minus => "-",
                    _ => {
                        return Err(system_message(
                            "system",
                            "Unsupported unary operator.".to_string(),
                        ));
                    }
                };

                Ok(format!("{}{}", prefix, value))
            }
            _ => {
                return Err(system_message(
                    "system",
                    "Unsupported value. Check your query.".to_string(),
                ));
            }
        }
    }

    fn _parse_value(&self, value: &ValueWithSpan) -> Result<String, String> {
        //! Match the [`Value`] object properly to its subtype and return the
        //! engine-specific (value, datatype) mapping.
        //!
        //! This function is a utility to allow data insertion format to align
        //! with the one accepted by the persistence API

        let value = match &value.value {
            Value::Number(value, _) => value,
            Value::SingleQuotedString(value) => value,
            Value::DoubleQuotedString(value) => value,
            _ => {
                return Err(system_message(
                    "system",
                    format!(
                        "Unsupported value: {}",
                        highlight_argument(&value.to_string())
                    ),
                ));
            }
        };

        Ok(value.into())
    }

    fn _extract_row(&self, values: Vec<Expr>) -> Result<Vec<String>, String> {
        //! Extract a row form a [`Vec<Expr>`] to create a [`Vec<String>`]
        //! which is compatible with the persistence api.
        //!
        //! Internally, uses an extraction function to extract all supported
        //! expression types into a String that can be stored inside the engine
        //!
        //! # Issues
        //! - This function is not recursive, which it should be; it should be able
        //! to call recursively itself, untul a [`ValueWithSpan`] is obtained, that
        //! can then be parsed using the [`self._parse_value_to_engine`] function.

        values.into_iter().map(|e| self._parse_expr(&e)).collect()
    }

    fn _parse_selection(
        &self,
        selection: &Expr,
        table_schema_vec: &Vec<String>,
    ) -> Result<Box<dyn Fn(&Row) -> bool>, String> {
        //! Parse the [`Expr::BinaryOp`] variant to a filter.
        //!
        //! Returns a closure `Fn(&Row) -> bool` that takes a row to check
        //! if it is a fit over the filter. I intend it to be used inside a filter
        //! function on a tabler reader as well.
        //!
        //! Filter flow: the filter function can NOT directly access the table and
        //! the row index, therefore, we rely directly on the database to do this for
        //! us. In that case, we will have to pass the left and right values to the
        //! database, restructure them to map them to the table and run the final
        //! filter to get the resultant rows.

        match selection {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Or => {
                    let left_filter = self._parse_selection(left, table_schema_vec)?;
                    let right_filter = self._parse_selection(right, table_schema_vec)?;

                    Ok(Box::new(move |row| left_filter(row) || right_filter(row)))
                }
                BinaryOperator::And => {
                    let left_filter = self._parse_selection(left, table_schema_vec)?;
                    let right_filter = self._parse_selection(right, table_schema_vec)?;

                    Ok(Box::new(move |row| left_filter(row) && right_filter(row)))
                }
                BinaryOperator::Eq => {
                    let (col_index, value) =
                        self._parse_operands(left.as_ref(), right.as_ref(), table_schema_vec)?;

                    Ok(Box::new(move |row| {
                        row.0
                            .get(col_index)
                            .and_then(|v| v.as_ref())
                            .map_or(false, |v| v == &value)
                    }))
                }
                BinaryOperator::NotEq => {
                    let (col_index, value) =
                        self._parse_operands(left.as_ref(), right.as_ref(), table_schema_vec)?;

                    Ok(Box::new(move |row| {
                        row.0
                            .get(col_index)
                            .and_then(|v| v.as_ref())
                            .map_or(false, |v| v != &value)
                    }))
                }
                _ => Err(format!("Invalid query filter. Check your query.")),
            },
            _ => Err(format!("Invalid column selection. Check your query.")),
        }
    }

    fn _parse_operands(
        &self,
        left: &Expr,
        right: &Expr,
        table_schema_vec: &Vec<String>,
    ) -> Result<(usize, String), String> {
        let col_name = self._parse_expr(left)?;
        let value = self._parse_expr(right)?;

        let col_index = table_schema_vec
            .iter()
            .position(|col| col == &col_name)
            .ok_or_else(|| format!("Column {} does not exist!", highlight_argument(&col_name)))?;

        Ok((col_index, value))
    }

    fn _parse_assignment(&self, assignment: Assignment) -> Result<(String, String), String> {
        let col_name = match assignment.target {
            sqlparser::ast::AssignmentTarget::ColumnName(object) => {
                if let Some(object_name) = object.0.first() {
                    object_name.as_ident().unwrap().value.clone()
                } else {
                    return Err(format!("Invalid column name format."));
                }
            }
            _ => return Err(format!("Invalid column name. Check your query.")),
        };
        let value = self._parse_expr(&assignment.value)?;

        Ok((col_name, value))
    }

    fn _order_by(&self, query_result: SqlResult, order_by: &OrderBy) -> Result<SqlResult, String> {
        //! Process filters on the resulting query and return the final result.

        match &order_by.kind {
            sqlparser::ast::OrderByKind::Expressions(expressions) => {
                // Iterate over each and make a filter that orders by priority
                // meaning the first filter applies first, then the second among
                // each of the first and then the third among each group of second
                let mut sort_index = vec![];

                for order in expressions.iter() {
                    if order.options.asc.is_none() {
                        return Ok(query_result);
                    } else {
                        if let Some(asc) = order.options.asc {
                            let identifier = self._parse_expr(&order.expr)?;

                            // get the table object ref and then derive index of that column from
                            let schema = {
                                let _tr = query_result.table.as_ref().unwrap();
                                _tr.schema.read().unwrap()
                            };
                            let col_index = schema
                                .get_vec()
                                .iter()
                                .position(|(col_name, _)| &identifier == col_name);

                            if let Some(col_index) = col_index {
                                sort_index.push((col_index, asc));
                            }
                        }
                    }
                }

                println!(
                    "{}",
                    system_message(
                        "sorter",
                        format!(
                            "Sorting data by order: {}",
                            highlight_argument(format!("{:?}", sort_index).as_str())
                        )
                    )
                );

                let table_reader = query_result.table.unwrap();
                let table_reader_rows = table_reader.count_rows();

                Ok(SqlResult {
                    table: Some(table_reader.order_by(sort_index)),
                    n_rows_processed: Some(table_reader_rows),
                })
            }
            _ => {
                return Err(system_message(
                    "exctr",
                    "Can not order by this kind of filter!".to_string(),
                ));
            }
        }
    }

    fn _limit_offset(
        &self,
        query_result: SqlResult,
        limit_clause: &LimitClause,
    ) -> Result<SqlResult, String> {
        //! Limit and offset the results of the query.
        //!
        //! Returns a new [`SqlResult`] object.

        let mut row_limit: Option<usize> = None;
        let mut row_offset: Option<usize> = None;

        match limit_clause {
            LimitClause::LimitOffset { limit, offset, .. } => {
                if let Some(limit_expr) = limit {
                    row_limit = Some(
                        self._parse_expr(limit_expr)?
                            .parse()
                            .expect("String values not allowed as limits."),
                    );
                }

                if let Some(offset_expr) = offset {
                    row_offset = Some(
                        self._parse_expr(&offset_expr.value)?
                            .parse()
                            .expect("String values not allowed as limits."),
                    );
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                row_offset = Some(
                    self._parse_expr(offset)?
                        .parse()
                        .expect("String values not allowed as limits."),
                );

                row_limit = Some(
                    self._parse_expr(limit)?
                        .parse()
                        .expect("String values not allowed as limits."),
                );
            }
        }

        println!(
            "{}",
            system_message(
                "limoft",
                format!(
                    "Selecting {} rows from row {} onwards.",
                    highlight_argument(format!("{:?}", row_limit).as_str()),
                    highlight_argument(format!("{:?}", row_offset).as_str())
                )
            )
        );

        let old_table_reader = query_result.table.unwrap();
        let new_table_reader = old_table_reader.offset(row_offset)?.limit(row_limit)?;
        let new_table_reader_rows = new_table_reader.count_rows();

        Ok(SqlResult {
            table: Some(new_table_reader),
            n_rows_processed: Some(new_table_reader_rows),
        })
    }

    fn _get_db_from_session(&self) -> Result<Arc<RwLock<Database>>, String> {
        let session = self.session.read().unwrap();

        if let Some(database) = session.get_active_database() {
            Ok(database)
        } else {
            return Err(format!("no database currently selected."));
        }
    }

    fn _parse_object_name(&self, obj_name: &ObjectName) -> String {
        //! Parse the annoying name object and obtain its string value.

        obj_name
            .0
            .first()
            .unwrap()
            .as_ident()
            .take()
            .unwrap()
            .value
            .clone()
    }

    pub fn new(statement: Statement, session: &Arc<RwLock<Session>>) -> SqlExecutor {
        SqlExecutor {
            statement,
            session: Arc::clone(session),
        }
    }

    pub fn execute(&self) -> Result<SqlResult, String> {
        //! Run the assigned command and display results if any are to be displayed.
        //!
        //! Currently, an arc has to be acquired first, in every branch, and then the
        //! database is read or modified.

        match &self.statement {
            Statement::Query(query) => {
                let mut query_result = match query.body.as_ref() {
                    SetExpr::Select(select) => {
                        let (column_names, select_mode) = self._extract_column_names(select)?;
                        let table_with_joins = select.from.first().ok_or(system_message(
                            "exctr",
                            "There is no table name after FROM keyword.".to_string(),
                        ))?;
                        let table_name = self._extract_table_name(table_with_joins)?;

                        println!(
                            "{}",
                            system_message(
                                "exctr",
                                format!(
                                    "Selecting {} in table {}.",
                                    column_names
                                        .iter()
                                        .map(|sel_col| format!("{}", sel_col))
                                        .collect::<Vec<String>>()
                                        .join(", "),
                                    table_name
                                ),
                            )
                        );

                        // database.get_table()
                        // table.reader().scan()
                        // TODO: parse the col names and check if * or list of cols is required
                        // from table_name

                        let db_arc = self._get_db_from_session()?;
                        let database = db_arc.read().unwrap();

                        if let Some(table) = database.get_table(&table_name) {
                            let table = table.read().unwrap();
                            let table_schema_vec: Vec<String> = {
                                let schema = table.schema.read().unwrap();
                                schema
                                    .get_vec()
                                    .iter()
                                    .map(|(col, _)| col)
                                    .cloned()
                                    .collect()
                            };

                            match select_mode {
                                SqlExecutorSelectMode::Aggregate => {
                                    let aggregate_result =
                                        table.perform_aggregate(&column_names)?;

                                    Ok(SqlResult {
                                        table: Some(aggregate_result),
                                        n_rows_processed: None,
                                    })
                                }
                                SqlExecutorSelectMode::Column => {
                                    let reader = table.reader();
                                    let mut result_table;

                                    let cols: Vec<String> = column_names
                                        .iter()
                                        .filter_map(|col| {
                                            if let SelectColumn::Column { name, .. } = col {
                                                Some(name.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();

                                    let sclrs: Vec<SelectColumn> = column_names
                                        .into_iter()
                                        .filter_map(|col| {
                                            if matches!(col, SelectColumn::Function { .. }) {
                                                Some(col)
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();

                                    // This check could be made better with:
                                    // - A vec based wrapper
                                    // - A wildcard check method or enum variant
                                    if cols.contains(&"*".to_string()) {
                                        result_table = table.reader();
                                    } else {
                                        // TODO: Update this call to include alias, so reader can display readable
                                        // column names.
                                        result_table = reader.select(cols)?;
                                    }

                                    if let Some(selection) = select.selection.as_ref() {
                                        let filter =
                                            self._parse_selection(selection, &table_schema_vec)?;
                                        result_table = result_table.filter(filter).unwrap();
                                    }

                                    if sclrs.len() > 0 {
                                        println!("Performing {} scalars", sclrs.len());
                                        result_table = result_table.perform_function(&sclrs)?;
                                    }

                                    Ok(SqlResult {
                                        table: Some(result_table),
                                        n_rows_processed: Some(table._rows()),
                                    })
                                }
                            }
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
                }?;

                if let Some(order_by) = query.order_by.as_ref() {
                    query_result = self._order_by(query_result, order_by)?;
                }

                if let Some(limit_clause) = query.limit_clause.as_ref() {
                    query_result = self._limit_offset(query_result, limit_clause)?;
                }

                Ok(query_result)
            }
            Statement::Insert(insert) => {
                // check if the table exists in the database
                // process the rows one by one to convert them to persistence api
                // compatible rows
                // processed row is inserted into the table

                let table_name = match insert.table.clone() {
                    TableObject::TableName(obj) => obj.0[0].as_ident().unwrap().value.clone(),
                    _ => return Err("Invalid table name. Please check your query.".to_string()),
                };

                let db_arc = self._get_db_from_session()?;
                let mut database = db_arc.write().unwrap();
                if database.contains_table(&table_name) {
                    let query_body = insert.source.clone().expect(&system_message(
                        "system",
                        "No values to insert.".to_string(),
                    ));

                    let query_rows = match query_body.body.as_ref() {
                        SetExpr::Values(values) => values.rows.clone(),
                        _ => {
                            return Err("Invalid values list. Please check your query.".to_string());
                        }
                    };

                    let mut rows = vec![];

                    for row in query_rows {
                        // extract a row and add to a list of rows
                        // use the insert many to insert the rows together
                        rows.push(self._extract_row(row)?);
                    }

                    let inserted_row_count = database.insert_many_into_table(&table_name, rows)?;

                    Ok(SqlResult {
                        table: None,
                        n_rows_processed: Some(inserted_row_count),
                    })
                } else {
                    return Err(system_message(
                        "system",
                        format!("Table {} does not exist.", highlight_argument(&table_name)),
                    ));
                }
            }
            Statement::ShowTables { .. } => {
                let db_arc = self._get_db_from_session()?;
                let database = db_arc.read().unwrap();
                let table_names = database.get_table_names();

                if table_names.is_empty() {
                    println!("There are no tables in this database.");
                } else {
                    println!("There are {} tables in this database.", table_names.len());
                    for (index, table_name) in table_names.iter().enumerate() {
                        println!("{:5}. {:10}", index + 1, table_name);
                    }
                }

                return Ok(SqlResult {
                    table: None,
                    n_rows_processed: Some(0),
                });
            }
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

                let db_arc = self._get_db_from_session()?;
                let mut database = db_arc.write().unwrap();
                let column_definitions = col_def_map
                    .values()
                    .into_iter()
                    .map(|def| def.join(" "))
                    .collect();

                database.create_table(table_name, column_definitions)?;

                Ok(SqlResult {
                    table: None,
                    n_rows_processed: Some(0),
                })
            }
            Statement::Delete(delete) => {
                let table_name = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(joins) => {
                        let table_with_joins = joins.first().ok_or(system_message(
                            "exctr",
                            "There is no table name after FROM keyword.".to_string(),
                        ))?;
                        self._extract_table_name(table_with_joins)?
                    }
                    _ => return Err("Invalid DELETE statement.".to_string()),
                };

                let db_arc = self._get_db_from_session()?;
                let mut database = db_arc.write().unwrap();
                if database.contains_table(&table_name) {
                    // CAUTION: LOCK HOLD PROBLEM
                    // The following expression solves an issue of infinite read locking
                    // on table (first two lines were before table_schema_vec definition)
                    // causing the database.delete... methods to wait on read lock to
                    // finish and then start a write lock, which would cause the engine to
                    // hang indefinitely. The database api might need a few additions
                    let table_schema_vec = {
                        let _tl = database.get_table(&table_name).unwrap();
                        let _t = _tl.read().unwrap();
                        let _s = _t.schema.read().unwrap();
                        _s.get_vec().iter().map(|(col, _)| col.clone()).collect()
                    };

                    let mut filter = None;
                    if let Some(selection) = delete.selection.as_ref() {
                        filter = self._parse_selection(selection, &table_schema_vec).ok();
                    }

                    let deleted_row_count =
                        database.delete_from_table_with_filter(&table_name, filter)?;

                    Ok(SqlResult {
                        table: None,
                        n_rows_processed: Some(deleted_row_count),
                    })
                } else {
                    Err(system_message(
                        "system",
                        format!("Table {} does not exist!", highlight_argument(&table_name)),
                    ))
                }
            }
            Statement::Update(update) => {
                let table_with_joins = &update.table;
                let table_name = self._extract_table_name(table_with_joins)?;

                let db_arc = self._get_db_from_session()?;
                let mut database = db_arc.write().unwrap();

                if database.contains_table(&table_name) {
                    // CAUTION: LOCK HOLD PROBLEM
                    // The following expression solves an issue of infinite read locking
                    // on table (first two lines were before table_schema_vec definition)
                    // causing the database.delete... methods to wait on read lock to
                    // finish and then start a write lock, which would cause the engine to
                    // hang indefinitely. The database api might need a few additions
                    let table_schema_vec: Vec<String> = {
                        let _tl = database.get_table(&table_name).unwrap();
                        let _t = _tl.read().unwrap();
                        let _s = _t.schema.read().unwrap();
                        _s.get_vec().iter().map(|(col, _)| col.clone()).collect()
                    };

                    let mut filter = None;
                    if let Some(selection) = update.selection.clone() {
                        filter = self._parse_selection(&selection, &table_schema_vec).ok();
                    }

                    let mut updates = HashMap::new();
                    for assignment in update.assignments.clone() {
                        let update = self._parse_assignment(assignment)?;
                        updates.insert(update.0, update.1);
                    }

                    let updated_row_count;
                    updated_row_count =
                        database.update_table_set_with_filters(&table_name, filter, updates)?;

                    Ok(SqlResult {
                        table: None,
                        n_rows_processed: Some(updated_row_count),
                    })
                } else {
                    Err(system_message(
                        "system",
                        format!("Table {} does not exist!", highlight_argument(&table_name)),
                    ))
                }
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => {
                let database_name = self._parse_object_name(&db_name);

                let mut session = self.session.write().unwrap();
                session.create_database(&database_name, *if_not_exists)?;

                Ok(SqlResult {
                    table: None,
                    n_rows_processed: Some(0),
                })
            }
            Statement::Use(use_stmt) => {
                let db_name = match use_stmt {
                    Use::Object(db) => self._parse_object_name(db),
                    _ => {
                        return Err(system_message(
                            "system",
                            format!(
                                "No other case than {} is handled yet.",
                                highlight_argument("USE <db_name>")
                            ),
                        ));
                    }
                };

                let mut session = self.session.write().unwrap();
                session.use_database(&db_name)?;

                Ok(SqlResult {
                    table: None,
                    n_rows_processed: Some(0),
                })
            }
            Statement::ShowDatabases { .. } => {
                // Display all databases but does NOT deal with compliated SQL features like
                // TERSE, HISTORY, LIMIT, STARTS WITH etc...
                let session = self.session.read().unwrap();
                let database_names = session.get_available_databases();

                if database_names.is_empty() {
                    println!("There are no databases in the registry yet.");
                } else {
                    println!(
                        "There are {} databases in the registry.",
                        database_names.len()
                    );
                    for (index, table_name) in database_names.iter().enumerate() {
                        println!("{:5}. {:10}", index + 1, table_name);
                    }
                }

                Ok(SqlResult {
                    table: None,
                    n_rows_processed: Some(0),
                })
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                // Handles simple drop, and ignores complicated SQL features like
                // CASCADE and RESTRICT
                // CASCADE by defauly and there is no other option

                match object_type {
                    sqlparser::ast::ObjectType::Database => {
                        let database = names.first().unwrap();
                        let db_name = self._parse_object_name(database);

                        let mut session = self.session.write().unwrap();

                        if let None = session.drop_database(&db_name) {
                            if !*if_exists {
                                return Err(system_message(
                                    "system",
                                    format!(
                                        "No other case than {} is handled yet.",
                                        highlight_argument("USE <db_name>")
                                    ),
                                ));
                            }
                        }

                        Ok(SqlResult {
                            table: None,
                            n_rows_processed: Some(0),
                        })
                    }
                    sqlparser::ast::ObjectType::Table => {
                        // Removes the table from the registry.

                        return Err(system_message(
                            "system",
                            format!("This feature will be implemented soon."),
                        ));
                    }
                    _ => {
                        return Err(system_message(
                            "system",
                            format!(
                                "No other case than {} is handled yet.",
                                highlight_argument("DROP DATABASE <db_name>")
                            ),
                        ));
                    }
                }
            }
            _ => Err(system_message(
                "exctr",
                "This statement is not handled by the engine yet!".to_string(),
            )),
        }
    }
}
