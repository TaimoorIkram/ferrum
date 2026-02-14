use log::warn;

use crate::cli::{FunctionArg, SelectColumn};
use crate::functions::{aggregators, scalars};

use super::index::{ForeignKeyConstraint, Index, Key};
use super::row::Row;
use super::schema::{ColumnInformation, DataType, Schema};

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::sync::{Arc, RwLock};

/// Creates a new table with the specified schema.
///
/// # Column Format
/// Each column definition is a space-separated string:
/// - `"column_name datatype [pk]"`
/// - Datatypes: `num` (number), `txt` (text)
/// - Optional: `pk` marks column as part of primary key
///
/// # Note
/// In case a `pk` value is not mentioned, the first column
/// will automatically be taken as a key column. Remember that
/// this does not create any index mappings but is just an
/// internal marker for the table to use if no indexes are
/// present. This will be replaced with a default compulsory
/// index and optional further indexes in the future.
///
/// # Examples
/// ```
/// use ferrum_engine::persistence::Table;
///
/// // Single column primary key
/// let table = Table::new(
/// "test".to_string(),
/// vec![
///     "id num pk".to_string(),
///     "name txt".to_string(),
/// ])?;
///
/// // Composite primary key
/// let table = Table::new(
/// "test".to_string(),
/// vec![
///     "user_id num pk".to_string(),
///     "order_id num pk".to_string(),
///     "amount num".to_string(),
/// ])?;
///
/// Ok::<(), String>(())
/// ```
///
/// # Errors
/// Returns `Err` if:
/// - Column list is empty
/// - Column definition is malformed
/// - Duplicate column names exist
pub struct Table {
    pub(crate) name: String,
    pub(crate) schema: Arc<RwLock<Schema>>,
    pub(crate) rows: Arc<RwLock<Vec<Row>>>,
    pub(crate) primary_key_columns: Vec<usize>,
    pub(crate) is_indexed: bool,
    pub(crate) index: Index,
}

/// Creates a reader object over a [Table]'s data snapshot.
///
/// A Table is mutable itself, so performing multiple read operations on the same
/// table is not optimal, especially when the architecture becomes multi-threaded.
/// To solve this, everytime a table is to be used as read-only, a [TableReader]
/// object is used via the [Table::reader] method.
///
/// A [TableReader] object stores a snapshot of the original table and can perform
/// the following operations over that snapshot:
/// - [TableReader::scan] returns all the [Row]s as a [Vec] object.
/// - [TableReader::filter] runs a filter closure on the rows, returns another
/// [TableReader] object.
/// - [TableReader::select] selects specific columns of the table to convert to
/// another [TableReader] object.
///
/// # Issues
/// - TableReader does NOT support indexing, because it does not know how to use that
/// index for a shrunk dataset when chaining its methods.
pub struct TableReader {
    pub schema: Arc<RwLock<Schema>>,
    pub rows: Arc<RwLock<Vec<Row>>>,
}

impl Table {
    pub fn _rows(&self) -> usize {
        //! Get the total number of rows as of the time of this call.
        //!
        //! Returns a cloned value of row count, may behave differently
        //! for multi-threaded system.

        self.rows.read().unwrap().len()
    }

    fn _validate_field(
        &self,
        item: String,
        col_name: &String,
        col_info: &ColumnInformation,
    ) -> Result<Option<String>, String> {
        //! An extended validator function to validate a single field.
        //!
        //! Returns an [`Option<String>`] if the field is valid, that can be directly pushed to row.

        if item.is_empty() && col_info.nullable {
            return Ok(None);
        } else if item.is_empty() && !col_info.nullable {
            return Err(format!(
                "invalid NULL: empty strings not allowed on columm '{}'",
                col_name
            ));
        } else {
            match col_info.datatype {
                DataType::Number => {
                    if item.parse::<u64>().is_err() {
                        return Err(format!(
                            "invalid {}: value not allowed on column '{}' ({})",
                            item, col_name, col_info.datatype
                        ));
                    }
                }
                DataType::Text => {
                    if let Some(max_limit) = col_info.max_limit {
                        if item.len() > max_limit {
                            return Err(format!(
                                "invalid {}: value not allowed on column '{}' ({})",
                                item, col_name, col_info.datatype
                            ));
                        }
                    }
                }
            }
            return Ok(Some(item));
        }
    }

    fn _validate_data(&self, data: Vec<String>) -> Result<Row, String> {
        //! Validate the row with respect to the schema.
        //!
        //! Returns the row if the data is correct.

        let schema = self.schema.read().unwrap();
        if data.len() != schema.len() {
            return Err(format!(
                "invalid data: schema has {} column(s), but {} were provided",
                schema.len(),
                data.len(),
            ));
        }

        let mut row: Vec<Option<String>> = Vec::new();

        for (item, (col_name, col_info)) in data.into_iter().zip(schema.get_vec()) {
            row.push(self._validate_field(item, col_name, col_info)?)
        }

        Ok(Row(row))
    }

    fn _parse_column(
        col_def: &str,
    ) -> Result<(Option<String>, Option<DataType>, Option<Key>), String> {
        //! Parse the column definition string.
        //!
        //! Returns the name of the column, and the column information.

        let mut col_def_vec: VecDeque<&str> = col_def.split(" ").collect();
        let (mut column, mut datatype, mut key) = (None, None, None);

        // Get the name of the column making sure it is not a keyword
        if let Some(col_name) = col_def_vec.pop_front() {
            if vec!["pk", "fk", "num", "txt"].contains(&col_name) {
                return Err(format!(
                    "invalid input {}: keywords not allowed as column names",
                    col_name
                ));
            } else {
                column = Some(col_name.to_string());
            }
        }

        // Get the datatype of the column
        if let Some(col_type) = col_def_vec.pop_front() {
            match col_type {
                "num" => datatype = Some(DataType::Number),
                "txt" => datatype = Some(DataType::Text),
                _ => {
                    return Err(format!(
                        "invalid datatype {}: not supported, on column {}",
                        col_type,
                        column.unwrap()
                    ));
                }
            }
        }

        // Get the keytype (if mentioned) of the column
        if let Some(col_key) = col_def_vec.pop_front() {
            match col_key {
                "pk" => key = Some(Key::PrimaryKey),
                "fk" => {
                    let fk_ref = col_def_vec
                        .pop_front()
                        .ok_or("invalid reference table: format <table.col>")?;

                    let mut fk_ref_args: VecDeque<String> = fk_ref
                        .split(".")
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect();

                    if fk_ref_args.len() == 2 {
                        key = Some(Key::ForeignKey(
                            fk_ref_args.pop_front().unwrap(),
                            fk_ref_args.pop_front().unwrap(),
                        ))
                    } else {
                        return Err(format!("invalid reference: check your fk argument again"));
                    }
                }
                _ => return Err(format!("invalid key type {}: expected pk or fk", col_key)),
            }
        }

        return Ok((column, datatype, key));
    }

    fn _create_index_key_from_row(&self, row: &Row) -> Result<String, String> {
        let mut values: Vec<String> = Vec::new();

        for index in self.primary_key_columns.iter() {
            let value_at_index = row.0.get(*index).unwrap();
            values.push(value_at_index.as_ref().unwrap().clone());
        }

        if values.len() == 0 && self.primary_key_columns.len() != 0 {
            return Err("err: failed to index: unable to read columns".to_string());
        }

        Ok(values.join("|"))
    }

    fn _extract_pk_values<'a>(&self, row: &'a Row) -> Vec<&'a str> {
        self.primary_key_columns
            .iter()
            .filter_map(|&idx| row.0.get(idx)?.as_ref().map(|s| s.as_str()))
            .collect()
    }

    fn _find_row_unindexed(&self, keys: Vec<&str>) -> Option<usize> {
        //! Search function by key, for tables with no index.
        //!
        //! Returns an index to a row.

        let rows = self.rows.read().unwrap();
        let key = keys.join("|");
        rows.iter()
            .position(|row| self._extract_pk_values(row).join("|") == key)
    }

    fn _find_row(&self, pk: Vec<&str>) -> Option<usize> {
        //! Search the row, in a table either with or without
        //! the index.
        //!
        //! Returns a pointer of the found row.

        if self.is_indexed {
            self.index.get(pk.join("|").as_str())
        } else {
            self._find_row_unindexed(pk)
        }
    }

    fn _validate_pk(&self, pk: &Vec<&str>) -> Result<(), String> {
        let key_components = self.primary_key_columns.len();
        if self.is_indexed && pk.len() != key_components {
            Err(format!(
                "err: invalid key arguments: {} expected, {} provided",
                key_components,
                pk.len()
            ))
        } else if pk.len() == 0 {
            Err(format!("err: need a key for non-indexed search"))
        } else {
            Ok(())
        }
    }

    pub fn pk_exists(&self, pk: &str) -> bool {
        self.index.get(pk).is_some()
    }

    pub fn new(name: String, columns: Vec<String>) -> Result<Table, String> {
        //! Return a new table with the said schema. The `columns` is a string mapping
        //! of column names and their datatypes.
        //!
        //! Returns an owned [Table] object.

        if columns.len() == 0 {
            return Err(String::from(
                "invalid arguments: 0 arguments does not make a schema",
            ));
        }

        let mut schema = vec![];
        let mut primary_key_columns = vec![];

        let n_columns = columns.len();

        for (index, col_def) in columns.iter().enumerate() {
            let (column, datatype, key) = Self::_parse_column(col_def)?;
            let max_limit = match datatype.as_ref().unwrap() {
                DataType::Number => None,
                DataType::Text => Some(50),
            };
            let mut col_info = ColumnInformation::from(datatype.unwrap(), max_limit, false);

            if let Some(key) = key {
                match key {
                    Key::PrimaryKey => primary_key_columns.push(index),
                    Key::ForeignKey(table_name, column_name) => {
                        col_info.foreign_key_constraint =
                            Some(ForeignKeyConstraint::new(table_name, column_name))
                    }
                }
            }
            schema.push((column.unwrap().clone(), col_info));
        }

        let schema = Arc::new(RwLock::new(Schema::new(schema)));
        let rows = Arc::new(RwLock::new(Vec::with_capacity(n_columns)));
        let index = Index::new();

        let mut is_indexed = true;
        if primary_key_columns.len() == 0 {
            // a fail-safe to assume some form of key to run non-indexed searches
            primary_key_columns.push(0);

            warn!("warn: no index; manual indexing is not available yet so searches may be slower");
            is_indexed = false;
        }

        Ok(Table {
            name,
            schema,
            rows,
            primary_key_columns,
            is_indexed,
            index,
        })
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn insert(&mut self, data: Vec<String>) -> Result<Row, String> {
        //! Basic insert function that inserts a row of values by matching their data-
        //! types and nullability.
        //!
        //! Returns a [Result<Row, String>] containing a copy of the row inserted.

        let row = self._validate_data(data)?;
        let mut rows = self.rows.write().unwrap();
        let row_index = rows.len();

        if self.is_indexed {
            self.index
                .insert(self._create_index_key_from_row(&row)?, row_index);
        }

        rows.push(row.clone());
        Ok(row)
    }

    pub fn insert_many(&mut self, values: Vec<Vec<String>>) -> Result<usize, String> {
        //! Bulk insert operation, uses the same insert function inside it.
        //!
        //! Returns the total number of successful entries
        //!
        //! Insertion is not transactional! Error during insertion stops the
        //! insertions after it, but keeps the ones prior.
        //!
        //! In the future, multi-threading may help speed up the working of
        //! this function.

        let mut n_insertions = 0;

        for value in values {
            self.insert(value)?;
            n_insertions += 1;
        }

        Ok(n_insertions)
    }

    pub fn update(
        &mut self,
        pk: Vec<&str>,
        updates: &HashMap<String, String>,
    ) -> Result<usize, String> {
        //! Update specific columns of a row of a table from its primary key.
        //!
        //! Returns a boolean for the number of columns updated.

        self._validate_pk(&pk)?;
        let row_index = self._find_row(pk).unwrap();

        let mut rows = self.rows.write().unwrap();
        let row: &mut Vec<Option<String>> = rows.get_mut(row_index).unwrap().0.as_mut();
        let mut col_updated = 0;

        let schema = self.schema.read().unwrap();
        for (col_name, col_data) in updates {
            let index = schema
                .get_vec()
                .iter()
                .position(|(s_key, _)| col_name == s_key)
                .ok_or_else(|| format!("unexpected {}: no such column exists", col_name))?;

            let (_, col_info) = schema.get(index).expect("err: invalid index");

            let validated_value =
                self._validate_field(col_data.to_string(), &col_name, col_info)?;

            row[index] = validated_value;
            col_updated += 1;
        }

        Ok(col_updated)
    }

    pub fn filter_rows(
        &self,
        filter: Box<dyn Fn(&Row) -> bool>,
    ) -> Result<Vec<Vec<String>>, String> {
        //! Apply a filter to obtain qualifying rows.
        //!
        //! Returns the primary keys of these rows to lookup and validate
        //! constraints before updating the table.

        let pks: Vec<Vec<String>> = {
            let rows = self.rows.read().unwrap();
            rows.iter()
                .filter(|row| filter(*row))
                .map(|row| {
                    self._extract_pk_values(row)
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                })
                .collect()
        };

        Ok(pks)
    }

    pub fn update_all(&mut self, updates: &HashMap<String, String>) -> Result<usize, String> {
        //! A non-atomic update for applying an update across the table if no filter
        //! is provided.
        //!
        //! Returns the total number of updated rows, which should always be the size
        //! of the table (total rows in it).

        let pks: Vec<Vec<String>> = {
            let rows = self.rows.read().unwrap();
            rows.iter()
                .map(|row| {
                    self._extract_pk_values(row)
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                })
                .collect()
        };

        let mut updated_row_count = 0;
        for pk in pks {
            self.update(pk.iter().map(|s| s.as_str()).collect(), updates)?;
            updated_row_count += 1;
        }

        Ok(updated_row_count)
    }

    pub fn delete(&mut self, pk: Vec<&str>) -> Result<Row, String> {
        //! A simple delete operation by the `pk`.
        //!
        //! Looks for the exact index inside the index to get to the row.
        //! If the table supports indexing, then the index is also
        //! reconstructed to remove the empty space from row deletion.
        //!
        //! Returns a snapshot of the deleted row.

        self._validate_pk(&pk)?;

        let key = pk.join("|");
        match self._find_row(pk) {
            Some(index) => {
                let mut rows = self.rows.write().unwrap();
                let deleted_row = rows.remove(index);

                if self.is_indexed {
                    self.index.remove(key.as_str());
                    self.index.shift_index_back(index);
                }

                Ok(deleted_row)
            }
            None => Err("err: invalid key; no match for this index".to_string()),
        }
    }

    pub fn delete_with_filter(
        &mut self,
        filter: Box<dyn Fn(&Row) -> bool>,
    ) -> Result<usize, String> {
        let pks: Vec<Vec<String>> = {
            let rows = self.rows.read().unwrap();
            rows.iter()
                .filter(|row| filter(*row))
                .map(|row| {
                    self._extract_pk_values(row)
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                })
                .collect()
        };

        let mut updated_row_count = 0;
        for pk in pks {
            self.delete(pk.iter().map(|s| s.as_str()).collect())?;
            updated_row_count += 1;
        }

        Ok(updated_row_count)
    }

    pub fn delete_all(&mut self) -> usize {
        //! Clears all rows of the table.
        //!
        //! Returns the total number of deleted rows, the size of the table.

        let mut rows = self.rows.write().unwrap();
        let row_count = rows.iter().count();
        rows.clear();

        row_count
    }

    pub fn reader(&self) -> TableReader {
        //! Get a reader for the table to perform read queries.
        //!
        //! Creates asynchronous copies of the schema and rows so
        //! multiple reads can be performed also enabling a locked
        //! write.

        TableReader {
            schema: Arc::clone(&self.schema),
            rows: Arc::clone(&self.rows),
        }
    }

    pub(crate) fn update_foreign_key_index(&mut self, schema_index: usize, key_index: usize) {
        let mut schema = self.schema.write().unwrap();
        schema.update_foreign_key_index(schema_index, key_index);
    }

    pub fn perform_aggregate(&self, func_vec: &Vec<SelectColumn>) -> Result<TableReader, String> {
        //! Perform all aggregate functions, create a single row [`TableReader`]
        //! for showing results.
        //!
        //! Note that this only returns one row and therefore it is inside the
        //! Table module itself. For functions that are performed on a single row
        //! and appended, see [`TableReader::perform_function`].
        //!
        //! # Intended Flow
        //!
        //! The function takes in the entire list of aggregates i.e. [`Vec<SelectColumn>`] and
        //! then iterates over each to perform the function. During the performing of the
        //! aggregator, it should pass a read only handle of itself, or the rows, and the
        //! schema.

        let mut result = TableReader::new();

        let rows = {
            let _rl = self.rows.read().unwrap();
            let _r = _rl.clone();
            _r
        };

        for aggr in func_vec.iter() {
            if let SelectColumn::Function {
                name, args, alias, ..
            } = aggr
            {
                let mut aggr_args = vec![];

                for arg in args.iter() {
                    match arg {
                        FunctionArg::Wildcard => {
                            // Process the command for all columns, no distinction
                            aggr_args.push("*".to_string());
                        }
                        FunctionArg::Column(column) => {
                            // Process the command for a set of columns
                            let col_index = {
                                let _s = self.schema.read().unwrap();
                                _s.get_vec()
                                    .iter()
                                    .position(|(col_name, _)| col_name == column)
                            }
                            .unwrap();

                            aggr_args.push(col_index.to_string());
                        }
                    }
                }

                let aggr_value = aggregators::run(name, &aggr_args, &rows)?;
                result = result.add_column(
                    (
                        alias.clone().unwrap_or(name.clone()),
                        ColumnInformation::default(),
                    ),
                    aggr_value,
                );
            }
        }

        Ok(result)
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        let rows: Vec<String> = self
            .rows
            .read()
            .unwrap()
            .iter()
            .map(|row| format!("{}", row))
            .collect();
        let schema = self.schema.read().unwrap();

        writeln!(f, "{}", "=".repeat(name.len() + 10))
            .and_then(|()| writeln!(f, "Table: {}", name))
            .and_then(|()| writeln!(f, "{}", "=".repeat(name.len() + 10)))
            .and_then(|()| writeln!(f, "{}\n{}", schema, rows.join("\n")))
    }
}

impl TableReader {
    pub fn new() -> TableReader {
        let schema = RwLock::new(Schema::new(Vec::new()));
        let rows = RwLock::new(Vec::<Row>::new());

        TableReader {
            schema: Arc::new(schema),
            rows: Arc::new(rows),
        }
    }

    pub fn add_column(
        self,
        (col_name, col_info): (String, ColumnInformation),
        value: String,
    ) -> TableReader {
        //! Used by the aggregator to add a column to its temporary table.

        let mut schema = self.schema.write().unwrap();
        let mut rows = self.rows.write().unwrap();

        println!("Adding {} to result row.", value);

        schema.get_vec_mut().push((col_name, col_info));

        if rows.len() == 0 {
            rows.push(Row(vec![]));
        }

        for row in rows.iter_mut() {
            row.0.push(Some(value.clone()));
        }

        TableReader {
            schema: self.schema.clone(),
            rows: self.rows.clone(),
        }
    }

    pub fn add_column_scalar(
        self,
        (col_name, col_info): (String, ColumnInformation),
        scalar: fn(&Vec<String>, &Row) -> Result<String, String>,
        args: &Vec<String>,
    ) -> TableReader {
        //! Used by the scalar to add a column to its temporary table.
        //!
        //! Takes a pointer to the scalar function and applies it over all
        //! rows.

        // TODO: FIX THIS. FIND A WAY TO RUN FUNCTIONS AND OBTAIN VALUES TO ADD.

        let mut schema = self.schema.write().unwrap();
        let mut rows = self.rows.write().unwrap();

        schema.get_vec_mut().push((col_name, col_info));

        for row in rows.iter_mut() {
            let value = scalar(args, row);
            row.0.push(Some(value.unwrap()));
        }

        TableReader {
            schema: self.schema.clone(),
            rows: self.rows.clone(),
        }
    }

    pub fn scan(&self) -> Vec<Row> {
        //! Returns a copy of all the rows of the table, so the read is not locked anymore.

        let rows = self.rows.read().unwrap();
        rows.clone()
    }

    pub fn filter<F>(self, filter: F) -> Result<TableReader, String>
    where
        F: Fn(&Row) -> bool,
    {
        //! Runs a filter over the read only rows and clones the ones matching
        //! the filter criteria
        //!
        //! Returns a [Clone] of the matching rows in the original table.

        let rows = self.rows.read().unwrap();
        let rows = rows.iter().filter(|row| filter(*row)).cloned().collect();

        Ok(TableReader {
            schema: self.schema,
            rows: Arc::new(RwLock::new(rows)),
        })
    }

    pub fn select(self, fields: Vec<String>) -> Result<TableReader, String> {
        //! Get specific columns from the table and return that table.
        //!
        //! Returns a table [`TableReader`] object as a projection of the current
        //! reader.

        let schema = self.schema.read().unwrap();

        let indices: Vec<usize> = fields
            .iter()
            .map(|field| {
                schema
                    .get_vec()
                    .iter()
                    .position(|(name, _)| name == field)
                    .expect(format!("invalid column {}: does not exist", field).as_str())
            })
            .collect();

        // TODO: Include alias into the schema, replace the original name, if possible.
        let new_schema: Schema = Schema::new(
            indices
                .iter()
                .map(|&index| {
                    schema
                        .get(index)
                        .expect("err: invalid schema entry")
                        .clone()
                })
                .collect(),
        );

        let rows = self.rows.read().unwrap();
        let rows = rows
            .iter()
            .map(|row| Row(indices.iter().map(|&index| row.0[index].clone()).collect()))
            .collect();

        Ok(TableReader {
            schema: Arc::new(RwLock::new(new_schema)),
            rows: Arc::new(RwLock::new(rows)),
        })
    }

    pub fn order_by(self, sort_index: Vec<(usize, bool)>) -> TableReader {
        //! Order the table by the index, in ascending or descending order.
        //!
        //! Takes in the sort index which is the index of the column, and the ascending
        //! order flag.
        //!
        //! Returns a new [`TableReader`] with sorted rows, consuming itself.

        let mut rows = {
            let _rl = self.rows.write().unwrap();
            _rl.clone()
        };

        rows.sort_by(|a, b| {
            let mut ordering = std::cmp::Ordering::Equal;

            for (col_index, is_ascending) in sort_index.iter() {
                let cmp = a.0.get(*col_index).cmp(&b.0.get(*col_index));
                ordering = if *is_ascending { cmp } else { cmp.reverse() };

                if ordering != std::cmp::Ordering::Equal {
                    break;
                }
            }

            ordering
        });

        return TableReader {
            schema: self.schema,
            rows: Arc::new(RwLock::new(rows)),
        };
    }

    pub fn limit(self, rows: Option<usize>) -> Result<TableReader, String> {
        //! Select only a portion of the resulting data.
        //!
        //! Returns the said number of rows from the table as a new
        //! [`TableReader`] object.
        if rows.is_none() {
            Ok(self)
        } else {
            let limited_rows = {
                let _rl = self.rows.read().unwrap();
                let _r = _rl.get(..rows.unwrap().min(_rl.len())).unwrap();
                _r.to_vec()
            };

            Ok(TableReader {
                schema: self.schema,
                rows: Arc::new(RwLock::new(limited_rows)),
            })
        }
    }

    pub fn offset(self, rows: Option<usize>) -> Result<TableReader, String> {
        //! Jump the first number of rows and start capturing next
        //! rows.
        //!
        //! Returns a new [`TableReader`] with the remainder of
        //! selected rows.

        if rows.is_none() {
            Ok(self)
        } else {
            let offsetted_rows = {
                let _rl = self.rows.read().unwrap();
                let _r = _rl.get(rows.unwrap().min(_rl.len())..).unwrap();
                _r.to_vec()
            };

            Ok(TableReader {
                schema: self.schema,
                rows: Arc::new(RwLock::new(offsetted_rows)),
            })
        }
    }

    pub fn perform_function(self, func_vec: &Vec<SelectColumn>) -> Result<TableReader, String> {
        //! ~Takes the `name` for the column name, an `alias` for custom display names, if specified
        //! and the `args` which are either column names or a wildcard.~
        //!
        //! Takes the list of all functions to apply, then applies them one after the other.
        //!
        //! Performs function on a particular argument column of the table and appends returns the
        //! rows with data appended.

        let schema = self.schema.clone();
        let mut result = self;

        for sclr in func_vec.iter() {
            if let SelectColumn::Function {
                name, args, alias, ..
            } = sclr
            {
                let mut sclr_args = vec![];

                for (index, arg) in args.iter().enumerate() {
                    match arg {
                        FunctionArg::Wildcard => {
                            // Process the command for all columns, no distinction
                            return Err(format!(
                                "Invalid {}; wildcard not allowed inside scalars.",
                                name
                            ));
                        }
                        FunctionArg::Column(column) => {
                            // Process the command for a set of columns
                            if index == 0 {
                                let col_index = {
                                    let _s = schema.read().unwrap();
                                    _s.get_vec()
                                        .iter()
                                        .position(|(col_name, _)| col_name == column)
                                }
                                .ok_or_else(|| {
                                    format!("Column {} does not exist. Select it first.", &column)
                                })?;

                                sclr_args.push(col_index.to_string());
                            } else {
                                sclr_args.push(column.clone());
                            }
                        }
                    }
                }

                result = result.add_column_scalar(
                    (
                        alias.clone().unwrap_or(name.clone()),
                        ColumnInformation::default(),
                    ),
                    scalars::get_runner(name).unwrap(),
                    &sclr_args,
                );
            }
        }

        Ok(result)
    }
}

impl Display for TableReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows: Vec<String> = self
            .rows
            .read()
            .unwrap()
            .iter()
            .map(|row| format!("{}", row))
            .collect();
        let schema = self.schema.read().unwrap();
        let schema_str = schema.to_string();

        writeln!(f, "+-{}-+", "-".repeat(schema_str.len()))?;
        writeln!(f, "| {} |", schema_str)?;
        writeln!(f, "+-{}-+", "-".repeat(schema_str.len()))?;
        write!(f, "{}", rows.join(""))?;
        writeln!(f, "+-{}-+", "-".repeat(schema_str.len()))
    }
}
