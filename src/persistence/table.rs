use super::row::Row;
use super::schema::{ColumnInformation, DataType, Schema};

use std::fmt::Display;
use std::sync::{Arc, RwLock};

pub struct Table {
    schema: Arc<Schema>,
    rows: Arc<RwLock<Vec<Row>>>,
}

pub struct TableReader {
    pub schema: Arc<Schema>,
    pub rows: Arc<RwLock<Vec<Row>>>,
}

impl Table {
    // TODO: try improving this guy
    fn _validate_data(&self, data: Vec<String>) -> Result<Row, String> {
        //! Validate the row with respect to the schema.
        //!
        //! Returns the row if the data is correct.

        if data.len() != self.schema.0.len() {
            return Err(format!(
                "invalid data: schema has {} column(s), but {} were provided",
                self.schema.0.len(),
                data.len(),
            ));
        }

        let mut row: Vec<Option<String>> = Vec::new();

        for (item, (col_name, col_info)) in data.into_iter().zip(&self.schema.0) {
            if item.is_empty() && col_info.nullable {
                row.push(None);
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
                row.push(Some(item));
            }
        }

        Ok(Row(row))
    }

    pub fn from(columns: Vec<(String, String)>) -> Result<Table, String> {
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
        let n_columns = columns.len();

        for (column, datatype) in columns.iter() {
            let col_info = match datatype.as_str() {
                "num" => ColumnInformation::from(DataType::Number, None, false),
                "txt" => ColumnInformation::from(DataType::Text, Some(50), false),
                other => {
                    return Err(format!(
                        "invalid datatype {}: not supported, on column {}",
                        other, column
                    ));
                }
            };
            schema.push((column.clone(), col_info));
        }

        let schema = Arc::new(Schema(schema));
        let rows = Arc::new(RwLock::new(Vec::with_capacity(n_columns)));

        Ok(Table { schema, rows })
    }

    pub fn insert(&self, data: Vec<String>) -> Result<Row, String> {
        //! Basic insert function that inserts a row of values by matching their data-
        //! types and nullability.
        //!
        //! Returns a [Result<Row, String>] containing a copy of the row inserted.

        let row = self._validate_data(data)?;
        self.rows.write().unwrap().push(row.clone());
        Ok(row)
    }

    pub fn insert_many(&self, values: Vec<Vec<String>>) -> Result<usize, String> {
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
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rows: Vec<String> = self
            .rows
            .read()
            .unwrap()
            .iter()
            .map(|row| format!("{}", row))
            .collect();

        writeln!(f, "{}\n{}", self.schema, rows.join("\n"))
    }
}

impl TableReader {
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

        let indices: Vec<usize> = fields
            .iter()
            .map(|field| {
                self.schema
                    .0
                    .iter()
                    .position(|(name, _)| name == field)
                    .expect(format!("invalid column {}: does not exist", field).as_str())
            })
            .collect();

        let schema: Schema = Schema(
            indices
                .iter()
                .map(|&index| self.schema.0[index].clone())
                .collect(),
        );

        let rows = self.rows.read().unwrap();
        let rows = rows
            .iter()
            .map(|row| Row(indices.iter().map(|&index| row.0[index].clone()).collect()))
            .collect();

        Ok(TableReader {
            schema: Arc::new(schema),
            rows: Arc::new(RwLock::new(rows)),
        })
    }
}
