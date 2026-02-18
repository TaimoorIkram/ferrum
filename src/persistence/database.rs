use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use indexmap::IndexMap;

use crate::persistence::{Row, index::ForeignKeyConstraint};

use super::table::Table;

/// The collective of multiple [`Table`] objects.
///
/// A [`Database`] object is responsible for managing all the internal handling.
/// Currently, I have provided a simple implementation for single-threaded mode.
///
/// This is the smart class that does all the needed work of verifying integrity of
/// data and then inserting it into the table as needed. [`Table`] is the dumb class
/// that only knows how to feed the data into itself and verify the contents of the
/// data before doing so.
///
/// In the future maybe the following form of querying is available to be used
/// to query the table
/// - `+:table_name:[(<col> <type> <pk?>,)*]:[(<other_table>.<col> <col?> <on_del> <on_upd>,)*]`
///
/// Currently, the table DOES NOT support constraints.
///
/// # Issues
/// - How does the database handle the table, in multi-threaded scenarios?
/// - Exporting and restoring a database from file into memory. How does the engine handle
/// brining an offline database into memory?
/// - Implementing basic constraints like on_delete and on_update.
/// - Implementing the basic constraint resolution methods like cascade, set_null and do_nothing.
pub struct Database {
    name: String,
    tables: HashMap<String, Arc<RwLock<Table>>>,
}

/// A single place to store all databases.
///
/// A [`DatabaseRegistry`] controls and provides connections to sessions to mutate the database
/// and provides utilities to check if the databse exists in the registry already or not.
///
/// Simply, it is also based on an [`IndexMap`] registry pattern. The order prevails and keys
/// will always appear in the same order.
///
/// An [`Arc<RwLock<Database>>`] keeps things simple to deal with, when providing sessions with
/// handles to these databases. Using a simple Database asks for trait derivation cascades down
/// to the [`Row`] which is much more effort than needed for a simple registry like this.
pub struct DatabaseRegistry {
    registry: IndexMap<String, Arc<RwLock<Database>>>,
}

impl Database {
    fn _validate_foreign_key_constraint(
        &self,
        constraint: &ForeignKeyConstraint,
    ) -> Result<usize, String> {
        let ForeignKeyConstraint {
            table_name,
            column_name,
            ..
        } = constraint;

        if let Some(table) = self.tables.get(table_name) {
            let table_ro = table.read().unwrap();
            let table_ro_schema = table_ro.schema.read().unwrap();

            if let Some(index) = table_ro_schema
                .get_vec()
                .iter()
                .position(|(col, _)| col == column_name)
            {
                Ok(index)
            } else {
                Err(format!(
                    "invalid foreign key on {}; column {} doesn't exist ",
                    table_name, column_name
                ))
            }
        } else {
            Err(format!(
                "invalid foreign key on {}; column {} doesn't exist ",
                table_name, column_name
            ))
        }
    }

    fn _validate_foreign_key(&self, table_name: &str, value: &String) -> Result<bool, String> {
        //! Validate the given key exists in the target table according to the defined foreign key
        //! relationship.

        let fk_table_ro = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("err: does not exist: table '{}'", table_name))?;
        Ok(fk_table_ro.read().unwrap().pk_exists(value))
    }

    pub fn new(name: String) -> Database {
        //! Create a new database with no tables.

        Database {
            name,
            tables: HashMap::new(),
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn create_table(
        &mut self,
        name: String,
        column_definitions: Vec<String>,
    ) -> Result<(), String> {
        //! Create a [`super::table::Table`] and store inside the database's hash map
        //! for quick retrieval and relationship management.

        let mut table = Table::new(name, column_definitions)?;
        let constraints = {
            let table = table.schema.read().unwrap();
            table.get_foreign_key_constraints()
        };

        for (column_index, constraint) in constraints {
            if let Ok(key_index) = self._validate_foreign_key_constraint(&constraint) {
                table.update_foreign_key_index(column_index, key_index);
            }
        }

        self.tables
            .insert(table.name(), Arc::new(RwLock::new(table)));

        Ok(())
    }

    pub fn insert_into_table(&mut self, table_name: &str, data: Vec<String>) -> Result<(), String> {
        //! Insert the `data` row into the table.
        //!
        //! - The function first reads through the table's schema to verify the foreign keys.
        //! - After all foreign keys have been checked, insertion takes place.
        //!
        //! # Issues
        //! - How will a null value be placed in place of the foreign key?

        let table = self.tables.get(table_name).unwrap();
        let constraints = {
            let table = table.read().unwrap();
            let schema = table.schema.read().unwrap();
            schema.get_foreign_key_constraints()
        };

        for (index, constraint) in constraints.iter() {
            let value = data
                .get(*index)
                .ok_or_else(|| format!("err: out of bound; index {}", *index))?;
            let table_name = &constraint.table_name;
            let column_name = &constraint.column_name;

            if !self._validate_foreign_key(table_name, value)? {
                return Err(format!(
                    "err: does not exist: `{}` in `{}.{}`",
                    value, table_name, column_name
                ));
            }
        }

        table.write().unwrap().insert(data)?;

        Ok(())
    }

    pub fn insert_many_into_table(
        &mut self,
        table_name: &str,
        rows: Vec<Vec<String>>,
    ) -> Result<usize, String> {
        //! Bulk insert operation, that uses the singular row insertion function
        //! under the hood.

        let mut n_insertions = 0;

        for row in rows {
            self.insert_into_table(table_name, row)?;
            n_insertions += 1;
        }

        Ok(n_insertions)
    }

    // TODO: Change this to a key validator before filters are applied.
    pub fn update_table_set(
        &mut self,
        table_name: &str,
        pk: Vec<&str>,
        data: &HashMap<String, String>,
    ) -> Result<usize, String> {
        //! Update the data in `pk` row to `data` and cascade changes.
        //!
        //! - The function first reads through the table's schema to verify new data.
        //! - If the foreign key is to be updated, then the key is checked as well
        //! against the schema.
        //! - After all data and foreign keys have been checked, updation takes place.
        //!
        //! # Issues
        //! - How does cascading effect take place after a successful update?

        let mut table = {
            if let Some(_t) = self.tables.get(table_name) {
                _t.write().unwrap()
            } else {
                return Err(format!("err: does not exist: table {}", table_name));
            }
        };

        for (column_name, value) in data.iter() {
            if let Some(constraint) = table
                .schema
                .read()
                .unwrap()
                .get_foreign_key_constraint(column_name)
            {
                if !self._validate_foreign_key(&constraint.table_name, &value)? {
                    return Err(format!(
                        "err: does not exist: key '{}' on table '{}'",
                        &value, &constraint.table_name
                    ));
                }
            }
        }

        table.update(pk, &data)
    }

    pub fn update_table_set_with_filters(
        &mut self,
        table_name: &str,
        filter: Option<Box<dyn Fn(&Row) -> bool>>,
        updates: HashMap<String, String>,
    ) -> Result<usize, String> {
        let mut updated_row_count = 0;
        if let Some(filter) = filter {
            // Filter the data then validate the constraints and then make the insertion
            let filtered_pks = {
                let table = {
                    if let Some(_t) = self.tables.get(table_name) {
                        _t.write().unwrap()
                    } else {
                        return Err(format!("err: does not exist: table {}", table_name));
                    }
                };
                table.filter_rows(filter)?
            };

            for pk in filtered_pks {
                self.update_table_set(
                    table_name,
                    pk.iter().map(|s| s.as_str()).collect(),
                    &updates,
                )?;
                updated_row_count += 1;
            }
        } else {
            let mut table = {
                if let Some(_t) = self.tables.get_mut(table_name) {
                    _t.write().unwrap()
                } else {
                    return Err(format!("err: does not exist: table {}", table_name));
                }
            };

            updated_row_count = table.update_all(&updates)?;
        }

        Ok(updated_row_count)
    }

    pub fn delete_from_table_value(
        &mut self,
        table_name: &str,
        pk: Vec<&str>,
    ) -> Result<Row, String> {
        //! Delete the data in `pk` row and cascade changes.
        //!
        //! - Find the target row and remove it.
        //! - Update all associated foreign key linkages according to the definition
        //! of the constraints.
        //!
        //! # Issues
        //! - How does cascading effect take place after a successful delete?

        let mut table = {
            if let Some(_t) = self.tables.get_mut(table_name) {
                _t.write().unwrap()
            } else {
                return Err(format!("err: does not exist: table {}", table_name));
            }
        };

        table.delete(pk)
    }

    pub fn delete_from_table_values(
        &mut self,
        table_name: &str,
        pks: Vec<Vec<&str>>,
    ) -> Result<usize, String> {
        //! Perform more than one deletions on the same table.
        //!
        //! Returns the total number of processed rows.
        //! This is not atomic. Rows processed before error will not be reversed post-error.
        //!
        //! Issues
        //! - The way the values are currently handled could be made better but the strategy
        //! is not obvious for now

        let mut n_deleted = 0;

        for pk in pks {
            self.delete_from_table_value(table_name, pk)?;
            n_deleted += 1;
        }

        Ok(n_deleted)
    }

    pub fn delete_from_table_with_filter(
        &mut self,
        table_name: &str,
        filter: Option<Box<dyn Fn(&Row) -> bool>>,
    ) -> Result<usize, String> {
        //! Perform more than one deletions on the same table via filters.
        //!
        //! Returns the total number of processed rows.
        //! This is not atomic. Rows processed before error will not be reversed post-error.

        let table = self.get_table(table_name).unwrap();
        let mut table_ref = table.write().unwrap();

        let deleted_row_count;
        if let Some(filter) = filter {
            deleted_row_count = table_ref.delete_with_filter(filter)?;
        } else {
            deleted_row_count = table_ref.delete_all();
        }

        Ok(deleted_row_count)
    }

    pub fn get_table(&self, table_name: &str) -> Option<Arc<RwLock<Table>>> {
        let table = self.tables.get(table_name)?;
        Some(Arc::clone(table))
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn contains_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
}

impl DatabaseRegistry {
    pub fn new() -> DatabaseRegistry {
        DatabaseRegistry {
            registry: IndexMap::new(),
        }
    }

    pub fn exists(&self, db_name: &str) -> bool {
        //! Check if a database already exists in the registry.

        self.registry.contains_key(db_name)
    }

    pub fn create_database(
        &mut self,
        db_name: &str,
        if_not_exists: bool,
    ) -> Result<Arc<RwLock<Database>>, String> {
        //! Create a new database if it does not already exist.
        //!
        //! Throws an exception if a database with the same name already exists.

        let db = Arc::new(RwLock::new(Database::new(db_name.to_string())));

        if self.registry.contains_key(db_name) {
            if if_not_exists {
                self.get_database(db_name)
            } else {
                Err(format!(
                    "Integrity violation; database {} already exists",
                    db_name
                ))
            }
        } else {
            self.registry.insert(db_name.to_string(), db);
            let created_db = self
                .registry
                .get(db_name)
                .expect("Failed to create database.");

            Ok(Arc::clone(&created_db))
        }
    }

    pub fn get_database(&self, db_name: &str) -> Result<Arc<RwLock<Database>>, String> {
        let db = self
            .registry
            .get(db_name)
            .expect(format!("Database {} does not exist.", db_name).as_str());
        Ok(Arc::clone(db))
    }

    pub fn get_database_names(&self) -> Vec<String> {
        //! Get a list of all available databases in the registry.

        self.registry.keys().cloned().collect()
    }

    pub fn drop_database(&mut self, db_name: &str) -> Option<Arc<RwLock<Database>>> {
        //! Delete an existing database
        //! 
        //! The registry will FORCE a delete regardless of whether there are foreign key connections
        //! or not.
        //! 
        //! It will also ignore any RESTRICT in the command lines if such a statement is run.
        
        self.registry.shift_remove(db_name)
    }
}
