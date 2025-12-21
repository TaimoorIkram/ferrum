use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::persistence::index::ForeignKeyConstraint;

use super::table::Table;

/// The collective of multiple [super::Table] objects.
///
/// A [Database] object is responsible for managing all the internal handling.
/// Currently, I have provided a simple implementation for single-threaded mode.
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
        let constraints = table.schema.read().unwrap().get_foreign_key_constraints();

        for (column_index, constraint) in constraints {
            if let Ok(key_index) = self._validate_foreign_key_constraint(&constraint) {
                table.update_foreign_key_index(column_index, key_index);
            }
        }

        self.tables
            .insert(table.name(), Arc::new(RwLock::new(table)));

        Ok(())
    }

    pub fn insert_into_table(&mut self, table_name: &str, data: Vec<String>) {
        //! Insert the `data` row into the table.
        //!
        //! - The function first reads through the table's schema to verify the foreign keys.
        //! - After all foreign keys have been checked, insertion takes place.
        //!
        //! # Issues
        //! - How does cascading effect take place after a successful insert?
    }

    pub fn update_table_set(
        &mut self,
        table_name: &str,
        pk: Vec<&str>,
        data: HashMap<String, String>,
    ) {
        //! Update the data in `pk` row to `data` and cascade changes.
        //!
        //! - The function first reads through the table's schema to verify new data.
        //! - If the foreign key is to be updated, then the key is checked as well
        //! against the schema.
        //! - After all data and foreign keys have been checked, updation takes place.
        //!
        //! # Issues
        //! - How does cascading effect take place after a successful update?
    }

    pub fn delete_from_table_value(&mut self, table_name: &str, pk: Vec<&str>) {
        //! Delete the data in `pk` row and cascade changes.
        //!
        //! - Find the target row and remove it.
        //! - Update all associated foreign key linkages according to the definition
        //! of the constraints.
        //!
        //! # Issues
        //! - How does cascading effect take place after a successful update?
    }
}
