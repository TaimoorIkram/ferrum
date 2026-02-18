//! Persistence as tables needs to have the following components
//! - Schema (mapping of column names to datatypes allowed, order is important)
//! - Row (based on a Schema, fixed per table, only one write and many reads)
//! - Table (made of many Rows, multi-threadable)
//!

//  All modules of this lib
mod database;
mod table;
mod row;
mod schema;
mod index;

//  External API
pub use database::{Database, DatabaseRegistry};
pub use table::{Table, TableReader};
pub use row::Row;