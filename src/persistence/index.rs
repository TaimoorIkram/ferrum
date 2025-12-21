use std::collections::HashMap;

/// The basic types of key linkages allowed between records.
/// [Key::PrimaryKey] is an indicator for the
/// [Key::ForeignKey] contains tracking features for the column so as to
/// bind to the column of the other table.
///
/// # Issues
/// - PrimaryKey is merely a signal for now, but may get tracking features
/// for cascading deletions and updates in the future
pub(crate) enum Key {
    PrimaryKey,
    ForeignKey(String, String),
}

/// A simple foreign key constraint, that will be returned and saved in
/// the [super::schema::Schema]'s [super::schema::ColumnInformation].
#[derive(Clone)]
pub(crate) struct ForeignKeyConstraint {
    pub(crate) table_name: String,
    pub(crate) column_name: String,
    column_index: Option<usize>,
}

/// A simple index implementation to find the rows by primary key quickly.
///
/// Composite keys are concatenated with a separator.
///
/// # Issues
/// - The index is NOT multi-thread compatible. This means there is a grave danger
/// that the data becomes corrupted upon running in multi-threaded mode!
/// - Index management to allow more than one indexes to be created for a [super::Table],
/// making more efficient searching possible on different column combinations.
pub(crate) struct Index {
    key_index_map: HashMap<String, usize>,
}

impl Index {
    pub fn new() -> Index {
        let key_index_map = HashMap::new();
        Index { key_index_map }
    }

    pub fn insert(&mut self, key: String, index: usize) {
        self.key_index_map.insert(key, index);
    }

    pub fn get(&self, key: &str) -> Option<usize> {
        self.key_index_map.get(key).copied()
    }

    pub fn remove(&mut self, key: &str) -> Option<usize> {
        self.key_index_map.remove(key)
    }

    pub fn shift_index_back(&mut self, start_index: usize) {
        //! Re-shape the index so as to remove the empty space in
        //! the index from a deleted row in the `rows` vector of the
        //! [super::Table].

        for row_index in self.key_index_map.values_mut() {
            if *row_index > start_index {
                *row_index -= 1;
            }
        }
    }
}

impl ForeignKeyConstraint {
    pub(crate) fn update_index(&mut self, index: usize) {
        self.column_index = Some(index);
    }

    pub fn new(table_name: String, column_name: String) -> ForeignKeyConstraint {
        ForeignKeyConstraint {
            table_name,
            column_name,
            column_index: None,
        }
    }
}
