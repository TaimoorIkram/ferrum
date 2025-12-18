use std::collections::HashMap;

pub(crate) enum Key {
    PrimaryKey,
    ForeignKey,
}

/// A simple index implementation to find the rows by primary key quickly.
///
/// Composite keys are concatenated with a separator.
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

    pub fn shift_index_back(&mut self) {
        //! TODO: impl after [`super::table::Table::delete`] on [`super::table::Table`]
    }
}
