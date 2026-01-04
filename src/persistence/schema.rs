use std::fmt::Display;

use crate::persistence::index::ForeignKeyConstraint;

#[derive(Clone)]
pub enum DataType {
    Number,
    Text,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let datatype = match self {
            DataType::Number => "NUM",
            DataType::Text => "TXT",
        };
        write!(f, "{}", datatype)
    }
}

#[derive(Clone)]
pub struct ColumnInformation {
    pub(super) datatype: DataType,
    pub(super) max_limit: Option<usize>,
    pub(super) nullable: bool,
    pub(super) foreign_key_constraint: Option<ForeignKeyConstraint>,
}

impl ColumnInformation {
    pub fn from(datatype: DataType, max_limit: Option<usize>, nullable: bool) -> ColumnInformation {
        ColumnInformation {
            datatype,
            max_limit,
            nullable,
            foreign_key_constraint: None,
        }
    }
}

pub struct Schema(Vec<(String, ColumnInformation)>);

impl Schema {
    pub fn new(schema: Vec<(String, ColumnInformation)>) -> Schema {
        //! Create a schema from a vector of column names and its associated
        //! [`ColumnInformation`]s

        Schema(schema)
    }

    pub fn get(&self, index: usize) -> Option<&(String, ColumnInformation)> {
        //! Get schema column name and its information at the `index`.

        self.0.get(index)
    }

    pub fn get_vec(&self) -> &Vec<(String, ColumnInformation)> {
        //! Get the 0 attribute as a read-only reference.
        //!
        //! Returns the reference to the vector with column name and
        //! column information.

        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn get_foreign_key_constraints(&self) -> Vec<(usize, ForeignKeyConstraint)> {
        //! Get all the non-none fk constraints.
        //!
        //! Returns a vector of [`super::index::ForeignKeyConstraint`]s, in order, ignoring those that
        //! are `None`.

        self.0
            .iter()
            .enumerate()
            .filter_map(|(index, (_, info))| {
                if info.foreign_key_constraint.is_some() {
                    Some((index, info.foreign_key_constraint.clone().unwrap()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn get_foreign_key_constraint(
        &self,
        column_name: &str,
    ) -> Option<ForeignKeyConstraint> {
        //! Get fk constraint for the said column name.
        //!
        //! Returns a vector of [`super::index::ForeignKeyConstraint`]s, in order, ignoring those that
        //! are `None`.

        self.0.iter().find_map(|(name, info)| {
            if name == column_name {
                info.foreign_key_constraint.clone()
            } else {
                None
            }
        })
    }

    pub(crate) fn update_foreign_key_index(&mut self, schema_index: usize, key_index: usize) {
        if let Some((_, col_info)) = self.0.get_mut(schema_index) {
            col_info
                .foreign_key_constraint
                .as_mut()
                .unwrap()
                .update_index(key_index);
        }
    }

    pub fn get_vec_mut(&mut self) -> &mut Vec<(String, ColumnInformation)> {
        //! Get the 0 attribute as a read-only reference.
        //!
        //! Returns the reference to the vector with column name and
        //! column information.

        self.0.as_mut()
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let schema: Vec<String> = self
            .0
            .iter()
            .map(|(col, info)| format!("{} ({})", col.as_str(), info.datatype))
            .collect();
        write!(f, "{}", schema.join(" | "))
    }
}
