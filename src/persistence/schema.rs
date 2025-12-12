use std::fmt::Display;

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

pub struct ColumnInformation {
    pub(super) datatype: DataType,
    pub(super) max_limit: Option<usize>,
    pub(super) nullable: bool,
}

impl ColumnInformation {
    pub fn from(datatype: DataType, max_limit: Option<usize>, nullable: bool) -> ColumnInformation {
        ColumnInformation {
            datatype,
            max_limit,
            nullable,
        }
    }
}

pub struct Schema(pub Vec<(String, ColumnInformation)>);

impl Schema {
    pub fn at(&self, index: usize) -> &(String, ColumnInformation) {
        self.0.get(index).unwrap()
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
