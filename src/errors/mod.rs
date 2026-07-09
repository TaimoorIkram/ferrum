use std::fmt::Display;

/// A central singular error type for the engine to handle errors.
///
/// This will be used in all places where Err(...) is being returned
/// at the low level executing functions, then propagated upwards to
/// use Display trait to convert each of these into readable messages.
///
/// The types of the enum are classified into the objects that can
/// produce errors in them when they are incorrectly interacted with.
/// 
/// To propagate an error from the code, follow the following order of
/// coding it into the project.
/// 1. Create a msg object containing the formatted message (variables
/// replaced into the {} markers already)
/// 2. Log the error via log_error(&msg). Be sure to give ref only.
/// 3. Send back an Err() enclosed FerrumError::Variant(msg). Be sure to
/// hand over the ownership of the error message to this final call in any
/// function.
pub(crate) enum FerrumError {
    DatabaseError(String), // database operations (use, create, drop, ...)
    TableError(String),    // table operations (create, update, drop, ...)
    FilterError(String),   // filter operations (inside joins and where clauses, ...)
    FunctionError(String), // scalar and aggregator related errors
    SchemaError(String),   // schema issues (key errors, invalid col infos, ...)
    IndexError(String),    // issues in the index (no pk, index lookup or create fail, ...)
    ParseError(String),    // error when parsing query to generate ast
    QueryError(String),    // error in query after parsing query to ast (while in executor, ...)
    FatalError(String),    // no point of return error (file corruption, ...)
}

pub(crate) type FerrumResult<T> = Result<T, FerrumError>;

impl Display for FerrumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseError(msg) => writeln!(f, "Database Error: '{}'", msg),
            Self::TableError(msg) => writeln!(f, "Table Error: '{}'", msg),
            Self::FilterError(msg) => writeln!(f, "Filter Error: '{}'", msg),
            Self::FunctionError(msg) => writeln!(f, "Function Error: '{}'", msg),
            Self::SchemaError(msg) => writeln!(f, "Schema Error: '{}'", msg),
            Self::IndexError(msg) => writeln!(f, "Index Error: '{}'", msg),
            Self::ParseError(msg) => writeln!(f, "Parse Error: '{}'", msg),
            Self::QueryError(msg) => writeln!(f, "Query Error: '{}'", msg),
            Self::FatalError(msg) => writeln!(f, "Fatal Error: '{}'", msg),
        }
    }
}

// LAST POINT OF CONTACT: Impelemnt Errors in Display and then integrate into existing Err(...)
// and update all return types to FerrumError.
