//! The place where CLI and SQL parsers are defined.
//!
//! When the functionality becomes extensive, they will
//! each have their own files.

use clap::{Parser, ValueEnum, arg, command};
use sqlparser::{ast::Statement, dialect::Dialect, parser};

use crate::{
    cli::messages::{highlight_argument, system_message}, logging::{log_debug, log_error},
};

#[derive(Parser)]
#[command(name = "ferrum")]
#[command(about = "A Rust-powered relational database", long_about = None)]
pub struct CliParser {
    // Either operate in the client or server mode.
    #[arg(required = true)]
    pub mode: Option<CliMode>,
}

#[derive(Clone, ValueEnum)]
pub enum CliMode {
    // Start a deployment that listens for requests.
    Server,

    // Start a REPL client instance (no-remote).
    Client,
}

/// An SQL parser that performs the parsing and execution of the SQL
/// statements.
///
/// For now, it only supports a single dialect, but in future, may support
/// multiple ones.
pub struct SqlParser {
    dialect: Box<dyn Dialect>,
}

impl SqlParser {
    pub fn new(dialect: Box<dyn Dialect>) -> SqlParser {
        SqlParser { dialect: dialect }
    }

    pub fn parse_sql(&self, statement: &str) -> Result<Vec<Statement>, String> {
        //! Parse one or more SQL queries at once.
        //!
        //! Returns an AST of statements.

        let ast = parser::Parser::parse_sql(self.dialect.as_ref(), statement);
        log_debug(&format!("{:#?}", ast));

        ast.map_err(|e| {
            log_error(&format!("Error parsing query: {}", e.to_string().as_str()));
            system_message(
                "parser",
                format!(
                    "Error parsing query: {}",
                    highlight_argument(e.to_string().as_str())
                ),
            )
        })
    }

    pub fn parse_single_sql(&self, statement: &str) -> Result<Statement, String> {
        //! Parse only one SQL query at once.
        //!
        //! Returns an AST of the statement.

        let mut statements = self.parse_sql(statement)?;

        if statements.len() > 1 {
            log_error("More than one SQL statements entered at a time.");
            Err(system_message(
                "parser",
                "Please write a single statement at a time.".to_string(),
            ))
        } else {
            Ok(statements.remove(0))
        }
    }
}
