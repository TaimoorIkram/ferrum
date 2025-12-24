//! The place where CLI and SQL parsers are defined.
//! 
//! When the functionality becomes extensive, they will
//! each have their own files.

use clap::{Parser, command, arg, ValueEnum};
use sqlparser;

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
    Client
}

pub type SqlParser<'a> = sqlparser::parser::Parser<'a>;