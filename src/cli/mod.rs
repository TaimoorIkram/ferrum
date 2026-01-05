use std::{
    io::{self, Write},
    sync::{Arc, RwLock},
};

use colored::Colorize;
use sqlparser::dialect::MySqlDialect;

use crate::{
    cli::{
        colors::FERRUM_RED,
        commands::{SqlExecutor, SqlResult},
        messages::{highlight_argument, system_message},
        parsers::SqlParser,
    },
    persistence::Database,
};

mod colors;
mod commands;
mod messages;
pub mod parsers;
mod splash_screen;

pub fn run_client() {
    splash_screen::splash_screen();
    start_repl();
}

pub fn run_server() {
    println!("Mode server is not supported yet. Try 'client'.");
}

fn start_repl() {
    println!(
        "{}",
        system_message(
            "system",
            format!(
                "Use '{}' to quit and '{}' to know all commands available.",
                highlight_argument("corrode"),
                highlight_argument("help"),
            ),
        )
    );

    let database = Arc::new(RwLock::new(Database::new("cli_user_database".to_string())));
    println!(
        "{}",
        system_message(
            "system",
            format!(
                "A new database '{}' was automatically created for the duration of this session.",
                highlight_argument("cli_user_database"),
            )
        )
    );

    loop {
        let mut query_result: Option<SqlResult> = None;

        print!("{:6} > ", "ferrum".color(FERRUM_RED).bold());
        io::stdout().flush().unwrap();

        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).unwrap();

        match buffer.trim() {
            "help" => println!(
                "{}",
                system_message(
                    "system",
                    format!(
                        "Use '{}' to quit. All other inputs to terminal are treated as {}.",
                        highlight_argument("corrode"),
                        highlight_argument("sql statements"),
                    ),
                )
            ),
            "exit" => println!("did you mean '{}'?", "corrode".color(FERRUM_RED)),
            "corrode" => break,
            sql => {
                let dialect = Box::new(MySqlDialect {});
                let parser = SqlParser::new(dialect);

                match parser.parse_single_sql(sql) {
                    Ok(statement) => {
                        println!(
                            "{}",
                            system_message(
                                "ferrum",
                                "The statement was parsed successfully!".to_string(),
                            )
                        );

                        let executor = SqlExecutor::new(statement, &database);
                        match executor.execute() {
                            Ok(result) => {
                                println!(
                                    "{}",
                                    system_message(
                                        "ferrum",
                                        format!(
                                            "{} row(s) processed!",
                                            result.n_rows_processed.unwrap()
                                        )
                                    )
                                );

                                query_result = Some(result);
                            }
                            Err(error) => println!("{}", error),
                        }
                    }
                    Err(error) => {
                        println!("{}", error);
                    }
                };
            }
        }

        if let Some(result) = query_result.take() {
            if let Some(table) = result.table {
                println!("{}", table)
            }
        }
    }
}
