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
    sessions::session::Session,
};

mod colors;
mod commands;
mod messages;
pub mod parsers;
mod splash_screen;

pub use commands::FunctionArg;
pub use commands::SelectColumn;

const DEFAULT_LAST_COMMAND_DELIMITER: &str = "!";

const FERRUM_ENGINE_COMMANDS_LIST: [(&str, &str); 4] = [
    (
        "!",
        "execute the last command, add more to go further back",
    ),
    ("help", "list all available commands"),
    ("history", "list command history for this session"),
    (
        "corrode",
        "iron corrodes and so does this session when you exit",
    ),
];

pub fn run_client() {
    splash_screen::splash_screen();

    let session = Session::client();

    start_repl(session);
}

pub fn run_server() {
    println!("Mode server is not supported yet. Try 'client'.");
}

pub fn show_help() {
    println!(
        "{}",
        system_message(
            "info",
            format!(
                "Any other statements are considered {}.",
                highlight_argument("sql statements")
            )
        )
    );

    println!();
    println!("{:10} {}", "COMMAND".color(FERRUM_RED), "DETAILS");
    for (command, details) in FERRUM_ENGINE_COMMANDS_LIST {
        println!("{:10} {}", command.color(FERRUM_RED), details)
    }
}

fn start_repl(mut session: Session) {
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

    let tmp_db = Arc::new(RwLock::new(Database::new("cli_user_database".to_string())));
    session.use_database(&tmp_db);

    let session_start_time = session.start_time_string();
    println!(
        "{}",
        system_message(
            "system",
            format!(
                "New session initiated at '{}'.",
                highlight_argument(&session_start_time)
            ),
        )
    );

    let database = session
        .get_active_database()
        .expect("Connection referred before it was assigned.");

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

        println!();
        print!("{:6} > ", "ferrum".color(FERRUM_RED).bold());
        io::stdout().flush().unwrap();

        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).unwrap();

        if buffer.starts_with(DEFAULT_LAST_COMMAND_DELIMITER) {
            let last = buffer.matches(DEFAULT_LAST_COMMAND_DELIMITER).count();
            let last_command = session.get_last_command(last);

            if last_command.is_none() {
                println!(
                    "{}",
                    system_message(
                        "system",
                        format!(
                            "No command {} steps back.",
                            highlight_argument(&last.to_string())
                        ),
                    )
                );
                continue;
            } else {
                buffer = last_command.unwrap().to_string();
            }
        }

        session.add_to_command_history(buffer.clone().trim());

        match buffer.trim() {
            "history" => session.show_command_history(None),
            "help" => show_help(),
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
                                            result.n_rows_processed.unwrap_or(0)
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

    println!("Goodbye!")
}
