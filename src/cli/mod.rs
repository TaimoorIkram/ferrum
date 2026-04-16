use std::{
    io::{self, Write},
    path::Path,
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
    config::EngineConfig,
    persistence::DatabaseRegistry,
    serialization::{deserializers::load_registry, registry_exists, serializers::save_registry},
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
    ("!", "execute the last command, add more to go further back"),
    ("help", "list all available commands"),
    ("history", "list command history for this session"),
    (
        "corrode",
        "iron corrodes and so does this session when you exit",
    ),
];

pub fn run_client(config: &EngineConfig) {
    splash_screen::splash_screen();

    let _reg: DatabaseRegistry;
    let _reg_path = Path::new(config.registry_file_path.as_str());

    if registry_exists(_reg_path) {
        _reg = load_registry(_reg_path).unwrap();
        println!(
            "{}",
            system_message(
                "info",
                format!("A restored database registry was created at the session level.")
            )
        );
    } else {
        println!(
            "{}",
            system_message(
                "info",
                format!(
                    "Failed to find registry at '{}'.",
                    highlight_argument(_reg_path.to_str().unwrap())
                )
            )
        );
        _reg = DatabaseRegistry::new();
        println!(
            "{}",
            system_message(
                "info",
                format!("A default database registry was created at the session level.")
            )
        );
    }

    let registry = Arc::new(RwLock::new(_reg));
    let session = Arc::new(RwLock::new(Session::client(&registry)));

    start_repl(session);

    println!(
        "{}",
        system_message(
            "info",
            format!("Saving registry to '{}'.", _reg_path.to_str().unwrap())
        )
    );
    save_registry(&registry.read().unwrap().clone(), _reg_path).unwrap();
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

fn start_repl(client_session: Arc<RwLock<Session>>) {
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

    {
        let session = client_session.read().unwrap();
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
    }

    loop {
        let mut query_result: Option<SqlResult> = None;

        println!();
        print!("{:6} > ", "ferrum".color(FERRUM_RED).bold());
        io::stdout().flush().unwrap();

        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).unwrap();

        if buffer.starts_with(DEFAULT_LAST_COMMAND_DELIMITER) {
            let session = client_session.read().unwrap();
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

        {
            let mut session = client_session.write().unwrap();
            session.add_to_command_history(buffer.clone().trim());
        }

        match buffer.trim() {
            "history" => {
                let session = client_session.read().unwrap();
                session.show_command_history(None);
            }
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

                        let executor = SqlExecutor::new(statement, &client_session);
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
}
