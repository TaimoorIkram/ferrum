use std::io::{self, Write};

use colored::Colorize;

use crate::cli::{
    colors::FERRUM_RED,
    messages::{highlight_argument, system_message},
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
    system_message(
        "system",
        format!(
            "Use '{}' to quit and '{}' to know all commands available.",
            highlight_argument("corrode"),
            highlight_argument("help"),
        ),
    );

    loop {
        print!("{:6} > ", "ferrum".color(FERRUM_RED).bold());
        io::stdout().flush().unwrap();

        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).unwrap();

        match buffer.trim() {
            "help" => println!("mate... there's like, only 2 commands, use the other one to exit."),
            "exit" => println!("did you mean '{}'?", "corrode".color(FERRUM_RED)),
            "corrode" => break,
            other => println!("invalid command: {}", other),
        }
    }
}
