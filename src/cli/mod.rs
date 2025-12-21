use std::io::{self, Write};

use colored::Colorize;

use crate::cli::colors::FERRUM_RED;

mod colors;
mod commands;
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
        "[{}] Use '{}' to quit and '{}' to know all commands available.",
        "help".color(FERRUM_RED).bold(),
        "corrode".color(FERRUM_RED),
        "help".color(FERRUM_RED),
    );

    loop {
        print!("{}> ", "ferrum".color(FERRUM_RED).bold());
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
