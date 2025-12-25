//! General message formatting functions for prettifying the CLI.
//! Includes basic utility functions such as:
//!
//! - Highlight Text (make the text ferrum red but not bold)
//! - Highlight Text Hard (make the text ferrum red and bold)
//! - System message formatting functions that produce the same
//! format messages.

use colored::Colorize;

use crate::cli::colors::FERRUM_RED;

pub fn highlight_argument(argument: &str) -> String {
    //! Highlight a piece of text in the ferrum red
    //! color to make it obvious.
    //!
    //! Returns a formatted string.

    format!("{}", argument.color(FERRUM_RED))
}

pub fn system_message(source_name: &str, message: String) -> String {
    //! Write a system message on the command line, properly
    //! formatted, according to the command line theme.
    //!
    //! Takes in a source name (like 'system') as [`String`] and
    //! the message as a formatted text; output of [`format!`].

    let source_formatted = format!("{:6}", source_name.color(FERRUM_RED).bold());

    let message = format!("[{}] {}", source_formatted, message);
    message
}
