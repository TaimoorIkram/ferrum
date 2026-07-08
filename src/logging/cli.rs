//! This file is a duplicate of the logging file located in ../cli/messages.rs
//! Will probably be removed
//! 
//! - FERRUM_RED: Main Color
//! 
//! Should we keep the cli utils open for lower end functions
//! to customize their messages?
//! 
//! This will be useful if the errors are to be designed to be
//! easier to read and customized to the kind of error message 
//! to be show?
//! 
//! Add a check for whether the code is running in server mode or 
//! client mode.
//! - Server mode means no text coloring to be applied.
//! - Client mode means CLI is being run so apply formatting.
//! 

use colored::Colorize;


use colored::Color;

pub(crate) const FERRUM_RED: Color = Color::TrueColor {
    r: 255,
    g: 87,
    b: 87,
};


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
