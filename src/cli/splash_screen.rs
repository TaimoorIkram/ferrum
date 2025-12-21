//! The module contains function for displaying CLI splash screen.
//!
//! The list of things that I intend to implement on the CLI side
//! include or intend to include:
//! - Title
//! - Short Description
//! - Version Information

use colored::*;

use crate::cli::colors::FERRUM_RED;

pub fn splash_screen() {
    show_splash_screen();
    show_version_info();
}

fn show_splash_screen() {
    print!(
        r#"
    {}                               
        "#,
        r"
    ███████╗███████╗██████╗ ██████╗ ██╗   ██╗███╗   ███╗
    ██╔════╝██╔════╝██╔══██╗██╔══██╗██║   ██║████╗ ████║
    █████╗  █████╗  ██████╔╝██████╔╝██║   ██║██╔████╔██║
    ██╔══╝  ██╔══╝  ██╔══██╗██╔══██╗██║   ██║██║╚██╔╝██║
    ██║     ███████╗██║  ██║██║  ██║╚██████╔╝██║ ╚═╝ ██║
    ╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝                             
        "
        .color(FERRUM_RED)
    )
}

fn show_version_info() {
    println!(
        r"
    {}
    
    Version {}
    Authored by {}
        ",
        env!("CARGO_PKG_DESCRIPTION").color(FERRUM_RED),
        env!("CARGO_PKG_VERSION").color(FERRUM_RED).italic(),
        env!("CARGO_PKG_AUTHORS").color(FERRUM_RED).italic(),
    )
}
