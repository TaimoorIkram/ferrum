//! This module where all the commands are stored.
//! 
//! Ferrum command line syntax:
//! 
//! - ferrum --help | Command Line Help
//! - ferrum client | Run the CLI app.
//! - ferrum server | Run the server listener (soon).
//! 
//! Once the user is inside the REPL, SQL parser takes over. Here is an initial syntax for 
//! SQL queries that Ferrum will support.
//! 
//! - USE database;
//! - CREATE database;
//! - SELECT cols* FROM table;
//! - INSERT INTO table VALUES (values)*;
//! - CREATE TABLE table (schema*)
//! 
//! Here * means more than one such values separated by a comma.
