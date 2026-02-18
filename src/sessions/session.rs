//! The only point of truth for all information that is related to the
//! current user session in the engine. A session will contain information
//! that is related to the command line session of the user. This includes
//! things like command history, currently active database connection,
//! current user if authentication is enabled, and config management for
//! the current user.
//!
//! # Modes
//! A single [`Session`] can be either in client mode or server mode. That
//! is the only difference for now between the two. Both have almost exactly
//! the same use case in either of the two modes.
//!
//! ## Client Session
//! A session in client mode will deal with client specific attributes. Things
//! like, but not exactly:
//! - command history
//! - event logs
//!
//! ## Server Session
//! A server session (reserved for later use) will allow a separate listener
//! thread to run inside the server thread pool. This will allow server-side
//! user session logging. Server specific attrivutes may include:
//! - client information
//!
//! At the end of each of these sessions, these objects will be discarded until
//! I decide to implement a session logging mechanism to store separately the
//! user data.

use std::{
    fmt::Display,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use chrono::{DateTime, Local};

use crate::persistence::{Database, DatabaseRegistry};

struct CommandHistory {
    command: String,
    command_time: SystemTime,
}

impl CommandHistory {
    pub fn command_time_string(&self) -> String {
        let datetime: DateTime<Local> = self.command_time.into();
        datetime.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

impl Display for CommandHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.command_time_string(), self.command)
    }
}

pub struct Session {
    command_history: Vec<CommandHistory>,
    start_time: SystemTime,
    active_database: Option<Arc<RwLock<Database>>>,
    database_registry: Arc<RwLock<DatabaseRegistry>>,
}

impl Session {
    pub fn client(db_reg: &Arc<RwLock<DatabaseRegistry>>) -> Session {
        //! Returns a new client session.

        Session {
            command_history: vec![],
            start_time: SystemTime::now(),
            active_database: None,
            database_registry: Arc::clone(db_reg),
        }
    }

    pub fn use_database(&mut self, db_name: &str) -> Result<(), String> {
        //! Set the currently active database connection for future
        //! querying.

        let db_reg = self.database_registry.read().unwrap();
        let db = db_reg.get_database(db_name)?;
        self.active_database = Some(db);
        Ok(())
    }

    pub fn get_active_database(&self) -> Option<Arc<RwLock<Database>>> {
        //! Get a reference to the currently active database connection,
        //! otherwise return [`None`]

        self.active_database.as_ref().map(Arc::clone)
    }

    pub fn add_to_command_history(&mut self, command: &str) {
        self.command_history.push(CommandHistory {
            command: command.to_string(),
            command_time: SystemTime::now(),
        });
    }

    pub fn start_time_string(&self) -> String {
        //! Conver the [`SystemTime`] object into a string representation
        //! to be more readable.

        let datetime: DateTime<Local> = self.start_time.into();
        datetime.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    pub fn show_command_history(&self, n_prev: Option<usize>) {
        //! Show the list of previously invoked comamnds.
        //! Use `n_prev` to limit the number of commands you see.

        let limit = n_prev.unwrap_or(self.command_history.len());

        for (index, command) in self.command_history.iter().rev().enumerate() {
            if index < limit {
                println!("{:3} | {}", index, command);
            }
        }
    }

    pub fn get_last_command(&self, nth_back: usize) -> Option<&str> {
        //! Gets the [`recent`]th last command from the history
        //! and runs it.

        self.command_history
            .iter()
            .nth_back(nth_back - 1)
            .map(|cmd| cmd.command.as_str())
    }

    pub fn create_database(
        &mut self,
        db_name: &str,
        if_not_exists: bool,
    ) -> Result<Arc<RwLock<Database>>, String> {
        let mut db_reg = self.database_registry.write().unwrap();
        db_reg.create_database(db_name, if_not_exists)
    }

    pub fn get_available_databases(&self) -> Vec<String> {
        //! Returns a list of all available database names.
        
        let db_reg = self.database_registry.read().unwrap();
        db_reg.get_database_names()
    }

    pub fn drop_database(&mut self, db_name: &str) -> Option<Arc<RwLock<Database>>> {
        //! Deletes the existing registry value of the registry.
        
        let mut db_reg = self.database_registry.write().unwrap();
        db_reg.drop_database(db_name)
    }
}
