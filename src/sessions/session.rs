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
    sync::{Arc, RwLock},
    time::SystemTime,
};

use crate::persistence::Database;

pub enum Session {
    ClientSession {
        command_history: Vec<String>,
        start_time: SystemTime,
        active_database: Arc<RwLock<Database>>,
    },
    ServerSession,
}
