use std::{
    fs::{File, OpenOptions}, io::Write, sync::Mutex, time::SystemTime,
};

use chrono::{DateTime, Local};

/// The fundamental entity to show logs.
///
/// Logs can be of any of the following types:
/// 1. File logs: used to add all kinds of events into a file (default is ../data/ferrum.log)
/// 2. CLI logs: used to log events on the user screen, in the client session
///
/// The kind of information that can be exchanged in logs is mainly:
/// 1. Errors
/// 2. Events (CREATE, UPDATE, DELETE)
/// 3. Information
///
/// Logging format: this is tentative and could change
/// [date][time]/[timestamp] [entity]: [message in function]/[Display trait decodes to text as defined in the trait]
pub struct FerrumLogger {
    debug_mode: bool,
    file: Option<Mutex<File>>,
}

impl FerrumLogger {
    pub fn new(debug_mode: bool, log_file: Option<&str>) -> FerrumLogger {
        let file_path: &str;

        if let Some(path) = log_file {
            file_path = path;
        } else {
            file_path = "../data/ferrum.log";
        }

        // Obtain the handle of the log file for writing
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .ok()
            .map(Mutex::new)
            .map(Some)
            .unwrap();

        FerrumLogger { debug_mode, file }
    }

    pub fn log(&self, level: &str, msg: &str) {
        let datetime: DateTime<Local> = SystemTime::now().into();
        let log_timestamp: String = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        
        let final_msg = format!("[{} {}] {}", log_timestamp, level, msg);
        if self.debug_mode {
            // Use a system_message like API here, either move the previous
            // function and make use of it here.
            println!("{}", &final_msg)
        }

        // Log to file if available
        if let Some(ref file) = self.file {
            if let Ok(mut f) = file.lock() {
                let _ = writeln!(f, "{}", final_msg);
            }
        }
    }

    pub fn info(&self, msg: &str) {
        if !self.debug_mode {
            println!("{}", msg);
        }

        self.log("INFO", msg);
    }

    pub fn error(&self, msg: &str) {
        if !self.debug_mode {
            println!("{}", msg);
        }

        self.log("ERROR", msg);
    }

    pub fn debug(&self, msg: &str) {
        if self.debug_mode {
            self.log("DEBUG", msg);
        }
    }
}
