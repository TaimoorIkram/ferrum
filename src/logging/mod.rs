//! CAUTION
//! 
//! Claude was used to write this part of the code so I am unable to
//! explain the wizardry behind this but it works. Will it be able to
//! do the same on multi-threading? Hmm...

pub mod cli;
pub mod logger;

use std::cell::RefCell;

use logger::FerrumLogger;

thread_local! {
    static LOGGER: RefCell<Option<FerrumLogger>> = RefCell::new(None);
}

// Initialize logger once at startup
pub fn init_logger(debug_mode: bool, log_path: Option<&str>) {
    let logger = FerrumLogger::new(debug_mode, log_path);
    LOGGER.with(|l| {
        *l.borrow_mut() = Some(logger);
    });
}

// Use these functions everywhere instead of println
pub fn log_info(msg: &str) {
    LOGGER.with(|l| {
        if let Some(logger) = l.borrow().as_ref() {
            logger.info(msg);
        }
    });
}

pub fn log_error(msg: &str) {
    LOGGER.with(|l| {
        if let Some(logger) = l.borrow().as_ref() {
            logger.error(msg);
        }
    });
}

pub fn log_debug(msg: &str) {
    LOGGER.with(|l| {
        if let Some(logger) = l.borrow().as_ref() {
            logger.debug(msg);
        }
    });
}
