use crate::persistence::Row;

mod add;

const ALLOWED_SCALARS: [&str; 1] = [add::SCLR_NAME];

/// A central method that works as a registry for all scalars.
///
/// To add one, simply add another match arm.
///
/// Get the pointer to the function, to call it later.
pub fn get_runner(
    name: &String,
) -> Result<fn(&Vec<String>, &Row) -> Result<String, String>, String> {
    match name.to_uppercase().as_str() {
        add::SCLR_NAME => Ok(add::run),
        _ => Err(format!("Unknown scalar function: {}", name)),
    }
}

pub fn is_allowed(name: &String) -> bool {
    ALLOWED_SCALARS.contains(&name.as_str())
}
