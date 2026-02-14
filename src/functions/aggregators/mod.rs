use crate::persistence::Row;

mod count;
mod max;
mod min;

const ALLOWED_AGGREGATORS: [&str; 3] = [count::AGGR_NAME, min::AGGR_NAME, max::AGGR_NAME];

/// A central method that works as a registry for all aggregators.
///
/// To add one, simple add another match arm.
pub fn run(name: &String, args: &Vec<String>, rows: &Vec<Row>) -> Result<String, String> {
    match name.to_uppercase().as_str() {
        count::AGGR_NAME => count::run(args, rows),
        max::AGGR_NAME => max::run(args, rows),
        min::AGGR_NAME => min::run(args, rows),
        _ => Err(format!("Unknown aggregate function: {}", name)),
    }
}

pub fn is_allowed(name: &String) -> bool {
    ALLOWED_AGGREGATORS.contains(&name.as_str())
}
