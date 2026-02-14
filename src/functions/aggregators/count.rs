/// The COUNT(arg) aggregator.
///
/// Returns the total number of non-numm values in the given data.
use crate::persistence::Row;

pub(super) const AGGR_NAME: &str = "COUNT";

pub(super) fn run(args: &Vec<String>, rows: &Vec<Row>) -> Result<String, String> {
    if args.contains(&"*".to_string()) {
        Ok(rows.len().to_string())
    } else {
        if args.len() > 1 {
            Err(format!(
                "{} takes in a wildcard or a single column.",
                AGGR_NAME
            ))
        } else {
            let mut total_count = 0;
            let col_index = {
                let _a = args.first().unwrap();
                _a.parse::<usize>().expect("No index specified.")
            };

            rows.iter().for_each(|row| {
                if row.0.get(col_index).is_some() {
                    total_count += 1;
                }
            });

            Ok(total_count.to_string())
        }
    }
}
