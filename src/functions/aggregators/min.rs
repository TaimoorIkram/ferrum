/// Find the largest value of a particular index.
use crate::persistence::Row;

pub(super) const AGGR_NAME: &str = "MIN";

pub(super) fn run(args: &Vec<String>, rows: &Vec<Row>) -> Result<String, String> {
    if args.len() > 1 {
        Err(format!("{} strictly allows a single column.", AGGR_NAME))
    } else {
        let col_index = {
            let _a = args.first().unwrap();
            _a.parse::<usize>().expect("No index specified.")
        };
        let mut max = None;

        rows.iter().for_each(|row| {
            if let Some(value) = row.0.get(col_index) {
                if matches!(max, None) {
                    max = value.clone();
                } else if value.as_ref().unwrap().lt(max.as_ref().unwrap()) {
                    max = value.clone()
                }
            }
        });

        Ok(max.unwrap())
    }
}
