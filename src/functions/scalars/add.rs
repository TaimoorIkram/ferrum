/// Find the largest value of a particular index.
use crate::persistence::Row;

pub(super) const SCLR_NAME: &str = "ADD";

pub(super) fn run(args: &Vec<String>, row: &Row) -> Result<String, String> {
    let col_index = {
        let _a = args.first().unwrap();
        _a.parse::<usize>().expect("No index specified.")
    };

    let add_value = {
        let _a = args.get(1).unwrap();
        _a.parse::<usize>()
            .expect("Strictly integer value allowed.")
    };

    let mut value = {
        let _v = row.0.get(col_index).unwrap();
        _v.clone().unwrap().parse::<usize>()
    }
    .unwrap();

    value += add_value;

    Ok(value.to_string())
}
