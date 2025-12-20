use ferrum_engine::persistence::Table;
use log::error;

fn main() {
    let columns = vec!["id num pk", "name txt"];

    let table = match Table::new(
        "test".to_string(),
        columns.iter().map(|col_def| col_def.to_string()).collect(),
    ) {
        Ok(t) => {
            error!("Table successfully created.");
            Some(t)
        }
        Err(msg) => {
            error!("err: {}", msg);
            None
        }
    };

    if table.is_some() {
        let mut table = table.unwrap();

        let dataset = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
            (
                "5",
                "dajioajsiopdoaisjdoijaijoajosjoaksadkl;kdskdlasaklmmadl",
            ),
            ("6", "Malaika"),
            ("7", "Jimmy"),
            ("ski", "Jeffrey"),
            ("9", "Rango"),
            ("10", "Danish"),
        ];

        for (id, name) in dataset.iter().to_owned() {
            if let Err(message) = table.insert(vec![id.to_string(), name.to_string()]) {
                println!("err: {}", message);
            }
        }

        let _id_only = table
            .reader()
            .select(vec!["id".to_string(), "name".to_string()])
            .unwrap();

        println!("{}", table)
    }
}
