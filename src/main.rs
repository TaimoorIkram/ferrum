use ferrum_engine::persistence::Table;
use std::sync::Arc;

fn main() {
    let columns = vec![("id", "num"), ("name", "txt")];

    let table = match Table::from(
        columns
            .iter()
            .map(|(id, datatype)| (id.to_string(), datatype.to_string()))
            .collect(),
    ) {
        Ok(t) => {
            println!("Table successfully created.");
            Some(Arc::new(t))
        }
        Err(msg) => {
            println!("err: {}", msg);
            None
        }
    };

    if table.is_some() {
        let table = table.unwrap();

        let dataset = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
            ("5", "dajioajsiopdoaisjdoijaijoajosjoaksadkl;kdskdlasaklmmadl"),
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

        let id_only = table.reader().select(vec![
            "id".to_string(),
            "name".to_string()
        ]).unwrap();

        for row in id_only.scan().iter() {
            println!("{}", row);
        }
    }
}
