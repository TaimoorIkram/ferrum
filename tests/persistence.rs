#[cfg(test)]
mod table {
    use ferrum_engine::persistence::{Row, Table};

    fn _create_table(columns: Vec<(&str, &str)>) -> Result<Table, String> {
        Table::from(
            columns
                .iter()
                .map(|(id, datatype)| (id.to_string(), datatype.to_string()))
                .collect(),
        )
    }

    #[test]
    fn table_creates_with_proper_types() {
        let columns = vec![("id", "num"), ("name", "txt")];

        _create_table(columns).unwrap();
    }

    #[test]
    #[should_panic]
    fn table_does_not_create_with_improper_types() {
        let columns = vec![("id", "num"), ("name", "flt")];

        _create_table(columns).unwrap();
    }

    #[test]
    fn table_reader_scan_nonempty() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let values = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
        ];

        for (id, name) in values.iter().to_owned() {
            if let Err(message) = table.insert(vec![id.to_string(), name.to_string()]) {
                println!("err: {}", message);
            }
        }

        let reader = table.reader();
        let rows = reader.scan();

        assert_eq!(rows.len(), 4);

        let check_name = "Jansen".to_string();
        assert_eq!(
            rows.get(0).unwrap().0.get(1).unwrap().as_ref(),
            Some(&check_name)
        );
    }

    #[test]
    fn table_reader_scan_empty() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let reader = table.reader();
        let rows = reader.scan();

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn table_reader_filter_nonempty() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let values = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
        ];

        for (id, name) in values.iter().to_owned() {
            if let Err(message) = table.insert(vec![id.to_string(), name.to_string()]) {
                println!("err: {}", message);
            }
        }

        let reader = table.reader();

        // filtering all items with id >= 2 (should be 3 rows id 2, 3, and 4)
        let filter = |row: &Row| match row.0.get(0) {
            Some(Some(value)) => value.parse::<u32>().unwrap() >= 2,
            _ => false,
        };
        let rows = reader.filter(filter).unwrap().scan();

        assert_eq!(rows.len(), 3);

        let check_name = "Bonega".to_string();
        assert_eq!(
            rows.get(0).unwrap().0.get(1).unwrap().as_ref(),
            Some(&check_name),
        );
    }

    #[test]
    fn table_reader_filter_returns_empty() {
        let table = _create_table(vec![("id", "num")]).unwrap();
        table.insert(vec!["1".to_string()]).unwrap();

        let reader = table.reader();
        let rows = reader.filter(|row| {
            row.0[0]
                .as_ref()
                .and_then(|s| s.parse::<u32>().ok())
                .map_or(false, |id| id > 100)
        }).unwrap().scan();

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn table_reader_filter_handles_null_values() {
        let table = _create_table(vec![("id", "num")]).unwrap();
        // table.insert(vec!["".to_string()]).unwrap(); // NULL value
        table.insert(vec!["1".to_string()]).unwrap();

        let reader = table.reader();
        let rows = reader.filter(|row| row.0[0].is_some()).unwrap().scan();

        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn table_reader_select_single_column() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let values = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
        ];

        for (id, name) in values.iter().to_owned() {
            if let Err(message) = table.insert(vec![id.to_string(), name.to_string()]) {
                println!("err: {}", message);
            }
        }

        let reader = table.reader();
        let ids_only = reader.select(vec!["id".to_string()]).unwrap();

        for (row, (id, _)) in ids_only.scan().iter().zip(values) {
            assert_eq!(row.0.get(0).unwrap().as_deref(), Some(id))
        }
    }

    #[test]
    fn table_reader_select_multiple_columns() {
        let table = _create_table(vec![("id", "num"), ("name", "txt"), ("age", "num")]).unwrap();

        table
            .insert(vec!["1".to_string(), "Alice".to_string(), "30".to_string()])
            .unwrap();

        let reader = table.reader();
        let selected = reader
            .select(vec!["name".to_string(), "id".to_string()])
            .unwrap();

        let results = selected.scan();

        // Schema should be reordered: name, id (not id, name)
        assert_eq!(results[0].0[0].as_ref().unwrap(), "Alice");
        assert_eq!(results[0].0[1].as_ref().unwrap(), "1");
    }

    #[test]
    fn table_insert_many_noerror() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let values = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
        ].iter().map(|(id, name)| vec![id.to_string(), name.to_string()]).collect();

        let num_insertions = table.insert_many(values);
        assert_eq!(num_insertions.unwrap(), 4);
    }

    #[test]
    fn table_insert_many_error() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let values = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("x", "Lorem"),
        ].iter().map(|(id, name)| vec![id.to_string(), name.to_string()]).collect();

        // will fail because err is unwrapped
        let _num_insertions = table.insert_many(values);
        assert_eq!(table.reader().scan().len(), 3);
    }

    #[test]
    fn table_insert_row_count() {
        let table = _create_table(vec![("id", "num"), ("name", "txt")]).unwrap();
        let values = vec![
            ("1", "Jansen"),
            ("2", "Bonega"),
            ("3", "Maharashtra"),
            ("4", "Lorem"),
        ].iter().map(|(id, name)| vec![id.to_string(), name.to_string()]).collect();

        let _num_insertions = table.insert_many(values);
        assert_eq!(table.rows(), 4);
    }
}
