use std::sync::{Arc, RwLock};

use ferrum_engine::persistence::{Database, Row, Table};

fn _prepare_database() -> Database {
    let db_name = "test_db".to_string();
    Database::new(db_name)
}

fn _create_table(
    database: &mut Database,
    name: String,
    column_definitions: Vec<String>,
    data: Vec<Vec<String>>,
) -> Result<Arc<RwLock<Table>>, String> {
    database
        .create_table(name.clone(), column_definitions)
        .expect("invalid column definitions or table exists already");
    database.insert_many_into_table(name.as_ref(), data)?;
    Ok(database.get_table(&name).unwrap())
}

fn _delete_from_table(database: &mut Database, name: &str, pk: Vec<&str>) -> Result<Row, String> {
    database.delete_from_table_value(name, pk)
}

fn _delete_from_table_many(
    database: &mut Database,
    name: &str,
    pks: Vec<Vec<&str>>,
) -> Result<usize, String> {
    database.delete_from_table_values(name, pks)
}

#[test]
fn database_create_table() {
    let mut database = _prepare_database();
    let columns = vec!["id num pk".to_string(), "name txt".to_string()];
    let values = vec![("1", "Jansen"), ("2", "Bonega"), ("3", "Maharashtra")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    let test_tb1 = _create_table(&mut database, "test_tb1".to_string(), columns, values);
    assert_eq!(test_tb1.is_ok(), true)
}

#[test]
fn database_create_table_with_fk() {
    let mut database = _prepare_database();
    let columns = vec!["id num pk".to_string(), "name txt".to_string()];
    let values = vec![("1", "Jansen"), ("2", "Bonega"), ("3", "Maharashtra")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    _create_table(&mut database, "test_tb1".to_string(), columns, values).unwrap();

    let columns = vec![
        "id num pk".to_string(),
        "t1_id num fk test_tb1.id".to_string(),
    ];
    let values = vec![("1", "1"), ("2", "2"), ("3", "3")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    _create_table(&mut database, "test_tb2".to_string(), columns, values).unwrap();
}

#[test]
#[should_panic(expected = "err: does not exist:")]
fn database_create_table_with_fk_fail() {
    let mut database = _prepare_database();
    let columns = vec!["id num pk".to_string(), "name txt".to_string()];
    let values = vec![("1", "Jansen"), ("2", "Bonega"), ("3", "Maharashtra")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    _create_table(&mut database, "test_tb1".to_string(), columns, values).unwrap();

    let columns = vec![
        "id num pk".to_string(),
        "t1_id num fk test_tb1.id".to_string(),
    ];
    let values = vec![("1", "1"), ("2", "2"), ("3", "7")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    _create_table(&mut database, "test_tb2".to_string(), columns, values).unwrap();
}

#[test]
fn database_delete_from_table_with_pk() {
    let mut database = _prepare_database();
    let columns = vec!["id num pk".to_string(), "name txt".to_string()];
    let values = vec![("1", "Jansen"), ("2", "Bonega"), ("3", "Maharashtra")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    _create_table(&mut database, "test_tb1".to_string(), columns, values).unwrap();

    let deleted_row = _delete_from_table(&mut database, "test_tb1", vec!["1"]);
    assert_eq!(deleted_row.is_ok(), true);
    assert_eq!(
        deleted_row.unwrap().0.get(0).unwrap().clone().unwrap(),
        "1".to_string()
    );

    let table = database.get_table("test_tb1").unwrap();
    assert_eq!(table.read().unwrap()._rows(), 2);
}

#[test]
fn database_delete_many_from_table_with_pk() {
    let mut database = _prepare_database();
    let columns = vec!["id num pk".to_string(), "name txt".to_string()];
    let values = vec![("1", "Jansen"), ("2", "Bonega"), ("3", "Maharashtra")]
        .iter()
        .map(|(id, name)| vec![id.to_string(), name.to_string()])
        .collect();

    _create_table(&mut database, "test_tb1".to_string(), columns, values).unwrap();

    let deleted_count =
        _delete_from_table_many(&mut database, "test_tb1", vec![vec!["1"], vec!["3"]]);
    assert_eq!(deleted_count.is_ok(), true);
    assert_eq!(deleted_count.unwrap(), 2);

    let table = database.get_table("test_tb1").unwrap();
    assert_eq!(table.read().unwrap()._rows(), 1);
}
