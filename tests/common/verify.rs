use lightning_db::Database;

pub fn verify_key_exists(db: &Database, key: &[u8]) -> bool {
    db.get(key).unwrap().is_some()
}

pub fn verify_key_missing(db: &Database, key: &[u8]) -> bool {
    db.get(key).unwrap().is_none()
}

pub fn verify_key_value(db: &Database, key: &[u8], expected_value: &[u8]) {
    let actual = db.get(key).unwrap();
    assert_eq!(
        actual.as_deref(),
        Some(expected_value),
        "Value mismatch for key {:?}",
        String::from_utf8_lossy(key)
    );
}

pub fn verify_data_integrity(db: &Database, data: &[(Vec<u8>, Vec<u8>)]) {
    for (key, expected_value) in data {
        verify_key_value(db, key, expected_value);
    }
}