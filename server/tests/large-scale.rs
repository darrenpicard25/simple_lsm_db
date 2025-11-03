use server::database::Database;
use tempfile::TempDir;

#[test]
fn test_large_scale() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = Database::new(temp_dir.path(), Some(1000)).unwrap();

    for i in 0..=10_000 {
        db.set(
            format!("key_{}", i).as_bytes(),
            format!("value_{}", i).as_bytes(),
        )
        .unwrap();
    }

    let result1 = db.get(b"key_0").unwrap();
    assert_eq!(result1, Some(b"value_0".to_vec()));
    let result2 = db.get(b"key_5000").unwrap();
    assert_eq!(result2, Some(b"value_5000".to_vec()));
    let result3 = db.get(b"key_10000").unwrap();
    assert_eq!(result3, Some(b"value_10000".to_vec()));
    let result4 = db.get(b"key_10001").unwrap();
    assert_eq!(result4, None);
}
