use std::path::PathBuf;

use server::database::Database;
use tempfile::TempDir; // Fixed unresolved import

#[test]
fn insert_multiple_records_into_multiple_files() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = Database::new(PathBuf::from(temp_dir.path()), Some(5)).unwrap();
    db.set(b"key3", b"value3").unwrap();
    db.set(b"key1", b"value1").unwrap();
    db.set(b"key4", b"value4").unwrap();
    db.set(b"key2", b"value2").unwrap();

    // List files in temp_dir
    let directory = std::fs::read_dir(temp_dir.path()).unwrap();

    let files = directory
        .map(|entry| entry.unwrap().path())
        .inspect(|file| println!("File: {:?}", file))
        .collect::<Vec<_>>();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].clone().file_name().unwrap(), "wal.log");
    let wal_contents = std::fs::read_to_string(files[0].clone()).unwrap();
    assert_eq!(
        wal_contents,
        "key3 value3\nkey1 value1\nkey4 value4\nkey2 value2\n"
    );

    db.set(b"key5", b"value5").unwrap();

    let files = std::fs::read_dir(temp_dir.path()).unwrap();
    let files = files
        .map(|entry| entry.unwrap().path())
        .inspect(|file| println!("File: {:?}", file))
        .collect::<Vec<_>>();

    assert_eq!(files.len(), 4);
    assert_eq!(files[0].clone().file_name().unwrap(), "wal.log");
    assert_eq!(files[1].clone().file_name().unwrap(), "segment_0.bf");
    assert_eq!(files[2].clone().file_name().unwrap(), "segment_0.sst");
    assert_eq!(files[3].clone().file_name().unwrap(), "segment_0.idx");

    let wal_contents = std::fs::read_to_string(files[0].clone()).unwrap();
    assert_eq!(wal_contents, "");

    let segment_contents = std::fs::read_to_string(files[2].clone()).unwrap();
    assert_eq!(
        segment_contents,
        "key1 value1\nkey2 value2\nkey3 value3\nkey4 value4\nkey5 value5\n"
    );

    let index_contents = std::fs::read_to_string(files[3].clone()).unwrap();
    assert_eq!(
        index_contents.as_bytes(),
        [b"key1".as_slice(), &0u64.to_le_bytes(), b"\n".as_slice()].concat()
    );
}
