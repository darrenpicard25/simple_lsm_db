use criterion::{Criterion, criterion_group, criterion_main};
use server::database::Database;
use tempfile::TempDir;

pub fn get_bench(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut db = Database::new(temp_dir.path(), Some(1000)).unwrap();

    for i in 0..=100_000 {
        db.set(
            format!("key_{}", i).as_bytes(),
            format!("value_{}", i).as_bytes(),
        )
        .unwrap();
    }

    c.bench_function("get_bench", |batch| {
        batch.iter(|| {
            let result1 = db.get(b"key_0").unwrap();
            assert_eq!(result1, Some(b"value_0".to_vec()));
            let result2 = db.get(b"key_10000").unwrap();
            assert_eq!(result2, Some(b"value_10000".to_vec()));
            let result3 = db.get(b"key_50000").unwrap();
            assert_eq!(result3, Some(b"value_50000".to_vec()));
            let result4 = db.get(b"key_100001").unwrap();
            assert_eq!(result4, None);
        });
    });
}

criterion_group!(benches, get_bench);
criterion_main!(benches);
