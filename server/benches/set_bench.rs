use criterion::{Criterion, criterion_group, criterion_main};
use server::database::Database;
use tempfile::TempDir;

pub fn set_bench(c: &mut Criterion) {
    c.bench_function("set_bench", |batch| {
        batch.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let mut db = Database::new(temp_dir.path()).unwrap();

            for i in 0..10_000 {
                db.set(
                    format!("key_{}", i).as_bytes(),
                    format!("value_{}", i).as_bytes(),
                )
                .unwrap();
            }
        });
    });
}

criterion_group!(benches, set_bench);
criterion_main!(benches);
