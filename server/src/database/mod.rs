mod entry;
use entry::Entry;
use std::fs::{DirBuilder, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct Database {
    set_count: usize,
    database_dir: PathBuf,
    file: File,
}

impl Database {
    pub fn new() -> std::io::Result<Self> {
        // Create directory in tmp
        let database_dir = std::env::temp_dir().join("simple_lsm_db");
        DirBuilder::new().recursive(true).create(&database_dir)?;

        // Create a single file in that directory
        let file_path = database_dir.clone().join("data.db");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)?;

        Ok(Database {
            file,
            database_dir,
            set_count: 0,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        self.file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&self.file);

        let mut value = None::<Vec<u8>>;

        for line in reader.lines() {
            let line = line?;
            let entry = Entry::try_from(line.as_bytes())?;

            match entry {
                Entry::KeyValue {
                    key: entry_key,
                    value: entry_value,
                } if entry_key == key => {
                    value = Some(entry_value.to_vec());
                }
                Entry::Tombstone { key: entry_key } if entry_key == key => {
                    value = None;
                }
                _ => continue,
            }
        }
        Ok(value)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let entry = Entry::KeyValue { key, value };
        self.file.write(Vec::<u8>::from(entry).as_slice())?;
        self.set_count += 1;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        let entry = Entry::Tombstone { key };
        self.file.write(Vec::<u8>::from(entry).as_slice())?;
        Ok(())
    }
}
