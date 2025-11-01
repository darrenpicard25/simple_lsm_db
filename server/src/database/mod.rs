mod entry;
mod file_directory;
mod sstable;
mod wal;

use entry::Entry;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};

use crate::database::file_directory::FileDirectory;

const IN_MEMORY_TABLE_SIZE: usize = 5;

pub struct Database {
    file_directory: FileDirectory,
    in_memory_table: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Database {
    pub fn new() -> std::io::Result<Self> {
        let mut file_directory = FileDirectory::new()?;
        let in_memory_table = file_directory.wal().read_in_memory_table()?;

        Ok(Database {
            file_directory,
            in_memory_table,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        if let Some(value) = self.in_memory_table.get(key) {
            return Ok(value.clone());
        }

        for file in self.file_directory.segment_files() {
            let file = file?;
            let reader = BufReader::new(&file);

            for line in reader.lines() {
                let line = line?;
                let entry = Entry::try_from(line.as_bytes())?;

                match entry {
                    Entry::KeyValue {
                        key: entry_key,
                        value: entry_value,
                    } if entry_key == key => {
                        return Ok(Some(entry_value.to_vec()));
                    }
                    Entry::Tombstone { key: entry_key } if entry_key == key => {
                        return Ok(None);
                    }
                    _ => continue,
                }
            }
        }

        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.file_directory
            .wal()
            .append(Entry::KeyValue { key, value })?;
        self.in_memory_table
            .insert(key.to_vec(), Some(value.to_vec()));
        if self.in_memory_table.len() >= IN_MEMORY_TABLE_SIZE {
            tracing::info!("Flushing in-memory table to disk");
            self.file_directory.store_segment(&self.in_memory_table)?;
            self.file_directory.wal().clear()?;
            self.in_memory_table.clear();
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.file_directory.wal().append(Entry::Tombstone { key })?;
        self.in_memory_table.insert(key.to_vec(), None);
        if self.in_memory_table.len() >= IN_MEMORY_TABLE_SIZE {
            tracing::info!("Flushing in-memory table to disk");
            self.file_directory.store_segment(&self.in_memory_table)?;
            self.file_directory.wal().clear()?;
            self.in_memory_table.clear();
        }
        Ok(())
    }
}
