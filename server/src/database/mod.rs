mod bloom_filter;
mod bloom_filter_registry;
mod entry;
mod file_directory;
mod index_entry;
mod index_file;
mod index_file_registry;
mod mem_table;
mod segment_file;
mod segment_file_registry;
mod wal;

use entry::Entry;
use std::cmp::Ordering;
use std::path::Path;

use crate::database::file_directory::FileDirectory;
use crate::database::mem_table::MemTable;

pub struct Database<P: AsRef<Path> + Clone> {
    file_directory: FileDirectory<P>,
    mem_table: MemTable,
}

impl<P: AsRef<Path> + Clone> Database<P> {
    pub fn new(directory: P, max_table_size: Option<usize>) -> std::io::Result<Self> {
        let mut file_directory = FileDirectory::new(directory)?;
        // Collect valid WAL entries into a MemTable using FromIterator
        let wal_entries = file_directory.wal().entries()?.filter_map(Result::ok);
        let mem_table = MemTable::from_iter(wal_entries, max_table_size);

        Ok(Database {
            file_directory,
            mem_table,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        if let Some(value) = self.mem_table.get(key) {
            return Ok(value.clone());
        }

        for segment_file in self.file_directory.segment_files() {
            // Check bloom filter first to skip segments that definitely don't contain the key
            if let Some(bloom_filter) = self.file_directory.get_bloom_filter(segment_file.path()) {
                if !bloom_filter.might_contain(key) {
                    continue;
                }
            }

            let starting_position = self
                .file_directory
                .get_index_file(segment_file.path())
                .and_then(|index_file| {
                    index_file.entries().ok().and_then(|mut entry_file| {
                        let mut position = 0_u64;

                        while let Some(entry) = entry_file.next() {
                            let entry = entry.ok()?;

                            if entry.key() > key {
                                return Some(position);
                            }

                            position = entry.offset();
                        }

                        None
                    })
                });

            'line: for result in segment_file.entries(starting_position)? {
                let (_, entry) = result?;
                match entry {
                    Entry::KeyValue {
                        key: entry_key,
                        value,
                    } => match entry_key.as_slice().cmp(key) {
                        Ordering::Equal => return Ok(Some(value.to_vec())),
                        Ordering::Less => continue,
                        Ordering::Greater => break 'line,
                    },
                    Entry::Tombstone { key: entry_key } => match entry_key.as_slice().cmp(key) {
                        Ordering::Equal => return Ok(None),
                        Ordering::Less => continue,
                        Ordering::Greater => break 'line,
                    },
                }
            }
        }

        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.file_directory.wal().append(Entry::KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
        })?;
        self.mem_table.insert(key, value);
        if self.mem_table.should_flush() {
            self.flush()?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.file_directory
            .wal()
            .append(Entry::Tombstone { key: key.to_vec() })?;
        self.mem_table.remove(key);
        if self.mem_table.should_flush() {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        tracing::info!("Flushing in-memory table to disk");

        self.file_directory.store_segment(self.mem_table.clone())?;
        self.file_directory.wal().clear()?;
        self.mem_table.clear();
        Ok(())
    }
}
