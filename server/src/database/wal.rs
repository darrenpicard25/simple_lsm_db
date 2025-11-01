use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

use crate::database::{entry::Entry, file_directory::InMemoryTable};

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn new(database_dir: &PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(database_dir.join("wal.log"))?;

        Ok(Self { file })
    }

    pub fn append(&mut self, entry: Entry) -> std::io::Result<()> {
        self.file.write(Vec::<u8>::from(entry).as_slice())?;
        self.file.write_all(b"\n")?;
        Ok(())
    }

    pub fn clear(&mut self) -> std::io::Result<()> {
        self.file.set_len(0)?;
        Ok(())
    }

    pub fn read_in_memory_table(&mut self) -> std::io::Result<InMemoryTable> {
        let mut in_memory_table = InMemoryTable::new();
        let reader = BufReader::new(&self.file);
        for line in reader.lines() {
            let line = line?;
            let entry = Entry::try_from(line.as_bytes())?;
            match entry {
                Entry::KeyValue { key, value } => {
                    in_memory_table.insert(key.to_vec(), Some(value.to_vec()));
                }
                Entry::Tombstone { key } => {
                    in_memory_table.insert(key.to_vec(), None);
                }
            }
        }
        Ok(in_memory_table)
    }
}
