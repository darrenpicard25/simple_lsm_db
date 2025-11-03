use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::Path,
};

use crate::database::entry::Entry;

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn new<P: AsRef<Path>>(database_dir: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(database_dir.as_ref().join("wal.log"))?;

        Ok(Self { file })
    }

    pub fn append(&mut self, entry: Entry) -> std::io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write(Vec::<u8>::from(entry).as_slice())?;
        Ok(())
    }

    pub fn clear(&mut self) -> std::io::Result<()> {
        self.file.set_len(0)?;
        Ok(())
    }

    pub fn entries(&mut self) -> std::io::Result<impl Iterator<Item = std::io::Result<Entry>>> {
        self.file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&self.file);
        Ok(reader.lines().map(|line| {
            line.map(|line| Entry::from(line.as_bytes()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))
        }))
    }
}
