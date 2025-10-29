use std::fs::{DirBuilder, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};

#[derive(Debug)]
enum Entry<'a> {
    KeyValue { key: &'a [u8], value: &'a [u8] },
    Tombstone { key: &'a [u8] },
}

impl<'a> TryFrom<&'a [u8]> for Entry<'a> {
    type Error = std::io::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut parts = value.splitn(2, |&b| b == b' ');

        let key = parts.next().ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Missing key",
        ))?;

        match parts.next() {
            Some(value) => Ok(Entry::KeyValue { key, value }),
            None => Ok(Entry::Tombstone { key }),
        }
    }
}

impl<'a> From<Entry<'a>> for Vec<u8> {
    fn from(value: Entry<'a>) -> Self {
        match value {
            Entry::KeyValue { key, value } => [key, b" ", value, b"\n"].concat(),
            Entry::Tombstone { key } => [key, b"\n"].concat(),
        }
    }
}

pub struct Database {
    file: File,
}

impl Database {
    pub fn new() -> std::io::Result<Self> {
        // Create directory in tmp
        let temp_dir = std::env::temp_dir().join("simple_lsm_db");
        DirBuilder::new().recursive(true).create(&temp_dir)?;

        // Create a single file in that directory
        let file_path = temp_dir.join("data.db");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)?;

        Ok(Database { file })
    }

    pub fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        self.file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&self.file);

        let mut value = None::<Vec<u8>>;

        for line in reader.lines() {
            let line = line?;
            let entry = Entry::try_from(line.as_bytes())?;

            dbg!("Entry: {:?}", &entry);

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
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        let entry = Entry::Tombstone { key };
        self.file.write(Vec::<u8>::from(entry).as_slice())?;
        Ok(())
    }
}
