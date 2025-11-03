use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

use crate::database::index_entry::IndexEntry;

pub const INDEX_FILE_EXTENSION: &str = "idx";
pub struct IndexFile {
    path: PathBuf,
}

impl IndexFile {
    pub fn from_path(path: PathBuf) -> std::io::Result<Self> {
        if Self::is_index_file(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid index file extension",
            ));
        }

        Ok(Self { path })
    }

    pub fn create_and_store(path: PathBuf, entries: Vec<IndexEntry>) -> std::io::Result<Self> {
        assert!(Self::is_index_file(&path));
        let mut file = File::create(&path)?;
        for entry in entries {
            file.write(&Vec::<u8>::from(entry))?;
        }
        file.flush()?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn is_index_file(path: &PathBuf) -> bool {
        path.extension()
            .map(|ext| ext == INDEX_FILE_EXTENSION)
            .unwrap_or(false)
    }

    pub fn entries(&self) -> std::io::Result<impl Iterator<Item = std::io::Result<IndexEntry>>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        Ok(reader.lines().map(|possible_line| {
            possible_line.and_then(|line| IndexEntry::try_from(line.as_bytes().to_vec()))
        }))
    }
}
