use std::{
    cmp::Ordering,
    fs::File,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

use crate::database::{entry::Entry, mem_table::MemTable};

pub const SEGMENT_FILE_EXTENSION: &str = "sst";

#[derive(PartialEq)]
pub struct SegmentFile {
    path: PathBuf,
}

impl SegmentFile {
    pub fn from_path(path: PathBuf) -> std::io::Result<Self> {
        if Self::is_segment_file(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid segment file extension",
            ));
        }

        Ok(Self { path })
    }

    pub fn create_and_store(path: PathBuf, map: MemTable) -> std::io::Result<Self> {
        let mut file = File::create(&path)?;
        for entry in map.into_iter() {
            file.write_all(Vec::<u8>::from(entry).as_slice())?;
        }

        file.flush()?;

        Ok(Self { path })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn entries(&self) -> std::io::Result<impl Iterator<Item = std::io::Result<Entry>>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        Ok(reader
            .lines()
            .map(|line| line.map(|line| Entry::from(line.as_bytes()))))
    }

    pub fn is_segment_file(path: &PathBuf) -> bool {
        path.extension()
            .map(|ext| ext == SEGMENT_FILE_EXTENSION)
            .unwrap_or(false)
    }
}

impl PartialOrd for SegmentFile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let extract_segment_number = |path: &PathBuf| -> Option<usize> {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem_str| {
                    stem_str
                        .strip_prefix("segment_")
                        .and_then(|num_str| num_str.parse().ok())
                })
        };

        Some(extract_segment_number(&self.path)?.cmp(&extract_segment_number(&other.path)?))
    }
}
