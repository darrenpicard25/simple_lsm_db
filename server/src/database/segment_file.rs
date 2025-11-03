use std::{
    cmp::Ordering,
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
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

    pub fn entries(
        &self,
        start_position: Option<u64>,
    ) -> std::io::Result<impl Iterator<Item = std::io::Result<(u64, Entry)>>> {
        let file = File::open(&self.path)?;
        let mut position = 0_u64;
        let mut reader = BufReader::new(file);

        if let Some(start_position) = start_position {
            reader.seek(SeekFrom::Start(start_position))?;
            position = start_position;
        }

        Ok(reader.lines().map(move |line| {
            let line = line?;
            let length = (line.len() + 1) as u64;
            let entry = Entry::from(line.as_bytes());
            let line_start_position = position;
            position += length;

            Ok((line_start_position, entry))
        }))
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
