use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use crate::database::{entry::Entry, file_directory::InMemoryTable};

const SEGMENT_FILE_EXTENSION: &str = ".sst";

pub struct SegmentFiles {
    segment_files: Vec<PathBuf>,
}

impl SegmentFiles {
    pub fn new<P: AsRef<Path>>(database_dir: P) -> std::io::Result<Self> {
        let mut segment_files = std::fs::read_dir(database_dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_type()
                    .ok()
                    .and_then(|ft| ft.is_file().then_some(entry.path()))
            })
            .filter(|path| {
                path.extension()
                    .map(|ext| ext == SEGMENT_FILE_EXTENSION)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        // Sort the files by the segment number extracted from the filename, descending
        segment_files.sort_by(|a, b| {
            // Helper function to extract the numeric part of the segment filename
            fn extract_segment_number(path: &PathBuf) -> usize {
                // Get the filename, and strip the extension and prefix
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem_str| {
                        // Expected: "segment_N"
                        stem_str.strip_prefix("segment_")
                    })
                    .and_then(|num_str| num_str.parse().ok())
                    .unwrap_or(0)
            }
            // Descending order (largest segment comes first)
            extract_segment_number(b).cmp(&extract_segment_number(a))
        });

        Ok(Self { segment_files })
    }

    pub fn files(&self) -> impl Iterator<Item = std::io::Result<File>> {
        self.segment_files.iter().rev().map(|path| File::open(path))
    }

    pub fn store<P: AsRef<Path>>(
        &mut self,
        directory_path: P,
        map: &InMemoryTable,
    ) -> std::io::Result<()> {
        let file_path = directory_path.as_ref().join(format!(
            "segment_{}{SEGMENT_FILE_EXTENSION}",
            self.segment_files.len()
        ));

        let mut file = File::create(&file_path)?;

        for entry in map.iter() {
            let entry = match entry {
                (key, Some(value)) => Entry::KeyValue { key, value },
                (key, None) => Entry::Tombstone { key },
            };
            file.write(Vec::<u8>::from(entry).as_slice())?;
        }

        self.segment_files.push(file_path);

        Ok(())
    }
}
