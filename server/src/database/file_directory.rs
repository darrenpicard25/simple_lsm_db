use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::DirBuilder;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use crate::database::bloom_filter::BloomFilter;
use crate::database::sstable::SegmentFiles;
use crate::database::wal::Wal;

pub type InMemoryTable = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

pub struct FileDirectory<P: AsRef<Path>> {
    directory: P,
    segment_files: SegmentFiles,
    wal: Wal,
    bloom_filters: HashMap<PathBuf, BloomFilter>,
}

impl<P: AsRef<Path> + Clone> FileDirectory<P> {
    pub fn new(directory: P) -> std::io::Result<Self> {
        DirBuilder::new()
            .recursive(true)
            .create(directory.clone())?;

        let segment_files = SegmentFiles::new(directory.clone())?;
        let wal = Wal::new(directory.clone())?;

        // Load bloom filters
        let mut bloom_filters = HashMap::new();
        for path in segment_files.paths() {
            let bloom_filter_path = path.with_extension("bf");
            if bloom_filter_path.exists() {
                match std::fs::read(&bloom_filter_path) {
                    Ok(data) => match BloomFilter::deserialize(&data) {
                        Ok(filter) => {
                            // Normalize path to ensure consistent lookup
                            // Use the actual path from segment_files (which matches what paths() returns)
                            bloom_filters.insert(path.clone(), filter);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to deserialize bloom filter for {:?}: {}",
                                bloom_filter_path,
                                e
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read bloom filter file {:?}: {}",
                            bloom_filter_path,
                            e
                        );
                    }
                }
            }
        }

        Ok(Self {
            directory: directory.clone(),
            segment_files,
            wal,
            bloom_filters,
        })
    }

    pub fn wal(&mut self) -> &mut Wal {
        &mut self.wal
    }

    pub fn segment_files(&self) -> impl Iterator<Item = std::io::Result<File>> {
        self.segment_files.files()
    }

    pub fn segment_paths(&self) -> impl Iterator<Item = &PathBuf> {
        self.segment_files.paths()
    }

    pub fn get_bloom_filter(&self, path: &PathBuf) -> Option<&BloomFilter> {
        self.bloom_filters.get(path)
    }

    pub fn store_segment(&mut self, map: &InMemoryTable) -> std::io::Result<()> {
        self.segment_files.store(&self.directory, map)?;

        // Reload bloom filters to include the newly created one
        // Get the last segment path (most recently created)
        if let Some(new_path) = self.segment_files.paths().next() {
            let bloom_filter_path = new_path.with_extension("bf");
            if bloom_filter_path.exists() {
                match std::fs::read(&bloom_filter_path) {
                    Ok(data) => match BloomFilter::deserialize(&data) {
                        Ok(filter) => {
                            self.bloom_filters.insert(new_path.clone(), filter);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to deserialize bloom filter for {:?}: {}",
                                bloom_filter_path,
                                e
                            );
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            "Failed to read bloom filter file {:?}: {}",
                            bloom_filter_path,
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
