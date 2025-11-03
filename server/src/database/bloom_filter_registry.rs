use crate::database::mem_table::MemTable;

use super::bloom_filter::BloomFilter;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

const BLOOM_FILTER_FILE_EXTENSION: &str = "bf";
pub struct BloomFilterRegistry {
    /// Bloom filters keyed by base file name (without extension), e.g., "segment_0"
    filters: HashMap<String, BloomFilter>,
    directory: PathBuf,
}

impl BloomFilterRegistry {
    pub fn new<P>(directory: P) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let bloom_filter_paths = Self::find_bloom_filter_files(&directory)?;
        let filters = bloom_filter_paths
            .into_iter()
            .filter_map(|path| Self::load_bloom_filter(&path))
            .collect();

        Ok(Self {
            filters,
            directory: directory.as_ref().to_path_buf(),
        })
    }

    /// Find all bloom filter files in the given directory
    fn find_bloom_filter_files<P: AsRef<Path>>(directory: P) -> std::io::Result<Vec<PathBuf>> {
        Ok(std::fs::read_dir(&directory)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry.path().extension().and_then(|ext| {
                    if ext == BLOOM_FILTER_FILE_EXTENSION {
                        Some(entry.path())
                    } else {
                        None
                    }
                })
            })
            .collect())
    }

    /// Load a bloom filter from a file path, returning None on any error
    /// Returns (base_name, filter) where base_name is the file name without extension
    fn load_bloom_filter(path: &PathBuf) -> Option<(String, BloomFilter)> {
        // Extract base name (file stem without extension) as owned String
        // This avoids lifetime issues since String is owned
        let base_name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(|s| s.to_string())?;

        // Read file contents
        let data = match fs::read(path) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!(
                    "Failed to read bloom filter file {}: {}",
                    path.display(),
                    error
                );
                return None;
            }
        };

        // Deserialize bloom filter
        match BloomFilter::deserialize(&data) {
            Ok(filter) => Some((base_name, filter)),
            Err(error) => {
                tracing::error!(
                    "Failed to deserialize bloom filter file {}: {}",
                    path.display(),
                    error
                );
                None
            }
        }
    }

    /// Get a bloom filter by path. Extracts the base name from the path
    /// and looks it up in the registry.
    pub fn get(&self, path: &PathBuf) -> Option<&BloomFilter> {
        // Extract base name from path (file stem without extension)
        let base_name = path.file_stem()?.to_str()?;
        self.filters.get(base_name)
    }

    pub fn store(&mut self, path: &PathBuf, mem_table: &MemTable) -> std::io::Result<()> {
        let mut bloom_filter_path = path.clone();
        bloom_filter_path.set_extension(BLOOM_FILTER_FILE_EXTENSION);

        let mut bloom_filter = BloomFilter::default_for_keys(mem_table.len());
        for (key, _) in mem_table.iter() {
            bloom_filter.insert(key);
        }

        let bloom_filter_base_name = bloom_filter_path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let data = bloom_filter.serialize();
        fs::write(bloom_filter_path, data)?;

        self.filters.insert(bloom_filter_base_name, bloom_filter);
        Ok(())
    }
}
