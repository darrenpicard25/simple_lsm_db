use std::fs::DirBuilder;
use std::path::Path;
use std::path::PathBuf;

use crate::database::bloom_filter::BloomFilter;
use crate::database::bloom_filter_registry::BloomFilterRegistry;
use crate::database::entry::Entry;
use crate::database::index_entry::IndexEntry;
use crate::database::index_file::IndexFile;
use crate::database::index_file_registry::IndexFileRegistry;
use crate::database::mem_table::MemTable;
use crate::database::segment_file::SegmentFile;
use crate::database::segment_file_registry::SegmentFileRegistry;
use crate::database::wal::Wal;

pub struct FileDirectory<P: AsRef<Path>> {
    directory: P,
    segment_file_registry: SegmentFileRegistry,
    wal: Wal,
    bloom_filter_registry: BloomFilterRegistry,
    index_file_registry: IndexFileRegistry,
}

impl<P: AsRef<Path> + Clone> FileDirectory<P> {
    pub fn new(directory: P) -> std::io::Result<Self> {
        DirBuilder::new()
            .recursive(true)
            .create(directory.clone())?;

        let segment_file_registry = SegmentFileRegistry::new(directory.clone())?;
        let wal = Wal::new(directory.clone())?;
        let bloom_filter_registry = BloomFilterRegistry::new(&directory)?;
        let index_file_registry = IndexFileRegistry::new(&directory)?;

        Ok(Self {
            directory: directory.clone(),
            segment_file_registry,
            index_file_registry,
            wal,
            bloom_filter_registry,
        })
    }

    pub fn wal(&mut self) -> &mut Wal {
        &mut self.wal
    }

    pub fn get_bloom_filter(&self, path: &PathBuf) -> Option<&BloomFilter> {
        self.bloom_filter_registry.get(path)
    }

    pub fn get_index_file(&self, path: &PathBuf) -> Option<&IndexFile> {
        self.index_file_registry.get(path)
    }

    pub fn segment_files(&self) -> impl Iterator<Item = &SegmentFile> {
        self.segment_file_registry.files()
    }

    pub fn store_segment(&mut self, map: MemTable) -> std::io::Result<()> {
        let size = map.len();
        let file_path = self.segment_file_registry.store_new(map.clone())?;
        self.bloom_filter_registry.store(&file_path, &map)?;

        let mut index_entries = Vec::with_capacity(size / 100);
        if let Some(segment_file) = self.segment_file_registry.get(&file_path) {
            for result in segment_file.entries(None)?.step_by(100) {
                let (line_start_position, entry) = result?;
                match entry {
                    Entry::KeyValue { key, .. } => {
                        index_entries.push(IndexEntry::new(key, line_start_position));
                    }
                    Entry::Tombstone { key } => {
                        index_entries.push(IndexEntry::new(key, line_start_position));
                    }
                }
            }
        }

        if !index_entries.is_empty() {
            self.index_file_registry
                .store_new(file_path.clone(), index_entries)?;
        }

        Ok(())
    }
}
