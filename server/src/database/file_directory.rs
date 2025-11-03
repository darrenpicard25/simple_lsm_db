use std::fs::DirBuilder;
use std::path::Path;
use std::path::PathBuf;

use crate::database::bloom_filter::BloomFilter;
use crate::database::bloom_filter_registry::BloomFilterRegistry;
use crate::database::mem_table::MemTable;
use crate::database::segment_file::SegmentFile;
use crate::database::segment_file_registry::SegmentFileRegistry;
use crate::database::wal::Wal;

pub struct FileDirectory<P: AsRef<Path>> {
    directory: P,
    segment_file_registry: SegmentFileRegistry,
    wal: Wal,
    bloom_filter_registry: BloomFilterRegistry,
}

impl<P: AsRef<Path> + Clone> FileDirectory<P> {
    pub fn new(directory: P) -> std::io::Result<Self> {
        DirBuilder::new()
            .recursive(true)
            .create(directory.clone())?;

        let segment_file_registry = SegmentFileRegistry::new(directory.clone())?;
        let wal = Wal::new(directory.clone())?;
        let bloom_filter_registry = BloomFilterRegistry::new(&directory)?;

        Ok(Self {
            directory: directory.clone(),
            segment_file_registry,
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

    pub fn segment_files(&self) -> impl Iterator<Item = &SegmentFile> {
        self.segment_file_registry.files()
    }

    pub fn store_segment(&mut self, map: MemTable) -> std::io::Result<()> {
        let file_path = self.segment_file_registry.store_new(map.clone())?;
        self.bloom_filter_registry.store(&file_path, &map)?;

        Ok(())
    }
}
