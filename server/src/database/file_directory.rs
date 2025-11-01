use std::collections::BTreeMap;
use std::fs::DirBuilder;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use crate::database::sstable::SegmentFiles;
use crate::database::wal::Wal;

pub type InMemoryTable = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

pub struct FileDirectory<P: AsRef<Path>> {
    directory: P,
    segment_files: SegmentFiles,
    wal: Wal,
}

impl<P: AsRef<Path> + Clone> FileDirectory<P> {
    pub fn new(directory: P) -> std::io::Result<Self> {
        DirBuilder::new()
            .recursive(true)
            .create(directory.clone())?;

        let segment_files = SegmentFiles::new(directory.clone())?;
        let wal = Wal::new(directory.clone())?;

        Ok(Self {
            directory: directory.clone(),
            segment_files,
            wal,
        })
    }

    pub fn wal(&mut self) -> &mut Wal {
        &mut self.wal
    }

    pub fn segment_files(&self) -> impl Iterator<Item = std::io::Result<File>> {
        self.segment_files.files()
    }

    pub fn store_segment(&mut self, map: &InMemoryTable) -> std::io::Result<()> {
        self.segment_files.store(&self.directory, map)
    }
}
