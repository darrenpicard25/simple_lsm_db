use std::collections::BTreeMap;
use std::fs::DirBuilder;
use std::fs::File;
use std::path::PathBuf;

use crate::database::sstable::SegmentFiles;
use crate::database::wal::Wal;

pub type InMemoryTable = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

pub struct FileDirectory {
    database_dir: PathBuf,
    segment_files: SegmentFiles,
    wal: Wal,
}

impl FileDirectory {
    pub fn new() -> std::io::Result<Self> {
        let database_dir = std::env::temp_dir().join("simple_lsm_db");
        DirBuilder::new().recursive(true).create(&database_dir)?;

        let segment_files = SegmentFiles::new(&database_dir)?;
        let wal = Wal::new(&database_dir)?;

        Ok(Self {
            database_dir,
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
        self.segment_files.store(&self.database_dir, map)
    }
}
