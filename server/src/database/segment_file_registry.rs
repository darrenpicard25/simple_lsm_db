use std::{
    cmp::Ordering,
    collections::VecDeque,
    path::{Path, PathBuf},
};

use crate::database::{
    mem_table::MemTable,
    segment_file::{SEGMENT_FILE_EXTENSION, SegmentFile},
};

pub struct SegmentFileRegistry {
    segment_files: VecDeque<SegmentFile>,
    directory_path: PathBuf,
}

impl SegmentFileRegistry {
    pub fn new<P: AsRef<Path>>(directory_path: P) -> std::io::Result<Self> {
        let mut segment_files = Self::find_segment_files(&directory_path)?;

        segment_files.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));

        Ok(Self {
            segment_files: VecDeque::from(segment_files),
            directory_path: directory_path.as_ref().to_path_buf(),
        })
    }

    pub fn store_new(&mut self, map: MemTable) -> std::io::Result<PathBuf> {
        let segment_number = self.segment_files.len();

        let mut file_path = self
            .directory_path
            .join(format!("segment_{}", segment_number));
        file_path.set_extension(SEGMENT_FILE_EXTENSION);

        let segment_file = SegmentFile::create_and_store(file_path.clone(), map)?;
        self.segment_files.push_back(segment_file);

        Ok(file_path)
    }

    pub fn get(&self, file_path: &PathBuf) -> Option<&SegmentFile> {
        self.segment_files
            .iter()
            .find(|file| file.path().file_stem() == file_path.file_stem())
    }

    pub fn files(&self) -> impl Iterator<Item = &SegmentFile> {
        self.segment_files.iter()
    }

    fn find_segment_files<P: AsRef<Path>>(directory_path: P) -> std::io::Result<Vec<SegmentFile>> {
        Ok(std::fs::read_dir(directory_path.as_ref())?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_type()
                    .ok()
                    .and_then(|ft| ft.is_file().then_some(entry.path()))
            })
            .filter(SegmentFile::is_segment_file)
            .map(|path| SegmentFile::from_path(path))
            .collect::<Result<Vec<_>, _>>()?)
    }
}
