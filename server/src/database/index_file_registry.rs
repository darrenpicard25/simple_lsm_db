use std::path::{Path, PathBuf};

use crate::database::{
    index_entry::IndexEntry,
    index_file::{INDEX_FILE_EXTENSION, IndexFile},
};

pub struct IndexFileRegistry {
    index_files: Vec<IndexFile>,
    directory_path: PathBuf,
}

impl IndexFileRegistry {
    pub fn new<P: AsRef<Path>>(directory_path: P) -> std::io::Result<Self> {
        let index_files = Self::find_index_files(&directory_path)?;
        Ok(Self {
            index_files,
            directory_path: directory_path.as_ref().to_path_buf(),
        })
    }

    fn find_index_files<P: AsRef<Path>>(directory_path: P) -> std::io::Result<Vec<IndexFile>> {
        Ok(std::fs::read_dir(directory_path.as_ref())?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_type()
                    .ok()
                    .and_then(|ft| ft.is_file().then_some(entry.path()))
            })
            .filter(IndexFile::is_index_file)
            .map(|path| IndexFile::from_path(path))
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get(&self, file_path: &PathBuf) -> Option<&IndexFile> {
        self.index_files
            .iter()
            .find(|file| file.path().file_stem() == file_path.file_stem())
    }

    pub fn store_new(
        &mut self,
        mut path: PathBuf,
        entries: Vec<IndexEntry>,
    ) -> std::io::Result<PathBuf> {
        path.set_extension(INDEX_FILE_EXTENSION);
        let index_file = IndexFile::create_and_store(path.clone(), entries)?;
        self.index_files.push(index_file);
        Ok(path)
    }
}
