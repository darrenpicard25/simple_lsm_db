use std::collections::BTreeMap;

use crate::database::entry::Entry;

pub type Table = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
const DEFAULT_MAX_TABLE_SIZE: usize = 1000;

#[derive(Clone)]
pub struct MemTable {
    table: Table,
    max_table_size: usize,
}

impl MemTable {
    pub fn new(max_table_size: Option<usize>) -> Self {
        Self {
            table: BTreeMap::new(),
            max_table_size: max_table_size.unwrap_or(DEFAULT_MAX_TABLE_SIZE),
        }
    }

    /// Returns the value for the given key if it exists, otherwise returns None
    /// If the key is a tombstone, returns Some(None)
    /// If the key is not found, returns None
    pub fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.table.get(key)
    }

    pub fn should_flush(&self) -> bool {
        self.table.len() >= self.max_table_size
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.table.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub fn remove(&mut self, key: &[u8]) {
        self.table.insert(key.to_vec(), None);
    }

    pub fn clear(&mut self) {
        self.table.clear();
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns an iterator over references to the entries in the table
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &Option<Vec<u8>>)> {
        self.table.iter().map(|(k, v)| (k.as_slice(), v))
    }

    pub fn from_iter<T: IntoIterator<Item = Entry>>(
        iter: T,
        max_table_size: Option<usize>,
    ) -> Self {
        let mut table = Table::new();

        for entry in iter {
            match entry {
                Entry::KeyValue { key, value } => table.insert(key, Some(value)),
                Entry::Tombstone { key } => table.insert(key, None),
            };
        }

        Self {
            table,
            max_table_size: max_table_size.unwrap_or(DEFAULT_MAX_TABLE_SIZE),
        }
    }
}

impl IntoIterator for MemTable {
    type Item = Entry;

    type IntoIter = std::iter::Map<
        std::collections::btree_map::IntoIter<Vec<u8>, Option<Vec<u8>>>,
        fn((Vec<u8>, Option<Vec<u8>>)) -> Entry,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.table.into_iter().map(|(key, value)| match value {
            Some(value) => Entry::KeyValue { key, value },
            None => Entry::Tombstone { key },
        })
    }
}
