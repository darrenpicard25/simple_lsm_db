pub struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
}

impl IndexEntry {
    pub fn new(key: Vec<u8>, offset: u64) -> Self {
        Self { key, offset }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl TryFrom<Vec<u8>> for IndexEntry {
    type Error = std::io::Error;

    fn try_from(mut value: Vec<u8>) -> Result<Self, Self::Error> {
        let len = value.len();
        if len < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Vec too short to contain offset",
            ));
        }
        let offset_bytes = value.split_off(len - 8);
        let offset = u64::from_le_bytes(offset_bytes.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid offset bytes")
        })?);
        Ok(Self::new(value, offset))
    }
}

impl From<IndexEntry> for Vec<u8> {
    fn from(value: IndexEntry) -> Self {
        let IndexEntry { mut key, offset } = value;
        key.reserve(std::mem::size_of::<u64>() + 1);
        key.extend_from_slice(&offset.to_le_bytes());
        key.push(b'\n');
        key
    }
}
