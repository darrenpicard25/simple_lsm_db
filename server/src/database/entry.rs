#[derive(Debug)]
pub enum Entry<'a> {
    KeyValue { key: &'a [u8], value: &'a [u8] },
    Tombstone { key: &'a [u8] },
}

impl<'a> TryFrom<&'a [u8]> for Entry<'a> {
    type Error = std::io::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut parts = value.splitn(2, |&b| b == b' ');

        let key = parts.next().ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Missing key",
        ))?;

        match parts.next() {
            Some(value) => Ok(Entry::KeyValue { key, value }),
            None => Ok(Entry::Tombstone { key }),
        }
    }
}

impl<'a> From<Entry<'a>> for Vec<u8> {
    fn from(value: Entry<'a>) -> Self {
        match value {
            Entry::KeyValue { key, value } => [key, b" ", value, b"\n"].concat(),
            Entry::Tombstone { key } => [key, b"\n"].concat(),
        }
    }
}
