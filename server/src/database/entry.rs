#[derive(Debug)]
pub enum Entry {
    KeyValue { key: Vec<u8>, value: Vec<u8> },
    Tombstone { key: Vec<u8> },
}

impl From<Vec<u8>> for Entry {
    fn from(value: Vec<u8>) -> Self {
        match value.iter().position(|&b| b == b' ') {
            Some(at) => {
                let mut key = value;
                let value = key.split_off(at);
                Entry::KeyValue { key, value }
            }
            None => Entry::Tombstone { key: value },
        }
    }
}

impl<'a> From<&'a [u8]> for Entry {
    fn from(value: &'a [u8]) -> Self {
        let separator_pos = value.iter().position(|&b| b == b' ');

        match separator_pos {
            Some(at) => {
                let key = value[..at].to_vec();
                let value = value[at + 1..].to_vec();
                Entry::KeyValue { key, value }
            }
            None => Entry::Tombstone {
                key: value.to_vec(),
            },
        }
    }
}

impl From<Entry> for Vec<u8> {
    fn from(value: Entry) -> Self {
        match value {
            Entry::KeyValue { mut key, mut value } => {
                key.push(b' ');
                key.append(&mut value);
                key.push(b'\n');

                key
            }
            Entry::Tombstone { mut key } => {
                key.push(b'\n');
                key
            }
        }
    }
}
