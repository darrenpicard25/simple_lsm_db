/// Commands that clients can send to the server
#[derive(Debug)]
pub enum Command<'a> {
    Get { key: &'a [u8] },
    Set { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

const GET: &[u8] = b"GET";
const SET: &[u8] = b"SET";
const DELETE: &[u8] = b"DELETE";
impl<'a> From<Command<'a>> for Vec<u8> {
    fn from(value: Command) -> Self {
        match value {
            Command::Get { key } => [GET, b" ", key].concat(),
            Command::Set { key, value } => [SET, b" ", key, b" ", value].concat(),
            Command::Delete { key } => [DELETE, b" ", key].concat(),
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for Command<'a> {
    type Error = std::io::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut parts = value.trim_ascii().splitn(3, |&b| b == b' ');

        match parts.next() {
            Some(GET) => Ok(Command::Get {
                key: parts.next().ok_or(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing key for GET command",
                ))?,
            }),
            Some(SET) => Ok(Command::Set {
                key: parts.next().ok_or(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing key for SET command",
                ))?,
                value: parts.next().ok_or(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing value for SET command",
                ))?,
            }),
            Some(DELETE) => Ok(Command::Delete {
                key: parts.next().ok_or(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Missing key for DELETE command",
                ))?,
            }),
            Some(unknown) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown command: {:?}", String::from_utf8_lossy(unknown)),
            )),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing command",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod deserialization {
        use super::*;

        #[test]
        fn test_get_command_deserialization() {
            let cmd = Command::try_from(b"get test".as_slice());
            assert!(matches!(cmd, Ok(Command::Get { key: b"test" })));
        }

        #[test]
        fn test_set_command_deserialization() {
            let cmd = Command::try_from(b"set test value".as_slice());
            assert!(matches!(
                cmd,
                Ok(Command::Set {
                    key: b"test",
                    value: b"value"
                })
            ));
        }

        #[test]
        fn test_delete_command_deserialization() {
            let cmd = Command::try_from(b"delete test".as_slice());
            assert!(matches!(cmd, Ok(Command::Delete { key: b"test" })));
        }

        #[test]
        fn test_unknown_command_deserialization() {
            let cmd = Command::try_from(b"unknown test".as_slice());
            assert!(cmd.is_err());
            let err = cmd.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            // The actual error message shows the unknown command as a byte array.
            let expected = format!("Unknown command: {:?}", b"unknown");
            assert_eq!(err.to_string(), expected);
        }
    }

    mod serialization {
        use super::*;

        #[test]
        fn test_get_command_serialization() {
            let cmd = Command::Get { key: b"test" };
            assert_eq!(Vec::<u8>::from(cmd), b"get test".to_vec());
        }

        #[test]
        fn test_set_command_serialization() {
            let cmd = Command::Set {
                key: b"test",
                value: b"value",
            };
            assert_eq!(Vec::<u8>::from(cmd), b"set test value".to_vec());
        }

        #[test]
        fn test_delete_command_serialization() {
            let cmd = Command::Delete { key: b"test" };
            assert_eq!(Vec::<u8>::from(cmd), b"delete test".to_vec());
        }
    }
}
