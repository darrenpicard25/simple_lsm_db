#[derive(Debug)]
pub enum Response {
    Ok(Option<Vec<u8>>),
    Err(String),
    Success,
}

impl From<Response> for Vec<u8> {
    fn from(value: Response) -> Self {
        match value {
            Response::Ok(Some(value)) => {
                [b"OK: ".as_slice(), value.as_slice(), b"\n".as_slice()].concat()
            }
            Response::Ok(None) => [b"OK:".as_slice(), b"\n".as_slice()].concat(),
            Response::Err(error) => {
                [b"ERROR: ".as_slice(), error.as_bytes(), b"\n".as_slice()].concat()
            }
            Response::Success => [b"OK".as_slice(), b"\n".as_slice()].concat(),
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for Response {
    type Error = std::io::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut parts = value.trim_ascii().splitn(2, |&b| b == b' ');

        let first_part = parts.next().ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Missing first part of response",
        ))?;

        match first_part {
            b"OK:" => Ok(Response::Ok(parts.next().map(|v| v.to_vec()))),
            b"ERROR:" => Ok(Response::Err(
                parts
                    .next()
                    .map(|message| String::from_utf8_lossy(message).to_string())
                    .unwrap_or_default(),
            )),
            b"OK" => Ok(Response::Success),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unknown response type",
            )),
        }
    }
}
