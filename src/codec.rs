use bincode::DefaultOptions;
use bincode::Options;
use serde::{de::DeserializeOwned, Serialize};
use std::io::Read;
use std::io::Write;
use thiserror::Error;

pub trait Codec: Clone {
    fn encode<T, B>(&self, element: &T, buffer: &mut B) -> Result<(), EncodeError>
    where
        T: Serialize,
        B: Write;

    fn decode<T, B>(&self, buffer: &mut B) -> Result<T, DecodeError>
    where
        T: DeserializeOwned,
        B: Read;
}

#[derive(Error, Debug)]
pub enum DecodeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("malformed input: {0}")]
    Encoding(#[from] bincode::Error),
}

#[derive(Error, Debug)]
pub enum EncodeError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("failed to serialize: {0}")]
    Encoding(#[from] bincode::Error),
}

#[derive(Default, Clone, Debug)]
pub struct BincodeCodec;

impl BincodeCodec {
    fn serde_config(&self) -> impl Options {
        DefaultOptions::new().allow_trailing_bytes()
    }
}

impl Codec for BincodeCodec {
    fn encode<T, B>(&self, element: &T, buffer: &mut B) -> Result<(), EncodeError>
    where
        T: Serialize,
        B: Write,
    {
        self.serde_config().serialize_into(buffer, element)?;
        Ok(())
    }

    fn decode<T, B>(&self, buffer: &mut B) -> Result<T, DecodeError>
    where
        T: DeserializeOwned,
        B: Read,
    {
        let element = self.serde_config().deserialize_from(buffer)?;
        Ok(element)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn encode_decode() {
        let encoder = BincodeCodec::default();
        let mut buffer: Vec<u8> = vec![0; 100];
        let element: Vec<u32> = vec![1, 2, 3];
        encoder
            .encode(&element, &mut Cursor::new(&mut buffer))
            .unwrap();
        let decoded_element: Vec<u32> = encoder.decode(&mut Cursor::new(&mut buffer)).unwrap();
        assert_eq!(decoded_element, element);
    }
}
