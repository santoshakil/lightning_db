use bytes::Bytes;
use crate::core::error::Error;

#[derive(Debug, Clone, Copy)]
pub enum ColumnEncoding {
    Plain,
    Dictionary,
    RunLength,
    Delta,
}

#[derive(Debug, Clone, Copy)]
pub enum EncodingType {
    Plain,
    Dictionary,
    RunLength,
    Delta,
    BitPacked,
}

pub struct Encoder {
    encoding_type: EncodingType,
}

impl Encoder {
    pub fn new(encoding_type: EncodingType) -> Self {
        Self { encoding_type }
    }

    pub fn encode(&self, data: &Bytes) -> Result<Bytes, Error> {
        Ok(data.clone())
    }
}