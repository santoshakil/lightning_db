use bytes::{Bytes, BytesMut, Buf, BufMut};
use crate::core::error::Error;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnEncoding {
    Plain,
    Dictionary,
    RunLength,
    Delta,
    BitPacked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingType {
    Plain,
    Dictionary,
    RunLength,
    Delta,
    DeltaOfDelta,
    BitPacked,
    Varint,
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    pub values: Vec<Bytes>,
    pub indices: HashMap<Bytes, u32>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            indices: HashMap::new(),
        }
    }

    pub fn add_value(&mut self, value: Bytes) -> u32 {
        if let Some(&index) = self.indices.get(&value) {
            return index;
        }
        
        let index = self.values.len() as u32;
        self.values.push(value.clone());
        self.indices.insert(value, index);
        index
    }

    pub fn get_value(&self, index: u32) -> Option<&Bytes> {
        self.values.get(index as usize)
    }

    pub fn size(&self) -> usize {
        self.values.len()
    }

    pub fn encode_to_bytes(&self) -> Result<Bytes, Error> {
        let mut buffer = BytesMut::new();
        
        buffer.put_u32_le(self.values.len() as u32);
        
        for value in &self.values {
            buffer.put_u32_le(value.len() as u32);
            buffer.put(value.as_ref());
        }
        
        Ok(buffer.freeze())
    }

    pub fn decode_from_bytes(data: &mut impl Buf) -> Result<Self, Error> {
        if data.remaining() < 4 {
            return Err(Error::InvalidData("Not enough data for dictionary size".to_string()));
        }
        
        let dict_size = data.get_u32_le();
        let mut values = Vec::with_capacity(dict_size as usize);
        let mut indices = HashMap::new();
        
        for i in 0..dict_size {
            if data.remaining() < 4 {
                return Err(Error::InvalidData("Not enough data for value length".to_string()));
            }
            
            let value_len = data.get_u32_le() as usize;
            
            if data.remaining() < value_len {
                return Err(Error::InvalidData("Not enough data for value".to_string()));
            }
            
            let mut value_bytes = vec![0u8; value_len];
            data.copy_to_slice(&mut value_bytes);
            let value = Bytes::from(value_bytes);
            
            indices.insert(value.clone(), i);
            values.push(value);
        }
        
        Ok(Dictionary { values, indices })
    }
}

pub struct Encoder {
    encoding_type: EncodingType,
}

impl Encoder {
    pub fn new(encoding_type: EncodingType) -> Self {
        Self { encoding_type }
    }

    pub fn encode(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        match self.encoding_type {
            EncodingType::Plain => Ok((data.clone(), None)),
            EncodingType::Dictionary => self.encode_dictionary(data),
            EncodingType::RunLength => self.encode_run_length(data),
            EncodingType::Delta => self.encode_delta(data),
            EncodingType::DeltaOfDelta => self.encode_delta_of_delta(data),
            EncodingType::BitPacked => self.encode_bit_packed(data),
            EncodingType::Varint => self.encode_varint(data),
        }
    }

    pub fn decode(&self, data: &Bytes, dictionary: Option<&Dictionary>) -> Result<Bytes, Error> {
        match self.encoding_type {
            EncodingType::Plain => Ok(data.clone()),
            EncodingType::Dictionary => self.decode_dictionary(data, dictionary),
            EncodingType::RunLength => self.decode_run_length(data),
            EncodingType::Delta => self.decode_delta(data),
            EncodingType::DeltaOfDelta => self.decode_delta_of_delta(data),
            EncodingType::BitPacked => self.decode_bit_packed(data),
            EncodingType::Varint => self.decode_varint(data),
        }
    }

    fn encode_dictionary(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        if data.is_empty() {
            return Ok((data.clone(), None));
        }

        let mut dictionary = Dictionary::new();
        let mut buffer = BytesMut::new();
        let mut cursor = 0;

        while cursor < data.len() {
            if cursor + 4 > data.len() {
                break;
            }
            
            let value_len = u32::from_le_bytes([
                data[cursor], data[cursor + 1], 
                data[cursor + 2], data[cursor + 3]
            ]) as usize;
            cursor += 4;
            
            if cursor + value_len > data.len() {
                break;
            }
            
            let value = Bytes::from(data[cursor..cursor + value_len].to_vec());
            cursor += value_len;
            
            let index = dictionary.add_value(value);
            buffer.put_u32_le(index);
        }

        Ok((buffer.freeze(), Some(dictionary)))
    }

    fn decode_dictionary(&self, data: &Bytes, dictionary: Option<&Dictionary>) -> Result<Bytes, Error> {
        let dict = dictionary.ok_or_else(|| Error::InvalidData("Dictionary required for decoding".to_string()))?;
        let mut buffer = BytesMut::new();
        let mut cursor = 0;

        while cursor + 4 <= data.len() {
            let index = u32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]);
            cursor += 4;

            let value = dict.get_value(index)
                .ok_or_else(|| Error::InvalidData(format!("Dictionary index {} not found", index)))?;
            
            buffer.put_u32_le(value.len() as u32);
            buffer.put(value.as_ref());
        }

        Ok(buffer.freeze())
    }

    fn encode_run_length(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        if data.is_empty() {
            return Ok((data.clone(), None));
        }

        let mut buffer = BytesMut::new();
        let mut i = 0;

        while i < data.len() {
            let current_byte = data[i];
            let mut count = 1u32;
            let mut j = i + 1;

            while j < data.len() && data[j] == current_byte && count < u32::MAX {
                count += 1;
                j += 1;
            }

            buffer.put_u8(current_byte);
            buffer.put_u32_le(count);
            i = j;
        }

        Ok((buffer.freeze(), None))
    }

    fn decode_run_length(&self, data: &Bytes) -> Result<Bytes, Error> {
        let mut buffer = BytesMut::new();
        let mut cursor = 0;

        while cursor + 5 <= data.len() {
            let value = data[cursor];
            cursor += 1;
            
            let count = u32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]);
            cursor += 4;

            for _ in 0..count {
                buffer.put_u8(value);
            }
        }

        Ok(buffer.freeze())
    }

    fn encode_delta(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        if data.len() < 4 {
            return Ok((data.clone(), None));
        }

        let mut buffer = BytesMut::new();
        let mut cursor = 0;
        
        let first_value = i32::from_le_bytes([
            data[cursor], data[cursor + 1],
            data[cursor + 2], data[cursor + 3]
        ]);
        buffer.put_i32_le(first_value);
        cursor += 4;
        
        let mut previous = first_value;
        
        while cursor + 4 <= data.len() {
            let current = i32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]);
            cursor += 4;
            
            let delta = current - previous;
            buffer.put_i32_le(delta);
            previous = current;
        }

        Ok((buffer.freeze(), None))
    }

    fn decode_delta(&self, data: &Bytes) -> Result<Bytes, Error> {
        if data.len() < 4 {
            return Ok(data.clone());
        }

        let mut buffer = BytesMut::new();
        let mut cursor = 0;
        
        let first_value = i32::from_le_bytes([
            data[cursor], data[cursor + 1],
            data[cursor + 2], data[cursor + 3]
        ]);
        buffer.put_i32_le(first_value);
        cursor += 4;
        
        let mut previous = first_value;
        
        while cursor + 4 <= data.len() {
            let delta = i32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]);
            cursor += 4;
            
            let current = previous + delta;
            buffer.put_i32_le(current);
            previous = current;
        }

        Ok(buffer.freeze())
    }

    fn encode_delta_of_delta(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        if data.len() < 8 {
            return Ok((data.clone(), None));
        }

        let mut buffer = BytesMut::new();
        let mut cursor = 0;
        
        let first = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let second = i32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        
        buffer.put_i32_le(first);
        buffer.put_i32_le(second);
        cursor += 8;
        
        let mut prev_delta = second - first;
        
        while cursor + 4 <= data.len() {
            let current = i32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]);
            cursor += 4;
            
            let delta = current - (first + prev_delta);
            let delta_of_delta = delta - prev_delta;
            
            buffer.put_i32_le(delta_of_delta);
            prev_delta = delta;
        }

        Ok((buffer.freeze(), None))
    }

    fn decode_delta_of_delta(&self, data: &Bytes) -> Result<Bytes, Error> {
        if data.len() < 8 {
            return Ok(data.clone());
        }

        let mut buffer = BytesMut::new();
        let mut cursor = 0;
        
        let first = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let second = i32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        
        buffer.put_i32_le(first);
        buffer.put_i32_le(second);
        cursor += 8;
        
        let mut prev_value = second;
        let mut prev_delta = second - first;
        
        while cursor + 4 <= data.len() {
            let delta_of_delta = i32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]);
            cursor += 4;
            
            let current_delta = prev_delta + delta_of_delta;
            let current_value = prev_value + current_delta;
            
            buffer.put_i32_le(current_value);
            prev_value = current_value;
            prev_delta = current_delta;
        }

        Ok(buffer.freeze())
    }

    fn encode_bit_packed(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        Ok((data.clone(), None))
    }

    fn decode_bit_packed(&self, data: &Bytes) -> Result<Bytes, Error> {
        Ok(data.clone())
    }

    fn encode_varint(&self, data: &Bytes) -> Result<(Bytes, Option<Dictionary>), Error> {
        let mut buffer = BytesMut::new();
        let mut cursor = 0;
        
        while cursor + 4 <= data.len() {
            let value = u32::from_le_bytes([
                data[cursor], data[cursor + 1],
                data[cursor + 2], data[cursor + 3]
            ]) as u64;
            cursor += 4;
            
            self.encode_varint_u64(&mut buffer, value);
        }
        
        Ok((buffer.freeze(), None))
    }

    fn decode_varint(&self, data: &Bytes) -> Result<Bytes, Error> {
        let mut buffer = BytesMut::new();
        let mut cursor = 0;
        
        while cursor < data.len() {
            let (value, bytes_read) = self.decode_varint_u64(&data[cursor..])?
                .ok_or_else(|| Error::InvalidData("Invalid varint encoding".to_string()))?;
            
            buffer.put_u32_le(value as u32);
            cursor += bytes_read;
        }
        
        Ok(buffer.freeze())
    }

    fn encode_varint_u64(&self, buffer: &mut BytesMut, mut value: u64) {
        while value >= 0x80 {
            buffer.put_u8((value & 0xFF) as u8 | 0x80);
            value >>= 7;
        }
        buffer.put_u8(value as u8);
    }

    fn decode_varint_u64(&self, data: &[u8]) -> Result<Option<(u64, usize)>, Error> {
        let mut result = 0u64;
        let mut shift = 0;
        
        for (i, &byte) in data.iter().enumerate() {
            if shift >= 64 {
                return Err(Error::InvalidData("Varint too long".to_string()));
            }
            
            result |= ((byte & 0x7F) as u64) << shift;
            
            if byte & 0x80 == 0 {
                return Ok(Some((result, i + 1)));
            }
            
            shift += 7;
        }
        
        Ok(None)
    }
}