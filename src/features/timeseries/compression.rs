use std::io::{Read, Write};
use bytes::{Bytes, BytesMut, BufMut};
use crate::core::error::Error;
use super::engine::DataPoint;
use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;

const GORILLA_BLOCK_SIZE: usize = 128;
const XOR_THRESHOLD: u32 = 11;

#[derive(Debug, Clone, Copy)]
pub enum CompressionAlgorithm {
    None,
    Delta,
    Gorilla,
    Zstd,
    Snappy,
    DoubleDelta,
}

pub struct TimeSeriesCompressor {
    algorithm: CompressionAlgorithm,
    compression_level: u32,
}

impl TimeSeriesCompressor {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            compression_level: 6,
        }
    }

    pub fn with_level(algorithm: CompressionAlgorithm, level: u32) -> Self {
        Self {
            algorithm,
            compression_level: level,
        }
    }

    pub fn compress(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        if points.is_empty() {
            return Ok(Bytes::new());
        }

        match self.algorithm {
            CompressionAlgorithm::None => self.compress_none(points),
            CompressionAlgorithm::Delta => self.compress_delta(points),
            CompressionAlgorithm::Gorilla => self.compress_gorilla(points),
            CompressionAlgorithm::Zstd => self.compress_zstd(points),
            CompressionAlgorithm::Snappy => self.compress_snappy(points),
            CompressionAlgorithm::DoubleDelta => self.compress_double_delta(points),
        }
    }

    pub fn decompress(&self, data: &Bytes, expected_count: usize) -> Result<Vec<DataPoint>, Error> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        match self.algorithm {
            CompressionAlgorithm::None => self.decompress_none(data),
            CompressionAlgorithm::Delta => self.decompress_delta(data, expected_count),
            CompressionAlgorithm::Gorilla => self.decompress_gorilla(data, expected_count),
            CompressionAlgorithm::Zstd => self.decompress_zstd(data, expected_count),
            CompressionAlgorithm::Snappy => self.decompress_snappy(data, expected_count),
            CompressionAlgorithm::DoubleDelta => self.decompress_double_delta(data, expected_count),
        }
    }

    fn compress_none(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        let mut buf = BytesMut::with_capacity(points.len() * std::mem::size_of::<DataPoint>());
        
        for point in points {
            buf.put_u64_le(point.timestamp);
            buf.put_f64_le(point.value);
            buf.put_u64_le(point.tags);
        }

        Ok(buf.freeze())
    }

    fn decompress_none(&self, data: &Bytes) -> Result<Vec<DataPoint>, Error> {
        let point_size = std::mem::size_of::<DataPoint>();
        let count = data.len() / point_size;
        let mut points = Vec::with_capacity(count);
        let mut cursor = 0;

        while cursor + point_size <= data.len() {
            let timestamp = u64::from_le_bytes(
                data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
            );
            cursor += 8;

            let value = f64::from_le_bytes(
                data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
            );
            cursor += 8;

            let tags = u64::from_le_bytes(
                data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
            );
            cursor += 8;

            points.push(DataPoint { timestamp, value, tags });
        }

        Ok(points)
    }

    fn compress_delta(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        if points.is_empty() {
            return Ok(Bytes::new());
        }

        let mut buf = BytesMut::with_capacity(points.len() * 12);
        
        buf.put_u32_le(points.len() as u32);
        buf.put_u64_le(points[0].timestamp);
        buf.put_f64_le(points[0].value);
        buf.put_u64_le(points[0].tags);

        let mut prev_timestamp = points[0].timestamp;
        let mut prev_value_bits = points[0].value.to_bits();

        for point in &points[1..] {
            let delta_time = (point.timestamp - prev_timestamp) as i64;
            self.encode_varint(&mut buf, delta_time);

            let value_bits = point.value.to_bits();
            let xor_value = value_bits ^ prev_value_bits;
            buf.put_u64_le(xor_value);

            let tag_delta = (point.tags as i64) - (points[0].tags as i64);
            self.encode_varint(&mut buf, tag_delta);

            prev_timestamp = point.timestamp;
            prev_value_bits = value_bits;
        }

        Ok(buf.freeze())
    }

    fn decompress_delta(&self, data: &Bytes, _expected_count: usize) -> Result<Vec<DataPoint>, Error> {
        if data.len() < 4 {
            return Err(Error::Corruption);
        }

        let mut cursor = 0;
        let count = u32::from_le_bytes(
            data[cursor..cursor + 4].try_into().map_err(|_| Error::Corruption)?
        ) as usize;
        cursor += 4;

        let mut points = Vec::with_capacity(count);

        if count == 0 {
            return Ok(points);
        }

        let first_timestamp = u64::from_le_bytes(
            data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
        );
        cursor += 8;

        let first_value = f64::from_le_bytes(
            data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
        );
        cursor += 8;

        let first_tags = u64::from_le_bytes(
            data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
        );
        cursor += 8;

        points.push(DataPoint {
            timestamp: first_timestamp,
            value: first_value,
            tags: first_tags,
        });

        let mut prev_timestamp = first_timestamp;
        let mut prev_value_bits = first_value.to_bits();

        for _ in 1..count {
            let (delta_time, bytes_read) = self.decode_varint(&data[cursor..])?;
            cursor += bytes_read;
            let timestamp = (prev_timestamp as i64 + delta_time) as u64;

            let xor_value = u64::from_le_bytes(
                data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
            );
            cursor += 8;
            let value_bits = xor_value ^ prev_value_bits;
            let value = f64::from_bits(value_bits);

            let (tag_delta, bytes_read) = self.decode_varint(&data[cursor..])?;
            cursor += bytes_read;
            let tags = (first_tags as i64 + tag_delta) as u64;

            points.push(DataPoint { timestamp, value, tags });

            prev_timestamp = timestamp;
            prev_value_bits = value_bits;
        }

        Ok(points)
    }

    fn compress_gorilla(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        let mut encoder = GorillaEncoder::new();
        
        for point in points {
            encoder.encode_timestamp(point.timestamp);
            encoder.encode_value(point.value);
        }

        Ok(encoder.finish())
    }

    fn decompress_gorilla(&self, data: &Bytes, expected_count: usize) -> Result<Vec<DataPoint>, Error> {
        let mut decoder = GorillaDecoder::new(data);
        let mut points = Vec::with_capacity(expected_count);

        while let Some((timestamp, value)) = decoder.decode_next()? {
            points.push(DataPoint::new(timestamp, value));
        }

        Ok(points)
    }

    fn compress_double_delta(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        if points.len() < 2 {
            return self.compress_delta(points);
        }

        let mut buf = BytesMut::with_capacity(points.len() * 8);
        
        buf.put_u32_le(points.len() as u32);
        buf.put_u64_le(points[0].timestamp);
        buf.put_f64_le(points[0].value);

        if points.len() > 1 {
            let delta1 = (points[1].timestamp - points[0].timestamp) as i64;
            self.encode_varint(&mut buf, delta1);
            buf.put_f64_le(points[1].value);

            let mut prev_delta = delta1;
            for i in 2..points.len() {
                let delta = (points[i].timestamp - points[i-1].timestamp) as i64;
                let delta_of_delta = delta - prev_delta;
                self.encode_varint(&mut buf, delta_of_delta);
                
                let value_delta = (points[i].value - points[i-1].value).to_bits() as i64;
                self.encode_varint(&mut buf, value_delta);
                
                prev_delta = delta;
            }
        }

        Ok(buf.freeze())
    }

    fn decompress_double_delta(&self, data: &Bytes, _expected_count: usize) -> Result<Vec<DataPoint>, Error> {
        if data.len() < 4 {
            return Err(Error::Corruption);
        }

        let mut cursor = 0;
        let count = u32::from_le_bytes(
            data[cursor..cursor + 4].try_into().map_err(|_| Error::Corruption)?
        ) as usize;
        cursor += 4;

        if count == 0 {
            return Ok(Vec::new());
        }

        let mut points = Vec::with_capacity(count);

        let first_timestamp = u64::from_le_bytes(
            data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
        );
        cursor += 8;

        let first_value = f64::from_le_bytes(
            data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
        );
        cursor += 8;

        points.push(DataPoint::new(first_timestamp, first_value));

        if count > 1 {
            let (delta1, bytes_read) = self.decode_varint(&data[cursor..])?;
            cursor += bytes_read;

            let second_value = f64::from_le_bytes(
                data[cursor..cursor + 8].try_into().map_err(|_| Error::Corruption)?
            );
            cursor += 8;

            let second_timestamp = (first_timestamp as i64 + delta1) as u64;
            points.push(DataPoint::new(second_timestamp, second_value));

            let mut prev_delta = delta1;
            let mut prev_timestamp = second_timestamp;
            let mut prev_value = second_value;

            for _ in 2..count {
                let (delta_of_delta, bytes_read) = self.decode_varint(&data[cursor..])?;
                cursor += bytes_read;

                let delta = prev_delta + delta_of_delta;
                let timestamp = (prev_timestamp as i64 + delta) as u64;

                let (value_delta_bits, bytes_read) = self.decode_varint(&data[cursor..])?;
                cursor += bytes_read;
                let value = f64::from_bits((prev_value.to_bits() as i64 + value_delta_bits) as u64);

                points.push(DataPoint::new(timestamp, value));

                prev_delta = delta;
                prev_timestamp = timestamp;
                prev_value = value;
            }
        }

        Ok(points)
    }

    fn compress_zstd(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        let raw = self.compress_none(points)?;
        
        #[cfg(feature = "zstd-compression")]
        {
            use zstd::stream::encode_all;
            let compressed = encode_all(raw.as_ref(), self.compression_level as i32)
                .map_err(|_| Error::CompressionError)?;
            Ok(Bytes::from(compressed))
        }
        
        #[cfg(not(feature = "zstd-compression"))]
        Ok(raw)
    }

    fn decompress_zstd(&self, data: &Bytes, _expected_count: usize) -> Result<Vec<DataPoint>, Error> {
        #[cfg(feature = "zstd-compression")]
        {
            use zstd::stream::decode_all;
            let decompressed = decode_all(data.as_ref())
                .map_err(|_| Error::CompressionError)?;
            self.decompress_none(&Bytes::from(decompressed))
        }
        
        #[cfg(not(feature = "zstd-compression"))]
        self.decompress_none(data)
    }

    fn compress_snappy(&self, points: &[DataPoint]) -> Result<Bytes, Error> {
        let raw = self.compress_none(points)?;
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(raw.as_ref())
            .map_err(|_| Error::CompressionError)?;
        Ok(Bytes::from(compressed))
    }

    fn decompress_snappy(&self, data: &Bytes, _expected_count: usize) -> Result<Vec<DataPoint>, Error> {
        let mut decoder = snap::raw::Decoder::new();
        let decompressed = decoder.decompress_vec(data.as_ref())
            .map_err(|_| Error::CompressionError)?;
        self.decompress_none(&Bytes::from(decompressed))
    }

    fn encode_varint(&self, buf: &mut BytesMut, mut value: i64) {
        let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
        
        while zigzag >= 0x80 {
            buf.put_u8((zigzag as u8) | 0x80);
            zigzag >>= 7;
        }
        buf.put_u8(zigzag as u8);
    }

    fn decode_varint(&self, data: &[u8]) -> Result<(i64, usize), Error> {
        let mut result = 0u64;
        let mut shift = 0;
        let mut bytes_read = 0;

        for &byte in data {
            bytes_read += 1;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                let zigzag = result;
                let value = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
                return Ok((value, bytes_read));
            }
            shift += 7;
            if shift >= 64 {
                return Err(Error::Corruption);
            }
        }

        Err(Error::Corruption)
    }
}

struct GorillaEncoder {
    buffer: BytesMut,
    bit_buffer: u64,
    bit_count: usize,
    prev_timestamp: Option<u64>,
    prev_delta: Option<i64>,
    prev_value: Option<u64>,
}

impl GorillaEncoder {
    fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(1024),
            bit_buffer: 0,
            bit_count: 0,
            prev_timestamp: None,
            prev_delta: None,
            prev_value: None,
        }
    }

    fn encode_timestamp(&mut self, timestamp: u64) {
        if self.prev_timestamp.is_none() {
            self.write_bits(timestamp, 64);
            self.prev_timestamp = Some(timestamp);
            return;
        }

        let prev = self.prev_timestamp.unwrap();
        let delta = (timestamp as i64) - (prev as i64);

        if let Some(prev_delta) = self.prev_delta {
            let delta_of_delta = delta - prev_delta;
            
            if delta_of_delta == 0 {
                self.write_bits(0, 1);
            } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
                self.write_bits(0b10, 2);
                self.write_bits((delta_of_delta + 63) as u64, 7);
            } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
                self.write_bits(0b110, 3);
                self.write_bits((delta_of_delta + 255) as u64, 9);
            } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
                self.write_bits(0b1110, 4);
                self.write_bits((delta_of_delta + 2047) as u64, 12);
            } else {
                self.write_bits(0b1111, 4);
                self.write_bits(delta as u64, 32);
            }
        } else {
            self.write_bits(delta as u64, 14);
        }

        self.prev_timestamp = Some(timestamp);
        self.prev_delta = Some(delta);
    }

    fn encode_value(&mut self, value: f64) {
        let bits = value.to_bits();
        
        if let Some(prev) = self.prev_value {
            let xor = bits ^ prev;
            
            if xor == 0 {
                self.write_bits(0, 1);
            } else {
                self.write_bits(1, 1);
                
                let leading_zeros = xor.leading_zeros();
                let trailing_zeros = xor.trailing_zeros();
                
                if leading_zeros >= 32 {
                    self.write_bits(0, 1);
                    self.write_bits(xor, 32);
                } else {
                    self.write_bits(1, 1);
                    self.write_bits(leading_zeros as u64, 5);
                    let significant_bits = 64 - leading_zeros - trailing_zeros;
                    self.write_bits(significant_bits as u64, 6);
                    self.write_bits(xor >> trailing_zeros, significant_bits as usize);
                }
            }
        } else {
            self.write_bits(bits, 64);
        }

        self.prev_value = Some(bits);
    }

    fn write_bits(&mut self, value: u64, bits: usize) {
        self.bit_buffer |= value << self.bit_count;
        self.bit_count += bits;
        
        while self.bit_count >= 8 {
            self.buffer.put_u8(self.bit_buffer as u8);
            self.bit_buffer >>= 8;
            self.bit_count -= 8;
        }
    }

    fn finish(mut self) -> Bytes {
        if self.bit_count > 0 {
            self.buffer.put_u8(self.bit_buffer as u8);
        }
        self.buffer.freeze()
    }
}

struct GorillaDecoder {
    data: Bytes,
    cursor: usize,
    bit_buffer: u64,
    bit_count: usize,
    prev_timestamp: Option<u64>,
    prev_delta: Option<i64>,
    prev_value: Option<u64>,
}

impl GorillaDecoder {
    fn new(data: &Bytes) -> Self {
        Self {
            data: data.clone(),
            cursor: 0,
            bit_buffer: 0,
            bit_count: 0,
            prev_timestamp: None,
            prev_delta: None,
            prev_value: None,
        }
    }

    fn decode_next(&mut self) -> Result<Option<(u64, f64)>, Error> {
        if self.cursor >= self.data.len() && self.bit_count == 0 {
            return Ok(None);
        }

        let timestamp = self.decode_timestamp()?;
        let value = self.decode_value()?;
        
        Ok(Some((timestamp, value)))
    }

    fn decode_timestamp(&mut self) -> Result<u64, Error> {
        if self.prev_timestamp.is_none() {
            let timestamp = self.read_bits(64)?;
            self.prev_timestamp = Some(timestamp);
            return Ok(timestamp);
        }

        let prev = self.prev_timestamp.unwrap();
        
        let delta = if let Some(prev_delta) = self.prev_delta {
            let marker = self.read_bits(1)?;
            if marker == 0 {
                prev_delta
            } else {
                let marker = self.read_bits(1)?;
                if marker == 0 {
                    let delta_of_delta = self.read_bits(7)? as i64 - 63;
                    prev_delta + delta_of_delta
                } else {
                    let marker = self.read_bits(1)?;
                    if marker == 0 {
                        let delta_of_delta = self.read_bits(9)? as i64 - 255;
                        prev_delta + delta_of_delta
                    } else {
                        let marker = self.read_bits(1)?;
                        if marker == 0 {
                            let delta_of_delta = self.read_bits(12)? as i64 - 2047;
                            prev_delta + delta_of_delta
                        } else {
                            self.read_bits(32)? as i64
                        }
                    }
                }
            }
        } else {
            self.read_bits(14)? as i64
        };

        let timestamp = (prev as i64 + delta) as u64;
        self.prev_timestamp = Some(timestamp);
        self.prev_delta = Some(delta);
        
        Ok(timestamp)
    }

    fn decode_value(&mut self) -> Result<f64, Error> {
        if self.prev_value.is_none() {
            let bits = self.read_bits(64)?;
            self.prev_value = Some(bits);
            return Ok(f64::from_bits(bits));
        }

        let prev = self.prev_value.unwrap();
        
        let marker = self.read_bits(1)?;
        let bits = if marker == 0 {
            prev
        } else {
            let control = self.read_bits(1)?;
            let xor = if control == 0 {
                self.read_bits(32)?
            } else {
                let leading = self.read_bits(5)? as u32;
                let significant = self.read_bits(6)? as u32;
                let value = self.read_bits(significant as usize)?;
                value << (64 - leading - significant)
            };
            prev ^ xor
        };

        self.prev_value = Some(bits);
        Ok(f64::from_bits(bits))
    }

    fn read_bits(&mut self, bits: usize) -> Result<u64, Error> {
        while self.bit_count < bits && self.cursor < self.data.len() {
            self.bit_buffer |= (self.data[self.cursor] as u64) << self.bit_count;
            self.bit_count += 8;
            self.cursor += 1;
        }

        if self.bit_count < bits {
            return Err(Error::Corruption);
        }

        let mask = (1u64 << bits) - 1;
        let value = self.bit_buffer & mask;
        self.bit_buffer >>= bits;
        self.bit_count -= bits;
        
        Ok(value)
    }
}

pub fn compression_ratio(original_size: usize, compressed_size: usize) -> f64 {
    if compressed_size == 0 {
        return 0.0;
    }
    original_size as f64 / compressed_size as f64
}

pub fn select_best_algorithm(points: &[DataPoint]) -> CompressionAlgorithm {
    if points.len() < 10 {
        return CompressionAlgorithm::None;
    }

    let mut best_algorithm = CompressionAlgorithm::None;
    let mut best_size = usize::MAX;

    for algorithm in &[
        CompressionAlgorithm::Delta,
        CompressionAlgorithm::Gorilla,
        CompressionAlgorithm::DoubleDelta,
        CompressionAlgorithm::Snappy,
    ] {
        let compressor = TimeSeriesCompressor::new(*algorithm);
        if let Ok(compressed) = compressor.compress(points) {
            if compressed.len() < best_size {
                best_size = compressed.len();
                best_algorithm = *algorithm;
            }
        }
    }

    best_algorithm
}