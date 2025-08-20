use std::borrow::Cow;
use std::cmp::Ordering;

/// Trait for efficient key encoding/decoding
pub trait KeyEncoder: Send + Sync {
    /// Encode a key for storage
    fn encode<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]>;

    /// Decode a key from storage
    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]>;

    /// Compare two encoded keys
    fn compare_encoded(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

/// Default key encoder (no transformation)
#[derive(Debug, Clone, Default)]
pub struct IdentityKeyEncoder;

impl KeyEncoder for IdentityKeyEncoder {
    fn encode<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]> {
        Cow::Borrowed(key)
    }

    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        Cow::Borrowed(encoded)
    }
}

/// Prefix compression key encoder
#[derive(Debug, Clone)]
pub struct PrefixKeyEncoder {
    common_prefix: Vec<u8>,
}

impl PrefixKeyEncoder {
    pub fn new(common_prefix: Vec<u8>) -> Self {
        Self { common_prefix }
    }

    pub fn from_keys(keys: &[Vec<u8>]) -> Self {
        if keys.is_empty() {
            return Self {
                common_prefix: Vec::new(),
            };
        }

        // Find common prefix
        let mut prefix = keys[0].clone();
        for key in &keys[1..] {
            let common_len = prefix
                .iter()
                .zip(key.iter())
                .take_while(|(a, b)| a == b)
                .count();
            prefix.truncate(common_len);
            if prefix.is_empty() {
                break;
            }
        }

        Self {
            common_prefix: prefix,
        }
    }
}

impl KeyEncoder for PrefixKeyEncoder {
    fn encode<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]> {
        if key.starts_with(&self.common_prefix) {
            // Store only the suffix
            Cow::Owned(key[self.common_prefix.len()..].to_vec())
        } else {
            // Key doesn't match prefix, store with special marker
            let mut encoded = vec![0xFF]; // Marker for full key
            encoded.extend_from_slice(key);
            Cow::Owned(encoded)
        }
    }

    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        if encoded.is_empty() || encoded[0] != 0xFF {
            // Reconstruct from prefix + suffix
            let mut key = self.common_prefix.clone();
            key.extend_from_slice(encoded);
            Cow::Owned(key)
        } else {
            // Full key stored
            Cow::Borrowed(&encoded[1..])
        }
    }
}

/// Variable-length integer encoding for numeric keys
#[derive(Debug, Clone, Default)]
pub struct VarIntKeyEncoder;

impl VarIntKeyEncoder {
    /// Encode a u64 using variable-length encoding
    pub fn encode_u64(&self, value: u64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(10);
        let mut v = value;

        while v >= 0x80 {
            encoded.push((v & 0x7F) as u8 | 0x80);
            v >>= 7;
        }
        encoded.push(v as u8);

        encoded
    }

    /// Decode a u64 from variable-length encoding
    pub fn decode_u64(&self, bytes: &[u8]) -> Option<(u64, usize)> {
        let mut value = 0u64;
        let mut shift = 0;

        for (i, &byte) in bytes.iter().enumerate() {
            if shift >= 64 {
                return None; // Overflow
            }

            value |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                return Some((value, i + 1));
            }

            shift += 7;
        }

        None // Incomplete varint
    }
}

impl KeyEncoder for VarIntKeyEncoder {
    fn encode<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]> {
        // Try to parse as u64
        if key.len() == 8 {
            if let Ok(bytes) = key.try_into() {
                let value = u64::from_be_bytes(bytes);
                Cow::Owned(self.encode_u64(value))
            } else {
                // Should never happen since we checked length, but handle gracefully
                Cow::Borrowed(key)
            }
        } else {
            Cow::Borrowed(key)
        }
    }

    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        // Try to decode as varint
        if let Some((value, len)) = self.decode_u64(encoded) {
            if len == encoded.len() {
                return Cow::Owned(value.to_be_bytes().to_vec());
            }
        }
        Cow::Borrowed(encoded)
    }

    fn compare_encoded(&self, a: &[u8], b: &[u8]) -> Ordering {
        // Try to decode both as varints for proper numeric comparison
        let a_decoded = self.decode_u64(a);
        let b_decoded = self.decode_u64(b);

        match (a_decoded, b_decoded) {
            (Some((a_val, a_len)), Some((b_val, b_len)))
                if a_len == a.len() && b_len == b.len() =>
            {
                a_val.cmp(&b_val)
            }
            _ => a.cmp(b), // Fall back to lexicographic comparison
        }
    }
}

/// Composite key encoder for multi-part keys
#[derive(Debug, Clone)]
pub struct CompositeKeyEncoder {
    delimiter: u8,
}

impl CompositeKeyEncoder {
    pub fn new(delimiter: u8) -> Self {
        Self { delimiter }
    }

    /// Encode multiple key parts into a single key
    pub fn encode_parts(&self, parts: &[&[u8]]) -> Vec<u8> {
        let total_len = parts.iter().map(|p| p.len() + 1).sum::<usize>();
        let mut encoded = Vec::with_capacity(total_len);

        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                encoded.push(self.delimiter);
            }
            // Escape delimiter in the part
            for &byte in *part {
                if byte == self.delimiter {
                    encoded.push(self.delimiter);
                    encoded.push(self.delimiter);
                } else {
                    encoded.push(byte);
                }
            }
        }

        encoded
    }

    /// Decode a composite key into parts
    pub fn decode_parts(&self, encoded: &[u8]) -> Vec<Vec<u8>> {
        let mut parts = Vec::new();
        let mut current_part = Vec::new();
        let mut i = 0;

        while i < encoded.len() {
            if encoded[i] == self.delimiter {
                if i + 1 < encoded.len() && encoded[i + 1] == self.delimiter {
                    // Escaped delimiter
                    current_part.push(self.delimiter);
                    i += 2;
                } else {
                    // Part delimiter
                    parts.push(current_part);
                    current_part = Vec::new();
                    i += 1;
                }
            } else {
                current_part.push(encoded[i]);
                i += 1;
            }
        }

        if !current_part.is_empty() || !parts.is_empty() {
            parts.push(current_part);
        }

        parts
    }
}

impl KeyEncoder for CompositeKeyEncoder {
    fn encode<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]> {
        // Check if escaping is needed
        if key.contains(&self.delimiter) {
            let mut encoded = Vec::with_capacity(key.len() + 4);
            for &byte in key {
                if byte == self.delimiter {
                    encoded.push(self.delimiter);
                    encoded.push(self.delimiter);
                } else {
                    encoded.push(byte);
                }
            }
            Cow::Owned(encoded)
        } else {
            Cow::Borrowed(key)
        }
    }

    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        // Check if unescaping is needed
        let mut needs_unescaping = false;
        let mut i = 0;
        while i < encoded.len() {
            if encoded[i] == self.delimiter
                && i + 1 < encoded.len()
                && encoded[i + 1] == self.delimiter
            {
                needs_unescaping = true;
                break;
            }
            i += 1;
        }

        if needs_unescaping {
            let mut decoded = Vec::with_capacity(encoded.len());
            let mut i = 0;
            while i < encoded.len() {
                if encoded[i] == self.delimiter
                    && i + 1 < encoded.len()
                    && encoded[i + 1] == self.delimiter
                {
                    decoded.push(self.delimiter);
                    i += 2;
                } else {
                    decoded.push(encoded[i]);
                    i += 1;
                }
            }
            Cow::Owned(decoded)
        } else {
            Cow::Borrowed(encoded)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_encoder() {
        let encoder = IdentityKeyEncoder;
        let key = b"test_key";

        let encoded = encoder.encode(key);
        assert_eq!(&*encoded, key);

        let decoded = encoder.decode(&encoded);
        assert_eq!(&*decoded, key);
    }

    #[test]
    fn test_prefix_encoder() {
        let keys = vec![
            b"user:1234".to_vec(),
            b"user:5678".to_vec(),
            b"user:9012".to_vec(),
        ];

        let encoder = PrefixKeyEncoder::from_keys(&keys);

        // Should find "user:" as common prefix
        let encoded = encoder.encode(b"user:1234");
        assert_eq!(&*encoded, b"1234");

        let decoded = encoder.decode(&encoded);
        assert_eq!(&*decoded, b"user:1234");

        // Test key without prefix
        let encoded = encoder.encode(b"other:key");
        assert_eq!(encoded[0], 0xFF);

        let decoded = encoder.decode(&encoded);
        assert_eq!(&*decoded, b"other:key");
    }

    #[test]
    fn test_varint_encoder() {
        let encoder = VarIntKeyEncoder;

        // Test small number
        let encoded = encoder.encode_u64(127);
        assert_eq!(encoded, vec![127]);

        // Test larger number
        let encoded = encoder.encode_u64(300);
        assert_eq!(encoded, vec![172, 2]); // 300 = 172 + 2*128

        // Test decode
        let (value, len) = encoder.decode_u64(&encoded).unwrap();
        assert_eq!(value, 300);
        assert_eq!(len, 2);

        // Test comparison
        let a = encoder.encode_u64(100);
        let b = encoder.encode_u64(200);
        assert_eq!(encoder.compare_encoded(&a, &b), Ordering::Less);
    }

    #[test]
    fn test_composite_encoder() {
        let encoder = CompositeKeyEncoder::new(b':');

        // Test encoding parts
        let parts: Vec<&[u8]> = vec![b"user", b"1234", b"profile"];
        let encoded = encoder.encode_parts(&parts);
        assert_eq!(&encoded, b"user:1234:profile");

        // Test decoding parts
        let decoded = encoder.decode_parts(&encoded);
        assert_eq!(decoded, parts);

        // Test with delimiter in part
        let parts: Vec<&[u8]> = vec![b"a:b", b"c"];
        let encoded = encoder.encode_parts(&parts);
        assert_eq!(&encoded, b"a::b:c");

        let decoded = encoder.decode_parts(&encoded);
        assert_eq!(decoded, parts);
    }
}
