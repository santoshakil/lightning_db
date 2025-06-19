pub mod key_compare;
pub mod batch_ops;
pub mod checksum;
pub mod hash;

pub use key_compare::{simd_compare_keys, simd_find_key, SimdKeyComparator};
pub use batch_ops::{simd_batch_validate, simd_batch_hash as simd_batch_hash_from_batch_ops};
pub use checksum::{simd_crc32, simd_checksum_verify, simd_is_zero};
pub use hash::{simd_batch_hash, simd_prefix_match};