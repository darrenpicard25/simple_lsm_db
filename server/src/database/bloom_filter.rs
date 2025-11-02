use std::io;

// Default: ~10 bits per key, 3 hash functions
const DEFAULT_BITS_PER_KEY: usize = 10;
const DEFAULT_NUM_HASHES: usize = 3;

pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with a given size in bits and number of hash functions
    pub fn new(num_bits: usize, num_hashes: usize) -> Self {
        let num_bytes = (num_bits + 7) / 8; // Round up to nearest byte
        Self {
            bits: vec![0; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    /// Create a bloom filter sized for approximately `bits_per_key` bits per key
    /// with `num_hashes` hash functions
    pub fn with_capacity(expected_keys: usize, bits_per_key: usize, num_hashes: usize) -> Self {
        let num_bits = expected_keys * bits_per_key;
        Self::new(num_bits, num_hashes)
    }

    /// Create a bloom filter with default parameters
    pub fn default_for_keys(expected_keys: usize) -> Self {
        Self::with_capacity(expected_keys, DEFAULT_BITS_PER_KEY, DEFAULT_NUM_HASHES)
    }

    /// Add a key to the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i as u64);
            let bit_index = hash % self.num_bits as u64;
            self.set_bit(bit_index as usize);
        }
    }

    /// Check if a key might be in the bloom filter
    /// Returns false if the key is definitely not present
    /// Returns true if the key might be present (could be a false positive)
    pub fn might_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i as u64);
            let bit_index = hash % self.num_bits as u64;
            if !self.get_bit(bit_index as usize) {
                return false;
            }
        }
        true
    }

    /// Hash function: simple hash using FNV-1a style with seed variation
    fn hash(&self, key: &[u8], seed: u64) -> u64 {
        let mut hash = 14695981039346656037u64; // FNV offset basis
        hash ^= seed; // Mix in seed for different hash functions

        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1099511628211u64); // FNV prime
        }
        hash
    }

    /// Set a bit at the given index
    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;
        self.bits[byte_index] |= 1 << bit_index;
    }

    /// Get a bit at the given index
    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;
        (self.bits[byte_index] & (1 << bit_index)) != 0
    }

    /// Serialize the bloom filter to bytes for disk storage
    /// Format: [num_bits (8 bytes), num_hashes (8 bytes), bits (variable)]
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Write num_bits (8 bytes)
        result.extend_from_slice(&self.num_bits.to_le_bytes());

        // Write num_hashes (8 bytes)
        result.extend_from_slice(&self.num_hashes.to_le_bytes());

        // Write bit array
        result.extend_from_slice(&self.bits);

        result
    }

    /// Deserialize a bloom filter from bytes
    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        if data.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bloom filter data too short",
            ));
        }

        // Read num_bits (8 bytes)
        let num_bits = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]) as usize;

        // Read num_hashes (8 bytes)
        let num_hashes = u64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]) as usize;

        // Read bit array
        let bits = data[16..].to_vec();

        Ok(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_insert_and_check() {
        let mut filter = BloomFilter::new(100, 3);

        filter.insert(b"key1");
        filter.insert(b"key2");

        assert!(filter.might_contain(b"key1"));
        assert!(filter.might_contain(b"key2"));
        assert!(!filter.might_contain(b"key3")); // Should not be present
    }

    #[test]
    fn test_bloom_filter_false_positive() {
        let mut filter = BloomFilter::new(10, 3); // Very small filter

        filter.insert(b"key1");

        // With a small filter, we might get false positives
        // This is expected behavior
        assert!(filter.might_contain(b"key1"));
    }

    #[test]
    fn test_bloom_filter_serialize_deserialize() {
        let mut filter = BloomFilter::new(100, 3);
        filter.insert(b"test_key");

        let serialized = filter.serialize();
        let deserialized = BloomFilter::deserialize(&serialized).unwrap();

        assert!(deserialized.might_contain(b"test_key"));
        assert_eq!(deserialized.num_bits, filter.num_bits);
        assert_eq!(deserialized.num_hashes, filter.num_hashes);
    }
}
