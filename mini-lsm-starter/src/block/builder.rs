#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::<u16>::new(),
            data: Vec::<u8>::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let mut serialized_data = Vec::<u8>::new();
        let key_len: u16 = key.len().try_into().unwrap();
        let value_len: u16 = value.len().try_into().unwrap();
        serialized_data.extend_from_slice(&key_len.to_ne_bytes());
        serialized_data.extend_from_slice(key.raw_ref());
        serialized_data.extend_from_slice(&value_len.to_ne_bytes());
        serialized_data.extend_from_slice(value);
        self.offsets.push(self.data.len().try_into().unwrap());
        self.data.extend_from_slice(&serialized_data);
        // block should never be "full" when only one key-value pair is in the block
        if self.offsets.len() == 1 {
            return true;
        }
        self.data.len() < self.block_size
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
