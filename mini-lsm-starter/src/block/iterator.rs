// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block = Self::new(block);
        block.seek_to_first();
        block
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block = Self::create_and_seek_to_first(block);
        block.seek_to_key(key);
        block
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let offset = self.block.offsets[0] as usize;
        self.set_key_value_from_offset(offset);
        self.first_key = self.key.clone();
    }

    fn set_key_value_from_offset(&mut self, offset: usize) {
        if offset == 0 {
            // get first two bytes from block
            let key_len =
                u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
            self.key =
                Key::from_slice(&self.block.data[offset + 2..offset + 2 + key_len]).to_key_vec();
            let bytes_len = u16::from_ne_bytes([
                self.block.data[offset + 2 + key_len],
                self.block.data[offset + 2 + key_len + 1],
            ]) as usize;
            self.value_range = (offset + 4 + key_len, offset + 4 + key_len + bytes_len);
        } else {
            // first two bytes are overlap len
            let overlap_len =
                u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
            let remaining_key_len =
                u16::from_ne_bytes([self.block.data[offset + 2], self.block.data[offset + 3]])
                    as usize;
            self.key = Key::from_slice(
                &[
                    &self.first_key.as_key_slice().into_inner()[..overlap_len],
                    &self.block.data[offset + 4..offset + 4 + remaining_key_len],
                ]
                .concat(),
            )
            .to_key_vec();
            let bytes_len = u16::from_ne_bytes([
                self.block.data[offset + 4 + remaining_key_len],
                self.block.data[offset + 4 + remaining_key_len + 1],
            ]) as usize;
            self.value_range = (
                offset + 6 + remaining_key_len,
                offset + 6 + remaining_key_len + bytes_len,
            );
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let end_of_current = self.value_range.1 as u16;
        match self
            .block
            .offsets
            .clone()
            .into_iter()
            .find(|x| x >= &end_of_current)
        {
            Some(v) => {
                let offset = v as usize;
                self.set_key_value_from_offset(offset);
            }
            None => self.key = Key::new(),
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.key() < key && !self.key().is_empty() {
            self.next()
        }
    }
}
