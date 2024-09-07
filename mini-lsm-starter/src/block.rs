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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data_slice = self.data.clone();
        for x in self.offsets.iter() {
            let xb = x.to_ne_bytes();
            data_slice.extend(xb);
        }
        let num_of_elements = self.offsets.len() as u16;
        data_slice.extend(num_of_elements.to_ne_bytes());
        Bytes::from(data_slice)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut block = Block {
            data: Vec::<u8>::new(),
            offsets: Vec::<u16>::new(),
        };
        let data_len = data.len();
        if data_len < 2 {
            return block;
        }
        let num_of_elements = u16::from_ne_bytes([data[data_len - 2], data[data_len - 1]]);
        let size_of_offset_block = 2 * num_of_elements as usize;
        let offset_block = &data[data_len - 2 - size_of_offset_block..data_len - 2];
        let mut i = 0;
        while i <= offset_block.len() - 2 {
            block
                .offsets
                .push(u16::from_ne_bytes([offset_block[i], offset_block[i + 1]]));
            i += 2;
        }
        block.data = data[..data_len - 2 - size_of_offset_block].to_vec();
        block
    }
}
