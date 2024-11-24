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
use std::{mem::replace, path::Path};

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::<u8>::new(),
            last_key: Vec::<u8>::new(),
            data: Vec::<u8>::with_capacity(block_size),
            meta: Vec::<BlockMeta>::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let has_space = self.builder.add(key, value);
        let vec_key: Vec<u8> = key.raw_ref().to_vec();
        if vec_key > self.last_key || self.last_key.len() == 0 {
            self.last_key = vec_key;
        }
        let vec_key: Vec<u8> = key.raw_ref().to_vec();
        if vec_key < self.first_key || self.first_key.len() == 0 {
            self.first_key = vec_key;
        }

        if !has_space {
            self.serialize_builder();
        }
    }

    fn serialize_builder(&mut self) {
        let offset = self.data.len();
        let full = replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let first_key = replace(&mut self.first_key, Vec::<u8>::new());
        let last_key = replace(&mut self.last_key, Vec::<u8>::new());
        if first_key.len() != 0 && last_key.len() != 0 {
            let full_block = full.build();
            self.data.extend(full_block.encode());
            self.meta.push(BlockMeta {
                offset,
                first_key: Key::from_vec(first_key).into_key_bytes(),
                last_key: Key::from_vec(last_key).into_key_bytes(),
            });
        }
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.serialize_builder();
        let len = self.data.len();
        let meta_ref = self.meta.clone();
        let first_key = match meta_ref
            .iter()
            .map(|x| &x.first_key)
            .reduce(|first, second| std::cmp::min(first, second))
        {
            Some(v) => v,
            None => &Key::new().into_key_bytes(),
        };
        let last_key = match meta_ref
            .iter()
            .map(|x| &x.last_key)
            .reduce(|first, second| std::cmp::max(first, second))
        {
            Some(v) => v,
            None => &Key::new().into_key_bytes(),
        };
        let mut payload = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut payload);
        let block_meta_offset: Vec<u8> = (len as u32).to_ne_bytes().to_vec();
        payload.extend(block_meta_offset);
        let f = FileObject::create(path.as_ref(), payload);
        Ok(SsTable {
            file: f?,
            id,
            block_meta: self.meta,
            block_meta_offset: len,
            block_cache: None,
            bloom: None,
            first_key: first_key.clone(),
            last_key: last_key.clone(),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
