#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        println!("{:?} {:?}", table.id, table.block_meta);
        let block = table.read_block_cached(0)?;
        let block_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            blk_iter: block_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut new = Self::create_and_seek_to_first(table)?;
        new.seek_to_key(key)?;
        Ok(new)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        println!("{:?}", String::from_utf8(key.to_key_vec().into_inner()));
        for (i, meta) in self.table.block_meta.iter().enumerate() {
            println!("{:?} {:?}", meta.first_key, meta.last_key);
            if key >= meta.first_key.as_key_slice() && key <= meta.last_key.as_key_slice() {
                println!("found match!");
                let block = self.table.read_block_cached(i)?;
                self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
                self.blk_idx = i;
                return Ok(());
            } else if key < meta.first_key.as_key_slice() {
                let block = self.table.read_block_cached(i)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
                self.blk_idx = i;
                return Ok(());
            }
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() {
            self.blk_idx += 1;
            match self.table.read_block_cached(self.blk_idx) {
                Ok(block) => self.blk_iter = BlockIterator::create_and_seek_to_first(block),
                Err(e) => {}
            }
        }
        Ok(())
    }
}
