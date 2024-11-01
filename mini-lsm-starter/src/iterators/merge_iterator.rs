#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::{Key, KeySlice};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::<HeapWrapper<I>>::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter))
            }
        }
        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match &self.current {
            Some(v) => v.1.key(),
            None => Key::from_slice(&[]),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            Some(v) => v.1.value(),
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            Some(v) => v.1.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        let mut result = Ok(());
        let prev_key = self.key().to_key_vec();
        loop {
            let current = self.current.take();
            if let Some(mut c) = current {
                if c.1.is_valid() {
                    let next_result = c.1.next();
                    match next_result {
                        Ok(()) => {
                            if c.1.is_valid() {
                                self.iters.push(c);
                            }
                        }
                        Err(e) => {
                            result = Err(e);
                        }
                    }
                }
            }
            self.current = self.iters.pop();
            if self.key() != prev_key.as_key_slice() {
                break;
            }
        }
        result
    }
}
