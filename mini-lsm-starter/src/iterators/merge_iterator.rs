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
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
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
        let it = Self {
            iters: heap,
            current,
        };
        it
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
        loop {
            match self.iters.pop() {
                Some(mut iter) => {
                    if iter.1.is_valid() {
                        let mut should_push = true;
                        if iter.1.key() <= self.key() {
                            let next_result = iter.1.next();
                            match next_result {
                                Ok(()) => {}
                                Err(e) => {
                                    should_push = false;
                                    result = Err(e);
                                }
                            }
                        } else {
                            break;
                        }
                        if should_push {
                            self.iters.push(iter);
                        }
                    }
                }
                None => break,
            }
        }
        if let Some(ref mut c) = self.current {
            if c.1.is_valid() {
                let next_result = c.1.next();
                match next_result {
                    Ok(()) => {}
                    Err(e) => {
                        result = Err(e);
                    }
                }
            }
        }
        let current = self.current.take();
        if let Some(c) = current {
            self.iters.push(c);
        }
        self.current = self.iters.pop();
        result
    }
}
