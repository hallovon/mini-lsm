#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

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
        let mut map = BinaryHeap::new();

        for (idx, boxed_element) in iters.into_iter().enumerate() {
            if boxed_element.is_valid() {
                map.push(HeapWrapper(idx, boxed_element));
            }
        }

        let current = map.pop();

        Self {
            iters: map,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let key = self.current.as_ref().unwrap().1.key().to_key_vec();

        loop {
            let mut current = self.current.take().unwrap();

            if current.1.is_valid() {
                current.1.next()?;

                // 1. 当前子迭代器合法且next之后仍合法
                if current.1.is_valid() {
                    self.iters.push(current);
                }
            }

            // 2. 当前子迭代器合法且next之后不合法
            // 3. 当前子迭代器已不合法
            self.current = self.iters.pop();

            if self.current.is_none()
                || self.current.as_ref().unwrap().1.key() != key.as_key_slice()
            {
                break;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if self.current.is_some() {
            self.iters.len() + 1
        } else {
            self.iters.len()
        }
    }
}
