#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    /// true for StorageIterator `a`, false for StorageIterator `b`
    flag: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let flag = if !a.is_valid() {
            false
        } else if !b.is_valid() {
            true
        } else {
            a.key() <= b.key()
        };

        Ok(Self { a, b, flag })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.flag {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.flag {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.flag {
            if self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next()?;
                self.a.next()?;
            } else {
                self.a.next()?;
            }
        } else if self.b.is_valid() {
            self.b.next()?;
        }

        if !self.a.is_valid() {
            self.flag = false;
        } else if !self.b.is_valid() {
            self.flag = true;
        } else {
            self.flag = self.a.key() <= self.b.key();
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
