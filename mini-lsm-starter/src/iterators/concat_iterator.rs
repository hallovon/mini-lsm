#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
    is_valid: bool,
}

impl SstConcatIterator {
    fn new(sstables: Vec<Arc<SsTable>>) -> Self {
        Self {
            current: None,
            next_sst_idx: 0,
            sstables,
            is_valid: false,
        }
    }

    fn seek_to_first(&mut self) -> Result<()> {
        if let Some(table) = self.sstables.first() {
            self.current = Some(SsTableIterator::create_and_seek_to_first(table.clone())?);
            self.next_sst_idx = 1;
            self.is_valid = true;
        }

        Ok(())
    }

    fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        for (idx, table) in self.sstables.iter().enumerate() {
            if table.last_key().as_key_slice() < key {
                continue;
            }

            self.current = Some(SsTableIterator::create_and_seek_to_key(table.clone(), key)?);
            self.next_sst_idx = idx + 1;
            self.is_valid = true;
            return Ok(());
        }

        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut iter = Self::new(sstables);
        iter.seek_to_first()?;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut iter = Self::new(sstables);
        iter.seek_to_key(key)?;
        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        current.next()?;

        if !current.is_valid() {
            if let Some(table) = self.sstables.get(self.next_sst_idx) {
                self.current = Some(SsTableIterator::create_and_seek_to_first(table.clone())?);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
                self.is_valid = false;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
