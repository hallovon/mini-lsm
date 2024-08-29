#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

const KEY_LEN: usize = 2;
const VALUE_LEN: usize = 2;
const OFFSET_LEN: usize = 2;

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
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty()
            && self.data.len()
                + self.offsets.len() * 2
                + KEY_LEN
                + key.len()
                + VALUE_LEN
                + value.len()
                + OFFSET_LEN
                >= self.block_size
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let key_overlap_len = key_overlap_len(self.first_key.raw_ref(), key.raw_ref());
        let rest_key_len = key.len() as u16 - key_overlap_len;

        self.data.put_u16_le(key_overlap_len);
        self.data.put_u16_le(rest_key_len);
        self.data
            .put_slice(&key.raw_ref()[(key_overlap_len as usize)..]);
        self.data.put_u16_le(value.len() as u16);
        self.data.put_slice(value);

        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

fn key_overlap_len(first_key: &[u8], curr_key: &[u8]) -> u16 {
    let len = first_key.len().min(curr_key.len());

    for i in 0..len {
        if first_key[i] != curr_key[i] {
            return i as u16;
        }
    }

    len as u16
}
