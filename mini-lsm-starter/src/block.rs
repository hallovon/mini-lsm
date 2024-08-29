#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes_mut = BytesMut::new();

        bytes_mut.extend(&self.data);
        for offset in self.offsets.iter() {
            bytes_mut.extend(offset.to_le_bytes());
        }
        bytes_mut.extend((self.offsets.len() as u16).to_le_bytes());

        bytes_mut.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // let num_of_element = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);
        let num_of_element = u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]);

        let offsets = Bytes::copy_from_slice(
            &data[(data.len() - 2 - num_of_element as usize * 2)..(data.len() - 2)],
        )
        .chunks(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<u16>>();

        let data = data[..(data.len() - 2 - num_of_element as usize * 2)].to_vec();

        Self { data, offsets }
    }
}
