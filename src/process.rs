use crate::station::{Pair, Stations};
use anyhow::Result;
use memmap2::Mmap;
use rayon::prelude::*;
use std::{fs::File, slice::from_raw_parts};

pub fn process_bin(filename: &str) -> Result<()> {
    let file = File::open(filename)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let rows = mmap.len() / size_of::<Pair>();
    let buf: &[Pair] = unsafe { from_raw_parts(mmap.as_ptr() as *const Pair, rows) };
    buf.par_iter()
        .fold(Stations::default, Stations::insert)
        .reduce(Stations::default, Stations::merge)
        .print();
    Ok(())
}
