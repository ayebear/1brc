use crate::station::Stations;
use anyhow::Result;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;

pub fn process_bin(filename: &str) -> Result<()> {
    eprintln!("Processing {filename}...");
    let file = File::open(filename)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let len = mmap.len();
    eprintln!("File {filename} is {len} bytes.");
    let buf: &[(i16, i16)] =
        unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const (i16, i16), len / 4) };
    eprintln!("Converted to buf of len: {}", buf.len());
    buf.par_iter()
        .fold(Stations::default, Stations::insert)
        .reduce(Stations::default, Stations::merge)
        .print();
    Ok(())
}
