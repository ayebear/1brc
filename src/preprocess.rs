use crate::{cities::CITIES, station::Pair};
use anyhow::Result;
use hashbrown::HashMap;
use memmap2::Mmap;
use std::{fs::File, io::Write, slice::from_raw_parts, str::from_utf8_unchecked};

pub fn preprocess_txt(filename: &str) -> Result<()> {
    // City map
    let city_map: HashMap<&str, i16> = CITIES
        .iter()
        .enumerate()
        .map(|(i, &city)| (city, i as i16))
        .collect();
    // Open file
    eprintln!("Preprocessing {filename}...");
    let file = File::open(filename)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let len = mmap.len();
    let s = unsafe { from_utf8_unchecked(&mmap) };
    eprintln!("In file {filename} is {len} bytes");
    // Process file into binary format
    let v: Vec<_> = s
        .lines()
        .flat_map(|line| parse_line(line, &city_map))
        .collect();
    let buf: &[u8] =
        unsafe { from_raw_parts(v.as_ptr() as *const u8, v.len() * size_of::<Pair>()) };
    // Write bin file
    let out = filename.replace(".txt", ".bin");
    let mut writer = File::create(&out)?;
    writer.write_all(buf)?;
    eprintln!("Out file {out} is {} bytes", buf.len());
    Ok(())
}

fn parse_line(line: &str, city_map: &HashMap<&str, i16>) -> Option<Pair> {
    let mut parts = line.split(';');
    // Map city name to i16
    let name = parts.next()?;
    let key = *city_map.get(name)?;
    // Parse float value as i16
    let f: f32 = parts.next()?.parse().ok()?;
    let value = (f * 10.0).round() as i16;
    Some((key, value))
}
