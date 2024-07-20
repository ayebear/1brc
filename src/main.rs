use anyhow::Result;
use hashbrown::HashMap;
use memmap2::Mmap;
use std::{
    env,
    fs::File,
    str::from_utf8_unchecked,
    sync::RwLock,
    thread::{self, available_parallelism},
};

fn main() -> Result<()> {
    // Open file with mmap
    let filename = env::args()
        .nth(1)
        .unwrap_or_else(|| "measurements.txt".to_string());
    let file = File::open(&filename)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let len = mmap.len();
    let threads: usize = available_parallelism()?.into();
    eprintln!("File {filename} is {len} bytes. Using {threads} thread(s).");
    // Spawn a thread for each chunk of input
    let results = RwLock::new(Stations::default());
    thread::scope(|s| {
        let chunks = get_chunks(&mmap, len, threads);
        for (start, end) in chunks {
            let chunk = &mmap[start..end];
            s.spawn(|| {
                // Process stations locally and then merge to global results
                let stations = process_chunk(chunk);
                results.write().unwrap().merge(stations);
            });
        }
    });
    results.read().unwrap().print();
    Ok(())
}

type Chunk = (usize, usize);
fn get_chunks(mmap: &[u8], len: usize, threads: usize) -> Vec<Chunk> {
    let mut v = Vec::new();
    let mut c = (0, 0);
    for t in 1..threads {
        // Start at next line at chunk point
        let mut i = t * (len / threads);
        while i < len && mmap[i] != b'\n' {
            i += 1;
        }
        i += 1;
        c.1 = i;
        v.push(c);
        c.0 = i;
    }
    c.1 = len;
    v.push(c);
    v
}

fn process_chunk(chunk: &[u8]) -> Stations {
    let mut stations = Stations::default();
    let len = chunk.len();
    let mut i = 0;
    while i < len {
        // Read station name
        let name = eat(chunk, i, b';');
        i += name.len() + 1;
        let name = unsafe { from_utf8_unchecked(name) };
        // Parse float value
        let value = eat(chunk, i, b'\n');
        i += value.len() + 1;
        let value = parse_float(value);
        // Record results locally
        stations.insert(name, value);
    }
    stations
}

fn eat(chunk: &[u8], start: usize, target: u8) -> &[u8] {
    let mut i = start;
    let len = chunk.len();
    while i < len && chunk[i] != target {
        i += 1;
    }
    &chunk[start..i]
}

fn parse_float(chunk: &[u8]) -> f64 {
    let str = unsafe { from_utf8_unchecked(chunk) };
    str.parse().unwrap()
}

#[derive(Default, Clone, Copy, Debug)]
struct Station {
    min: f64,
    max: f64,
    total: f64,
    count: usize,
}

impl Station {
    fn new(value: f64) -> Self {
        Self {
            min: value,
            max: value,
            total: value,
            count: 1,
        }
    }

    fn add_value(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.total += value;
        self.count += 1;
    }

    fn add_station(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.total += other.total;
        self.count += other.count;
    }
}

#[derive(Default, Debug)]
struct Stations {
    map: HashMap<String, Station>,
}

impl Stations {
    fn insert(&mut self, name: &str, value: f64) {
        self.map
            .entry_ref(name)
            .and_modify(|e| e.add_value(value))
            .or_insert(Station::new(value));
    }

    fn merge(&mut self, other: Self) {
        for (name, station) in other.map {
            self.map
                .entry(name)
                .and_modify(|e| e.add_station(station))
                .or_insert(station);
        }
    }

    fn print(&self) {
        let results = self
            .map
            .iter()
            .map(|(name, station)| {
                let &Station {
                    min,
                    max,
                    total,
                    count,
                } = station;
                format!("{name}={:.1}/{:.1}/{:.1}", min, total / count as f64, max)
            })
            .collect::<Vec<_>>()
            .join(", ");
        println!("{{{results}}}");
    }
}
