use anyhow::Result;
use memmap2::Mmap;
use std::{
    collections::BTreeMap,
    env,
    fs::File,
    sync::{Arc, RwLock},
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
    // println!("Header: {:?}", std::str::from_utf8(&mmap[0..800])?);
    let stations = Arc::new(RwLock::new(Stations::default()));
    thread::scope(|s| {
        let chunks = get_chunks(&mmap, len, threads);
        for (start, end) in chunks {
            eprintln!("START {start}..{end}");
            let chunk = &mmap[start..end];
            s.spawn(|| {
                process_chunk(chunk, stations.clone());
            });
        }
    });
    stations.read().unwrap().print();
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

fn process_chunk(chunk: &[u8], stations: Arc<RwLock<Stations>>) {
    let end = chunk.len();
    // let mut stations = Stations::default();
    let mut i = 0;
    while i < end {
        // Read station name
        let mut name = String::new();
        while i < end && chunk[i] != b';' {
            name.push(chunk[i] as char);
            i += 1;
        }
        i += 1;
        // Read float value
        let mut value = String::new();
        while i < end && chunk[i] != b'\n' {
            value.push(chunk[i] as char);
            i += 1;
        }
        i += 1;
        let value: f64 = value.parse().unwrap();
        // Try to get station to modify if it exists, otherwise add it
        if let Some(station) = stations.read().unwrap().get_station(&name) {
            station.write().unwrap().add(value);
            continue;
        }
        stations.write().unwrap().insert(name, value);
    }
    eprintln!("END 0..{end} at i: {i}");
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

    fn add(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.total += value;
        self.count += 1;
    }
}

#[derive(Default, Debug)]
struct Stations {
    map: BTreeMap<String, Arc<RwLock<Station>>>,
}

impl Stations {
    fn get_station(&self, name: &str) -> Option<Arc<RwLock<Station>>> {
        self.map.get(name).cloned()
    }

    fn insert(&mut self, name: String, value: f64) {
        let station = Station::new(value);
        self.map.insert(name, Arc::new(RwLock::new(station)));
    }

    fn print(&self) {
        let results = self
            .map
            .iter()
            .map(|(name, station)| {
                let station = station.read().unwrap();
                format!(
                    "{name}={:.1}/{:.1}/{:.1}",
                    station.min,
                    station.total / station.count as f64,
                    station.max
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        println!("{{{results}}}");
    }
}
