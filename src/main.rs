use anyhow::Result;
use memmap2::Mmap;
use std::{
    collections::BTreeMap,
    env,
    fs::File,
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
    // println!("Header: {:?}", std::str::from_utf8(&mmap[0..800])?);
    // let stations = RwLock::new(Stations::default());
    thread::scope(|s| {
        let chunks = get_chunks(&mmap, len, threads);
        for (start, end) in chunks {
            eprintln!("Thread: start={start}, end={end}");
            let buf = &mmap;
            s.spawn(move || {
                process_chunk(buf, start, end);
            });
        }
    });
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

fn process_chunk(mmap: &[u8], start: usize, end: usize) {
    // let mut stations = Stations::default();
    let mut i = start;
    while i < end {
        // Read station name
        // let mut name = String::new();
        while i < end && mmap[i] != b';' {
            // name.push(mmap[i] as char);
            i += 1;
        }
        i += 1;
        // Read float value
        // let mut value = String::new();
        while i < end && mmap[i] != b'\n' {
            // value.push(mmap[i] as char);
            i += 1;
        }
        i += 1;
        // Insert data
        // stations.try_write().unwrap().insert(name, value.parse()?);
        // stations.insert(name, value.parse()?);
    }
    eprintln!("thread {start}..{end} ended at i: {i}");
    // stations.try_read().unwrap().print();
    // stations.print();
}

// type Line = (String, f64);
// fn parse_line(line: &str) -> Option<Line> {
//     let mut parts = line.split(';');
//     let name = parts.next()?.to_string();
//     let value = parts.next()?.parse().ok()?;
//     Some((name, value))
// }

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

    fn add(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.total += other.total;
        self.count += other.count;
    }
}

#[derive(Default, Clone, Debug)]
struct Stations {
    map: BTreeMap<String, Station>,
}

impl Stations {
    fn insert(&mut self, name: String, value: f64) {
        let station = Station::new(value);
        self.map
            .entry(name)
            .and_modify(|e| e.add(station))
            .or_insert(station);
    }

    fn print(&self) {
        let results = self
            .map
            .iter()
            .map(|(name, station)| {
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
