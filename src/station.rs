use crate::cities::CITIES;

pub type Pair = (i16, i16);

#[derive(Default, Clone, Copy, Debug)]
pub struct Station {
    min: i32,
    max: i32,
    total: i32,
    count: usize,
}

impl Station {
    fn new(value: i32) -> Self {
        Self {
            min: value,
            max: value,
            total: value,
            count: 1,
        }
    }

    fn add_value(&mut self, value: i32) {
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

#[derive(Debug)]
pub struct Stations {
    map: Vec<Station>,
}

impl Default for Stations {
    fn default() -> Self {
        Self {
            map: vec![Station::default(); CITIES.len()],
        }
    }
}

impl Stations {
    pub fn insert(mut self, &(id, value): &Pair) -> Self {
        let station = unsafe { self.map.get_unchecked_mut(id as usize) };
        if station.count == 0 {
            *station = Station::new(value as i32);
        } else {
            station.add_value(value as i32);
        }
        self
    }

    pub fn merge(mut self, other: Self) -> Self {
        for (i, station) in other.map.iter().enumerate() {
            if station.count == 0 {
                continue;
            }
            let self_station = unsafe { self.map.get_unchecked_mut(i) };
            if self_station.count == 0 {
                *self_station = *station;
            } else {
                self_station.add_station(*station);
            }
        }
        self
    }

    pub fn print(&self) {
        print!("{{");
        for (i, (station, name)) in self.map.iter().zip(CITIES.iter()).enumerate() {
            if i != 0 {
                print!(", ");
            }
            let mean = (station.total as f64 * 0.1) / (station.count as f64 * 0.1);
            let min = station.min as f64 * 0.1;
            let max = station.max as f64 * 0.1;
            print!("{name}={min:.1}/{mean:.1}/{max:.1}");
        }
        println!("}}");
    }
}
