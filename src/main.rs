mod cities;
mod preprocess;
mod process;
mod station;

use anyhow::Result;
use preprocess::preprocess_txt;
use process::process_bin;
use std::env;

fn main() -> Result<()> {
    let filename = env::args()
        .nth(1)
        .unwrap_or_else(|| "measurements.txt".to_string());
    if filename.ends_with(".txt") {
        preprocess_txt(&filename)?;
    } else if filename.ends_with(".bin") {
        process_bin(&filename)?;
    } else {
        eprintln!("Unknown file type: {filename}");
    }
    Ok(())
}
