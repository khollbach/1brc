use anyhow::{ensure, Context, Result};
use std::{
    collections::HashMap,
    env, fmt,
    fs::File,
    io::{BufRead, BufReader},
};

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    ensure!(args.len() == 1, "expected filename");
    let filename = &args[0];
    let file = File::open(filename).with_context(|| format!("couldn't open file {filename:?}"))?;

    let mut stats = HashMap::<String, Stats>::new();
    for l in BufReader::new(file).lines() {
        let l = l?;
        let (name, value) = l.split_once(';').context("expected semicolon")?;
        let name = name.to_owned();
        let value = value.parse()?;
        stats.entry(name).or_default().update(value);
    }
    print_stats(&stats);

    Ok(())
}

/// Aggregated statistics for a single weather station.
struct Stats {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.,
            count: 0,
        }
    }
}

impl Stats {
    fn update(&mut self, value: f32) {
        self.min = f32::min(self.min, value);
        self.max = f32::max(self.max, value);
        self.sum += value;
        self.count += 1;
    }

    fn avg(&self) -> f32 {
        self.sum / self.count as f32
    }
}

fn print_stats(stats: &HashMap<String, Stats>) {
    print!("{{");

    let mut pairs: Vec<_> = stats.iter().collect();
    pairs.sort_unstable_by_key(|&(name, _)| name);

    let mut first = true;
    for (name, stat) in pairs {
        if first {
            first = false;
        } else {
            print!(", ");
        }

        print!("{name}={stat}");
    }

    print!("}}");
    println!();
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Note: this rounds to nearest.
        //
        // The challenge rules say to round away from zero (the opposite of
        // truncate), but their example code doesn't do what they say -- it
        // rounds to nearest.
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.avg(), self.max)
    }
}
