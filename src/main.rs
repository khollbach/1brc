use anyhow::{ensure, Context, Result};
use itertools::Itertools;
use std::{
    collections::HashMap,
    env, fmt,
    fs::File,
    io::{BufRead, BufReader},
    str,
};

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    ensure!(args.len() == 1, "expected filename");
    let filename = &args[0];
    let file = File::open(filename).with_context(|| format!("couldn't open file {filename:?}"))?;

    let mut stats = HashMap::<Vec<u8>, Stats>::with_capacity(10_000);

    let mut file = BufReader::new(file);
    let mut line = Vec::<u8>::with_capacity(128);
    loop {
        line.clear();
        if file.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.ends_with(b"\n") {
            line.pop();
        }

        let (name, value) = split_once(&line, b';').context("expected semicolon")?;
        let value = parse_f32(value).context("failed to parse special-case f32")?;

        match stats.get_mut(name) {
            None => {
                let mut st = Stats::default();
                st.update(value);
                stats.insert(name.to_owned(), st);
            }
            Some(st) => st.update(value),
        }
    }

    print_stats(&stats)?;

    Ok(())
}

fn split_once(s: &[u8], delim: u8) -> Option<(&[u8], &[u8])> {
    let i = s.iter().position(|&b| b == delim)?;
    Some((&s[..i], &s[i + 1..]))
}

fn parse_f32(mut s: &[u8]) -> Option<f32> {
    let minus = if s[0] == b'-' {
        s = &s[1..];
        -1.
    } else {
        1.
    };

    let magnitude = match s.len() {
        3 => {
            if s[1] != b'.' {
                return None;
            }
            let x = to_digit(s[0])? * 1.;
            let y = to_digit(s[2])? * 0.1;
            x + y
        }
        4 => {
            if s[2] != b'.' {
                return None;
            }
            let a = to_digit(s[0])? * 10.;
            let b = to_digit(s[1])? * 1.;
            let c = to_digit(s[3])? * 0.1;
            a + b + c
        }
        _ => return None,
    };

    Some(minus * magnitude)
}

fn to_digit(c: u8) -> Option<f32> {
    let d = (c as char).to_digit(10)?;
    Some(d as f32)
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

fn print_stats(stats: &HashMap<Vec<u8>, Stats>) -> Result<()> {
    let mut pairs: Vec<_> = stats
        .iter()
        .map(|(name, value)| anyhow::Ok((str::from_utf8(name)?, value)))
        .try_collect()?;
    pairs.sort_unstable_by_key(|&(name, _)| name);

    print!("{{");
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

    Ok(())
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
