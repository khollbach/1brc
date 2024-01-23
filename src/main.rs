use ahash::{HashMap, HashMapExt};
use anyhow::{ensure, Context, Result};
use itertools::Itertools;
use std::{
    env, fmt,
    fs::File,
    io::{prelude::*, BufReader, SeekFrom},
    str, thread,
};

/// Max number of unique names in the input.
const NUM_KEYS: usize = 10_000;

/// Max length of input lines.
const LINE_LEN: usize = 128;

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    ensure!(args.len() == 1, "expected filename");

    let filename = &args[0];
    let file = File::open(filename).with_context(|| format!("couldn't open file {filename:?}"))?;
    let num_threads = thread::available_parallelism()?.into();
    let chunks = chunks(file, num_threads)?;

    let mut threads = Vec::with_capacity(chunks.len());
    for ch in chunks {
        let file = File::open(filename)?;
        let t = thread::spawn(move || chunk_stats(file, ch));
        threads.push(t);
    }

    let mut stats = HashMap::<Vec<u8>, Stats>::with_capacity(NUM_KEYS);
    for t in threads {
        let chunk_stats = t.join().expect("thread panic")?;
        for (k, st) in chunk_stats {
            stats.entry(k).or_default().merge(st);
        }
    }
    print_stats(&stats)?;

    Ok(())
}

/// Partition a file into exactly n chunks, each represented as `start..end`.
///
/// Chunk boundaries are always after a newline (except the first, and possibly
/// the last).
///
/// Chunks may be empty (unlikely for large files).
fn chunks(file: File, n: usize) -> Result<Vec<(u64, u64)>> {
    assert_ne!(n, 0);
    let len = file.metadata()?.len();
    let mut file = BufReader::new(file);

    let mut boundaries = Vec::with_capacity(n);
    boundaries.push(0);

    let mut buf = Vec::with_capacity(LINE_LEN);
    let n = n as u64;
    for i in 1..=n - 1 {
        let offset = len * i / n;
        file.seek(SeekFrom::Start(offset))?;
        buf.clear();
        file.read_until(b'\n', &mut buf)?;
        boundaries.push(offset + buf.len() as u64);
    }

    boundaries.push(len);

    Ok(boundaries.into_iter().tuple_windows().collect())
}

fn chunk_stats(file: File, (start, end): (u64, u64)) -> Result<HashMap<Vec<u8>, Stats>> {
    let mut stats = HashMap::with_capacity(NUM_KEYS);

    let mut file = BufReader::new(file);
    file.seek(SeekFrom::Start(start))?;

    let mut curr_offset = start;
    let mut line = Vec::<u8>::with_capacity(LINE_LEN);
    while curr_offset < end {
        line.clear();
        let n = file.read_until(b'\n', &mut line)? as u64;
        if n == 0 {
            break;
        }
        curr_offset += n;
        if line.ends_with(b"\n") {
            line.pop();
        }

        let (name, value) = split_once(&line, b';').context("expected semicolon")?;
        let value = parse_f32(value).context("failed to parse special-case f32")?;

        match stats.get_mut(name) {
            None => {
                stats.insert(name.to_owned(), Stats::singleton(value));
            }
            Some(st) => st.update(value),
        }
    }

    Ok(stats)
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
    fn singleton(value: f32) -> Self {
        Self {
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    fn update(&mut self, value: f32) {
        self.merge(Self::singleton(value))
    }

    fn merge(&mut self, other: Self) {
        self.min = f32::min(self.min, other.min);
        self.max = f32::max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
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
