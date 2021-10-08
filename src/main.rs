use csv::ReaderBuilder;
use csv::{Error, StringRecord};
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::{io, path};
use structopt::StructOpt;

#[macro_use]
extern crate log;

#[derive(Debug, StructOpt)]
#[structopt(
    about = "command line tool for randomly sampling tsv files",
    rename_all = "kebab-case"
)]

struct Cli {
    #[structopt(short, long, parse(from_os_str), help = "input tree file")]
    infile: path::PathBuf,
    #[structopt(short, long, help = "the number of samples we'd like to get")]
    n: usize,
    #[structopt(short, long, help = "the column with the weights")]
    weights: Option<String>,
    #[structopt(long, help = "include these rows - names by Id column")]
    include: Option<Vec<String>>,
    #[structopt(short, long, help = "exclude these rows - names by Id column")]
    exclude: Option<Vec<String>>,
    #[structopt(long, help = "id column -  default is the first one")]
    id_col: Option<String>,
}

#[derive(Debug, PartialEq)]
struct Line {
    record: StringRecord,
    weight: f64,
    r: f64,
    index: f64,
    breaker: usize, // tie breaker
}

impl Eq for Line {}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap
// instead of a max-heap.
impl Ord for Line {
    fn cmp(&self, other: &Self) -> Ordering {
        // Notice that the we flip the ordering on costs.
        // In case of a tie we compare positions - this step is necessary
        // to make implementations of `PartialEq` and `Ord` consistent.
        if let Some(ordering) = other.index.partial_cmp(&self.index) {
            ordering
        } else {
            warn!("Numerical inprecision! Unable to distinguish between float indexes. picking randomly");
            let mut rng = rand::thread_rng();
            if rng.gen::<f64>() > 0.5 {
                other.breaker.cmp(&self.breaker)
            } else {
                self.breaker.cmp(&other.breaker)
            }
        }
    }
}
// `PartialOrd` needs to be implemented as well.
impl PartialOrd for Line {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
fn main() -> Result<(), Error> {
    env_logger::init();
    trace!("starting up");
    let args = Cli::from_args();
    debug!("{:?}", args);

    // println!("Hello, world!");
    // let data = "name\tplace\tid
    //     Mark\tMelbourne\t46
    //     Ashley\tZurich\t92";
    let delim = b'\t';

    let mut wtr = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .from_writer(io::stdout());

    let mut rng = rand::thread_rng();

    let mut reader = ReaderBuilder::new()
        .delimiter(delim)
        .from_path(&args.infile)
        .expect("Error reading file");

    let weight_column = match args.weights {
        Some(w) => Some(
            reader
                .headers()?
                .iter()
                .position(|r| r == w)
                .expect("weight column not found"),
        ),
        None => None,
    };
    let id_column = match args.id_col {
        Some(w) => reader
            .headers()?
            .iter()
            .position(|r| r == w)
            .expect("id column not found"),
        None => 0,
    };

    let include: Vec<String> = match args.include {
        Some(tips) => tips,
        None => vec![],
    };

    let exclude: Vec<String> = match args.exclude {
        Some(tips) => tips,
        None => vec![],
    };

    reader = ReaderBuilder::new()
        .delimiter(delim)
        .from_path(&args.infile)
        .expect("Error reading file");

    let mut heap = BinaryHeap::new();

    let mut i = 0;
    for record in reader.records() {
        let record = record?;
        if !exclude.contains(&String::from(record.get(id_column).unwrap())) {
            let weight: f64 = get_weight(weight_column, &record);
            let r = rng.gen::<f64>();
            let index = if include.contains(&String::from(record.get(id_column).unwrap())) {
                f64::INFINITY
            } else {
                (1.0 / weight) * r.log2()
            }; // log(index) where index = r^1/w
            if index == 0.0 {
                panic!("Weights are too small for numerical precision")
            }
            let line = Line {
                weight,
                r,
                index,
                record,
                breaker: i,
            };
            trace!("pushing line {:?}", &line);
            heap.push(line);
            i += 1;
            if heap.len() > args.n {
                let smallest = heap.pop();
                if let Some(poor_soul) = smallest {
                    trace!("removing line  {:?}", poor_soul)
                }
            }
        }
    }

    wtr.write_record(reader.headers()?)?;
    for line in heap.iter() {
        wtr.write_record(&line.record)?;
    }
    wtr.flush()?;

    Ok(())
}

fn get_weight(weight_column: Option<usize>, record: &StringRecord) -> f64 {
    match weight_column {
        Some(i) => {
            if let Some(w) = record.get(i) {
                let weight = w.parse::<f64>().unwrap();
                weight
            } else {
                return f64::NEG_INFINITY;
            }
        }
        None => 1.0,
    }
}
