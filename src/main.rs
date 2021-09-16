use csv::Error;
use csv::ReaderBuilder;
use rand::Rng;
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

    let mut total_weight: f64 = 0.0;
    let mut rng = rand::thread_rng();

    let mut total=0.0;
    let mut count=0;


    let mut reader = ReaderBuilder::new()
        .delimiter(delim)
        .from_path(&args.infile)
        .expect("Error reading file");

    let header_column = match args.weights {
            Some(w) => Some(
                reader
                    .headers()?
                    .iter()
                    .position(|r| r == w)
                    .expect("weight column not found"),
            ),
            None => None,
        };

    for record in reader.records() {
        let record = record?;
        match header_column {
            Some(i) => {
                if let Some(w) = record.get(i) {
                    let mut wd =  w.parse::<f64>().unwrap();
                    if wd*args.n as f64>1.0{
                        wd = 1.0 / (args.n as f64);
                    }
                    total_weight +=wd;
                }
            }
            None => total_weight += 1.0,
        };
    }


    reader = ReaderBuilder::new()
        .delimiter(delim)
        .from_path(&args.infile)
        .expect("Error reading file");

    wtr.write_record(reader.headers()?)?;
    for record in reader.records() {
        let record = record?;
        let weight: f64 = match header_column {
            Some(i) => {
                if let Some(w) = record.get(i) {
                    let mut wd =  w.parse::<f64>().unwrap();
                    if wd*args.n as f64>1.0{
                        wd = 1.0 / (args.n as f64);
                    }
                    wd
                } else {
                   0.0
                }
            }
            None => 1.0,
        };
        let threshold = (weight)*args.n as f64 / total_weight;
        total+=threshold.min(1.0);
        let r = rng.gen::<f64>();
        if r< threshold {
            count+=1;
            trace!("{}<{}",r,threshold);
            wtr.write_record(&record)?;
        }
    }
    wtr.flush()?;

    debug!("expectation: {}",total);
    debug!("found: {}",count);
    Ok(())
}
