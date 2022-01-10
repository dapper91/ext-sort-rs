use std::fs;
use std::io::{self, prelude::*};
use std::path;
use std::process;

use bytesize::ByteSize;
use clap::ArgEnum;
use env_logger;
use log;

use ext_sort::buffer::mem::MemoryLimitedBufferBuilder;
use ext_sort::{ExternalSorter, ExternalSorterBuilder};

fn main() {
    let arg_parser = build_arg_parser();

    let log_level: LogLevel = arg_parser.value_of_t_or_exit("log_level");
    init_logger(log_level);

    let order: Order = arg_parser.value_of_t_or_exit("sort");
    let tmp_dir: Option<&str> = arg_parser.value_of("tmp_dir");
    let chunk_size = arg_parser.value_of("chunk_size").expect("value is required");
    let threads: Option<usize> = arg_parser
        .is_present("threads")
        .then(|| arg_parser.value_of_t_or_exit("threads"));

    let input = arg_parser.value_of("input").expect("value is required");
    let input_stream = match fs::File::open(input) {
        Ok(file) => io::BufReader::new(file),
        Err(err) => {
            log::error!("input file opening error: {}", err);
            process::exit(1);
        }
    };

    let output = arg_parser.value_of("output").expect("value is required");
    let mut output_stream = match fs::File::create(output) {
        Ok(file) => io::BufWriter::new(file),
        Err(err) => {
            log::error!("output file creation error: {}", err);
            process::exit(1);
        }
    };

    let mut sorter_builder = ExternalSorterBuilder::new();
    if let Some(threads) = threads {
        sorter_builder = sorter_builder.with_threads_number(threads);
    }

    if let Some(tmp_dir) = tmp_dir {
        sorter_builder = sorter_builder.with_tmp_dir(path::Path::new(tmp_dir));
    }

    sorter_builder = sorter_builder.with_buffer(MemoryLimitedBufferBuilder::new(
        chunk_size.parse::<ByteSize>().expect("value is pre-validated").as_u64(),
    ));

    let sorter: ExternalSorter<String, io::Error, _> = match sorter_builder.build() {
        Ok(sorter) => sorter,
        Err(err) => {
            log::error!("sorter initialization error: {}", err);
            process::exit(1);
        }
    };

    let sorted_stream = match sorter.sort(input_stream.lines()) {
        Ok(sorted_stream) => sorted_stream,
        Err(err) => {
            log::error!("data sorting error: {}", err);
            process::exit(1);
        }
    };

    for line in sorted_stream {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                log::error!("sorting stream error: {}", err);
                process::exit(1);
            }
        };
        if let Err(err) = output_stream.write_all(format!("{}\n", line).as_bytes()) {
            log::error!("data saving error: {}", err);
            process::exit(1);
        };
    }

    if let Err(err) = output_stream.flush() {
        log::error!("data flushing error: {}", err);
        process::exit(1);
    }
}

#[derive(Copy, Clone, clap::ArgEnum)]
enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn possible_values() -> impl Iterator<Item = clap::PossibleValue<'static>> {
        Self::value_variants().iter().filter_map(|v| v.to_possible_value())
    }
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        <LogLevel as clap::ArgEnum>::from_str(s, false)
    }
}

#[derive(Copy, Clone, clap::ArgEnum)]
enum Order {
    Asc,
    Desc,
}

impl Order {
    pub fn possible_values() -> impl Iterator<Item = clap::PossibleValue<'static>> {
        Order::value_variants().iter().filter_map(|v| v.to_possible_value())
    }
}

impl std::str::FromStr for Order {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        <Order as clap::ArgEnum>::from_str(s, false)
    }
}

fn build_arg_parser() -> clap::ArgMatches {
    clap::App::new("ext-sort")
        .author("Dmitry P. <dapper1291@gmail.com>")
        .about("external sorter")
        .arg(
            clap::Arg::new("input")
                .short('i')
                .long("input")
                .help("file to be sorted")
                .required(true)
                .takes_value(true),
        )
        .arg(
            clap::Arg::new("output")
                .short('o')
                .long("output")
                .help("result file")
                .required(true)
                .takes_value(true),
        )
        .arg(
            clap::Arg::new("sort")
                .short('s')
                .long("sort")
                .help("sorting order")
                .takes_value(true)
                .default_value("asc")
                .possible_values(Order::possible_values()),
        )
        .arg(
            clap::Arg::new("log_level")
                .short('l')
                .long("loglevel")
                .help("logging level")
                .takes_value(true)
                .default_value("info")
                .possible_values(LogLevel::possible_values()),
        )
        .arg(
            clap::Arg::new("threads")
                .short('t')
                .long("threads")
                .help("number of threads to use for parallel sorting")
                .takes_value(true),
        )
        .arg(
            clap::Arg::new("tmp_dir")
                .short('d')
                .long("tmp-dir")
                .help("directory to be used to store temporary data")
                .takes_value(true),
        )
        .arg(
            clap::Arg::new("chunk_size")
                .short('c')
                .long("chunk-size")
                .help("chunk size")
                .required(true)
                .takes_value(true)
                .validator(|v| match v.parse::<ByteSize>() {
                    Ok(_) => Ok(()),
                    Err(err) => Err(format!("Chunk size format incorrect: {}", err)),
                }),
        )
        .get_matches()
}

fn init_logger(log_level: LogLevel) {
    env_logger::Builder::new()
        .filter_level(match log_level {
            LogLevel::Off => log::LevelFilter::Off,
            LogLevel::Error => log::LevelFilter::Error,
            LogLevel::Warn => log::LevelFilter::Warn,
            LogLevel::Info => log::LevelFilter::Info,
            LogLevel::Debug => log::LevelFilter::Debug,
            LogLevel::Trace => log::LevelFilter::Trace,
        })
        .format_timestamp_millis()
        .init();
}
