use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io::{self, prelude::*};
use std::path;

use env_logger;
use log;
use serde;

use ext_sort::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};

#[derive(Debug)]
enum CsvParseError {
    RowError(String),
    ColumnError(String),
}

impl Display for CsvParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvParseError::ColumnError(err) => write!(f, "column format error: {}", err),
            CsvParseError::RowError(err) => write!(f, "row format error: {}", err),
        }
    }
}

impl Error for CsvParseError {}

#[derive(PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Person {
    name: String,
    surname: String,
    age: u8,
}

impl Person {
    fn as_csv(&self) -> String {
        format!("{},{},{}", self.name, self.surname, self.age)
    }

    fn from_str(s: &str) -> Result<Self, CsvParseError> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 3 {
            Err(CsvParseError::RowError("wrong columns number".to_string()))
        } else {
            Ok(Person {
                name: parts[0].to_string(),
                surname: parts[1].to_string(),
                age: parts[2]
                    .parse()
                    .map_err(|err| CsvParseError::ColumnError(format!("age field format error: {}", err)))?,
            })
        }
    }
}

impl PartialOrd for Person {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for Person {
    fn cmp(&self, other: &Self) -> Ordering {
        self.surname
            .cmp(&other.surname)
            .then(self.name.cmp(&other.name))
            .then(self.age.cmp(&other.age))
    }
}

fn main() {
    env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();

    let input_reader = io::BufReader::new(fs::File::open("input.csv").unwrap());
    let mut output_writer = io::BufWriter::new(fs::File::create("output.csv").unwrap());

    let sorter: ExternalSorter<Person, io::Error, LimitedBufferBuilder> = ExternalSorterBuilder::new()
        .with_tmp_dir(path::Path::new("./"))
        .with_buffer(LimitedBufferBuilder::new(1_000_000, true))
        .build()
        .unwrap();

    let sorted = sorter
        .sort(
            input_reader
                .lines()
                .map(|line| line.map(|line| Person::from_str(&line).unwrap())),
        )
        .unwrap();

    for item in sorted.map(Result::unwrap) {
        output_writer
            .write_all(format!("{}\n", item.as_csv()).as_bytes())
            .unwrap();
    }
    output_writer.flush().unwrap();
}
