use std::fs;
use std::io::{self, prelude::*};
use std::path;

use bytesize::MB;
use env_logger;
use log;

use ext_sort::{buffer::mem::MemoryLimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};

fn main() {
    env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();

    let input_reader = io::BufReader::new(fs::File::open("input.txt").unwrap());
    let mut output_writer = io::BufWriter::new(fs::File::create("output.txt").unwrap());

    let sorter: ExternalSorter<String, io::Error, MemoryLimitedBufferBuilder> = ExternalSorterBuilder::new()
        .with_tmp_dir(path::Path::new("./"))
        .with_buffer(MemoryLimitedBufferBuilder::new(50 * MB))
        .build()
        .unwrap();

    let sorted = sorter.sort(input_reader.lines()).unwrap();

    for item in sorted.map(Result::unwrap) {
        output_writer.write_all(format!("{}\n", item).as_bytes()).unwrap();
    }
    output_writer.flush().unwrap();
}
