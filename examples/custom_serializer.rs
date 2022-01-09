use std::fs;
use std::fs::File;
use std::io::{self, prelude::*, BufReader, BufWriter, Take};
use std::path;

use env_logger;
use log;

use ext_sort::{ExternalChunk, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};

struct CustomExternalChunk {
    reader: io::Take<io::BufReader<fs::File>>,
}

impl ExternalChunk<u32> for CustomExternalChunk {
    type SerializationError = io::Error;
    type DeserializationError = io::Error;

    fn new(reader: Take<BufReader<File>>) -> Self {
        CustomExternalChunk { reader }
    }

    fn dump(
        chunk_writer: &mut BufWriter<File>,
        items: impl IntoIterator<Item = u32>,
    ) -> Result<(), Self::SerializationError> {
        for item in items {
            chunk_writer.write_all(&item.to_le_bytes())?;
        }

        return Ok(());
    }
}

impl Iterator for CustomExternalChunk {
    type Item = Result<u32, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.limit() == 0 {
            None
        } else {
            let mut buf: [u8; 4] = [0; 4];
            match self.reader.read_exact(&mut buf.as_mut_slice()) {
                Ok(_) => Some(Ok(u32::from_le_bytes(buf))),
                Err(err) => Some(Err(err)),
            }
        }
    }
}

fn main() {
    env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();

    let input_reader = io::BufReader::new(fs::File::open("input.txt").unwrap());
    let mut output_writer = io::BufWriter::new(fs::File::create("output.txt").unwrap());

    let sorter: ExternalSorter<u32, io::Error, LimitedBufferBuilder, CustomExternalChunk> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(path::Path::new("./"))
            .with_buffer(LimitedBufferBuilder::new(1_000_000, true))
            .build()
            .unwrap();

    let sorted = sorter
        .sort(input_reader.lines().map(|line| {
            let line = line.unwrap();
            let number = line.parse().unwrap();

            return Ok(number);
        }))
        .unwrap();

    for item in sorted.map(Result::unwrap) {
        output_writer.write_all(format!("{}\n", item).as_bytes()).unwrap();
    }
    output_writer.flush().unwrap();
}
