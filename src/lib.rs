//! `ext-sort` is a rust external sort algorithm implementation.
//!
//! External sorting is a class of sorting algorithms that can handle massive amounts of data. External sorting
//! is required when the data being sorted do not fit into the main memory (RAM) of a computer and instead must be
//! resided in slower external memory, usually a hard disk drive. Sorting is achieved in two passes. During the
//! first pass it sorts chunks of data that each fit in RAM, during the second pass it merges the sorted chunks
//! together. For more information see [External Sorting](https://en.wikipedia.org/wiki/External_sorting).
//!
//! # Overview
//!
//! `ext-sort` supports the following features:
//!
//! * **Data agnostic:**
//!   it supports all data types that implement `serde` serialization/deserialization by default,
//!   otherwise you can implement your own serialization/deserialization mechanism.
//! * **Serialization format agnostic:**
//!   the library uses `MessagePack` serialization format by default, but it can be easily substituted by your custom
//!   one if `MessagePack` serialization/deserialization performance is not sufficient for your task.
//! * **Multithreading support:**
//!   multi-threaded sorting is supported, which means data is sorted in multiple threads utilizing maximum CPU
//!   resources and reducing sorting time.
//! * **Memory limit support:**
//!   memory limited sorting is supported. It allows you to limit sorting memory consumption
//!   (`memory-limit` feature required).
//!
//! # Example
//!
//! ```no_run
//! use std::fs;
//! use std::io::{self, prelude::*};
//! use std::path;
//!
//! use bytesize::MB;
//! use env_logger;
//! use log;
//!
//! use ext_sort::{buffer::mem::MemoryLimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
//!
//! fn main() {
//!     env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();
//!
//!     let input_reader = io::BufReader::new(fs::File::open("input.txt").unwrap());
//!     let mut output_writer = io::BufWriter::new(fs::File::create("output.txt").unwrap());
//!
//!     let sorter: ExternalSorter<String, io::Error, MemoryLimitedBufferBuilder> = ExternalSorterBuilder::new()
//!         .with_tmp_dir(path::Path::new("./"))
//!         .with_buffer(MemoryLimitedBufferBuilder::new(50 * MB))
//!         .build()
//!         .unwrap();
//!
//!     let sorted = sorter.sort(input_reader.lines()).unwrap();
//!
//!     for item in sorted.map(Result::unwrap) {
//!         output_writer.write_all(format!("{}\n", item).as_bytes()).unwrap();
//!     }
//!     output_writer.flush().unwrap();
//! }
//! ```

pub mod buffer;
pub mod chunk;
pub mod merger;
pub mod sort;

pub use buffer::{ChunkBuffer, ChunkBufferBuilder, LimitedBuffer, LimitedBufferBuilder};
pub use chunk::{ExternalChunk, RmpExternalChunk};
pub use merger::BinaryHeapMerger;
pub use sort::{ExternalSorter, ExternalSorterBuilder, SortError};
