//! External sort algorithm implementation. External sorting is a class of sorting algorithms
//! that can handle massive amounts of data. External sorting is required when the data being
//! sorted do not fit into the main memory (RAM) of a computer and instead must be resided in
//! slower external memory, usually a hard disk drive. Sorting is achieved in two passes.
//! During the first pass it sorts chunks of data that each fit in RAM, during the second pass
//! it merges the sorted chunks together.
//! For more information see https://en.wikipedia.org/wiki/External_sorting.

pub mod chunk;
pub mod merger;

pub use chunk::{ExternalChunk, RmpExternalChunk};
pub use merger::BinaryHeapMerger;
