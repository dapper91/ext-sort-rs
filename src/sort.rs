//! External sorter implementation.

use log;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io;
use std::marker::PhantomData;
use std::path::Path;

use rayon::prelude::*;

use crate::chunk::{ExternalChunk, RmpExternalChunk};
use crate::merger::BinaryHeapMerger;

/// Sorting error.
#[derive(Debug)]
pub enum SortError {
    /// Temporary directory or file creation error.
    TempDir(io::Error),
    /// Workers thread pool initialization error.
    ThreadPoolBuildError(rayon::ThreadPoolBuildError),
    /// Data serialization/deserialization error.
    DeSerializationError(Box<dyn Error>),
    /// Input data stream error
    InputError(Box<dyn Error>),
}

impl Error for SortError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match &self {
            SortError::TempDir(err) => err,
            SortError::ThreadPoolBuildError(err) => err,
            SortError::DeSerializationError(err) => err.as_ref(),
            SortError::InputError(err) => err.as_ref(),
        })
    }
}

impl Display for SortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SortError::TempDir(err) => write!(f, "temporary directory or file not created: {}", err),
            SortError::ThreadPoolBuildError(err) => write!(f, "thread pool initialization failed: {}", err),
            SortError::DeSerializationError(err) => write!(f, "data serialization/deserialization error: {}", err),
            SortError::InputError(err) => write!(f, "input data stream error: {}", err),
        }
    }
}

/// External sorter builder.
#[derive(Clone)]
pub struct ExternalSorterBuilder {
    /// Number of threads to be used to sort data in parallel.
    threads_number: Option<usize>,
    /// Chunk size.
    chunk_size: usize,
    /// Directory to be used to store temporary data.
    tmp_dir: Option<Box<Path>>,
    /// Chunk file read/write buffer size.
    rw_buf_size: Option<usize>,
}

impl ExternalSorterBuilder {
    /// Creates an instance of builder with default parameters.
    pub fn new() -> Self {
        ExternalSorterBuilder::default()
    }

    /// Builds external sorter using provided configuration.
    pub fn build<T, C>(self) -> Result<ExternalSorter<T, C>, SortError>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Ord + Send,
        C: ExternalChunk<T>,
    {
        ExternalSorter::new(
            self.chunk_size,
            self.threads_number,
            self.tmp_dir.as_deref(),
            self.rw_buf_size,
        )
    }

    /// Sets number of threads to be used to sort data in parallel.
    pub fn with_threads_number(mut self, threads_number: usize) -> ExternalSorterBuilder {
        self.threads_number = Some(threads_number);
        return self;
    }

    /// Sets directory to be used to store temporary data.
    pub fn with_tmp_dir(mut self, path: &Path) -> ExternalSorterBuilder {
        self.tmp_dir = Some(path.into());
        return self;
    }

    /// Sets chunk size.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> ExternalSorterBuilder {
        self.chunk_size = chunk_size;
        return self;
    }

    /// Sets chunk read/write buffer size.
    pub fn with_rw_buf_size(mut self, buf_size: usize) -> ExternalSorterBuilder {
        self.rw_buf_size = Some(buf_size);
        return self;
    }
}

impl Default for ExternalSorterBuilder {
    fn default() -> Self {
        ExternalSorterBuilder {
            threads_number: None,
            tmp_dir: None,
            rw_buf_size: None,
            chunk_size: 1024 * 1024,
        }
    }
}

/// External sorter.
pub struct ExternalSorter<T, C = RmpExternalChunk<T>>
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned,
    C: ExternalChunk<T>,
{
    thread_pool: rayon::ThreadPool,
    chunk_size: usize,
    tmp_dir: tempfile::TempDir,
    rw_buf_size: Option<usize>,

    external_chunk_type: PhantomData<C>,
    item_type: PhantomData<T>,
}

impl<T, C> ExternalSorter<T, C>
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + Ord + Send,
    C: ExternalChunk<T>,
{
    /// Creates a new external sorter instance.
    pub fn new(
        chunk_size: usize,
        threads_number: Option<usize>,
        tmp_path: Option<&Path>,
        rw_buf_size: Option<usize>,
    ) -> Result<Self, SortError> {
        return Ok(ExternalSorter {
            chunk_size,
            rw_buf_size,
            thread_pool: Self::init_thread_pool(threads_number)?,
            tmp_dir: Self::init_tmp_directory(tmp_path)?,
            external_chunk_type: PhantomData,
            item_type: PhantomData,
        });
    }

    fn init_thread_pool(threads_number: Option<usize>) -> Result<rayon::ThreadPool, SortError> {
        let mut thread_pool_builder = rayon::ThreadPoolBuilder::new();

        if let Some(threads_number) = threads_number {
            log::info!("initializing thread-pool (threads: {})", threads_number);
            thread_pool_builder = thread_pool_builder.num_threads(threads_number);
        } else {
            log::info!("initializing thread-pool (threads: default)");
        }
        let thread_pool = thread_pool_builder
            .build()
            .map_err(|err| SortError::ThreadPoolBuildError(err))?;

        return Ok(thread_pool);
    }

    fn init_tmp_directory(tmp_path: Option<&Path>) -> Result<tempfile::TempDir, SortError> {
        let tmp_dir = if let Some(tmp_path) = tmp_path {
            tempfile::tempdir_in(tmp_path)
        } else {
            tempfile::tempdir()
        }
        .map_err(|err| SortError::TempDir(err))?;

        log::info!("using {} as a temporary directory", tmp_dir.path().display());

        return Ok(tmp_dir);
    }

    /// Sorts data from input using external sort algorithm.
    pub fn sort<I, E>(&self, input: I) -> Result<BinaryHeapMerger<T, impl ExternalChunk<T>>, SortError>
    where
        I: IntoIterator<Item = Result<T, E>>,
        E: Error + 'static,
    {
        let mut chunk_buf = Vec::with_capacity(self.chunk_size);
        let mut external_chunks = Vec::new();

        for item in input.into_iter() {
            match item {
                Ok(item) => chunk_buf.push(item),
                Err(err) => return Err(SortError::InputError(Box::new(err))),
            }

            if chunk_buf.len() == self.chunk_size {
                external_chunks.push(self.create_chunk(chunk_buf)?);
                chunk_buf = Vec::with_capacity(self.chunk_size);
            }
        }

        if chunk_buf.len() > 0 {
            external_chunks.push(self.create_chunk(chunk_buf)?);
        }

        log::debug!("external sort preparation done");

        return Ok(BinaryHeapMerger::new(external_chunks));
    }

    /// Sorts data and dumps it to an external chunk.
    fn create_chunk(&self, mut chunk: Vec<T>) -> Result<C, SortError> {
        log::debug!("sorting chunk data ...");
        self.thread_pool.install(|| {
            chunk.par_sort();
        });

        log::debug!("saving chunk data");
        let external_chunk = ExternalChunk::build(&self.tmp_dir, chunk, self.rw_buf_size)
            .map_err(|err| SortError::DeSerializationError(err))?;

        return Ok(external_chunk);
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::path::Path;

    use rand::seq::SliceRandom;

    use super::{ExternalSorter, ExternalSorterBuilder};

    #[test]
    fn test_external_sorter() {
        let input_sorted = 0..100;

        let mut input: Vec<Result<i32, io::Error>> = Vec::from_iter(input_sorted.clone().map(|item| Ok(item)));
        input.shuffle(&mut rand::thread_rng());

        let sorter: ExternalSorter<i32> = ExternalSorterBuilder::new()
            .with_chunk_size(8)
            .with_threads_number(2)
            .with_tmp_dir(Path::new("./"))
            .build()
            .unwrap();

        let result = sorter.sort(input).unwrap();

        let actual_result: Result<Vec<i32>, _> = result.collect();
        let actual_result = actual_result.unwrap();
        let expected_result = Vec::from_iter(input_sorted.clone());

        assert_eq!(actual_result, expected_result)
    }
}
