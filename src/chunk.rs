use std::error::Error;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::marker::PhantomData;

use tempfile;

/// External chunk interface. Provides methods for creating a chunk stored on file system and reading data from it.
pub trait ExternalChunk<T>: Sized + Iterator<Item = Result<T, Box<dyn Error>>> {
    /// Builds an instance of an external chunk.
    fn build(
        dir: &tempfile::TempDir,
        items: impl IntoIterator<Item = T>,
        buf_size: Option<usize>,
    ) -> Result<Self, Box<dyn Error>> {
        let tmp_file = tempfile::tempfile_in(dir)?;

        let mut chunk_writer = match buf_size {
            Some(buf_size) => io::BufWriter::with_capacity(buf_size, tmp_file.try_clone()?),
            None => io::BufWriter::new(tmp_file.try_clone()?),
        };

        Self::dump(&mut chunk_writer, items)?;

        chunk_writer.flush()?;

        let mut chunk_reader = match buf_size {
            Some(buf_size) => io::BufReader::with_capacity(buf_size, tmp_file.try_clone()?),
            None => io::BufReader::new(tmp_file.try_clone()?),
        };

        chunk_reader.rewind()?;
        let file_len = tmp_file.metadata()?.len();

        return Ok(Self::new(chunk_reader.take(file_len)));
    }

    /// Creates and instance of an external chunk.
    fn new(reader: io::Take<io::BufReader<fs::File>>) -> Self;

    /// Dumps items to an external file.
    fn dump(
        chunk_writer: &mut io::BufWriter<fs::File>,
        items: impl IntoIterator<Item = T>,
    ) -> Result<(), Box<dyn Error>>;
}

/// RMP (Rust MessagePack) external chunk implementation.
/// It uses MessagePack as a data serialization format.
/// For more information see https://msgpack.org/.
pub struct RmpExternalChunk<T> {
    reader: io::Take<io::BufReader<fs::File>>,

    item_type: PhantomData<T>,
}

impl<T> ExternalChunk<T> for RmpExternalChunk<T>
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    fn new(reader: io::Take<io::BufReader<fs::File>>) -> Self {
        RmpExternalChunk {
            reader,
            item_type: PhantomData,
        }
    }

    fn dump(
        mut chunk_writer: &mut io::BufWriter<fs::File>,
        items: impl IntoIterator<Item = T>,
    ) -> Result<(), Box<dyn Error>> {
        for item in items.into_iter() {
            rmp_serde::encode::write(&mut chunk_writer, &item)?;
        }

        return Ok(());
    }
}

impl<T> Iterator for RmpExternalChunk<T>
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    type Item = Result<T, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.limit() == 0 {
            None
        } else {
            match rmp_serde::decode::from_read(&mut self.reader) {
                Ok(result) => Some(Ok(result)),
                Err(err) => Some(Err(Box::new(err))),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use rstest::*;

    use super::{ExternalChunk, RmpExternalChunk};

    #[fixture]
    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir_in("./").unwrap()
    }

    #[rstest]
    fn test_rmp_chunk(tmp_dir: tempfile::TempDir) {
        let saved = Vec::from_iter(0..100);

        let chunk: RmpExternalChunk<_> = ExternalChunk::build(&tmp_dir, saved.clone(), None).unwrap();

        let restored: Result<Vec<i32>, Box<dyn Error>> = chunk.collect();
        let restored = restored.unwrap();

        assert_eq!(restored, saved);
    }
}
