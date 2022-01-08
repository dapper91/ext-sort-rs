//! Limited buffer implementations.

use rayon;

/// Buffer builder.
pub trait ChunkBufferBuilder<T: Send>: Default {
    type Buffer: ChunkBuffer<T>;

    /// Creates a new buffer.
    fn build(&self) -> Self::Buffer;
}

/// Base limited buffer interface.
pub trait ChunkBuffer<T: Send>: IntoIterator<Item = T> + rayon::slice::ParallelSliceMut<T> + Send {
    /// Adds a new element to the buffer.
    fn push(&mut self, item: T);

    /// Returns buffer length
    fn len(&self) -> usize;

    /// Checks if the buffer reached the limit.
    fn is_full(&self) -> bool;
}

pub struct LimitedBufferBuilder {
    buffer_limit: usize,
    preallocate: bool,
}

impl LimitedBufferBuilder {
    pub fn new(buffer_limit: usize, preallocate: bool) -> Self {
        LimitedBufferBuilder {
            buffer_limit,
            preallocate,
        }
    }
}

impl<T: Send> ChunkBufferBuilder<T> for LimitedBufferBuilder {
    type Buffer = LimitedBuffer<T>;

    fn build(&self) -> Self::Buffer {
        if self.preallocate {
            LimitedBuffer::new(self.buffer_limit)
        } else {
            LimitedBuffer::with_capacity(self.buffer_limit)
        }
    }
}

impl Default for LimitedBufferBuilder {
    fn default() -> Self {
        LimitedBufferBuilder {
            buffer_limit: usize::MAX,
            preallocate: false,
        }
    }
}

/// Buffer limited by elements count.
pub struct LimitedBuffer<T> {
    limit: usize,
    inner: Vec<T>,
}

impl<T> LimitedBuffer<T> {
    pub fn new(limit: usize) -> Self {
        LimitedBuffer {
            limit,
            inner: Vec::new(),
        }
    }

    pub fn with_capacity(limit: usize) -> Self {
        LimitedBuffer {
            limit,
            inner: Vec::with_capacity(limit),
        }
    }
}

impl<T: Send> ChunkBuffer<T> for LimitedBuffer<T> {
    fn push(&mut self, item: T) {
        self.inner.push(item);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_full(&self) -> bool {
        self.inner.len() >= self.limit
    }
}

impl<T> IntoIterator for LimitedBuffer<T> {
    type Item = T;
    type IntoIter = <Vec<T> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<T: Send> rayon::slice::ParallelSliceMut<T> for LimitedBuffer<T> {
    fn as_parallel_slice_mut(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }
}

#[cfg(test)]
mod test {
    use super::{ChunkBuffer, ChunkBufferBuilder, LimitedBufferBuilder};

    #[test]
    fn test_limited_buffer() {
        let builder = LimitedBufferBuilder::new(2, true);
        let mut buffer = builder.build();

        buffer.push(0);
        assert_eq!(buffer.is_full(), false);
        buffer.push(1);
        assert_eq!(buffer.is_full(), true);

        let data = Vec::from_iter(buffer);
        assert_eq!(data, vec![0, 1]);
    }
}

#[cfg(feature = "memory-limit")]
pub mod mem {
    use deepsize;
    use rayon;

    use super::{ChunkBuffer, ChunkBufferBuilder};

    pub struct MemoryLimitedBufferBuilder {
        buffer_limit: u64,
    }

    impl MemoryLimitedBufferBuilder {
        pub fn new(buffer_limit: u64) -> Self {
            MemoryLimitedBufferBuilder { buffer_limit }
        }
    }

    impl<T: Send> ChunkBufferBuilder<T> for MemoryLimitedBufferBuilder
    where
        T: deepsize::DeepSizeOf,
    {
        type Buffer = MemoryLimitedBuffer<T>;

        fn build(&self) -> Self::Buffer {
            MemoryLimitedBuffer::new(self.buffer_limit)
        }
    }

    impl Default for MemoryLimitedBufferBuilder {
        fn default() -> Self {
            MemoryLimitedBufferBuilder { buffer_limit: u64::MAX }
        }
    }

    /// Buffer limited by consumed memory.
    pub struct MemoryLimitedBuffer<T> {
        limit: u64,
        current_size: u64,
        inner: Vec<T>,
    }

    impl<T> MemoryLimitedBuffer<T> {
        pub fn new(limit: u64) -> Self {
            MemoryLimitedBuffer {
                limit,
                current_size: 0,
                inner: Vec::new(),
            }
        }

        pub fn mem_size(&self) -> u64 {
            self.current_size
        }
    }

    impl<T: Send> ChunkBuffer<T> for MemoryLimitedBuffer<T>
    where
        T: deepsize::DeepSizeOf,
    {
        fn push(&mut self, item: T) {
            self.current_size += item.deep_size_of() as u64;
            self.inner.push(item);
        }

        fn len(&self) -> usize {
            self.inner.len()
        }

        fn is_full(&self) -> bool {
            self.current_size >= self.limit
        }
    }

    impl<T> IntoIterator for MemoryLimitedBuffer<T> {
        type Item = T;
        type IntoIter = <Vec<T> as IntoIterator>::IntoIter;

        fn into_iter(self) -> Self::IntoIter {
            self.inner.into_iter()
        }
    }

    impl<T: Send> rayon::slice::ParallelSliceMut<T> for MemoryLimitedBuffer<T> {
        fn as_parallel_slice_mut(&mut self) -> &mut [T] {
            self.inner.as_mut_slice()
        }
    }

    #[cfg(test)]
    mod test {
        use deepsize;

        use super::{ChunkBuffer, ChunkBufferBuilder, MemoryLimitedBufferBuilder};

        #[derive(Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
        struct MyType {
            number: i64,
            string: String,
        }

        #[test]
        fn test_memory_limited_buffer() {
            let builder = MemoryLimitedBufferBuilder::new(76);
            let mut buffer = builder.build();

            let item1 = MyType {
                number: 0,               // 8 bytes
                string: "hello!".into(), // 8 + 8 + 8 + 6 = 30 bytes
            };
            buffer.push(item1.clone());
            assert_eq!(buffer.mem_size(), 38);
            assert_eq!(buffer.is_full(), false);

            let item2 = MyType {
                number: 1,               // 8 bytes
                string: "world!".into(), // 8 + 8 + 8 + 6 = 30 bytes
            };
            buffer.push(item2.clone());
            assert_eq!(buffer.mem_size(), 76);
            assert_eq!(buffer.is_full(), true);

            let actual_data = Vec::from_iter(buffer);
            let expected_data = vec![item1, item2];
            assert_eq!(actual_data, expected_data);
        }
    }
}
