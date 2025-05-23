//! Binary heap merger.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;

/// Value wrapper binding custom compare function to a value.
struct OrderedWrapper<T, F>
where
    F: Fn(&T, &T) -> Ordering,
{
    value: T,
    compare: F,
}

impl<T, F> OrderedWrapper<T, F>
where
    F: Fn(&T, &T) -> Ordering,
{
    fn wrap(value: T, compare: F) -> Self {
        OrderedWrapper { value, compare }
    }

    fn unwrap(self) -> T {
        self.value
    }
}

impl<T, F> PartialEq for OrderedWrapper<T, F>
where
    F: Fn(&T, &T) -> Ordering,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T, F> Eq for OrderedWrapper<T, F> where F: Fn(&T, &T) -> Ordering {}

impl<T, F> PartialOrd for OrderedWrapper<T, F>
where
    F: Fn(&T, &T) -> Ordering,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T, F> Ord for OrderedWrapper<T, F>
where
    F: Fn(&T, &T) -> Ordering,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (self.compare)(&self.value, &other.value)
    }
}

/// Binary heap merger implementation.
/// Merges multiple sorted inputs into a single sorted output.
/// Time complexity is *m* \* log(*n*) in worst case where *m* is the number of items,
/// *n* is the number of chunks (inputs).
pub struct BinaryHeapMerger<T, E, F, C>
where
    E: Error,
    F: Fn(&T, &T) -> Ordering,
    C: IntoIterator<Item = Result<T, E>>,
{
    // binary heap is max-heap by default so we reverse it to convert it to min-heap
    items: BinaryHeap<(std::cmp::Reverse<OrderedWrapper<T, F>>, std::cmp::Reverse<usize>)>,
    chunks: Vec<C::IntoIter>,
    initiated: bool,
    compare: F,
}

impl<T, E, F, C> BinaryHeapMerger<T, E, F, C>
where
    E: Error,
    F: Fn(&T, &T) -> Ordering,
    C: IntoIterator<Item = Result<T, E>>,
{
    /// Creates an instance of a binary heap merger using chunks as inputs.
    /// Chunk items should be sorted in ascending order otherwise the result is undefined.
    ///
    /// # Arguments
    /// * `chunks` - Chunks to be merged in a single sorted one
    pub fn new<I>(chunks: I, compare: F) -> Self
    where
        I: IntoIterator<Item = C>,
    {
        let chunks = Vec::from_iter(chunks.into_iter().map(|c| c.into_iter()));
        let items = BinaryHeap::with_capacity(chunks.len());

        return BinaryHeapMerger {
            chunks,
            items,
            compare,
            initiated: false,
        };
    }
}

impl<T, E, F, C> Iterator for BinaryHeapMerger<T, E, F, C>
where
    E: Error,
    F: Fn(&T, &T) -> Ordering + Copy,
    C: IntoIterator<Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    /// Returns the next item from the inputs in ascending order.
    fn next(&mut self) -> Option<Self::Item> {
        if !self.initiated {
            for (idx, chunk) in self.chunks.iter_mut().enumerate() {
                if let Some(item) = chunk.next() {
                    match item {
                        Ok(item) => self
                            .items
                            .push((std::cmp::Reverse(OrderedWrapper::wrap(item, self.compare)), std::cmp::Reverse(idx))),
                        Err(err) => return Some(Err(err)),
                    }
                }
            }
            self.initiated = true;
        }

        let (result, idx) = self.items.pop()?;
        if let Some(item) = self.chunks[idx.0].next() {
            match item {
                Ok(item) => self
                    .items
                    .push((std::cmp::Reverse(OrderedWrapper::wrap(item, self.compare)), idx)),
                Err(err) => return Some(Err(err)),
            }
        }

        return Some(Ok(result.0.unwrap()));
    }
}

#[cfg(test)]
mod test {
    use rstest::*;
    use std::error::Error;
    use std::io::{self, ErrorKind};

    use super::BinaryHeapMerger;

    #[rstest]
    #[case(
        vec![],
        vec![],
    )]
    #[case(
        vec![
            vec![],
            vec![]
        ],
        vec![],
    )]
    #[case(
        vec![
            vec![Ok(4), Ok(5), Ok(7)],
            vec![Ok(1), Ok(6)],
            vec![Ok(3)],
            vec![],
        ],
        vec![Ok(1), Ok(3), Ok(4), Ok(5), Ok(6), Ok(7)],
    )]
    #[case(
        vec![
            vec![Result::Err(io::Error::new(ErrorKind::Other, "test error"))]
        ],
        vec![
            Result::Err(io::Error::new(ErrorKind::Other, "test error"))
        ],
    )]
    #[case(
        vec![
            vec![Ok(3), Result::Err(io::Error::new(ErrorKind::Other, "test error"))],
            vec![Ok(1), Ok(2)],
        ],
        vec![
            Ok(1),
            Ok(2),
            Result::Err(io::Error::new(ErrorKind::Other, "test error")),
        ],
    )]
    fn test_merger(
        #[case] chunks: Vec<Vec<Result<i32, io::Error>>>,
        #[case] expected_result: Vec<Result<i32, io::Error>>,
    ) {
        let merger = BinaryHeapMerger::new(chunks, i32::cmp);
        let actual_result = merger.collect();
        assert!(
            compare_vectors_of_result::<_, io::Error>(&actual_result, &expected_result),
            "actual={:?}, expected={:?}",
            actual_result,
            expected_result
        );
    }

    fn compare_vectors_of_result<T: PartialEq, E: Error + 'static>(
        actual: &Vec<Result<T, E>>,
        expected: &Vec<Result<T, E>>,
    ) -> bool {
        actual
            .into_iter()
            .zip(expected)
            .all(
                |(actual_result, expected_result)| match (actual_result, expected_result) {
                    (Ok(actual_result), Ok(expected_result)) if actual_result == expected_result => true,
                    (Err(actual_err), Err(expected_err)) => actual_err.to_string() == expected_err.to_string(),
                    _ => false,
                },
            )
    }
}
