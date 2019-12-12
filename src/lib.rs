// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! > \<lqd> bikeshedding the name: something that is mapped folded and collected, a 🥐
//!
//! This crate offers a new parallel iterator adapter which allows you to
//! mapfold an iterator and reduce the result of the folds into a given
//! mutable reference.
//!
//! See [`ParallelIteratorExt::mapfold_reduce_into`] for more information.
//!
//! [`ParallelIteratorExt::mapfold_reduce_into`]: trait.ParallelIteratorExt.html#method.mapfold_reduce_into

#![forbid(missing_docs)]
#![forbid(unsafe_code)]

extern crate moite_moite;
extern crate rayon;

use rayon::iter::plumbing::{Consumer, ProducerCallback, UnindexedConsumer};
use rayon::iter::{IndexedParallelIterator, ParallelIterator};

mod consumer;
mod producer;

/// Extension methods for rayon's parallel iterators.
pub trait ParallelIteratorExt: ParallelIterator {
    /// Applies `mapfold` to each item of this iterator, producing a new iterator
    /// with the `Output` results, while folding and reducing each intermediate
    /// `Accumulator` to `accumulator`.
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate rayon;
    /// # extern crate rayon_croissant;
    /// use rayon::prelude::*;
    /// use rayon_croissant::ParallelIteratorExt;
    ///
    /// let ingredients = &[
    ///     "baguette",
    ///     "jambon",
    ///     "beurre",
    ///     "fromage",
    /// ];
    ///
    /// let mut ingredients_indices_with_odd_length = vec![];
    /// let ingredients_lengths = ingredients
    ///     .par_iter()
    ///     .enumerate()
    ///     .mapfold_reduce_into(
    ///         &mut ingredients_indices_with_odd_length,
    ///         |acc, (index, item)| {
    ///             let len = item.len();
    ///             if len % 2 == 1 {
    ///                 acc.push(index);
    ///             }
    ///             len
    ///         },
    ///         Default::default,
    ///         |left, mut right| left.append(&mut right),
    ///     )
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(ingredients_lengths, [8, 6, 6, 7]);
    /// assert_eq!(ingredients_indices_with_odd_length, [3]);
    /// ```
    fn mapfold_reduce_into<'acc, Output, Accumulator, Mapfold, Init, Reduce>(
        self,
        accumulator: &'acc mut Accumulator,
        mapfold: Mapfold,
        init: Init,
        reduce: Reduce,
    ) -> MapfoldReduce<'acc, Accumulator, Self, Mapfold, Init, Reduce>
    where
        Output: Send,
        Accumulator: Send + 'acc,
        Mapfold: Clone + Send + Fn(&mut Accumulator, Self::Item) -> Output,
        Init: Clone + Send + Fn() -> Accumulator,
        Reduce: Clone + Send + Fn(&mut Accumulator, Accumulator),
    {
        MapfoldReduce {
            accumulator,
            input: self,
            mapfold,
            init,
            reduce,
        }
    }
}

impl<Input> ParallelIteratorExt for Input where Input: ParallelIterator {}

/// `MapfoldReduce` is an iterator that transforms the elements of an underlying iterator.
///
/// This struct is created by the [`mapfold_reduce_into()`] method on [`ParallelIteratorExt`].
///
/// [`mapfold_reduce_into()`]: trait.ParallelIteratorExt.html#method.mapfold_reduce_into
/// [`ParallelIteratorExt`]: trait.ParallelIteratorExt.html
pub struct MapfoldReduce<'acc, Accumulator: 'acc, Input, Mapfold, Init, Reduce> {
    accumulator: &'acc mut Accumulator,
    input: Input,
    mapfold: Mapfold,
    init: Init,
    reduce: Reduce,
}

impl<'acc, Output, Accumulator, Input, Mapfold, Init, Reduce> ParallelIterator
    for MapfoldReduce<'acc, Accumulator, Input, Mapfold, Init, Reduce>
where
    Output: Send,
    Accumulator: Send + 'acc,
    Input: ParallelIterator,
    Mapfold: Clone + Fn(&mut Accumulator, Input::Item) -> Output + Send,
    Init: Clone + Send + Fn() -> Accumulator,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
{
    type Item = Output;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Output>,
    {
        let iter_consumer = consumer::MapfoldReduceConsumer::new(
            self.accumulator,
            self.mapfold,
            self.init,
            self.reduce,
            consumer,
        );
        self.input.drive_unindexed(iter_consumer).into_output()
    }

    fn opt_len(&self) -> Option<usize> {
        self.input.opt_len()
    }
}

impl<'acc, Output, Accumulator, Input, Mapfold, Init, Reduce> IndexedParallelIterator
    for MapfoldReduce<'acc, Accumulator, Input, Mapfold, Init, Reduce>
where
    Input: IndexedParallelIterator,
    Accumulator: Send + 'acc,
    Output: Send,
    Mapfold: Clone + Fn(&mut Accumulator, Input::Item) -> Output + Send,
    Init: Clone + Send + Fn() -> Accumulator,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
{
    fn len(&self) -> usize {
        self.input.len()
    }

    fn drive<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<Output>,
    {
        let iter_consumer = consumer::MapfoldReduceConsumer::new(
            self.accumulator,
            self.mapfold,
            self.init,
            self.reduce,
            consumer,
        );
        self.input.drive(iter_consumer).into_output()
    }

    fn with_producer<CB>(self, callback: CB) -> CB::Output
    where
        CB: ProducerCallback<Self::Item>,
    {
        self.input
            .with_producer(producer::MapfoldReduceCallback::new(
                self.accumulator,
                self.mapfold,
                self.init,
                self.reduce,
                callback,
            ))
    }
}

enum Sink<'t, T> {
    Borrowed(&'t mut T),
    Owned(T),
}

impl<'t, T> Sink<'t, T> {
    fn as_mut(&mut self) -> &mut T {
        match *self {
            Sink::Borrowed(ref mut borrowed) => borrowed,
            Sink::Owned(ref mut owned) => owned,
        }
    }

    fn into_owned(self) -> T {
        match self {
            Sink::Borrowed(_) => panic!("sink is borrowed"),
            Sink::Owned(owned) => owned,
        }
    }
}
