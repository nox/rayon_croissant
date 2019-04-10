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
    /// `Target` to `target`.
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
    ///         |left, mut right| left.append(&mut right),
    ///     )
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(ingredients_lengths, [8, 6, 6, 7]);
    /// assert_eq!(ingredients_indices_with_odd_length, [3]);
    /// ```
    fn mapfold_reduce_into<'t, Output, Target, Mapfold, Reduce>(
        self,
        target: &'t mut Target,
        mapfold: Mapfold,
        reduce: Reduce,
    ) -> MapfoldReduce<'t, Target, Self, Mapfold, Reduce>
    where
        Output: Send,
        Target: Default + Send + 't,
        Mapfold: Clone + Fn(&mut Target, Self::Item) -> Output,
        Reduce: Clone + Fn(&mut Target, Target),
    {
        MapfoldReduce {
            target: target,
            input: self,
            mapfold: mapfold,
            reduce: reduce,
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
pub struct MapfoldReduce<'t, Target: 't, Input, Mapfold, Reduce> {
    target: &'t mut Target,
    input: Input,
    mapfold: Mapfold,
    reduce: Reduce,
}

impl<'t, Output, Target, Input, Mapfold, Reduce> ParallelIterator
    for MapfoldReduce<'t, Target, Input, Mapfold, Reduce>
where
    Output: Send,
    Target: Default + Send + 't,
    Input: ParallelIterator,
    Mapfold: Clone + Fn(&mut Target, Input::Item) -> Output + Send,
    Reduce: Clone + Fn(&mut Target, Target) + Send,
{
    type Item = Output;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Output>,
    {
        let iter_consumer =
            consumer::MapfoldReduceConsumer::new(self.target, self.mapfold, self.reduce, consumer);
        self.input.drive_unindexed(iter_consumer).into_output()
    }

    fn opt_len(&self) -> Option<usize> {
        self.input.opt_len()
    }
}

impl<'t, Output, Target, Input, Mapfold, Reduce> IndexedParallelIterator
    for MapfoldReduce<'t, Target, Input, Mapfold, Reduce>
where
    Input: IndexedParallelIterator,
    Target: Default + Send + 't,
    Output: Send,
    Mapfold: Clone + Fn(&mut Target, Input::Item) -> Output + Send,
    Reduce: Clone + Fn(&mut Target, Target) + Send,
{
    fn len(&self) -> usize {
        self.input.len()
    }

    fn drive<C>(self, consumer: C) -> C::Result
    where
        C: Consumer<Output>,
    {
        let iter_consumer =
            consumer::MapfoldReduceConsumer::new(self.target, self.mapfold, self.reduce, consumer);
        self.input.drive(iter_consumer).into_output()
    }

    fn with_producer<CB>(self, callback: CB) -> CB::Output
    where
        CB: ProducerCallback<Self::Item>,
    {
        self.input
            .with_producer(producer::MapfoldReduceCallback::new(
                self.target,
                self.mapfold,
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
