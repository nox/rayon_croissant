// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rayon::iter::plumbing::{Producer, ProducerCallback};
use std::mem;
use std::ops::Drop;

use Sink;

pub struct MapfoldReduceCallback<'t, Accumulator: 't, Mapfold, Init, Reduce, OutputCallback> {
    accumulator: &'t mut Accumulator,
    mapfold: Mapfold,
    reduce: Reduce,
    init: Init,
    output_callback: OutputCallback,
}

struct MapfoldReduceProducer<
    't,
    Accumulator: 't,
    InputProducer,
    Mapfold,
    Init,
    Reduce: Fn(&mut Accumulator, Accumulator),
> {
    sink: Sink<'t, Accumulator>,
    part: Option<Part<'t, Accumulator, Reduce>>,
    input_producer: InputProducer,
    mapfold: Mapfold,
    init: Init,
    reduce: Reduce,
}

struct MapfoldReduceProducerIter<
    't,
    Accumulator: 't,
    Input,
    Mapfold,
    Init: Fn() -> Accumulator,
    Reduce: Fn(&mut Accumulator, Accumulator),
> {
    front_sink: Sink<'t, Accumulator>,
    back_sink: Accumulator,
    part: Option<Part<'t, Accumulator, Reduce>>,
    input_iter: Input,
    mapfold: Mapfold,
    init: Init,
    reduce: Reduce,
}

enum Part<'t, Accumulator: 't, Reduce: Fn(&mut Accumulator, Accumulator)> {
    Left(moite_moite::sync::Part<Option<Sink<'t, Accumulator>>, Joiner<'t, Accumulator, Reduce>>),
    Right(moite_moite::sync::Part<Option<Accumulator>, Joiner<'t, Accumulator, Reduce>>),
}

struct Joiner<'t, Accumulator: 't, Reduce: Fn(&mut Accumulator, Accumulator)> {
    left: Option<Sink<'t, Accumulator>>,
    right: Option<Accumulator>,
    reduce: Reduce,
    parent: Option<Part<'t, Accumulator, Reduce>>,
}

impl<'t, Accumulator, Mapfold, Reduce, Init, OutputCallback>
    MapfoldReduceCallback<'t, Accumulator, Mapfold, Init, Reduce, OutputCallback>
{
    pub fn new(
        accumulator: &'t mut Accumulator,
        mapfold: Mapfold,
        init: Init,
        reduce: Reduce,
        output_callback: OutputCallback,
    ) -> Self {
        MapfoldReduceCallback {
            accumulator,
            mapfold,
            init,
            reduce,
            output_callback,
        }
    }
}

impl<'t, Output, Accumulator, Input, Mapfold, Init, Reduce, OutputCallback> ProducerCallback<Input>
    for MapfoldReduceCallback<'t, Accumulator, Mapfold, Init, Reduce, OutputCallback>
where
    Output: Send,
    Accumulator: Send + 't,
    Input: Send,
    Mapfold: Clone + Fn(&mut Accumulator, Input) -> Output + Send,
    Init: Clone + Fn() -> Accumulator + Send,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
    OutputCallback: ProducerCallback<Output>,
{
    type Output = OutputCallback::Output;

    fn callback<P>(self, input_producer: P) -> Self::Output
    where
        P: Producer<Item = Input>,
    {
        self.output_callback.callback(MapfoldReduceProducer {
            sink: Sink::Borrowed(self.accumulator),
            part: None,
            input_producer,
            mapfold: self.mapfold,
            init: self.init,
            reduce: self.reduce,
        })
    }
}

impl<'t, Output, Accumulator, InputProducer, Mapfold, Init, Reduce> Producer
    for MapfoldReduceProducer<'t, Accumulator, InputProducer, Mapfold, Init, Reduce>
where
    Output: Send,
    Accumulator: Send + 't,
    InputProducer: Producer,
    Mapfold: Clone + Fn(&mut Accumulator, InputProducer::Item) -> Output + Send,
    Init: Clone + Fn() -> Accumulator + Send,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
{
    type Item = Output;
    type IntoIter =
        MapfoldReduceProducerIter<'t, Accumulator, InputProducer::IntoIter, Mapfold, Init, Reduce>;

    fn into_iter(self) -> Self::IntoIter {
        MapfoldReduceProducerIter {
            front_sink: self.sink,
            back_sink: (self.init)(),
            part: self.part,
            input_iter: self.input_producer.into_iter(),
            mapfold: self.mapfold,
            init: self.init,
            reduce: self.reduce,
        }
    }

    fn min_len(&self) -> usize {
        self.input_producer.min_len()
    }

    fn max_len(&self) -> usize {
        self.input_producer.min_len()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left_input_producer, right_input_producer) = self.input_producer.split_at(index);
        let joiner = Joiner {
            left: None,
            right: None,
            reduce: self.reduce.clone(),
            parent: self.part,
        };
        let (left_part, right_part) =
            moite_moite::sync::split_with(joiner, |j| (&mut j.left, &mut j.right));
        let left_producer = MapfoldReduceProducer {
            sink: self.sink,
            part: Some(Part::Left(left_part)),
            input_producer: left_input_producer,
            mapfold: self.mapfold.clone(),
            init: self.init.clone(),
            reduce: self.reduce.clone(),
        };
        let right_producer = MapfoldReduceProducer {
            sink: Sink::Owned((self.init)()),
            part: Some(Part::Right(right_part)),
            input_producer: right_input_producer,
            mapfold: self.mapfold,
            init: self.init,
            reduce: self.reduce,
        };
        (left_producer, right_producer)
    }
}

impl<'t, Output, Accumulator, Input, Mapfold, Init, Reduce> Iterator
    for MapfoldReduceProducerIter<'t, Accumulator, Input, Mapfold, Init, Reduce>
where
    Output: Send,
    Accumulator: Send + 't,
    Input: Iterator,
    Mapfold: Clone + Fn(&mut Accumulator, Input::Item) -> Output + Send,
    Init: Clone + Fn() -> Accumulator + Send,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
{
    type Item = Output;

    fn next(&mut self) -> Option<Self::Item> {
        let input = self.input_iter.next()?;
        let output = (self.mapfold)(self.front_sink.as_mut(), input);
        Some(output)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input_iter.size_hint()
    }
}

impl<'t, Output, Accumulator, Input, Mapfold, Init, Reduce> DoubleEndedIterator
    for MapfoldReduceProducerIter<'t, Accumulator, Input, Mapfold, Init, Reduce>
where
    Output: Send,
    Accumulator: Send + 't,
    Input: DoubleEndedIterator,
    Mapfold: Clone + Fn(&mut Accumulator, Input::Item) -> Output + Send,
    Init: Clone + Fn() -> Accumulator + Send,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let input = self.input_iter.next_back()?;
        let mut singleton = (self.init)();
        let output = (self.mapfold)(&mut singleton, input);
        mem::swap(&mut self.back_sink, &mut singleton);
        (self.reduce)(&mut self.back_sink, singleton);
        Some(output)
    }
}

impl<'t, Output, Accumulator, Input, Mapfold, Init, Reduce> ExactSizeIterator
    for MapfoldReduceProducerIter<'t, Accumulator, Input, Mapfold, Init, Reduce>
where
    Output: Send,
    Accumulator: Send + 't,
    Input: ExactSizeIterator,
    Mapfold: Clone + Fn(&mut Accumulator, Input::Item) -> Output + Send,
    Init: Clone + Fn() -> Accumulator + Send,
    Reduce: Clone + Fn(&mut Accumulator, Accumulator) + Send,
{
    fn len(&self) -> usize {
        self.input_iter.len()
    }
}

impl<'t, Accumulator, Input, Mapfold, Init, Reduce> Drop
    for MapfoldReduceProducerIter<'t, Accumulator, Input, Mapfold, Init, Reduce>
where
    Accumulator: 't,
    Init: Fn() -> Accumulator,
    Reduce: Fn(&mut Accumulator, Accumulator),
{
    fn drop(&mut self) {
        let mut front_sink = mem::replace(&mut self.front_sink, Sink::Owned((self.init)()));
        let back_sink = mem::replace(&mut self.back_sink, (self.init)());
        (self.reduce)(front_sink.as_mut(), back_sink);

        if let Some(split) = self.part.take() {
            split.commit(front_sink);
        } else if let Sink::Owned(_) = front_sink {
            panic!("front sink is owned and iter has no parent");
        }
    }
}

impl<'t, Accumulator, Reduce> Drop for Joiner<'t, Accumulator, Reduce>
where
    Accumulator: 't,
    Reduce: Fn(&mut Accumulator, Accumulator),
{
    fn drop(&mut self) {
        let mut left_sink = self.left.take().expect("left was not committed");
        let right_sink = self.right.take().expect("right was not committed");

        (self.reduce)(left_sink.as_mut(), right_sink);

        if let Some(parent) = self.parent.take() {
            parent.commit(left_sink);
        } else if let Sink::Owned(_) = left_sink {
            panic!("left sink is owned and joiner has no parent");
        }
    }
}

impl<'t, Accumulator, Reduce> Part<'t, Accumulator, Reduce>
where
    Accumulator: 't,
    Reduce: Fn(&mut Accumulator, Accumulator),
{
    fn commit(self, sink: Sink<'t, Accumulator>) {
        match self {
            Part::Left(mut left) => {
                assert!(left.is_none(), "left was already committed");
                *left = Some(sink);
            }
            Part::Right(mut right) => {
                assert!(right.is_none(), "right was already committed");
                *right = Some(sink.into_owned());
            }
        }
    }
}
