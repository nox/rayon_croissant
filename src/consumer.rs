// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use rayon::iter::plumbing::{Consumer, Folder, Reducer, UnindexedConsumer};

use Sink;

/// A mapfold-reduce consumer.
///
/// Used by `MapfoldReduce` when wrapping a consumer-based parallel iterator.
pub struct MapfoldReduceConsumer<'t, Target, Mapfold, Reduce, OutputConsumer> {
    sink: Sink<'t, Target>,
    mapfold: Mapfold,
    reduce: Reduce,
    output_consumer: OutputConsumer,
}

pub struct MapfoldReduceFolder<'t, Target, Mapfold, OutputFolder> {
    sink: Sink<'t, Target>,
    mapfold: Mapfold,
    output_folder: OutputFolder,
}

pub struct MapfoldReduceReducer<Reduce, OutputReducer> {
    reduce: Reduce,
    output_reducer: OutputReducer,
}

pub struct MapfoldReduceResult<'t, Target, Output> {
    sink: Sink<'t, Target>,
    output: Output,
}

impl<'t, Target, Mapfold, Reduce, OutputConsumer>
    MapfoldReduceConsumer<'t, Target, Mapfold, Reduce, OutputConsumer>
{
    pub fn new(
        target: &'t mut Target,
        mapfold: Mapfold,
        reduce: Reduce,
        output_consumer: OutputConsumer,
    ) -> Self {
        MapfoldReduceConsumer {
            sink: Sink::Borrowed(target),
            mapfold,
            reduce,
            output_consumer,
        }
    }
}

impl<'t, Output, Target, Input, Mapfold, Reduce, OutputConsumer> Consumer<Input>
    for MapfoldReduceConsumer<'t, Target, Mapfold, Reduce, OutputConsumer>
where
    Output: Send,
    Target: Default + Send + 't,
    Input: Send,
    Mapfold: Clone + Fn(&mut Target, Input) -> Output + Send,
    Reduce: Clone + Fn(&mut Target, Target) + Send,
    OutputConsumer: Consumer<Output>,
{
    type Folder = MapfoldReduceFolder<'t, Target, Mapfold, OutputConsumer::Folder>;
    type Reducer = MapfoldReduceReducer<Reduce, OutputConsumer::Reducer>;
    type Result = MapfoldReduceResult<'t, Target, OutputConsumer::Result>;

    /// Splits the consumer in two.
    ///
    /// The existing sink is put in the left consumer and the right one
    /// gets a new owned one initialised from `Target::default()`. This ensures
    /// that the mutable reference to the final target is always in the
    /// left-most consumer.
    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer) {
        let (left_output_consumer, right_output_consumer, output_reducer) =
            self.output_consumer.split_at(index);
        let left_consumer = MapfoldReduceConsumer {
            sink: self.sink,
            mapfold: self.mapfold.clone(),
            reduce: self.reduce.clone(),
            output_consumer: left_output_consumer,
        };
        let right_consumer = MapfoldReduceConsumer {
            sink: Sink::Owned(Target::default()),
            mapfold: self.mapfold,
            reduce: self.reduce.clone(),
            output_consumer: right_output_consumer,
        };
        let reducer = MapfoldReduceReducer {
            reduce: self.reduce,
            output_reducer: output_reducer,
        };
        (left_consumer, right_consumer, reducer)
    }

    fn into_folder(self) -> Self::Folder {
        MapfoldReduceFolder {
            sink: self.sink,
            mapfold: self.mapfold,
            output_folder: self.output_consumer.into_folder(),
        }
    }

    fn full(&self) -> bool {
        self.output_consumer.full()
    }
}

impl<'t, Output, Target, Input, Mapfold, Reduce, OutputConsumer> UnindexedConsumer<Input>
    for MapfoldReduceConsumer<'t, Target, Mapfold, Reduce, OutputConsumer>
where
    Output: Send,
    Target: Default + Send + 't,
    Input: Send,
    Mapfold: Clone + Fn(&mut Target, Input) -> Output + Send,
    Reduce: Clone + Fn(&mut Target, Target) + Send,
    OutputConsumer: UnindexedConsumer<Output>,
{
    /// See `split_at`.
    fn split_off_left(&self) -> Self {
        MapfoldReduceConsumer {
            sink: Sink::Owned(Target::default()),
            mapfold: self.mapfold.clone(),
            reduce: self.reduce.clone(),
            output_consumer: self.output_consumer.split_off_left(),
        }
    }

    fn to_reducer(&self) -> Self::Reducer {
        MapfoldReduceReducer {
            reduce: self.reduce.clone(),
            output_reducer: self.output_consumer.to_reducer(),
        }
    }
}

impl<'t, Output, Target, Input, Mapfold, OutputConsumer> Folder<Input>
    for MapfoldReduceFolder<'t, Target, Mapfold, OutputConsumer>
where
    Output: Send,
    Target: Send + 't,
    Mapfold: Fn(&mut Target, Input) -> Output,
    OutputConsumer: Folder<Output>,
{
    type Result = MapfoldReduceResult<'t, Target, OutputConsumer::Result>;

    fn consume(mut self, input: Input) -> Self {
        let output = (self.mapfold)(self.sink.as_mut(), input);
        self.output_folder = self.output_folder.consume(output);
        self
    }

    fn complete(self) -> Self::Result {
        MapfoldReduceResult {
            sink: self.sink,
            output: self.output_folder.complete(),
        }
    }

    fn full(&self) -> bool {
        self.output_folder.full()
    }
}

impl<'t, Output, Target, Reduce, OutputReducer> Reducer<MapfoldReduceResult<'t, Target, Output>>
    for MapfoldReduceReducer<Reduce, OutputReducer>
where
    Target: Send + 't,
    Reduce: FnOnce(&mut Target, Target),
    OutputReducer: Reducer<Output>,
{
    /// Reduces two intermediate results from an ongoing mapfold-reduce operation.
    ///
    /// If this is the reduce call from the left-most split, the left sink
    /// is the mutable reference to the final target.
    fn reduce(
        self,
        mut left: MapfoldReduceResult<'t, Target, Output>,
        right: MapfoldReduceResult<'t, Target, Output>,
    ) -> MapfoldReduceResult<'t, Target, Output> {
        (self.reduce)(left.sink.as_mut(), right.sink.into_owned());

        MapfoldReduceResult {
            sink: left.sink,
            output: self.output_reducer.reduce(left.output, right.output),
        }
    }
}

impl<'t, Target, Output> MapfoldReduceResult<'t, Target, Output> {
    /// Returns the final output of this intermediate result.
    ///
    /// Panics if the sink is owned. The only way this can happen is if a
    /// consumer split us and reduced us from right to left.
    pub fn into_output(self) -> Output {
        if let Sink::Owned(_) = self.sink {
            panic!("final sink is owned");
        }
        self.output
    }
}
