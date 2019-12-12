extern crate rayon;
extern crate rayon_croissant;

use rayon::prelude::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon_croissant::ParallelIteratorExt;

#[test]
fn test_zip_rev() {
    let ingredients = &["jambon", "beurre", "fromage", "baguette"];
    let scores = &[0, 1, 2, 3];

    let mut names = vec![];
    let rev_scores = ingredients
        .par_iter()
        .cloned()
        .zip(scores.par_iter().cloned())
        .with_min_len(2)
        .mapfold_reduce_into(
            &mut names,
            |names, (name, score)| {
                names.push(name);
                score
            },
            Default::default,
            |left_names, mut right_names| {
                left_names.append(&mut right_names);
            },
        )
        // Reversing *after* a call to mapfold_reduce_into shouldn't
        // change the result of the folding operation.
        .rev()
        .collect::<Vec<_>>();

    assert_eq!(names, ingredients);
    assert_eq!(rev_scores, [3, 2, 1, 0]);
}
