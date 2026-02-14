//! [ALTERNATIVE APPROACH - IF THE FUNCTIONS BECOME TOO MANY TO MANAGE]
//! 
//! This module contains all the available scalars that can be applied
//! on to any value in the cell of a [`Table`] struct via the of course,
//! the [`TableReader`] object.
//!
//! # Current Plan
//!
//! The scalar module will use the following pipeline based
//! on my current idea of where this is going.
//!
//! ScalarResolver()
//! --> ScalarRegistry(IndexMap<String, Box<dyn Scalar>>)
//!     --> Scalar :: run(&Row)
//! 
//! A scalar is to be invoked by a high level resolver, which is a safety
//! net that checks whether the scalar being called exists or not. All
//! scalars are expected to return a [`String`] value once they complete.
use crate::persistence::Row;

/// The persistence engine relies on all scalars to implement
/// this trait for it to run the scalar on a row.
///
/// The [`Scalar::run`] method takes in a single [`Row`] as read-ony
/// and returns a [`String`] value if the result can be processed, or
/// [`None`] otherwise, denoting the value could not be computed.
pub(crate) trait Scalar {
    fn run(row: &Row) -> Option<String>;
}