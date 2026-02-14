/// This module stores the in-built aggregators and functions alongwith
/// their traits and structs.
/// 
/// The main concept for making this was to have a single storage where 
/// all the different aggregators and scalars can reside.
/// 
/// The advantage of this is easier tracking and modifiability of the
/// engine when a new scalar or aggregator is required.
/// 
/// For the start, I will have COUNT(*), MAX(col) and MIN(col) in the
/// aggregators and ADD(col, value) for a scalar just to see how things
/// sit. Once they sit well, we can add more. 
/// 
/// This is a test layout. I have no idea how to go well about this at
/// the moment :)

pub(crate) mod scalars;
pub(crate) mod aggregators;