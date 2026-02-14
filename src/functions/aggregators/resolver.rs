//! [ALTERNATIVE APPROACH - IF THE FUNCTIONS BECOME TOO MANY TO MANAGE]
//! 
//! This module contains all the built-in aggregator functions.
//!
//! # Current Plan
//!
//! The aggregator module will use the following pipeline based
//! on my current idea of where this is going.
//!
//! AggregateResolver()
//! --> AggregateRegistry(IndexMap<String, Box<dyn Aggregate>)
//!     --> Aggregate :: run(Vec<String>)
//!
//! The aggregators will always be run by using a high level struct,
//! that keeps in itself a registry of avialable name -> closure mappings.
//! From all of these mappings, the resolver will decide what to do when
//! its own run() method is called which in this case will look somethings
//! like:
//! - Identify the name exists in the index keys for the aggregator name
//! - The aggregator to which that registry index points to is handed the
//!   list of arguments to run
//! - At the end, every aggregator is expected to return a single value
//!   (for now) that will always be a String value, for simplicity of scope.
use indexmap::IndexMap;

use crate::persistence::Row;

/// A trait that must be implemented by a struct to be registered and used as
/// an aggregate function by the persistence engine.
pub(crate) trait Aggregate {
    fn _has_wild_card(&self, args: &Vec<String>) -> bool {
        if args.contains(&"*".to_string()) {
            true
        } else {
            false
        }
    }

    fn run(&self, args: &Vec<String>, rows: &Vec<Row>) -> Result<String, String>;
}

pub(crate) struct AggregateResolver {
    registry: IndexMap<String, Box<dyn Aggregate>>,
}

impl AggregateResolver {
    fn _exists_in_registry(&self, name: &String) -> bool {
        self.registry
            .keys()
            .into_iter()
            .any(|reg_name| name == reg_name)
    }

    pub fn register(&mut self, name: String, aggregator: Box<dyn Aggregate>) {
        self.registry.insert(name, aggregator);
    }

    pub fn run(
        &self,
        name: &String,
        args: &Vec<String>,
        rows: &Vec<Row>,
    ) -> Result<String, String> {
        //! Run a particular aggregator.
        //!
        //! Takes the name of the aggregator and the arguments, alongwith
        //! a read-only reference to the rows.

        if !self._exists_in_registry(name) {
            Err(format!("Aggregator named {} does not exist.", name))
        } else {
            let aggregator = self.registry.get(name).unwrap();
            aggregator.run(args, rows)
        }
    }
}
