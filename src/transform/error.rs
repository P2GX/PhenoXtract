use polars::prelude::PolarsError;
use std::collections::HashMap;

#[derive(Debug)]
pub enum TransformError {
    #[allow(dead_code)]
    Building(String),
    #[allow(dead_code)]
    Strategy(String),
    #[allow(dead_code)]
    Mapping {
        strategy_name: String,
        old_value: String,
        possibles_mappings: HashMap<String, String>,
    },
    #[allow(dead_code)]
    Polars(PolarsError),
}
