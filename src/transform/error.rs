use polars::prelude::PolarsError;
use std::collections::HashMap;

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum TransformError {
    #[allow(dead_code)]
    BuildingError(String),
    #[allow(dead_code)]
    StrategyError(String),
    #[allow(dead_code)]
    MappingError {
        strategy_name: String,
        old_value: String,
        possibles_mappings: HashMap<String, String>,
    },
    #[allow(dead_code)]
    PolarsError(PolarsError),
}
