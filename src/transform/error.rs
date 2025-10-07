use std::collections::HashMap;
use std::fmt::Display;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MappingSuggestion {
    from: String,
    to: String,
}

impl MappingSuggestion {
    fn new(from: String, to: String) -> Self {
        MappingSuggestion { from, to }
    }

    pub fn from_hashmap<T: Display>(hashmap: &HashMap<String, T>) -> Vec<MappingSuggestion> {
        hashmap
            .iter()
            .map(|(key, value)| MappingSuggestion::new(key.clone(), value.to_string()))
            .collect()
    }

    #[allow(dead_code)]
    pub fn suggestions_to_hashmap(suggestions: Vec<MappingSuggestion>) -> HashMap<String, String> {
        suggestions
            .into_iter()
            .map(|mapping| (mapping.from, mapping.to))
            .collect()
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MappingErrorInfo {
    pub column: String,
    pub table: String,
    pub old_value: String,
    pub possible_mappings: Vec<MappingSuggestion>,
}

#[derive(Debug, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum TransformError {
    #[allow(dead_code)]
    StrategyError(String),
    #[allow(dead_code)]
    MappingError {
        strategy_name: String,
        info: Vec<MappingErrorInfo>,
    },
    #[allow(dead_code)]
    CollectionError(String),
    #[allow(dead_code)]
    BuilderError(String),
    CastingError(String),
}
