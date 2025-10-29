use crate::config::table_context::Context;
use crate::validation::error::ValidationError as PxValidationError;
use polars::error::PolarsError;
use polars::prelude::DataType;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::num::ParseFloatError;
use thiserror::Error;

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

impl Display for MappingErrorInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "column '{}' in table '{}' with value '{}'",
            self.column, self.table, self.old_value
        )?;
        if !self.possible_mappings.is_empty() {
            write!(f, " (possible mappings: ")?;
            for (i, mapping) in self.possible_mappings.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", mapping)?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl Display for MappingSuggestion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}' -> '{}'", self.from, self.to)
    }
}

#[derive(Debug, Error)]
pub enum DataProcessingError {
    #[error("DataFrame Filter result was unexpectedly empty.")]
    EmptyFilteringError,

    #[error("Unable to cast column '{col_name}' from type '{from}' to '{to}'")]
    CastingError {
        col_name: String,
        from: DataType,
        to: DataType,
    },
}
#[derive(Debug, Error)]
pub enum TransformError {
    #[error("StrategyError error: {0}")]
    StrategyError(#[from] StrategyError),
    #[error("CollectorError error: {0}")]
    CollectorError(#[from] Box<CollectorError>),
}

impl From<CollectorError> for TransformError {
    fn from(err: CollectorError) -> Self {
        TransformError::CollectorError(Box::new(err))
    }
}

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
pub enum StrategyError {
    #[error("Could not {transformation} column '{col_name}' for table '{table_name}'")]
    TransformationError {
        transformation: String,
        col_name: String,
        table_name: String,
    },

    #[error("Strategy '{strategy_name}' unable to map {}", info.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", "))]
    MappingError {
        strategy_name: String,
        info: Vec<MappingErrorInfo>,
    },
    #[error(transparent)]
    ValidationError(#[from] PxValidationError),
    #[error(transparent)]
    DataProcessing(#[from] Box<DataProcessingError>),
    #[error("Polars error: {0}")]
    PolarsError(Box<PolarsError>),
}

impl From<DataProcessingError> for StrategyError {
    fn from(err: DataProcessingError) -> Self {
        StrategyError::DataProcessing(Box::new(err))
    }
}

impl From<PolarsError> for StrategyError {
    fn from(err: PolarsError) -> Self {
        StrategyError::PolarsError(Box::new(err))
    }
}

#[derive(Debug, Error)]
pub enum CollectorError {
    #[error("Expected only one column for context '{context}' in table '{table_name}'")]
    ExpectedSingleColumn {
        table_name: String,
        context: Context,
    },
    #[error(
        "Found multiple values of {context} in table {table_name} for {patient_id} when there should only be one."
    )]
    ExpectedSingleValue {
        table_name: String,
        patient_id: String,
        context: Context,
    },
    #[error(transparent)]
    DataProcessing(Box<DataProcessingError>),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("ParseFloatError error: {0}")]
    ParseFloatError(#[from] ParseFloatError),
    #[error("PhenopacketBuilderError error: {0}")]
    PhenopacketBuilderError(#[from] PhenopacketBuilderError),
}

impl From<DataProcessingError> for CollectorError {
    fn from(err: DataProcessingError) -> Self {
        CollectorError::DataProcessing(Box::new(err))
    }
}
#[derive(Debug, Error)]
pub enum PhenopacketBuilderError {
    #[error("Could not parse {what} from value {value}.")]
    ParsingError { what: String, value: String },
    #[error("Missing BiDict for {0}")]
    MissingBiDict(String),
}
