use crate::config::table_context::Context;
use crate::ontology::error::ClientError;
use crate::validation::error::{ValidationError as PxValidationError, ValidationError};
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

impl Display for MappingSuggestion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}' -> '{}'", self.from, self.to)?;
        Ok(())
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
            "Column '{}' in table '{}' with value '{}'",
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
    #[error(transparent)]
    StrategyError(#[from] StrategyError),
    #[error(transparent)]
    PolarsError(#[from] PolarsError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
}
#[derive(Debug, Error)]
pub enum TransformError {
    #[error(transparent)]
    StrategyError(#[from] StrategyError),
    #[error(transparent)]
    CollectorError(#[from] Box<CollectorError>),
    #[error(transparent)]
    DataProcessingError(#[from] Box<DataProcessingError>),
}

impl From<CollectorError> for TransformError {
    fn from(err: CollectorError) -> Self {
        TransformError::CollectorError(Box::new(err))
    }
}

impl From<DataProcessingError> for TransformError {
    fn from(err: DataProcessingError) -> Self {
        TransformError::DataProcessingError(Box::new(err))
    }
}

fn format_grouped_errors(errors: &[MappingErrorInfo]) -> String {
    let mut grouped: HashMap<(&str, &str), Vec<&MappingErrorInfo>> = HashMap::new();

    for error in errors {
        grouped
            .entry((&error.column, &error.table))
            .or_default()
            .push(error);
    }

    let mut result = String::new();
    for ((column, table), group) in grouped {
        result.push_str(&format!("  Column '{}' in table '{}':\n", column, table));
        for error in group {
            result.push_str(&format!("    - '{}'", error.old_value));
            if !error.possible_mappings.is_empty() {
                result.push_str(&format!(
                    " (possible mappings: {})",
                    error
                        .possible_mappings
                        .iter()
                        .map(|pm| pm.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                ));
            }
            result.push('\n');
        }
    }

    result
}

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
pub enum StrategyError {
    #[error("Could not {transformation} column '{col_name}' for table '{table_name}'")]
    BuilderError {
        transformation: String,
        col_name: String,
        table_name: String,
    },

    #[error(
        "Strategy '{strategy_name}' unable to map: \n {}",
        format_grouped_errors(info)
    )]
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
        "Expected at most one column with data contexts '{contexts:?}' in the building block '{bb_id}' in table '{table_name}'"
    )]
    ExpectedAtMostOneLinkedColumnWithContexts {
        table_name: String,
        bb_id: String,
        contexts: Vec<Context>,
        amount_found: usize,
    },
    #[error(
        "Found multiple values of {context} in table {table_name} for {patient_id} when there should only be one."
    )]
    ExpectedSingleValue {
        table_name: String,
        patient_id: String,
        context: Context,
    },
    #[error(
        "Found conflicting information on phenotype '{phenotype}' for patient '{patient_id}' in table '{table_name}'"
    )]
    ExpectedUniquePhenotypeData {
        table_name: String,
        patient_id: String,
        phenotype: String,
    },
    #[error(transparent)]
    DataProcessing(Box<DataProcessingError>),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("ParseFloatError error: {0}")]
    ParseFloatError(#[from] ParseFloatError),
    #[error(transparent)]
    PhenopacketBuilderError(#[from] PhenopacketBuilderError),
    #[error("Error collecting gene variant data: {0}")]
    GeneVariantData(String),
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
    #[error(transparent)]
    HgncClient(#[from] ClientError),
    #[error("Error validating HGVS variant: {0}")]
    VariantValidation(String),
    #[error("Error fetching gene symbol-id pair: {0}")]
    HgncGenePair(String),
    #[error(
        "Could not interpret gene and variant configuration for disease {disease}: {invalid_configuration}"
    )]
    InvalidGeneVariantConfiguration {
        disease: String,
        invalid_configuration: String,
    },
    #[error(
        "The HGVS variant {variant} for patient {patient} did not have the correct reference:transcript format."
    )]
    HgvsFormat { patient: String, variant: String },
}
