use crate::config::context::{Context, ContextError, ContextKind};
use crate::config::table_context::Identifier;
use crate::extract::contextualized_data_frame::CdfBuilderError;
use crate::ontology::error::BiDictError;
use crate::validation::error::{ValidationError as PxValidationError, ValidationError};
use pivot::hgnc::HGNCError;
use pivot::hgvs::HGVSError;
use polars::error::PolarsError;
use polars::prelude::DataType;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::num::ParseIntError;
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

pub trait PushMappingError {
    fn insert_error(
        &mut self,
        column: String,
        table: String,
        old_value: String,
        possible_mappings: Vec<MappingSuggestion>,
    );
}
impl PushMappingError for HashSet<MappingErrorInfo> {
    fn insert_error(
        &mut self,
        column: String,
        table: String,
        old_value: String,
        possible_mappings: Vec<MappingSuggestion>,
    ) {
        let mapping_error_info = MappingErrorInfo {
            column,
            table,
            old_value,
            possible_mappings,
        };
        if !self.contains(&mapping_error_info) {
            self.insert(mapping_error_info);
        }
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
    #[error(transparent)]
    CdfBuilderError(#[from] CdfBuilderError),
}
#[derive(Debug, Error)]
pub enum TransformError {
    #[error(transparent)]
    StrategyError(#[from] StrategyError),
    #[error(transparent)]
    CollectorError(#[from] Box<CollectorError>),
    #[error(transparent)]
    DataProcessingError(#[from] Box<DataProcessingError>),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
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
    #[error(
        "{message}. Strategy '{strategy_name}' unable to map: \n {}",
        format_grouped_errors(info)
    )]
    MappingError {
        strategy_name: String,
        message: String,
        info: Vec<MappingErrorInfo>,
    },
    #[error(transparent)]
    ValidationError(#[from] PxValidationError),
    #[error(transparent)]
    CdfBuilderError(#[from] CdfBuilderError),
    #[error(transparent)]
    DataProcessing(#[from] Box<DataProcessingError>),
    #[error("Polars error: {0}")]
    PolarsError(Box<PolarsError>),
    #[error(
        "Multiplicity error for {context}: {message}. Invalid for the following patients: {patients:?}"
    )]
    MultiplicityError {
        context: Context,
        message: String,
        patients: Vec<String>,
    },
    #[error("Could not parse {unparseable_date} as a date or datetime for {subject_id}")]
    DateParsingError {
        subject_id: String,
        unparseable_date: String,
    },
    #[error(
        "Date of event occurs earlier than the date of birth of {subject_id}. Date of birth: {date_of_birth}, Date: {date}"
    )]
    NegativeAge {
        subject_id: String,
        date_of_birth: String,
        date: String,
    },
    #[error(
        "The column {column_name} had datatype {found_datatype} in strategy {strategy}. Only the datatypes {allowed_datatypes:?} are accepted."
    )]
    DataTypeError {
        column_name: String,
        strategy: String,
        found_datatype: String,
        allowed_datatypes: Vec<String>,
    },
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
    #[error(
        "Expected at most '{n_expected}' columns with data contexts '{contexts:?}' in the building block '{bb_id}' in table '{table_name}', but found '{n_found}'."
    )]
    ExpectedAtMostNLinkedColumnWithContexts {
        table_name: String,
        bb_id: String,
        contexts: Vec<Context>,
        n_found: usize,
        n_expected: usize,
    },
    #[error(
        "Found multiple values for context data: '{data_context}' header: '{header_context}' for '{patient_id}' when there should only be one."
    )]
    ExpectedSingleValue {
        patient_id: String,
        data_context: ContextKind,
        header_context: ContextKind,
    },
    #[error(
        "Found conflicting information on phenotype '{phenotype}' for patient '{patient_id}' in table '{table_name}'"
    )]
    ExpectedUniquePhenotypeData {
        table_name: String,
        patient_id: String,
        phenotype: String,
    },
    #[error(
        "Expected context '{context}' to be part of a building block in table '{table_name}' for patient '{patient_id}'."
    )]
    ExpectedBuildingBlock {
        table_name: String,
        patient_id: String,
        context: ContextKind,
    },
    #[error(transparent)]
    DataProcessing(Box<DataProcessingError>),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("ParseFloatError error: {0}")]
    ParseFloatError(#[from] ParseIntError),
    #[error(transparent)]
    PhenopacketBuilderError(#[from] PhenopacketBuilderError),
    #[error(transparent)]
    CdfBuilderError(#[from] CdfBuilderError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("Error collecting gene variant data: {0}")]
    GeneVariantDataError(String),
    #[error("Found unexpected context '{0}' on column with identifier '{1}'")]
    UnexpectedContextError(ContextKind, Identifier),
    #[error(transparent)]
    TryIntoContextError(#[from] ContextError),
    #[error(
        "The column {column_name} had datatype {found_datatype} during collection. This was not accepted. Allowed datatypes: {allowed_datatypes:?},"
    )]
    DataTypeError {
        column_name: String,
        found_datatype: DataType,
        allowed_datatypes: Vec<DataType>,
    },
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
    #[error("No {bidict_type} BiDict was found, despite being called.")]
    MissingBiDict { bidict_type: String },
    #[error("Cannot set {required_for}: prerequisite {missing} is missing.")]
    MissingPrerequisiteError {
        missing: String,
        required_for: String,
    },
    #[error(transparent)]
    HgvsError(#[from] HGVSError),
    #[error(transparent)]
    HgncError(#[from] HGNCError),
    #[error(transparent)]
    BidictError(#[from] BiDictError),
}
