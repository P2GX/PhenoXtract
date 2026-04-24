use crate::config::context::{Context, ContextKind};
use crate::extract::contextualized_data_frame::CdfBuilderError;
use crate::ontology::error::BiDictError;
use pivotal::hgnc::HGNCError;
use pivotal::hgvs::HGVSError;
use polars::error::PolarsError;
use polars::prelude::DataType;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::num::ParseIntError;
use thiserror::Error;
use validator::ValidationErrors;

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

fn format_mapping_errors(errors: &[MappingErrorInfo]) -> String {
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
    ValidationError(#[from] ValidationErrors),
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
    ValidationError(#[from] ValidationErrors),
    #[error(transparent)]
    CdfBuilderError(#[from] CdfBuilderError),
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

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DateToAgeErrorInfo {
    pub table_name: String,
    pub date_col: String,
    pub date: String,
    pub subject_id: String,
    pub problem: String,
}

pub trait PushDateToAgeError {
    fn insert_error(
        &mut self,
        table_name: String,
        date_col: String,
        date: String,
        subject_id: String,
        problem: String,
    );
}
impl PushDateToAgeError for HashSet<DateToAgeErrorInfo> {
    fn insert_error(
        &mut self,
        table_name: String,
        date_col: String,
        date: String,
        subject_id: String,
        problem: String,
    ) {
        let date_to_age_error_info = DateToAgeErrorInfo {
            table_name,
            date_col,
            date,
            subject_id,
            problem,
        };
        if !self.contains(&date_to_age_error_info) {
            self.insert(date_to_age_error_info);
        }
    }
}

fn format_date_to_age_errors(errors: &[DateToAgeErrorInfo]) -> String {
    let mut grouped: HashMap<(&str, &str), Vec<&DateToAgeErrorInfo>> = HashMap::new();

    for error in errors {
        grouped
            .entry((&error.date_col, &error.table_name))
            .or_default()
            .push(error);
    }

    let mut result = String::new();
    for ((date_column, table), group) in grouped {
        result.push_str(&format!(
            "Date column '{}' in table '{}':\n",
            date_column, table
        ));
        for error in group {
            result.push_str(&format!(
                "    - 'Patient: {}, Date: {}, Problem: {}'",
                error.subject_id, error.date, error.problem
            ));
            result.push('\n');
        }
    }

    result
}

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
pub enum StrategyError {
    #[error(
        "{message}. Strategy '{strategy_name}' unable to map: \n{}",
        format_mapping_errors(info)
    )]
    MappingError {
        strategy_name: String,
        message: String,
        info: Vec<MappingErrorInfo>,
    },
    #[error(
        "Errors applying DateToAgeStrategy: \n{}",
        format_date_to_age_errors(info)
    )]
    DateToAgeError { info: Vec<DateToAgeErrorInfo> },
    #[error(transparent)]
    ValidationError(#[from] ValidationErrors),
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
        "Expected linked required contexts {expected_contexts:?} in building block '{bb_id}', but found {found_contexts:?}."
    )]
    ExpectedLinkedContexts {
        bb_id: String,
        expected_contexts: Vec<Context>,
        found_contexts: Vec<Context>,
    },

    #[error(
        "Found multiple values for '{patient_id}' when there should only be one. Filter info: {filter_info}."
    )]
    ExpectedSingleValue {
        patient_id: String,
        filter_info: String,
    },
    #[error(
        "Found conflicting information on phenotype '{phenotype}' for patient '{patient_id}' in table '{table_name}'"
    )]
    ExpectedUniquePhenotypeData {
        table_name: String,
        patient_id: String,
        phenotype: String,
    },
    #[error("Error collecting gene variant data: {0}")]
    GeneVariantData(String),
    #[error("Context Error: {0}")]
    ContextError(String),
    #[error(
        "The disease/interpretation building block {bb_id} was invalid for subject {patient_id}. Such a building block can NOT be simultaneously: 1. spread across multiple sheets, 2. contain multiple diseases for a patient."
    )]
    InterpretationBlockFormat { patient_id: String, bb_id: String },
    #[error(
        "The column {column_name} had datatype {found_datatype} during collection. This was not accepted. Allowed datatypes: {allowed_datatypes:?},"
    )]
    DataTypeError {
        column_name: String,
        found_datatype: DataType,
        allowed_datatypes: Vec<DataType>,
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
    ValidationError(#[from] ValidationErrors),
    #[error(transparent)]
    GetterError(#[from] GetterError),
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
    #[error("Value {value_name} was missing, when building {struct_name}.")]
    MissingValueError {
        value_name: String,
        struct_name: String,
    },
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

#[derive(Debug, Error)]
pub enum GetterError {
    #[error("Missing value of context '{context}' in row {idx}")]
    RequiredValueMissingError { idx: usize, context: ContextKind },
    #[error("OutOfBounds")]
    OutOfBounds,
}
