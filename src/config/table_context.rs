pub(crate) use crate::config::series_context::SeriesContext;
use crate::validation::table_context_validation::validate_at_least_one_subject_id;
use crate::validation::table_context_validation::validate_series_linking;
use crate::validation::table_context_validation::validate_unique_identifiers;
use crate::validation::table_context_validation::validate_unique_series_linking;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use validator::Validate;

/// Represents the contextual information for an entire table.
///
/// This struct defines how to interpret a table, including its name and the
/// context for its series, which can be organized as columns or rows.
#[derive(Debug, Validate, Deserialize, Serialize, Clone, PartialEq)]
#[validate(schema(
    function = "validate_at_least_one_subject_id",
    skip_on_field_errors = false
))]
#[validate(schema(function = "validate_series_linking"))]
#[validate(schema(function = "validate_unique_series_linking"))]
pub struct TableContext {
    #[allow(unused)]
    pub name: String,
    #[allow(unused)]
    #[validate(custom(function = "validate_unique_identifiers"))]
    #[serde(default)]
    pub context: Vec<SeriesContext>,
}

impl TableContext {
    #[allow(dead_code)]
    pub(crate) fn new(name: String, context: Vec<SeriesContext>) -> Self {
        TableContext { name, context }
    }
}
/// Defines the semantic meaning or type of data in a cell or series.
///
/// This enum is used to tag data with a specific, machine-readable context,
/// such as identifying a column as containing HPO IDs or subject's sex.
#[derive(Debug, Clone, PartialEq, Deserialize, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Context {
    #[allow(unused)]
    HpoId,
    #[allow(unused)]
    HpoLabel,
    #[allow(unused)]
    OnSet,
    #[allow(unused)]
    OnSetDate,
    #[allow(unused)]
    SubjectId,
    #[allow(unused)]
    SubjectSex,
    #[allow(unused)]
    SubjectAge,
    #[default]
    None,
    //...
}

impl Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Represents the value of a single cell, which can be one of several primitive types.
///
/// This enum uses `serde(untagged)` to allow for flexible deserialization
/// of JSON values (string, integer, float, or boolean) into a single type.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum CellValue {
    #[allow(unused)]
    String(String),
    #[allow(unused)]
    Int(i64),
    #[allow(unused)]
    Float(f64),
    #[allow(unused)]
    Bool(bool),
}

/// Provides detailed context for processing the values within all cells of a column.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct CellContext {
    /// The semantic context of the cell's data.
    #[allow(unused)]
    #[serde(default)]
    pub context: Context,

    /// A default value to replace empty fields in a cell
    #[allow(unused)]
    fill_missing: Option<CellValue>,
    #[allow(unused)]
    #[serde(default)]
    /// A map to replace specific string values with another `CellValue`.
    ///
    /// This can be used for aliasing or correcting data, e.g., mapping "N/A" to a standard null representation.
    alias_map: HashMap<String, CellValue>,
    // Besides just strings, should also be able to hold operations like "gt(1)" or "eq(1)", which can be interpreted later.
}
impl CellContext {
    pub fn new(
        context: Context,
        fill_missing: Option<CellValue>,
        alias_map: HashMap<String, CellValue>,
    ) -> CellContext {
        CellContext {
            context,
            fill_missing,
            alias_map,
        }
    }
}
