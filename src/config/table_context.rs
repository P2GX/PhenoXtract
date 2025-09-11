use crate::validation::multi_series_context_validation::validate_multi_identifier;
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
#[derive(Debug, Validate, Deserialize, Serialize, Clone, PartialEq, Default)]
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
        write!(f, "{self:?}")
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

//todo add a doc string here
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum Identifier {
    Regex(String),
    Multi(Vec<String>),
}

/// Represents the context for one or more series in a table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct SeriesContext {
    /// The identifier for the (possibly multiple) series.
    pub(crate) identifier: Identifier,

    /// The semantic context found in the header(s) of the series.
    header_context: Option<Context>,
    /// The context that applies to every cell within this series.
    data_context: Option<Context>,

    /// A default value to replace empty fields in a cell
    #[allow(unused)]
    fill_missing: Option<CellValue>,

    #[allow(unused)]
    #[serde(default)]
    /// A map to replace specific string values with another `CellValue`.
    /// This can be used for aliasing or correcting data, e.g., mapping "N/A" to a standard null representation.
    alias_map: Option<HashMap<String, CellValue>>,
    // Besides just strings, should also be able to hold operations like "gt(1)" or "eq(1)", which can be interpreted later.

    #[serde(default)]
    /// List of IDs that link to other tables, can be used to determine the relationship between these columns
    pub linked_to: Vec<String>,
}

impl SeriesContext {

    #[allow(unused)]
    pub(crate) fn new(
        identifier: Identifier,
        header_context: Option<Context>,
        data_context: Option<Context>,
        fill_missing: Option<CellValue>,
        alias_map: Option<CellValue>,
        linked_to: Vec<String>,
    ) -> Self {
        SeriesContext {
            identifier,
            header_context,
            data_context,
            fill_missing,
            alias_map,
            linked_to,
        }
    }

    pub fn get_identifier(&self) -> Identifier {
        self.identifier.clone()
    }

    pub fn get_header_context(&self) -> Context {
        let header_context_opt = self.header_context.clone();
        header_context_opt
            .clone()
            .unwrap_or(Context::None)
    }

    pub fn get_data_context(&self) -> Context {
        let data_context_opt = self.data_context.clone();
        data_context_opt
            .clone()
            .unwrap_or(Context::None)
    }

    #[allow(unused)]
    pub fn with_header_context(mut self, context: Context) -> Self {
        let header_context_ref = &mut self.header_context;
        *header_context_ref = Some(context);
        self
    }

    #[allow(unused)]
    pub fn with_data_context(mut self, context: Context) -> Self {
        let data_context_ref = &mut self.data_context;
        *data_context_ref = Some(context);
        self
    }
}