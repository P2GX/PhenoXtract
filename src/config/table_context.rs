use crate::validation::multi_series_context_validation::validate_identifier;
use crate::validation::table_context_validation::validate_at_least_one_subject_id;
use crate::validation::table_context_validation::validate_series_linking;
use crate::validation::table_context_validation::validate_unique_identifiers;
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
/// Defines the semantic meaning or type of data in a column (either the header or the data itself).
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

/// The identifier will correspond to either one or multiple columns in a dataframe.
///
/// If it has Regex type, then the columns will be determined by the regular expression
/// NOTE: if the regex string corresponds exactly to a column name, then that single column will be identified.
/// If it has multi type, then the strings within the vector will be the headers of the relevant columns.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub(crate) enum Identifier {
    Regex(String),
    Multi(Vec<String>),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum AliasMap {
    #[allow(unused)]
    ToString(HashMap<String, String>),
    #[allow(unused)]
    ToInt(HashMap<String, i64>),
    #[allow(unused)]
    ToFloat(HashMap<String, f64>),
    #[allow(unused)]
    ToBool(HashMap<String, bool>),
}

/// Represents the context for one or more series in a table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Validate)]
#[validate(schema(function = "validate_identifier"))]
pub(crate) struct SeriesContext {
    /// The identifier for the (possibly multiple) series.
    pub(crate) identifier: Identifier,

    /// The semantic context found in the header(s) of the series.
    header_context: Context,
    /// The context that applies to every cell within this series.
    data_context: Context,

    /// A default value to replace empty fields in a cell
    #[allow(unused)]
    fill_missing: Option<CellValue>,

    #[allow(unused)]
    #[serde(default)]
    /// A map to replace specific cell values with other strings, ints, floats or bools.
    /// This can be used for aliasing or correcting data, e.g., mapping "N/A" to a standard null representation.
    alias_map: Option<AliasMap>,

    #[serde(default)]
    /// List of IDs that link to other tables, can be used to determine the relationship between these columns.
    pub linked_to: Vec<Identifier>,
}

impl SeriesContext {
    #[allow(unused)]
    pub(crate) fn new(
        identifier: Identifier,
        header_context: Context,
        data_context: Context,
        fill_missing: Option<CellValue>,
        alias_map: Option<AliasMap>,
        linked_to: Vec<Identifier>,
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
        self.header_context.clone()
    }

    pub fn get_data_context(&self) -> Context {
        self.data_context.clone()
    }

    pub fn get_links(&self) -> Vec<Identifier> {
        self.linked_to.clone()
    }

    #[allow(unused)]
    pub fn with_header_context(mut self, context: Context) -> Self {
        let header_context_ref = &mut self.header_context;
        *header_context_ref = context;
        self
    }

    #[allow(unused)]
    pub fn with_data_context(mut self, context: Context) -> Self {
        let data_context_ref = &mut self.data_context;
        *data_context_ref = context;
        self
    }
}
