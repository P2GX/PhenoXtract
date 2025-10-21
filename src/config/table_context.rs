use crate::validation::multi_series_context_validation::validate_identifier;
use crate::validation::table_context_validation::validate_at_least_one_subject_id;
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
pub struct TableContext {
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    #[validate(custom(function = "validate_unique_identifiers"))]
    #[serde(default)]
    context: Vec<SeriesContext>,
}

impl TableContext {
    #[allow(dead_code)]
    pub fn new(name: String, context: Vec<SeriesContext>) -> Self {
        TableContext { name, context }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn name_mut(&mut self) -> &mut String {
        &mut self.name
    }

    pub fn context(&self) -> &Vec<SeriesContext> {
        &self.context
    }
    pub fn context_mut(&mut self) -> &mut Vec<SeriesContext> {
        &mut self.context
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
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
    HpoLabelOrId,
    #[allow(unused)]
    OnsetDateTime,
    #[allow(unused)]
    OnsetAge,
    #[allow(unused)]
    SubjectId,
    #[allow(unused)]
    SubjectSex,
    #[allow(unused)]
    DateOfBirth,
    #[allow(unused)]
    VitalStatus,
    #[allow(unused)]
    SubjectAge,
    #[allow(unused)]
    WeightInKg,
    #[allow(unused)]
    TimeOfDeath,
    #[allow(unused)]
    CauseOfDeath,
    #[allow(unused)]
    SurvivalTimeDays,
    #[allow(unused)]
    SmokerBool,
    #[allow(unused)]
    ObservationStatus,
    #[allow(unused)]
    MultiHpoId,
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
pub enum CellValue {
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
pub enum Identifier {
    Regex(String),
    Multi(Vec<String>),
}

impl Default for Identifier {
    fn default() -> Self {
        Identifier::Regex(String::new())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum OutputDataType {
    Boolean,
    String,
    Float64,
    Int32,
    Date,
    Datetime,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct AliasMap {
    hash_map: HashMap<String, String>,
    output_dtype: OutputDataType,
}

impl AliasMap {
    pub fn new(hash_map: HashMap<String, String>, output_dtype: OutputDataType) -> Self {
        AliasMap {
            hash_map,
            output_dtype,
        }
    }

    pub fn get_output_dtype(&self) -> &OutputDataType {
        &self.output_dtype
    }

    pub fn get_hash_map(&self) -> &HashMap<String, String> {
        &self.hash_map
    }
}

/// Represents the context for one or more series in a table.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Validate, Default)]
#[validate(schema(function = "validate_identifier"))]
pub struct SeriesContext {
    /// The identifier for the (possibly multiple) series.
    identifier: Identifier,

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
    /// The output datatype of the column will be inferred
    alias_map: Option<AliasMap>,

    #[serde(default)]
    /// An ID that associates this series with a building block of a phenopacket. If the same ID is shared with other series, the pipeline will try to construct a building block from them.
    building_block_id: Option<String>,
}

impl SeriesContext {
    #[allow(unused)]
    pub fn new(
        identifier: Identifier,
        header_context: Context,
        data_context: Context,
        fill_missing: Option<CellValue>,
        alias_map: Option<AliasMap>,
        building_block_id: Option<String>,
    ) -> Self {
        SeriesContext {
            identifier,
            header_context,
            data_context,
            fill_missing,
            alias_map,
            building_block_id,
        }
    }

    pub fn get_identifier(&self) -> &Identifier {
        &self.identifier
    }

    pub fn get_header_context(&self) -> &Context {
        &self.header_context
    }

    pub fn get_data_context(&self) -> &Context {
        &self.data_context
    }

    pub fn get_alias_map(&self) -> Option<&AliasMap> {
        self.alias_map.as_ref()
    }

    pub fn get_building_block_id(&self) -> Option<&str> {
        self.building_block_id.as_deref()
    }
    pub fn get_fill_missing(&self) -> Option<&CellValue> {
        self.fill_missing.as_ref()
    }

    #[allow(unused)]
    pub fn with_identifier(mut self, identifier: Identifier) -> Self {
        let identifier_ref = &mut self.identifier;
        *identifier_ref = identifier;
        self
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

    #[allow(unused)]
    pub fn with_alias_map(mut self, alias_map: Option<AliasMap>) -> Self {
        let alias_ref = &mut self.alias_map;
        *alias_ref = alias_map;
        self
    }

    #[allow(unused)]
    pub fn with_building_block_id(mut self, building_block_id: Option<String>) -> Self {
        let building_block_id_ref = &mut self.building_block_id;
        *building_block_id_ref = building_block_id;
        self
    }

    pub fn with_fill_missing(mut self, fill_missing: Option<CellValue>) -> Self {
        self.fill_missing = fill_missing;
        self
    }
}
