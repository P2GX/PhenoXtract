use crate::config::context::Context;
use crate::config::traits::{IntoOptionalString, SeriesContextBuilding};
use crate::extract::contextualized_dataframe_filters::SeriesContextFilter;
use crate::validation::multi_series_context_validation::validate_identifier;
use crate::validation::table_context_validation::validate_subject_ids_context;
use crate::validation::table_context_validation::validate_unique_identifiers;
use polars::prelude::{DataType, TimeUnit};
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
    function = "validate_subject_ids_context",
    skip_on_field_errors = false
))]
pub struct TableContext {
    name: String,
    #[validate(custom(function = "validate_unique_identifiers"))]
    context: Vec<SeriesContext>,
}

impl TableContext {
    pub fn new(name: impl Into<String>, context: Vec<SeriesContext>) -> Self {
        TableContext {
            name: name.into(),
            context,
        }
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

    pub fn filter_series_context(&'_ self) -> SeriesContextFilter<'_> {
        SeriesContextFilter::new(self.context.as_ref())
    }
}

/// Represents the value of a single cell, which can be one of several primitive types.
///
/// This enum uses `serde(untagged)` to allow for flexible deserialization
/// of JSON values (string, integer, float, or boolean) into a single type.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum CellValue {
    String(String),
    Int(i64),
    Float(f64),
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

impl From<&str> for Identifier {
    fn from(value: &str) -> Self {
        Identifier::Regex(value.to_string())
    }
}
impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Identifier::Regex(value)
    }
}

impl From<Vec<String>> for Identifier {
    fn from(value: Vec<String>) -> Self {
        Identifier::Multi(value)
    }
}

impl From<Vec<&str>> for Identifier {
    fn from(value: Vec<&str>) -> Self {
        Identifier::Multi(value.iter().map(|s| s.to_string()).collect())
    }
}

impl From<&[String]> for Identifier {
    fn from(value: &[String]) -> Self {
        Identifier::Multi(value.to_vec())
    }
}

impl Default for Identifier {
    fn default() -> Self {
        Identifier::Regex(String::new())
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Identifier::Regex(regex) => write!(f, "{}", regex),
            Identifier::Multi(multi) => write!(f, "{}", multi.join(".")),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum OutputDataType {
    Boolean,
    String,
    Float64,
    Int64,
    Date,
    Datetime,
}

impl OutputDataType {
    pub fn as_polars(&self) -> DataType {
        match self {
            OutputDataType::Boolean => DataType::Boolean,
            OutputDataType::String => DataType::String,
            OutputDataType::Float64 => DataType::Float64,
            OutputDataType::Int64 => DataType::Int64,
            OutputDataType::Date => DataType::Date,
            OutputDataType::Datetime => DataType::Datetime(TimeUnit::Nanoseconds, None),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct AliasMap {
    hash_map: HashMap<String, Option<String>>,
    output_dtype: OutputDataType,
}

impl AliasMap {
    pub fn new(hash_map: HashMap<String, Option<String>>, output_dtype: OutputDataType) -> Self {
        AliasMap {
            hash_map,
            output_dtype,
        }
    }

    pub fn get_output_dtype(&self) -> &OutputDataType {
        &self.output_dtype
    }

    pub fn get_hash_map(&self) -> &HashMap<String, Option<String>> {
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
    fill_missing: Option<CellValue>,

    /// A map to replace specific cell values with other strings, ints, floats or bools.
    /// This can be used for aliasing or correcting data, e.g., mapping "N/A" to a standard null representation.
    /// The output datatype of the column will be inferred
    alias_map: Option<AliasMap>,

    /// An ID that associates this series with a building block of a phenopacket. If the same ID is shared with other series, the pipeline will try to construct a building block from them.
    building_block_id: Option<String>,

    sub_blocks: Vec<String>,
}

impl SeriesContext {
    pub fn new(
        identifier: Identifier,
        header_context: Context,
        data_context: Context,
        fill_missing: Option<CellValue>,
        alias_map: Option<AliasMap>,
        building_block_id: Option<String>,
        sub_blocks: Vec<String>,
    ) -> Self {
        SeriesContext {
            identifier,
            header_context,
            data_context,
            fill_missing,
            alias_map,
            building_block_id,
            sub_blocks,
        }
    }

    pub fn get_identifier(&self) -> &Identifier {
        &self.identifier
    }

    pub fn get_header_context(&self) -> &Context {
        &self.header_context
    }

    pub fn header_context_mut(&mut self) -> &mut Context {
        &mut self.header_context
    }

    pub fn get_data_context(&self) -> &Context {
        &self.data_context
    }

    pub fn data_context_mut(&mut self) -> &mut Context {
        &mut self.data_context
    }

    pub fn get_alias_map(&self) -> Option<&AliasMap> {
        self.alias_map.as_ref()
    }

    pub fn get_building_block_id(&self) -> Option<&str> {
        self.building_block_id.as_deref()
    }
    pub fn get_sub_blocks(&self) -> &[String] {
        self.sub_blocks.as_slice()
    }
    pub fn get_fill_missing(&self) -> Option<&CellValue> {
        self.fill_missing.as_ref()
    }
}
impl SeriesContextBuilding<AliasMap> for SeriesContext {
    fn from_identifier(identifier: impl Into<Identifier>) -> Self {
        Self {
            identifier: identifier.into(),
            header_context: Context::default(),
            data_context: Context::default(),
            fill_missing: None,
            alias_map: None,
            building_block_id: None,
            sub_blocks: vec![],
        }
    }

    fn with_identifier(mut self, identifier: impl Into<Identifier>) -> Self {
        self.identifier = identifier.into();
        self
    }

    fn with_header_context(mut self, header_context: Context) -> Self {
        self.header_context = header_context;
        self
    }

    fn with_data_context(mut self, data_context: Context) -> Self {
        self.data_context = data_context;
        self
    }

    fn with_fill_missing(mut self, fill_missing: CellValue) -> Self {
        self.fill_missing = Some(fill_missing);
        self
    }

    fn with_alias_map(mut self, alias_map_config: AliasMap) -> Self {
        self.alias_map = Some(alias_map_config);
        self
    }

    fn with_building_block_id(mut self, building_block_id: impl IntoOptionalString) -> Self {
        if let Some(id) = building_block_id.into_opt_string() {
            self.building_block_id = Some(id);
            self
        } else {
            self.building_block_id = None;
            self
        }
    }

    fn push_sub_block(mut self, building_block_id: impl Into<String>) -> Self {
        self.sub_blocks.push(building_block_id.into());
        self
    }

    fn with_sub_blocks(mut self, sub_building_blocks: Vec<impl Into<String>>) -> Self {
        self.sub_blocks = sub_building_blocks.into_iter().map(Into::into).collect();
        self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_output_datatype_as_polars() {
        assert_eq!(OutputDataType::Boolean.as_polars(), DataType::Boolean);
        assert_eq!(OutputDataType::String.as_polars(), DataType::String);
        assert_eq!(OutputDataType::Float64.as_polars(), DataType::Float64);
        assert_eq!(OutputDataType::Int64.as_polars(), DataType::Int64);
        assert_eq!(OutputDataType::Date.as_polars(), DataType::Date);
        assert_eq!(
            OutputDataType::Datetime.as_polars(),
            DataType::Datetime(TimeUnit::Nanoseconds, None)
        );
    }
}
