use crate::validation::multi_series_context_validation::validate_regex_multi_identifier;
use crate::validation::table_context_validation::validate_at_least_one_subject_id;
use crate::validation::table_context_validation::validate_series_linking;
use crate::validation::table_context_validation::validate_unique_identifiers;
use crate::validation::table_context_validation::validate_unique_series_linking;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    #[default]
    None,
    //...
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
    context: Context,

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

/// Represents the context for one or more series (columns or rows).
///
/// This enum acts as a dispatcher. It can either define the context for a
/// single, specifically identified series or for multiple series identified
/// by a regular expression.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum SeriesContext {
    #[allow(unused)]
    Single(SingleSeriesContext),
    #[allow(unused)]
    Multi(MultiSeriesContext),
}

impl SeriesContext {
    pub fn get_context(&self) -> Context {
        match self {
            SeriesContext::Single(single) => single.id_context.clone(),
            SeriesContext::Multi(multi) => multi.id_context.clone(),
        }
    }

    pub fn get_cell_context(&self) -> Context {
        let cells_option = match self {
            SeriesContext::Single(single) => &single.cells,
            SeriesContext::Multi(multi) => &multi.cells,
        };
        cells_option
            .clone()
            .map(|context_container| context_container.context)
            .unwrap_or(Context::None)
    }
    #[allow(unused)]
    pub fn with_context(mut self, context: Context) -> Self {
        let id_context_ref = match &mut self {
            SeriesContext::Single(single) => &mut single.id_context,
            SeriesContext::Multi(multi) => &mut multi.id_context,
        };

        *id_context_ref = context;

        self
    }

    #[allow(unused)]
    pub fn with_cell_context(mut self, context: Context) -> Self {
        let cells_option = match &mut self {
            SeriesContext::Single(single) => &mut single.cells,
            SeriesContext::Multi(multi) => &mut multi.cells,
        };
        if let Some(cell_context) = cells_option {
            cell_context.context = context;
        } else {
            *cells_option = Some(CellContext::new(context, None, HashMap::default()));
        }
        self
    }
}

/// Defines the context for a single, specific series (e.g., a column or row).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct SingleSeriesContext {
    #[allow(unused)]
    /// The unique identifier for the series.
    pub(crate) identifier: String,
    #[allow(unused)]
    #[serde(default)]
    /// The semantic context found in the header/index of the series.
    id_context: Context,
    #[allow(unused)]
    /// The context to apply to every cell within this series.
    cells: Option<CellContext>,
    /// A unique ID that can be used to link to other series
    #[allow(unused)]
    #[serde(default)]
    /// List of IDs that link to other tables, can be used to determine the relationship between these columns
    pub linked_to: Vec<String>,
}

impl SingleSeriesContext {
    #[allow(unused)]
    pub(crate) fn new(
        identifier: String,
        id_context: Context,
        cells: Option<CellContext>,
        linked_to: Vec<String>,
    ) -> Self {
        SingleSeriesContext {
            identifier,
            id_context,
            cells,
            linked_to,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum MultiIdentifier {
    #[allow(unused)]
    Regex(String),
    #[allow(unused)]
    Multi(Vec<String>),
}

/// Defines the context for multiple series identified by a regex pattern.
///
/// This is useful for applying the same logic to a group of related columns or rows,
/// for example, all columns whose names start with "measurement_".
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Validate)]
pub(crate) struct MultiSeriesContext {
    #[allow(unused)]
    /// A regular expression used to match and select multiple series identifiers.
    #[validate(custom(function = "validate_regex_multi_identifier"))]
    pub(crate) multi_identifier: MultiIdentifier,
    #[allow(unused)]
    /// The semantic context to apply to the identifiers of all matched column header or row indexes.
    id_context: Context,
    #[allow(unused)]
    /// The context to apply to every cell in all of the matched series.
    cells: Option<CellContext>,
}

impl MultiSeriesContext {
    #[allow(unused)]
    pub(crate) fn new(
        multi_identifier: MultiIdentifier,
        id_context: Context,
        cells: Option<CellContext>,
    ) -> Self {
        MultiSeriesContext {
            multi_identifier,
            id_context,
            cells,
        }
    }
}
