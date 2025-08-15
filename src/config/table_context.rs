use serde::Deserialize;
use std::collections::HashMap;

/// Represents the contextual information for an entire table.
///
/// This struct defines how to interpret a table, including its name and the
/// context for its series, which can be organized as columns or rows.
#[derive(Debug, Deserialize)]
pub struct TableContext {
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    columns: Option<Vec<SeriesContext>>,
    #[allow(unused)]
    rows: Option<Vec<SeriesContext>>,
}

/// Defines the semantic meaning or type of data in a cell or series.
///
/// This enum is used to tag data with a specific, machine-readable context,
/// such as identifying a column as containing HPO IDs or subject's sex.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
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
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum CellValue {
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
#[derive(Debug, Clone, Deserialize)]
struct CellContext {
    /// The semantic context of the cell's data.
    #[allow(unused)]
    #[serde(default)]
    context: Context,

    /// A default value to replace empty fields in a cell
    #[allow(unused)]
    fill_missing: CellValue,
    #[allow(unused)]
    #[serde(default)]
    /// A map to replace specific string values with another `CellValue`.
    ///
    /// This can be used for aliasing or correcting data, e.g., mapping "N/A" to a standard null representation.
    alias_map: HashMap<String, CellValue>,
    // Besides just strings, should also be able to hold operations like "gt(1)" or "eq(1)", which can be interpreted later.
}

/// An identifier for a series, which can be either a name or a numerical index.
///
/// This allows for selecting columns or rows by their header name (e.g., "PatientID")
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Identifier {
    #[allow(unused)]
    Name(String),
    #[allow(unused)]
    Number(isize),
}

/// Represents the context for one or more series (columns or rows).
///
/// This enum acts as a dispatcher. It can either define the context for a
/// single, specifically identified series or for multiple series identified
/// by a regular expression.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SeriesContext {
    #[allow(unused)]
    Single(SingleSeriesContext),
    #[allow(unused)]
    Multi(MultiSeriesContext),
}

/// Defines the context for a single, specific series (e.g., a column or row).
#[derive(Debug, Clone, Deserialize)]
struct SingleSeriesContext {
    #[allow(unused)]
    /// The unique identifier for the series.
    identifier: Identifier,
    #[allow(unused)]
    #[serde(default)]
    /// The semantic context found in the header/index of the series.
    id_context: Context,
    #[allow(unused)]
    /// The context to apply to every cell within this series.
    cells: Option<CellContext>,
    /// A unique ID that can be used to link to other series
    #[allow(unused)]
    linking_id: Option<String>,
    #[allow(unused)]
    /// List of IDs that link to other tables, can be used to determine the relationship between these columns
    linked_to: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum MultiIdentifier {
    #[allow(unused)]
    Regex(String),
    #[allow(unused)]
    Multi(Vec<String>),
}

/// Defines the context for multiple series identified by a regex pattern.
///
/// This is useful for applying the same logic to a group of related columns or rows,
/// for example, all columns whose names start with "measurement_".
#[derive(Debug, Clone, Deserialize)]
struct MultiSeriesContext {
    #[allow(unused)]
    /// A regular expression used to match and select multiple series identifiers.
    multi_identifier: MultiIdentifier,
    #[allow(unused)]
    /// The semantic context to apply to the identifiers of all matched column header or row indexes.
    id_context: Context,
    #[allow(unused)]
    /// The context to apply to every cell in all of the matched series.
    cells: Option<CellContext>,
}
