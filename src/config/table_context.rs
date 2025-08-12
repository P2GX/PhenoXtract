use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct TableContext {
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    columns: Option<Vec<SeriesContext>>,
    #[allow(unused)]
    rows: Option<Vec<SeriesContext>>,
}

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

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum PolyValue {
    #[allow(unused)]
    String(String),
    #[allow(unused)]
    Int(i64),
    #[allow(unused)]
    Float(f64),
    #[allow(unused)]
    Bool(bool),
}
#[derive(Debug, Clone, Deserialize)]
struct CellContext {
    #[allow(unused)]
    #[serde(default)]
    context: Context,
    #[allow(unused)]
    fill_missing: PolyValue,
    #[allow(unused)]
    #[serde(default)]
    alias_map: HashMap<String, PolyValue>,
    // Besides just strings, should also be able to hold operations like ">1" or "=1", which can be interpreted later.
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Identifier {
    #[allow(unused)]
    Name(String),
    #[allow(unused)]
    Number(isize),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SeriesContext {
    #[allow(unused)]
    Single(SingleSeriesContext),
    #[allow(unused)]
    Multi(MultiSeriesContext),
}
#[derive(Debug, Clone, Deserialize)]
struct SingleSeriesContext {
    #[allow(unused)]
    identifier: Identifier, // Not so sure if this works, when deserializing, because all values of the enum are strings.
    #[allow(unused)]
    #[serde(default)]
    id_context: Context,
    #[allow(unused)]
    cells: Option<CellContext>,
    #[allow(unused)]
    rename_id: Option<String>, // This only works, when the identifier is a name and not a regex. Maybe need, two different structs?
}

#[derive(Debug, Clone, Deserialize)]
struct MultiSeriesContext {
    #[allow(unused)]
    regex_identifier: String, // Not so sure if this works, when deserializing, because all values of the enum are strings.
    #[allow(unused)]
    id_context: Context,
    #[allow(unused)]
    cells: CellContext,
}
