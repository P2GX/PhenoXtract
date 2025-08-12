use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct TableContext {
    #[allow(unused)]
    name: String,
    #[allow(unused)]
    columns: Vec<SeriesContext>,
    #[allow(unused)]
    rows: Vec<SeriesContext>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Context {
    #[allow(unused)]
    HpoID,
    #[allow(unused)]
    HpoLabel,
    #[allow(unused)]
    OnSet,
    #[allow(unused)]
    OnSetDate,
    #[allow(unused)]
    SubjectID,
    #[allow(unused)]
    SubjectSex,
    //...
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum PolyValue {
    #[allow(unused)]
    String(String), // Can be just a string, but also a function call.
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
    context: Context,
    #[allow(unused)]
    fill_missing: PolyValue,
    #[allow(unused)]
    alias_map: HashMap<String, PolyValue>, // This is not complete. Needs to be able to take string, callables, int, floats and still be deserializable. Maybe use Tuple here, so the key can also be an alias value
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
    id_context: Option<Context>,
    #[allow(unused)]
    cells: CellContext,
    #[allow(unused)]
    rename_id: Option<String>, // This only works, when the identifier is a name and not a regex. Maybe need, two different structs?
}

#[derive(Debug, Clone, Deserialize)]
struct MultiSeriesContext {
    #[allow(unused)]
    regex_identifier: String, // Not so sure if this works, when deserializing, because all values of the enum are strings.
    #[allow(unused)]
    id_context: Option<Context>,
    #[allow(unused)]
    cells: CellContext,
}
