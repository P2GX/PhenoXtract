use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct TableContext {
    name: String,
    columns: Vec<SeriesContext>,
    rows: Vec<SeriesContext>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "identifier")]
enum Identifier {
    Name(String),
    Regex(String),
    Number(isize),
}
#[derive(Debug, Clone, Deserialize)]
enum Context {
    HpoID(String),
    HpoLabel(String),
    OnSet(String),
    OnSetDate(String),
    SubjectID(String),
    SubjectSex(String),
    //...
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum AliasValue {
    String(String), // Can be just a string, but also a function call.
    Int(i64),
    Float(f64),
}
#[derive(Debug, Clone, Deserialize)]
struct CellContext {
    context: Context,
    fill_missing: String, // Probably, should cover more than strings
    alias_map: HashMap<String, AliasValue>, // This is not complete. Needs to be able to take string, callables, int, floats and still be deserializable. Maybe use Tuple here, so the key can also be an alias value
}

#[derive(Debug, Clone, Deserialize)]
struct SeriesContext {
    identifier: Identifier, // Not so sure if this works, when deserializing, because all values of the enum are strings.
    id_context: Option<Context>,
    cells: CellContext,
    rename_id: Option<String>, // This only works, when the identifier is a name and not a regex. Maybe need, two different structs?
}
