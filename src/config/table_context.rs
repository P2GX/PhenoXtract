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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "identifier")]
enum Identifier {
    #[allow(unused)]
    Name(String),
    #[allow(unused)]
    Regex(String),
    #[allow(unused)]
    Number(isize),
}
#[derive(Debug, Clone, Deserialize)]
enum Context {
    #[allow(unused)]
    HpoID(String),
    #[allow(unused)]
    HpoLabel(String),
    #[allow(unused)]
    OnSet(String),
    #[allow(unused)]
    OnSetDate(String),
    #[allow(unused)]
    SubjectID(String),
    #[allow(unused)]
    SubjectSex(String),
    //...
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum AliasValue {
    #[allow(unused)]
    String(String), // Can be just a string, but also a function call.
    #[allow(unused)]
    Int(i64),
    #[allow(unused)]
    Float(f64),
}
#[derive(Debug, Clone, Deserialize)]
struct CellContext {
    #[allow(unused)]
    context: Context,
    #[allow(unused)]
    fill_missing: String, // Probably, should cover more than strings
    #[allow(unused)]
    alias_map: HashMap<String, AliasValue>, // This is not complete. Needs to be able to take string, callables, int, floats and still be deserializable. Maybe use Tuple here, so the key can also be an alias value
}

#[derive(Debug, Clone, Deserialize)]
struct SeriesContext {
    #[allow(unused)]
    identifier: Identifier, // Not so sure if this works, when deserializing, because all values of the enum are strings.
    #[allow(unused)]
    id_context: Option<Context>,
    #[allow(unused)]
    cells: CellContext,
    #[allow(unused)]
    rename_id: Option<String>, // This only works, when the identifier is a name and not a regex. Maybe need, two different structs?
}
