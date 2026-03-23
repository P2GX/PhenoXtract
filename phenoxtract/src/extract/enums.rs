use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum Filter<T> {
    Is(T),
    IsNot(T),
    IsSome,
    IsNone,
}

impl<T: std::fmt::Debug> std::fmt::Display for Filter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Filter::Is(value) => write!(f, "== {:?}", value),
            Filter::IsNot(value) => write!(f, "!= {:?}", value),
            Filter::IsSome => write!(f, "is some"),
            Filter::IsNone => write!(f, "is none"),
        }
    }
}
