use crate::config::context::Context;
use crate::config::table_context::{CellValue, Identifier};

pub trait SeriesContextBuilding<AliasMapType>: Sized {
    fn from_identifier(identifier: impl Into<Identifier>) -> Self;
    fn with_identifier(self, identifier: impl Into<Identifier>) -> Self;
    fn with_header_context(self, header_context: Context) -> Self;

    fn with_data_context(self, data_context: Context) -> Self;

    fn with_fill_missing(self, fill_missing: CellValue) -> Self;

    fn with_alias_map(self, alias_map: AliasMapType) -> Self;

    fn with_building_block_id(self, building_block_id: impl IntoOptionalString) -> Self;
}

pub trait IntoOptionalString {
    fn into_opt_string(self) -> Option<String>;
}

impl IntoOptionalString for String {
    fn into_opt_string(self) -> Option<String> {
        Some(self)
    }
}

impl IntoOptionalString for &str {
    fn into_opt_string(self) -> Option<String> {
        Some(self.to_string())
    }
}

impl IntoOptionalString for Option<String> {
    fn into_opt_string(self) -> Option<String> {
        self
    }
}

impl IntoOptionalString for Option<&str> {
    fn into_opt_string(self) -> Option<String> {
        self.map(|s| s.to_string())
    }
}

impl IntoOptionalString for &String {
    fn into_opt_string(self) -> Option<String> {
        Some(self.to_string())
    }
}
