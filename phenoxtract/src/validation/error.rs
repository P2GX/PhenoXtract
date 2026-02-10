use crate::config::table_context::Identifier;
use thiserror::Error;
use validator::ValidationErrors;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Created orphaned columns '{col_names:?}', when {when}")]
    OrphanedColumns {
        col_names: Vec<String>,
        when: String,
    },
    #[error(
        "SeriesContext Identifier '{sc_id}' does not point to any Column in table '{table_name}'"
    )]
    DanglingSeriesContext {
        sc_id: Identifier,
        table_name: String,
    },
    #[error(transparent)]
    ValidationCrateError(#[from] ValidationErrors),
}
