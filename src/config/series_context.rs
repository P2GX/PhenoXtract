use crate::config::table_context::{CellContext, Context};
use crate::validation::multi_series_context_validation::validate_regex_multi_identifier;
use anyhow::anyhow;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;

pub enum SetId {
    Single(String),
    #[allow(unused)]
    MultiList(Vec<String>),
    MultiRegex(String),
}

/// Represents the context for one or more series (columns or rows).
///
/// This enum acts as a dispatcher. It can either define the context for a
/// single, specifically identified series or for multiple series identified
/// by a regular expression.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum SeriesContext {
    #[allow(unused)]
    Single(SingleSeriesContext),
    #[allow(unused)]
    Multi(MultiSeriesContext),
}

impl SeriesContext {
    /// Returns the identifier context associated with this `SeriesContext`.
    ///
    /// For `Single` variants, this is the `id_context` of the contained single series.
    /// For `Multi` variants, this is the `id_context` of the contained multi series.
    pub fn get_context(&self) -> Context {
        match self {
            SeriesContext::Single(single) => single.id_context.clone(),
            SeriesContext::Multi(multi) => multi.id_context.clone(),
        }
    }

    /// Sets a new identifier for this `SeriesContext`.
    ///
    /// # Arguments
    /// * `new_id` - The new identifier to assign. Must match the variant type of this context:
    ///   - `SetId::Single` can only be applied to `SeriesContext::Single`.
    ///   - `SetId::MultiList` or `SetId::MultiRegex` can only be applied to `SeriesContext::Multi`.
    ///
    /// # Errors
    /// Returns an error if:
    /// * The `new_id` does not match the variant type of the context.
    /// * The provided regex in `SetId::MultiRegex` is invalid.
    pub fn set_id(&mut self, new_id: SetId) -> Result<(), anyhow::Error> {
        match (self, new_id) {
            (SeriesContext::Single(single), SetId::Single(id)) => {
                single.identifier = id;
                Ok(())
            }

            (SeriesContext::Multi(multi), SetId::MultiList(ids)) => {
                multi.multi_identifier = MultiIdentifier::Multi(ids);
                Ok(())
            }

            (SeriesContext::Multi(multi), SetId::MultiRegex(pattern)) => {
                Regex::new(&pattern).map_err(|e| anyhow!(e.to_string()))?;
                multi.multi_identifier = MultiIdentifier::Regex(pattern);
                Ok(())
            }

            (SeriesContext::Single(_), SetId::MultiList(_) | SetId::MultiRegex(_)) => {
                anyhow::bail!("Cant set multi identifier on single identifier");
            }
            (SeriesContext::Multi(_), SetId::Single(_)) => {
                anyhow::bail!("Cant set single identifier on multi identifier");
            }
        }
    }

    /// Checks if the given identifier matches this `SeriesContext`.
    ///
    /// For `Single` variants, it matches if the stored identifier is equal to the input.
    /// For `Multi` variants:
    /// * If the identifier type is a regex, it matches on string equality of the regex pattern.
    /// * If the identifier type is a list, it matches if any element of the list equals the input.
    ///
    /// # Arguments
    /// * `identifier` - The identifier string to compare.
    ///
    /// # Returns
    /// `true` if the given identifier matches; `false` otherwise.
    pub fn matches_identifier(&self, identifier: &str) -> bool {
        match self {
            SeriesContext::Single(single) => single.identifier == identifier,
            SeriesContext::Multi(multi) => match &multi.multi_identifier {
                MultiIdentifier::Regex(regex_id) => regex_id == identifier,
                MultiIdentifier::Multi(vector) => vector.iter().any(|v| v == identifier),
            },
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
pub struct SingleSeriesContext {
    /// The unique identifier for the series.
    pub(crate) identifier: String,
    #[serde(default)]
    /// The semantic context found in the header/index of the series.
    id_context: Context,
    /// The context to apply to every cell within this series.
    cells: Option<CellContext>,
    /// A unique ID that can be used to link to other series

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
pub enum MultiIdentifier {
    Regex(String),
    Multi(Vec<String>),
}

/// Defines the context for multiple series identified by a regex pattern.
///
/// This is useful for applying the same logic to a group of related columns or rows,
/// for example, all columns whose names start with "measurement_".
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Validate)]
pub struct MultiSeriesContext {
    /// A regular expression used to match and select multiple series identifiers.
    #[validate(custom(function = "validate_regex_multi_identifier"))]
    pub multi_identifier: MultiIdentifier,
    /// The semantic context to apply to the identifiers of all matched column header or row indexes.
    id_context: Context,
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
