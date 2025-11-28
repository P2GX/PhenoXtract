use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::{CollectorError, DataProcessingError};
use polars::datatypes::{DataType, StringChunked};
use polars::error::PolarsError;
use std::collections::HashSet;

/// Given a CDF, building block ID and data contexts
/// this function will find all columns
/// - within that building block
/// - and with data context in data_contexts
/// * if there are no such columns returns Ok(None)
/// * if there are several such columns returns CollectorError
/// * if there is exactly one such column,
///   this column is converted to StringChunked and Ok(Some(StringChunked)) is returned
pub(super) fn get_single_stringified_column_with_data_contexts_in_bb(
    patient_cdf: &ContextualizedDataFrame,
    bb_id: Option<&str>,
    data_contexts: Vec<&Context>,
) -> Result<Option<StringChunked>, CollectorError> {
    if let Some(bb_id) = bb_id {
        let mut linked_cols = vec![];

        for data_context in data_contexts.iter() {
            linked_cols.extend(
                patient_cdf
                    .filter_columns()
                    .where_building_block(Filter::Is(bb_id))
                    .where_header_context(Filter::IsNone)
                    .where_data_context(Filter::Is(data_context))
                    .collect(),
            )
        }

        if linked_cols.len() == 1 {
            let single_linked_col = linked_cols
                .first()
                .expect("Column empty despite len check.");
            let cast_linked_col = single_linked_col.cast(&DataType::String).map_err(|_| {
                DataProcessingError::CastingError {
                    col_name: single_linked_col.name().to_string(),
                    from: single_linked_col.dtype().clone(),
                    to: DataType::String,
                }
            })?;
            Ok(Some(cast_linked_col.str()?.clone()))
        } else if linked_cols.is_empty() {
            Ok(None)
        } else {
            Err(CollectorError::ExpectedAtMostOneLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: bb_id.to_string(),
                contexts: data_contexts.into_iter().cloned().collect(),
                amount_found: linked_cols.len(),
            })
        }
    } else {
        Ok(None)
    }
}

/// Given a CDF corresponding to a single patient and a desired property (encoded by the variable context)
/// for which there can only be ONE value, e.g. Age, Vital Status, Sex, Gender...
/// this function will:
/// -find all values for that context
/// -throw an error if it finds multiple distinct values
/// return Ok(None) if it finds no values
/// return Ok(unique_val) if there is a single unique value
pub(crate) fn collect_single_multiplicity_element(
    patient_cdf: &ContextualizedDataFrame,
    data_context: Context,
) -> Result<Option<String>, CollectorError> {
    let cols_of_element_type = patient_cdf
        .filter_columns()
        .where_data_context(Filter::Is(&data_context))
        .collect();

    if cols_of_element_type.is_empty() {
        return Ok(None);
    }

    let mut unique_values: HashSet<String> = HashSet::new();

    for col in cols_of_element_type {
        let stringified_col =
            col.cast(&DataType::String)
                .map_err(|_| DataProcessingError::CastingError {
                    col_name: col.name().to_string(),
                    from: col.dtype().clone(),
                    to: DataType::String,
                })?;
        let stringified_col_str = stringified_col.str()?;
        stringified_col_str.into_iter().for_each(|opt_val| {
            if let Some(val) = opt_val {
                unique_values.insert(val.to_string());
            }
        });
    }

    if unique_values.len() > 1 {
        let subject_id = patient_cdf
            .get_subject_id_col()
            .str()?
            .get(0)
            .expect("subject_id missing");

        Err(CollectorError::ExpectedSingleValue {
            table_name: patient_cdf.context().name().to_string(),
            patient_id: subject_id.to_string(),
            context: data_context,
        })
    } else {
        match unique_values.iter().next() {
            Some(unique_val) => Ok(Some(unique_val.clone())),
            None => Ok(None),
        }
    }
}

/// Extracts the columns from the cdf which have
/// Building Block ID = bb_id
/// data_context = context
/// header_context = None
/// and converts them to StringChunked
pub(crate) fn get_stringified_cols_with_data_context_in_bb<'a>(
    cdf: &'a ContextualizedDataFrame,
    bb_id: Option<&'a str>,
    context: &'a Context,
) -> Result<Vec<&'a StringChunked>, CollectorError> {
    let cols = bb_id.map_or(vec![], |bb_id| {
        cdf.filter_columns()
            .where_building_block(Filter::Is(bb_id))
            .where_header_context(Filter::IsNone)
            .where_data_context(Filter::Is(context))
            .collect()
    });

    Ok(cols
        .iter()
        .map(|col| col.str())
        .collect::<Result<Vec<&'a StringChunked>, PolarsError>>()?)
}
