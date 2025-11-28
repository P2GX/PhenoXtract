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
pub(crate) fn collect_single_multiplicity_element<'a>(
    patient_cdf: &'a ContextualizedDataFrame,
    data_context: &'a Context,
    header_context: &'a Context,
) -> Result<Option<String>, CollectorError> {
    let cols_of_element_type = patient_cdf
        .filter_columns()
        .where_data_context(Filter::Is(data_context))
        .where_header_context(Filter::Is(header_context))
        .collect();

    let mut unique_values: HashSet<String> = HashSet::new();

    for col in cols_of_element_type {
        let stringified_col =
            col.cast(&DataType::String)
                .map_err(|_| DataProcessingError::CastingError {
                    col_name: col.name().to_string(),
                    from: col.dtype().clone(),
                    to: DataType::String,
                })?;

        stringified_col.str()?.into_iter().for_each(|opt_val| {
            if let Some(val) = opt_val {
                unique_values.insert(val.to_string());
            }
        });
    }

    if unique_values.len() == 1 {
        match unique_values.iter().next() {
            Some(unique_val) => Ok(Some(unique_val.to_owned())),
            None => Ok(None),
        }
    } else if unique_values.len() > 1 {
        Err(CollectorError::ExpectedSingleValue {
            table_name: patient_cdf.context().name().to_string(),
            patient_id: patient_cdf
                .get_subject_id_col()
                .get(0)?
                .str_value()
                .to_string(),
            data_context: data_context.clone(),
            header_context: header_context.clone(),
        })
    } else {
        Ok(None)
    }
}

/// Extracts the columns from the cdf which have
/// Building Block ID = bb_id
/// data_context = data_context
/// header_context = header_context
/// and converts them to StringChunked
pub(crate) fn get_stringified_cols_with_data_context_in_bb<'a>(
    cdf: &'a ContextualizedDataFrame,
    bb_id: Option<&'a str>,
    data_context: &'a Context,
    header_context: &'a Context,
) -> Result<Vec<StringChunked>, CollectorError> {
    let cols = bb_id.map_or(vec![], |bb_id| {
        cdf.filter_columns()
            .where_building_block(Filter::Is(bb_id))
            .where_header_context(Filter::Is(header_context))
            .where_data_context(Filter::Is(data_context))
            .collect()
    });

    Ok(cols
        .iter()
        .map(|col| col.str().cloned())
        .collect::<Result<Vec<StringChunked>, PolarsError>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableContext;
    use crate::config::table_context::{Identifier, SeriesContext};
    use crate::test_utils::generate_patent_cdf_components;
    use polars::prelude::{AnyValue, Column, DataFrame};
    use rstest::rstest;

    #[rstest]
    fn test_collect_single_multiplicity_element_multiple() {
        let (subject_col, subject_tc) = generate_patent_cdf_components(1, 2);

        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new(
                "sex".into(),
                &[AnyValue::String("MALE"), AnyValue::String("MALE")],
            ),
        ])
        .unwrap();

        let context = TableContext::new(
            "test_collect_single_multiplicity_element_err".to_string(),
            vec![
                subject_tc,
                SeriesContext::default()
                    .with_identifier(Identifier::from("sex"))
                    .with_data_context(Context::SubjectSex),
            ],
        );
        let cdf = ContextualizedDataFrame::new(context, df).unwrap();

        let sme = collect_single_multiplicity_element(&cdf, &Context::SubjectSex, &Context::None)
            .unwrap()
            .unwrap();
        assert_eq!(sme, "MALE");
    }

    #[rstest]
    fn test_get_get_single_stringified_column_with_data_contexts_in_bb() {
        let bb_id = "bb1";
        let (subject_col, subject_sc) = generate_patent_cdf_components(1, 2);
        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new("sex".into(), &["FEMALE", "MALE"]),
        ])
        .unwrap();
        let tc = TableContext::new(
            "test_get_get_single_stringified_column_with_data_contexts_in_bb".to_string(),
            vec![
                subject_sc.with_building_block_id(Some(bb_id.to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::from("sex"))
                    .with_data_context(Context::SubjectSex)
                    .with_building_block_id(Some(bb_id.to_string())),
            ],
        );

        let cdf = ContextualizedDataFrame::new(tc, df).unwrap();

        let extracted_col = get_single_stringified_column_with_data_contexts_in_bb(
            &cdf,
            Some(bb_id),
            vec![&Context::SubjectSex],
        )
        .unwrap()
        .unwrap();

        assert_eq!(extracted_col.name().to_string(), "sex");
    }
    /*
    #[rstest]
    fn test_get_get_single_stringified_column_with_data_contexts_no_match() {
        let extracted_col = get_single_stringified_column_with_data_contexts_in_bb(
            &single_disease_phenotype_cdf,
            Some("Block_3"),
            vec![&Context::OrphanetLabelOrId],
        )
        .unwrap();

        assert!(extracted_col.is_none());
    }

    #[rstest]
    fn test_get_get_single_stringified_column_with_data_contexts_in_bb_err() {
        let result = get_single_stringified_column_with_data_contexts_in_bb(
            &single_disease_phenotype_cdf,
            Some("Block_3"),
            vec![&Context::MondoLabelOrId, &Context::OnsetAge],
        );

        assert!(result.is_err());
    }*/
}
