use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::CollectorError;
use polars::datatypes::DataType;

/// Extracts a uniquely-defined value from matching contexts given a collection of CDFs.
///
/// Hunts through the CDFs for all values matching the specified data and header contexts,
/// then enforces cardinality constraints: zero matches returns `None`, exactly one match
/// returns that value, but multiple distinct values trigger an error.
///
/// # Examples
///
/// ```ignore
/// // Extract a patient's date of birth from CDFs of info about them
/// let dob = get_single_multiplicity_element(
///     patient_cdfs,
///     Context::DateOfBirth,
///     Context::None
/// )?;
/// ```
///
/// # Errors
///
/// Returns `CollectorError::ExpectedSingleValue` when multiple distinct values are found
/// for the given context pair.
pub(crate) fn get_single_multiplicity_element(
    patient_cdfs: &[ContextualizedDataFrame],
    data_context: Context,
    header_context: Context,
) -> Result<Option<String>, CollectorError> {
    let mut cols_of_element_type = vec![];

    for patient_cdf in patient_cdfs {
        cols_of_element_type.extend(
            patient_cdf
                .filter_columns()
                .where_data_context(Filter::Is(&data_context))
                .where_header_context(Filter::Is(&header_context))
                .collect(),
        );
    }

    if cols_of_element_type.is_empty() {
        return Ok(None);
    }

    let mut combined_col = cols_of_element_type[0].clone();
    for col in cols_of_element_type.iter().skip(1) {
        combined_col.extend(col)?;
    }

    let unique_values = combined_col.drop_nulls().unique_stable()?;

    match unique_values.len() {
        0 => Ok(None),
        1 => {
            let cast_unique = unique_values.cast(&DataType::String)?;
            let val = cast_unique.get(0)?;
            Ok(Some(
                val.extract_str()
                    .expect("Should have been a string.")
                    .to_string(),
            ))
        }
        _ => Err(CollectorError::ExpectedSingleValue {
            patient_id: patient_cdfs[0]
                .get_subject_id_col()
                .get(0)?
                .str_value()
                .to_string(),
            data_context: ContextKind::from(data_context),
            header_context: ContextKind::from(header_context),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableContext;
    use crate::config::table_context::{Identifier, SeriesContext};
    use crate::test_suite::cdf_generation::generate_minimal_cdf_components;
    use polars::datatypes::AnyValue;
    use polars::prelude::{Column, DataFrame};
    use rstest::{fixture, rstest};

    #[fixture]
    fn sex_cdf() -> ContextualizedDataFrame {
        let bb_id = "bb1";
        let (subject_col, subject_sc) = generate_minimal_cdf_components(1, 2);
        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new("sex".into(), &["FEMALE", "MALE"]),
        ])
        .unwrap();
        let tc = TableContext::new(
            "sex_cdf".to_string(),
            vec![
                subject_sc.with_building_block_id(Some(bb_id.to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::from("sex"))
                    .with_data_context(Context::SubjectSex)
                    .with_building_block_id(Some(bb_id.to_string())),
            ],
        );
        ContextualizedDataFrame::new(tc, df).unwrap()
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_multiple() {
        let (subject_col, subject_tc) = generate_minimal_cdf_components(1, 2);

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

        let sme = get_single_multiplicity_element(&[cdf], Context::SubjectSex, Context::None)
            .unwrap()
            .unwrap();
        assert_eq!(sme, "MALE");
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_err() {
        let (subject_col, subject_sc) = generate_minimal_cdf_components(1, 2);
        let context = Context::AgeAtLastEncounter;

        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new("age".into(), &[46, 22]),
        ])
        .unwrap();
        let tc = TableContext::new(
            "test_collect_single_multiplicity_element_err".to_string(),
            vec![
                subject_sc,
                SeriesContext::default()
                    .with_identifier(Identifier::from("age"))
                    .with_data_context(context.clone()),
            ],
        );
        let cdf = ContextualizedDataFrame::new(tc, df).unwrap();

        let sme = get_single_multiplicity_element(&[cdf], context, Context::None);
        assert!(sme.is_err());
    }

    #[rstest]
    fn test_get_stringified_cols_with_data_context_in_bb() {
        let (subject_col, subject_sc) = generate_minimal_cdf_components(1, 2);
        let context = Context::AgeAtLastEncounter;

        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new("age".into(), &[46, 22]),
        ])
        .unwrap();
        let tc = TableContext::new(
            "tc".to_string(),
            vec![
                subject_sc,
                SeriesContext::default()
                    .with_identifier(Identifier::from("age"))
                    .with_data_context(context.clone())
                    .with_building_block_id(Some("B".to_string())),
            ],
        );
        let cdf = ContextualizedDataFrame::new(tc, df).unwrap();

        let sme = get_single_multiplicity_element(&[cdf], context, Context::None);
        assert!(sme.is_err());
    }
}
