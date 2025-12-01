use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;
use polars::error::PolarsError;

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
    use crate::test_utils::generate_minimal_cdf_components;
    use polars::prelude::{Column, DataFrame};
    use rstest::fixture;

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
}
