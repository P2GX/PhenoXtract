use crate::config::table_context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::borrow::Cow;
use std::collections::HashSet;
use validator::ValidationError;

pub(crate) fn validate_one_context_per_column(
    cdf: &ContextualizedDataFrame,
) -> Result<(), ValidationError> {
    let mut seen_cols = HashSet::new();
    let mut duplicates = vec![];

    let scs = cdf.get_series_contexts();
    scs.iter().for_each(|sc| {
        let cols = cdf.get_columns(sc.get_identifier());
        for col in cols {
            if !seen_cols.insert(col.name()) {
                duplicates.push(col.name());
            }
        }
    });

    let result = fail_validation_on_duplicates(duplicates.clone());
    result.map_err(|mut err| {
        err.add_param(
            Cow::from("contextualised_dataframe_name"),
            &cdf.context().name,
        );
        err.add_param(
            Cow::from("column_with_multiple_series_contexts"),
            &duplicates,
        );
        err.with_message(Cow::Owned(
            "There were columns in the CDF which were identified by multiple Series Contexts. A column can be identified by at most one Series Context.".to_string(),
        ))
    })
}
pub(crate) fn validate_single_patient_column(
    cdf: &ContextualizedDataFrame,
) -> Result<(), ValidationError> {
    if cdf
        .filter_columns()
        .where_data_context(Filter::Is(&Context::SubjectId))
        .collect()
        .len()
        != 1
    {
        let mut error = ValidationError::new("subject_missing");
        error.add_param(Cow::from("table_name"), &cdf.context().name);
        return Err(error.with_message(Cow::Owned(
            "Too many or to few subject id columns. Each table needs =1".to_string(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::ContextualizedDataFrame;
    use crate::validation::contextualised_dataframe_validation::{
        validate_one_context_per_column, validate_single_patient_column,
    };
    use polars::prelude::{Column, DataFrame};
    use rstest::{fixture, rstest};

    #[fixture]
    fn df() -> DataFrame {
        let col1 = Column::new("column_1".into(), ["P001"]);
        let col2 = Column::new("column_2".into(), ["P001"]);
        let col3 = Column::new("column_3".into(), ["P001"]);
        let col4 = Column::new("abcabc".into(), ["P001"]);
        let col5 = Column::new("abcabcabc".into(), ["P001"]);
        let col6 = Column::new("column_6".into(), ["P001"]);
        DataFrame::new(vec![col1, col2, col3, col4, col5, col6]).unwrap()
    }

    fn regex(regex: &str) -> SeriesContext {
        SeriesContext::default().with_identifier(Identifier::Regex(regex.to_string()))
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        SeriesContext::default().with_identifier(Identifier::Multi(
            ids.iter().map(|id| id.to_string()).collect(),
        ))
    }

    #[rstest]
    fn test_at_most_one_context_per_col(df: DataFrame) {
        let sc1 = regex("column_1"); //identifies just col1
        let sc2 = multi_ids(vec!["column_2", "column_3"]); //identifies col2 and col3
        let sc3 = regex("abc"); //identifies col4 and col5
        let tc = TableContext::new("patient_data".to_string(), vec![sc1, sc2, sc3]);

        let cdf = ContextualizedDataFrame::new(tc, df);

        assert!(validate_one_context_per_column(&cdf).is_ok());
    }

    #[rstest]
    fn test_more_than_one_context_per_col(df: DataFrame) {
        let sc1 = regex("column"); //identifies col1, col2, col3 and col6
        let sc2 = multi_ids(vec!["column_2", "abcabc"]); //identifies col2 and col4
        let sc3 = regex("abcabcabc"); //identifies col5
        let sc4 = regex("abcabcabc"); //identifies col5
        let tc = TableContext::new("patient_data".to_string(), vec![sc1, sc2, sc3, sc4]);

        let cdf = ContextualizedDataFrame::new(tc, df);

        let cols_with_multiple_scs = validate_one_context_per_column(&cdf)
            .err()
            .unwrap()
            .params
            .get("duplicates")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect::<Vec<String>>();

        assert!(
            cols_with_multiple_scs == vec!["column_2".to_string(), "abcabcabc".to_string()]
                || cols_with_multiple_scs == vec!["abcabcabc".to_string(), "column_2".to_string()]
        );
    }

    #[rstest]
    fn test_validate_single_patient_column_ok(df: DataFrame) {
        let sc1 = regex("column_1").with_data_context(Context::SubjectId);
        let sc2 = multi_ids(vec!["column_2", "column_3"]);
        let tc = TableContext::new("patient_data".to_string(), vec![sc1, sc2]);
        let cdf = ContextualizedDataFrame::new(tc, df);

        assert!(validate_single_patient_column(&cdf).is_ok());
    }

    #[rstest]
    fn test_validate_single_patient_column_too_many(df: DataFrame) {
        let sc1 = regex("column_1").with_data_context(Context::SubjectId);
        let sc2 = regex("column_2").with_data_context(Context::SubjectId);
        let tc = TableContext::new("patient_data".to_string(), vec![sc1, sc2]);
        let cdf = ContextualizedDataFrame::new(tc, df);

        let err = validate_single_patient_column(&cdf);
        assert!(err.is_err());
    }

    #[rstest]
    fn test_validate_single_patient_column_too_few(df: DataFrame) {
        let sc1 = regex("column_1");
        let sc2 = multi_ids(vec!["column_2", "column_3"]);
        let tc = TableContext::new("patient_data".to_string(), vec![sc1, sc2]);
        let cdf = ContextualizedDataFrame::new(tc, df);

        let err = validate_single_patient_column(&cdf);
        assert!(err.is_err());
    }
}
