use crate::config::context::Context;
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

    let scs = cdf.series_contexts();
    scs.iter().for_each(|sc| {
        let cols = cdf.get_columns(sc.get_identifier());
        for col in cols {
            if !seen_cols.insert(col.name()) {
                duplicates.push(col.name());
            }
        }
    });

    fail_validation_on_duplicates(
        &duplicates,
        "contextualised_dataframe_name",
        "There were columns in the CDF which were identified by multiple Series Contexts. A column can be identified by at most one Series Context.",
    )
}

pub(crate) fn validate_single_subject_id_column(
    cdf: &ContextualizedDataFrame,
) -> Result<(), ValidationError> {
    let is_valid = cdf
        .filter_columns()
        .where_data_context(Filter::Is(&Context::SubjectId))
        .collect()
        .len()
        == 1;

    if is_valid {
        Ok(())
    } else {
        let mut error = ValidationError::new("subject_id_column");
        error.add_param(Cow::from("table_name"), &cdf.context().name());

        let error_message = format!(
            "Found more than one or no column with data context {} in table {}",
            Context::SubjectId,
            cdf.context().name()
        );
        Err(error.with_message(Cow::Owned(error_message)))
    }
}

pub(crate) fn validate_subject_id_col_no_nulls(
    cdf: &ContextualizedDataFrame,
) -> Result<(), ValidationError> {
    validate_single_subject_id_column(cdf)?;
    let subject_id_col = cdf.get_subject_id_col();
    let null_count = subject_id_col.null_count();
    let is_valid = null_count == 0;
    if is_valid {
        Ok(())
    } else {
        let mut error = ValidationError::new("subject_id_column");
        error.add_param(Cow::from("table_name"), &cdf.context().name());

        let error_message = format!(
            "SubjectID column in table {} has gaps.",
            cdf.context().name()
        );
        Err(error.with_message(Cow::Owned(error_message)))
    }
}

pub(crate) fn validate_dangling_sc(cdf: &ContextualizedDataFrame) -> Result<(), ValidationError> {
    let mut error = ValidationError::new("dangling_series_context");
    let dangling_scs = cdf.get_dangling_scs();
    if !dangling_scs.is_empty() {
        error.add_param(Cow::from("series_contexts"), &dangling_scs);
        let error_message = format!(
            "The SeriesContexts with identifiers '{dangling_scs:?}' do not point to any column",
        );
        Err(error.with_message(Cow::Owned(error_message)))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::context::{Context, TimeElementType};
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::config::traits::SeriesContextBuilding;
    use crate::extract::ContextualizedDataFrame;
    use crate::validation::contextualised_dataframe_validation::{
        validate_one_context_per_column, validate_subject_id_col_no_nulls,
    };
    use crate::validation::error::ValidationError;
    use polars::df;
    use polars::prelude::{AnyValue, Column, DataFrame};
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
    use serde_json::from_value;
    use validator::ValidationErrorsKind;

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
        SeriesContext::from_identifier(regex.to_string())
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        SeriesContext::from_identifier(ids.iter().map(|id| id.to_string()).collect::<Vec<String>>())
    }

    #[rstest]
    fn test_at_most_one_context_per_col(df: DataFrame) {
        let sc1 = regex("column_1"); //identifies just col1
        let sc2 = multi_ids(vec!["column_2", "column_3"]); //identifies col2 and col3
        let sc3 = regex("abc"); //identifies col4 and col5
        let subject_col = regex("column_6").with_data_context(Context::SubjectId);
        let tc = TableContext::new("patient_data".to_string(), vec![sc1, sc2, sc3, subject_col]);

        let cdf = ContextualizedDataFrame::new(tc, df).unwrap();

        assert!(validate_one_context_per_column(&cdf).is_ok());
    }

    #[rstest]
    fn test_more_than_one_context_per_col(df: DataFrame) {
        let sc1 = regex("column_[0-57-9]\\d*"); //identifies col1, col2, col3
        let sc2 = multi_ids(vec!["column_2", "abcabc"]); //identifies col2 and col4
        let sc3 = regex("abcabcabc"); //identifies col5
        let sc4 = regex("abcabcabc"); //identifies col5
        let subject_col = regex("column_6").with_data_context(Context::SubjectId);

        let tc = TableContext::new(
            "patient_data".to_string(),
            vec![sc1, sc2, sc3, sc4, subject_col],
        );

        match ContextualizedDataFrame::new(tc, df).err().unwrap() {
            ValidationError::ValidationCrateError(val_error) => {
                let kind = val_error.0.values().next().unwrap();
                match kind {
                    ValidationErrorsKind::Field(err) => {
                        let f = err.first().unwrap();
                        let cols_with_multiple_scs = f
                            .params
                            .get("duplicates")
                            .unwrap()
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|s| s.as_str().unwrap().to_string())
                            .collect::<Vec<String>>();

                        assert!(
                            cols_with_multiple_scs
                                == vec!["column_2".to_string(), "abcabcabc".to_string()]
                                || cols_with_multiple_scs
                                    == vec!["abcabcabc".to_string(), "column_2".to_string()]
                        );
                    }
                    _ => panic!("unexpected field error"),
                }
            }

            _ => panic!("Expected an error"),
        }
    }

    #[rstest]
    fn test_validate_single_subject_id_column_no_subject_id() {
        let result = ContextualizedDataFrame::new(
            TableContext::default().with_name("test_table"),
            DataFrame::new(vec![]).unwrap(),
        );

        assert!(result.is_err());
        let error = result.unwrap_err();
        match error {
            ValidationError::ValidationCrateError(err) => {
                let kind = err.0.values().next().unwrap();
                match kind {
                    ValidationErrorsKind::Field(field) => {
                        let f = field.first().unwrap();
                        assert_eq!(f.code, "subject_id_column");
                        assert!(
                            f.message
                                .clone()
                                .unwrap()
                                .to_string()
                                .contains("test_table")
                        );
                        assert!(
                            f.message
                                .clone()
                                .unwrap()
                                .to_string()
                                .contains("more than one or no column")
                        );
                    }
                    _ => panic!("Expected ValidationCrateError"),
                }
            }
            _ => {
                panic!("Expected ValidationCrateError")
            }
        }
    }

    #[rstest]
    fn test_validate_single_subject_id_column_multiple_subject_ids() {
        let result = ContextualizedDataFrame::new(
            TableContext::new(
                "test_table".to_string(),
                vec![SeriesContext::default().with_identifier(Identifier::from("sub_col*"))],
            ),
            DataFrame::new(vec![
                Column::new("sub_col_1".into(), ["P001"]),
                Column::new("sub_col_2".into(), ["P001"]),
            ])
            .unwrap(),
        );

        assert!(result.is_err());
        let error = result.unwrap_err();

        match error {
            ValidationError::ValidationCrateError(err) => {
                let kind = err.0.values().next().unwrap();
                match kind {
                    ValidationErrorsKind::Field(field) => {
                        let f = field.first().unwrap();
                        assert_eq!(f.code, "subject_id_column");
                        let message = f.message.clone().unwrap().to_string();
                        assert!(message.contains("test_table"));
                        assert!(message.contains("more than one or no column"));
                    }
                    _ => panic!("Expected ValidationCrateError"),
                }
            }
            _ => {
                panic!("Expected ValidationCrateError")
            }
        }
    }

    #[rstest]
    fn test_validate_dangling_sc() {
        let sc_id = Identifier::from("no-column");
        let sc = SeriesContext::default().with_identifier(sc_id.clone());
        let result = ContextualizedDataFrame::new(
            TableContext::new(
                "test_table".to_string(),
                vec![
                    sc.clone(),
                    SeriesContext::default()
                        .with_identifier(Identifier::from("subject_id"))
                        .with_data_context(Context::SubjectId),
                ],
            ),
            df!["subject_id" => ["P001"]].unwrap(),
        );

        assert!(result.is_err());
        let error = result.unwrap_err();

        match error {
            ValidationError::ValidationCrateError(err) => {
                let kind = err.0.values().next().unwrap();
                match kind {
                    ValidationErrorsKind::Field(field) => {
                        let f = field.first().unwrap();
                        assert_eq!(f.code, "dangling_series_context");
                        assert!(f.message.clone().unwrap().to_string().contains("no-column"));
                        let extracted_sc_ids: Vec<Identifier> =
                            from_value(f.params.get("series_contexts").unwrap().clone()).unwrap();

                        assert_eq!(vec![sc_id], extracted_sc_ids);
                    }
                    _ => panic!("Expected ValidationCrateError"),
                }
            }
            _ => {
                panic!("Expected ValidationCrateError")
            }
        }
    }

    #[rstest]
    fn test_validate_subject_id_col_no_nulls_success() {
        let df = df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "age" => &[25, 30, 40],
        )
        .unwrap();

        let subject_id_sc = SeriesContext::default()
            .with_identifier(Identifier::from("subject_id"))
            .with_data_context(Context::SubjectId);
        let age_sc = SeriesContext::default()
            .with_identifier(Identifier::from("age"))
            .with_data_context(Context::Onset(TimeElementType::Age));
        let table_context = ContextualizedDataFrame::new(
            TableContext::new(
                "test_table".to_string(),
                vec![subject_id_sc.clone(), age_sc.clone()],
            ),
            df,
        )
        .unwrap();
        validate_subject_id_col_no_nulls(&table_context).unwrap();
    }

    #[rstest]
    fn test_validate_subject_id_col_no_nulls_fail() {
        let df = df!(
        "subject_id" => &[AnyValue::Null, AnyValue::String("bob"), AnyValue::String("charlie")],
        "age" => &[25, 30, 40],
        )
        .unwrap();

        let subject_id_sc = SeriesContext::from_identifier(Identifier::from("subject_id"))
            .with_data_context(Context::SubjectId);
        let age_sc = SeriesContext::from_identifier(Identifier::from("age"))
            .with_data_context(Context::Onset(TimeElementType::Age));
        let cdf_creation_attempt = ContextualizedDataFrame::new(
            TableContext::new(
                "test_table".to_string(),
                vec![subject_id_sc.clone(), age_sc.clone()],
            ),
            df,
        );
        assert!(cdf_creation_attempt.is_err());
    }
}
