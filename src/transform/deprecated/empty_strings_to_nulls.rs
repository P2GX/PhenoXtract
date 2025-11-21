use std::borrow::Cow;

fn empty_strings_to_nulls(
    cdf: &mut ContextualizedDataFrame,
) -> Result<(), DataProcessingError> {
    let string_col_names: Vec<String> = cdf
        .filter_columns()
        .where_dtype(Filter::Is(&DataType::String))
        .collect()
        .iter()
        .map(|col| col.name().to_string())
        .collect();

    for col_name in string_col_names {
        let column = cdf.data().column(&col_name)?;
        let nulled_col = column.str()?.apply(|opt_s| {
            if let Some(s) = opt_s {
                if s.is_empty() {
                    None
                } else {
                    Some(Cow::Borrowed(s))
                }
            } else {
                None
            }
        });
        cdf.builder()
            .replace_column(&col_name, nulled_col.into_series())?
            .build()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[rstest]
    fn test_empty_strings_to_nulls() {
        skip_in_ci!();
        let df = df![
            "subject_id" => ["P001", "P002", "P003", "P004", "P005"],
            "string_col" => &["", "hello", "", "blah", "  "],
            "int_col" => &[1, 2, 3, 4, 5],
        ]
            .unwrap();
        let mut cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "table".to_string(),
                vec![
                    SeriesContext::default()
                        .with_identifier(Identifier::Regex("subject_id".to_string()))
                        .with_data_context(Context::SubjectId),
                    SeriesContext::default()
                        .with_identifier(Identifier::Regex("string_col".to_string())),
                    SeriesContext::default()
                        .with_identifier(Identifier::Regex("int_col".to_string())),
                ],
            ),
            df,
        );

        TransformerModule::empty_strings_to_nulls(&mut cdf).unwrap();

        assert_eq!(
            cdf.data(),
            &df!["subject_id" => ["P001", "P002", "P003", "P004", "P005"],
                "string_col" => &[AnyValue::Null, AnyValue::String("hello"), AnyValue::Null, AnyValue::String("blah"), AnyValue::String("  ")],
                "int col" => &[1, 2, 3, 4, 5],
            ]
                .unwrap()
        );
    }
}