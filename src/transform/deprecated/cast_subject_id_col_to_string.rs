#[deprecated]
fn cast_subject_id_col_to_string(
    cdf: &mut ContextualizedDataFrame,
) -> Result<(), DataProcessingError> {
    cdf.builder()
        .cast(&Context::None, &Context::SubjectId, DataType::String)?
        .build()?;
    Ok(())
}

#[rstest]
fn test_cast_subject_id_col_to_string() {
    let df = df!(
            "subject_id" => &[1, 2, 3, 4],
                    "age" => &[15, 25, 35, 65],
            "name" => &["adam", "bertha", "carey", "denise"]
        )
        .unwrap();

    let mut cdf = ContextualizedDataFrame::new(
        TableContext::new(
            "patient_data".to_string(),
            vec![
                SeriesContext::default()
                    .with_data_context(Context::SubjectId)
                    .with_identifier(Identifier::from("subject_id")),
                SeriesContext::default()
                    .with_identifier(Identifier::from("age"))
                    .with_data_context(Context::SubjectAge),
                SeriesContext::default().with_identifier(Identifier::from("name")),
            ],
        ),
        df,
    )
        .unwrap();
    TransformerModule::cast_subject_id_col_to_string(&mut cdf).unwrap();

    let new_subject_id_col = cdf.data().column("subject_id").unwrap();
    assert_eq!(new_subject_id_col.dtype(), &DataType::String);
    assert_eq!(
        new_subject_id_col,
        &Column::new("subject_id".into(), vec!["1", "2", "3", "4"])
    );
    let age_col = cdf.data().column("age").unwrap();
    assert_eq!(age_col.dtype(), &DataType::Int32);
    let name_col = cdf.data().column("name").unwrap();
    assert_eq!(name_col.dtype(), &DataType::String);
}