pub(crate) fn generate_default_column_names(column_count: i64) -> Vec<String> {
    (0..column_count)
        .map(|index| format!("column_{}", index + 1))
        .collect()
}
