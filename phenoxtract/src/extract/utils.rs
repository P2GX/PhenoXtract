pub(crate) fn generate_default_column_names(column_count: i64) -> Vec<String> {
    (0..column_count).map(|index| format!("{index}")).collect()
}

pub(crate) fn fmt_vec<T: std::fmt::Display>(
    f: &mut std::fmt::Formatter<'_>,
    name: &str,
    values: &[T],
) -> std::fmt::Result {
    if values.is_empty() {
        return Ok(());
    }

    write!(f, "{}=[", name)?;
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", v)?;
    }
    write!(f, "] ")
}
