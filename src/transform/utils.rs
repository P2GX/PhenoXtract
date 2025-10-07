use crate::transform::error::TransformError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use chrono::{NaiveDate, NaiveDateTime};
use log::debug;
use polars::datatypes::DataType;
use polars::prelude::Column;

pub fn polars_column_cast(column: &Column) -> Result<Column, TransformError> {
    let col_name = column.name();
    debug!("Try casting Column: {col_name}");

    if column.dtype() != &DataType::String {
        debug!("Ignored {col_name}. Not of string type.");
        return Ok(column.clone());
    }

    if let Some(bools) = column
        .str()
        .map_err(|err| TransformError::CastingError(err.to_string()))?
        .iter()
        .map(|opt| {
            opt.as_ref().and_then(|s| match s.to_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })
        })
        .collect::<Option<Vec<bool>>>()
    {
        debug!("Casted column: {col_name} to bool.");
        let casted = Column::new(col_name.clone(), bools);
        return Ok(casted);
    }

    if let Ok(casted) = column.strict_cast(&DataType::Int32) {
        debug!("Casted column: {col_name} to i32.");
        return Ok(casted);
    }

    if let Ok(casted) = column.strict_cast(&DataType::Float64) {
        debug!("Casted column: {col_name} to f64.");
        return Ok(casted);
    }

    if let Some(dates) = column
        .str()
        .map_err(|err| TransformError::CastingError(err.to_string()))?
        .iter()
        .map(|s| s.and_then(try_parse_string_date))
        .collect::<Option<Vec<NaiveDate>>>()
    {
        debug!("Casted column: {col_name} to date.");
        let casted = Column::new(col_name.clone(), dates);
        return Ok(casted);
    }

    if let Some(datetimes) = column
        .str()
        .map_err(|err| TransformError::CastingError(err.to_string()))?
        .iter()
        .map(|s| s.and_then(try_parse_string_datetime))
        .collect::<Option<Vec<NaiveDateTime>>>()
    {
        debug!("Casted column: {col_name} to datetime.");
        let casted = Column::new(col_name.clone(), datetimes);
        return Ok(casted);
    }

    Ok(column.clone())
}

#[cfg(test)]
mod tests {
    use crate::transform::utils::polars_column_cast;
    use polars::datatypes::TimeUnit;
    use polars::prelude::{Column, DataType};
    use rstest::rstest;

    #[rstest]
    fn test_cast_to_int() {
        let col = Column::new("int_col".into(), [1, 2, 3]);
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Int32);
        assert_eq!(casted_col, Column::new("int_col".into(), [1, 2, 3]));
    }

    #[rstest]
    fn test_cast_to_float() {
        let col = Column::new("float_col".into(), ["1.5", "2.5", "3.5"]);
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Float64);
        assert_eq!(casted_col, Column::new("float_col".into(), [1.5, 2.5, 3.5]));
    }

    #[rstest]
    fn test_cast_to_bool() {
        let col = Column::new("bool_col".into(), ["True", "False", "True"]);
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(
            casted_col,
            Column::new("bool_col".into(), [true, false, true])
        );
    }

    #[rstest]
    fn test_cast_to_date() {
        let col = Column::new(
            "date_col".into(),
            ["2023-01-01", "2023-01-02", "2023-01-03"],
        );
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
    }

    #[rstest]
    fn test_cast_to_datetime() {
        let col = Column::new(
            "datetime_col".into(),
            [
                "2023-01-01T12:00:00",
                "2023-01-02T13:30:00",
                "2023-01-03T15:45:00",
            ],
        );
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
    }

    #[rstest]
    fn test_string_col_no_change() {
        let col = Column::new("string_col".into(), ["hello", "world", "test"]);
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::String);
        assert_eq!(
            casted_col,
            Column::new("int_col".into(), ["hello", "world", "test"])
        );
    }

    #[rstest]
    fn test_mixed_bag_no_change() {
        let col = Column::new("mixed_bag_col".into(), ["1", "hello", "6.4"]);
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::String);
        assert_eq!(
            casted_col,
            Column::new("mixed_bag_col".into(), ["1", "hello", "6.4"])
        );
    }
}
