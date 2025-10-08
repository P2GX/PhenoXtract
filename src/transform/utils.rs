use crate::transform::error::TransformError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use log::debug;
use polars::datatypes::DataType;
use polars::prelude::{AnyValue, Column, TimeUnit};

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
            if let Some(s) = opt {
                return match s.to_lowercase().as_str() {
                    "true" => Some(AnyValue::Boolean(true)),
                    "false" => Some(AnyValue::Boolean(false)),
                    _ => None,
                };
            }
            Some(AnyValue::Null)
        })
        .collect::<Option<Vec<AnyValue>>>()
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
        .map(|s| {
            if let Some(raw_datetime) = s {
                return try_parse_string_date(raw_datetime)
                    .map(|datetime| AnyValue::Date(datetime.to_epoch_days()));
            }
            Some(AnyValue::Null)
        })
        .collect::<Option<Vec<AnyValue>>>()
    {
        debug!("Casted column: {col_name} to date.");
        let casted = Column::new(col_name.clone(), dates);
        return Ok(casted);
    }

    if let Some(datetimes) = column
        .str()
        .map_err(|err| TransformError::CastingError(err.to_string()))?
        .iter()
        .map(|s| {
            if let Some(raw_datetime) = s {
                return try_parse_string_datetime(raw_datetime).map(|datetime| {
                    AnyValue::Datetime(datetime.and_utc().timestamp(), TimeUnit::Milliseconds, None)
                });
            }
            Some(AnyValue::Null)
        })
        .collect::<Option<Vec<AnyValue>>>()
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
    use polars::prelude::{AnyValue, Column, DataType};
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
    fn test_cast_to_bool_nulls() {
        let col = Column::new(
            "bool_col".into(),
            [
                AnyValue::String("True"),
                AnyValue::Null,
                AnyValue::String("False"),
            ],
        );
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(
            casted_col,
            Column::new(
                "bool_col".into(),
                [
                    AnyValue::Boolean(true),
                    AnyValue::Null,
                    AnyValue::Boolean(false)
                ]
            )
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
    fn test_cast_to_date_null() {
        let col = Column::new(
            "date_col".into(),
            [
                AnyValue::String("2023-01-01"),
                AnyValue::Null,
                AnyValue::String("2023-01-03"),
            ],
        );
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
        assert_eq!(casted_col.null_count(), 1);
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
    fn test_cast_to_datetime_null() {
        let col = Column::new(
            "datetime_col".into(),
            [
                AnyValue::String("2023-01-01T12:00:00"),
                AnyValue::Null,
                AnyValue::String("2023-01-03T15:45:00"),
            ],
        );
        let casted_col = polars_column_cast(&col).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(casted_col.null_count(), 1);
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
