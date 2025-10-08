use crate::config::table_context::OutputDataType;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::CastingError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use log::debug;
use polars::datatypes::DataType;
use polars::prelude::{AnyValue, Column, TimeUnit};

fn cast_to_bool(column: &Column) -> Option<Column> {
    let col_name = column.name();
    column
        .str()
        .ok()?
        .iter()
        .map(|opt| {
            if let Some(raw_bool) = opt {
                return match raw_bool.to_lowercase().as_str() {
                    "true" => Some(AnyValue::Boolean(true)),
                    "false" => Some(AnyValue::Boolean(false)),
                    _ => None,
                };
            }
            Some(AnyValue::Null)
        })
        .collect::<Option<Vec<AnyValue>>>()
        .map(|bools| Column::new(col_name.clone(), bools))
}

fn cast_to_date(column: &Column) -> Option<Column> {
    let col_name = column.name();
    column
        .str()
        .ok()?
        .iter()
        .map(|s| {
            if let Some(raw_date) = s {
                return try_parse_string_date(raw_date)
                    .map(|datetime| AnyValue::Date(datetime.to_epoch_days()));
            }
            Some(AnyValue::Null)
        })
        .collect::<Option<Vec<AnyValue>>>()
        .map(|dates| Column::new(col_name.clone(), dates))
}

fn cast_to_datetime(column: &Column) -> Option<Column> {
    let col_name = column.name();
    column
        .str()
        .ok()?
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
        .map(|datetimes| Column::new(col_name.clone(), datetimes))
}

pub fn polars_column_cast_ambivalent(column: &Column) -> Result<Column, TransformError> {
    let col_name = column.name();
    debug!("Trying to cast column: {col_name}.");

    if column.dtype() != &DataType::String {
        debug!("Ignored {col_name}. Not of string type.");
        return Ok(column.clone());
    }

    if let Some(casted) = cast_to_bool(column) {
        debug!("Casted column: {col_name} to bool.");
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

    if let Some(casted) = cast_to_date(column) {
        debug!("Casted column: {col_name} to date.");
        return Ok(casted);
    }

    if let Some(casted) = cast_to_datetime(column) {
        debug!("Casted column: {col_name} to datetime.");
        return Ok(casted);
    }

    Ok(column.clone())
}

pub fn polars_column_cast_specific(
    column: &Column,
    desired_output_dtype: &OutputDataType,
) -> Result<Column, TransformError> {
    let col_name = column.name();
    debug!("Trying to cast column: {col_name} to datatype: {desired_output_dtype:?}");

    if column.dtype() != &DataType::String {
        debug!("Ignored {col_name}. Not of string type.");
        return Ok(column.clone());
    }

    let failed_parse_err = || {
        CastingError(format!(
            "Unable to convert column {col_name} to {desired_output_dtype:?}."
        ))
    };

    match desired_output_dtype {
        OutputDataType::String => Ok(column.clone()),
        OutputDataType::Boolean => cast_to_bool(column)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to bool.");
            })
            .ok_or_else(failed_parse_err),
        OutputDataType::Int32 => column
            .strict_cast(&DataType::Int32)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to bool.");
            })
            .map_err(|_| failed_parse_err()),
        OutputDataType::Float64 => column
            .strict_cast(&DataType::Float64)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to bool.");
            })
            .map_err(|_| failed_parse_err()),
        OutputDataType::Date => cast_to_date(column)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to bool.");
            })
            .ok_or_else(failed_parse_err),
        OutputDataType::Datetime => cast_to_datetime(column)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to bool.");
            })
            .ok_or_else(failed_parse_err),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::OutputDataType;
    use crate::transform::utils::{
        cast_to_bool, cast_to_date, cast_to_datetime, polars_column_cast_ambivalent,
        polars_column_cast_specific,
    };
    use polars::datatypes::TimeUnit;
    use polars::prelude::{AnyValue, Column, DataType};
    use rstest::{fixture, rstest};

    #[fixture]
    fn int_col() -> Column {
        Column::new("int_col".into(), ["1", "2", "3"])
    }

    #[fixture]
    fn casted_int_col() -> Column {
        Column::new("int_col".into(), [1, 2, 3])
    }

    #[fixture]
    fn float_col() -> Column {
        Column::new("float_col".into(), ["1.5", "2.5", "3.5"])
    }

    #[fixture]
    fn casted_float_col() -> Column {
        Column::new("float_col".into(), [1.5, 2.5, 3.5])
    }

    #[fixture]
    fn bool_col() -> Column {
        Column::new("bool_col".into(), ["True", "False", "True"])
    }

    #[fixture]
    fn casted_bool_col() -> Column {
        Column::new("bool_col".into(), [true, false, true])
    }

    #[fixture]
    fn bool_col_with_nulls() -> Column {
        Column::new(
            "bool_col".into(),
            [
                AnyValue::String("True"),
                AnyValue::Null,
                AnyValue::String("False"),
            ],
        )
    }

    #[fixture]
    fn casted_bool_col_with_nulls() -> Column {
        Column::new(
            "bool_col".into(),
            [
                AnyValue::Boolean(true),
                AnyValue::Null,
                AnyValue::Boolean(false),
            ],
        )
    }

    #[fixture]
    fn date_col() -> Column {
        Column::new(
            "date_col".into(),
            ["2023-01-01", "2023-01-02", "2023-01-03"],
        )
    }

    #[fixture]
    fn date_col_with_null() -> Column {
        Column::new(
            "date_col".into(),
            [
                AnyValue::String("2023-01-01"),
                AnyValue::Null,
                AnyValue::String("2023-01-03"),
            ],
        )
    }

    #[fixture]
    fn datetime_col() -> Column {
        Column::new(
            "datetime_col".into(),
            [
                "2023-01-01T12:00:00",
                "2023-01-02T13:30:00",
                "2023-01-03T15:45:00",
            ],
        )
    }

    #[fixture]
    fn datetime_col_with_null() -> Column {
        Column::new(
            "datetime_col".into(),
            [
                AnyValue::String("2023-01-01T12:00:00"),
                AnyValue::Null,
                AnyValue::String("2023-01-03T15:45:00"),
            ],
        )
    }

    #[fixture]
    fn string_col() -> Column {
        Column::new("string_col".into(), ["hello", "world", "test"])
    }

    #[fixture]
    fn mixed_bag_col() -> Column {
        Column::new("mixed_bag_col".into(), ["1", "hello", "6.4"])
    }

    #[rstest]
    fn test_cast_to_int_ambivalent(int_col: Column, casted_int_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&int_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Int32);
        assert_eq!(casted_col, casted_int_col);
    }

    #[rstest]
    fn test_cast_to_float_ambivalent(float_col: Column, casted_float_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&float_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Float64);
        assert_eq!(casted_col, casted_float_col);
    }

    #[rstest]
    fn test_cast_to_bool_ambivalent(bool_col: Column, casted_bool_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&bool_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(casted_col, casted_bool_col);
    }

    #[rstest]
    fn test_cast_to_bool_nulls_ambivalent(
        bool_col_with_nulls: Column,
        casted_bool_col_with_nulls: Column,
    ) {
        let casted_col = polars_column_cast_ambivalent(&bool_col_with_nulls).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(casted_col, casted_bool_col_with_nulls);
    }

    #[rstest]
    fn test_cast_to_date_ambivalent(date_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&date_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
    }

    #[rstest]
    fn test_cast_to_date_null_ambivalent(date_col_with_null: Column) {
        let casted_col = polars_column_cast_ambivalent(&date_col_with_null).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
        assert_eq!(casted_col.null_count(), 1);
    }

    #[rstest]
    fn test_cast_to_datetime_ambivalent(datetime_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&datetime_col).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
    }

    #[rstest]
    fn test_cast_to_datetime_null_ambivalent(datetime_col_with_null: Column) {
        let casted_col = polars_column_cast_ambivalent(&datetime_col_with_null).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(casted_col.null_count(), 1);
    }

    #[rstest]
    fn test_string_col_no_change_ambivalent(string_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&string_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::String);
        assert_eq!(casted_col, string_col);
    }

    #[rstest]
    fn test_mixed_bag_no_change_ambivalent(mixed_bag_col: Column) {
        let casted_col = polars_column_cast_ambivalent(&mixed_bag_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::String);
        assert_eq!(casted_col, mixed_bag_col);
    }

    #[rstest]
    fn test_cast_to_int_specific(int_col: Column, casted_int_col: Column) {
        let casted_col = polars_column_cast_specific(&int_col, &OutputDataType::Int32).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Int32);
        assert_eq!(casted_col, casted_int_col);
    }

    #[rstest]
    fn test_cast_to_float_specific(float_col: Column, casted_float_col: Column) {
        let casted_col = polars_column_cast_specific(&float_col, &OutputDataType::Float64).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Float64);
        assert_eq!(casted_col, casted_float_col);
    }

    #[rstest]
    fn test_cast_to_bool_specific(bool_col: Column, casted_bool_col: Column) {
        let casted_col = polars_column_cast_specific(&bool_col, &OutputDataType::Boolean).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(casted_col, casted_bool_col);
    }

    #[rstest]
    fn test_cast_to_bool_nulls_specific(
        bool_col_with_nulls: Column,
        casted_bool_col_with_nulls: Column,
    ) {
        let casted_col =
            polars_column_cast_specific(&bool_col_with_nulls, &OutputDataType::Boolean).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(casted_col, casted_bool_col_with_nulls);
    }

    #[rstest]
    fn test_cast_to_date_specific(date_col: Column) {
        let casted_col = polars_column_cast_specific(&date_col, &OutputDataType::Date).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
    }

    #[rstest]
    fn test_cast_to_date_null_specific(date_col_with_null: Column) {
        let casted_col =
            polars_column_cast_specific(&date_col_with_null, &OutputDataType::Date).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
        assert_eq!(casted_col.null_count(), 1);
    }

    #[rstest]
    fn test_cast_to_datetime_specific(datetime_col: Column) {
        let casted_col =
            polars_column_cast_specific(&datetime_col, &OutputDataType::Datetime).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
    }

    #[rstest]
    fn test_cast_to_datetime_null_specific(datetime_col_with_null: Column) {
        let casted_col =
            polars_column_cast_specific(&datetime_col_with_null, &OutputDataType::Datetime)
                .unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(casted_col.null_count(), 1);
    }

    #[rstest]
    fn test_string_col_no_change_specific(string_col: Column) {
        let casted_col = polars_column_cast_specific(&string_col, &OutputDataType::String).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::String);
        assert_eq!(casted_col, string_col);
    }

    #[rstest]
    fn test_mixed_bag_err_specific(mixed_bag_col: Column) {
        assert!(polars_column_cast_specific(&mixed_bag_col, &OutputDataType::Float64).is_err());
    }

    #[rstest]
    fn test_cast_to_bool(bool_col: Column, string_col: Column, casted_bool_col: Column) {
        let casted_col = cast_to_bool(&bool_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Boolean);
        assert_eq!(casted_col, casted_bool_col);

        assert_eq!(cast_to_bool(&string_col), None)
    }

    #[rstest]
    fn test_cast_to_date(date_col: Column, string_col: Column) {
        let casted_col = cast_to_date(&date_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
        assert_eq!(cast_to_date(&string_col), None)
    }

    #[rstest]
    fn test_cast_to_datetime(datetime_col: Column, string_col: Column) {
        let casted_col = cast_to_datetime(&datetime_col).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(cast_to_datetime(&string_col), None)
    }
}
