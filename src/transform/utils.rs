use crate::config::table_context::OutputDataType;
use crate::transform::error::DataProcessingError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use log::debug;
use polars::datatypes::DataType;
use polars::prelude::{AnyValue, Column, TimeUnit};
use regex::Regex;
use crate::constants::ISO8601_DUR_PATTERN;

fn cast_to_bool(column: &Column) -> Result<Column, DataProcessingError> {
    let col_name = column.name();
    let str_col = column
        .str()
        .map_err(|_err| DataProcessingError::CastingError {
            col_name: col_name.as_str().to_string(),
            from: column.dtype().clone(),
            to: DataType::String,
        })?;

    let bools = str_col
        .iter()
        .map(|opt| match opt {
            Some(raw_bool) => match raw_bool.to_lowercase().as_str() {
                "true" => Ok(AnyValue::Boolean(true)),
                "false" => Ok(AnyValue::Boolean(false)),
                _ => Err(DataProcessingError::CastingError {
                    col_name: col_name.as_str().to_string(),
                    from: DataType::String,
                    to: DataType::Boolean,
                }),
            },
            None => Ok(AnyValue::Null),
        })
        .collect::<Result<Vec<AnyValue>, DataProcessingError>>()?;

    Ok(Column::new(col_name.clone(), bools))
}

fn cast_to_date(column: &Column) -> Result<Column, DataProcessingError> {
    let col_name = column.name();
    let str_col = column
        .str()
        .map_err(|_err| DataProcessingError::CastingError {
            col_name: col_name.to_string(),
            from: column.dtype().clone(),
            to: DataType::String,
        })?;

    let dates = str_col
        .iter()
        .map(|opt| match opt {
            Some(raw_date) => try_parse_string_date(raw_date)
                .map(|datetime| AnyValue::Date(datetime.to_epoch_days()))
                .ok_or(DataProcessingError::CastingError {
                    col_name: col_name.to_string(),
                    from: column.dtype().clone(),
                    to: DataType::Date,
                }),
            None => Ok(AnyValue::Null),
        })
        .collect::<Result<Vec<AnyValue>, DataProcessingError>>()?;

    Ok(Column::new(col_name.clone(), dates))
}

fn cast_to_datetime(column: &Column) -> Result<Column, DataProcessingError> {
    let col_name = column.name();
    let str_col = column
        .str()
        .map_err(|_err| DataProcessingError::CastingError {
            col_name: col_name.to_string(),
            from: column.dtype().clone(),
            to: DataType::String,
        })?;

    let datetimes = str_col
        .iter()
        .map(|opt| match opt {
            Some(raw_datetime) => try_parse_string_datetime(raw_datetime)
                .map(|datetime| {
                    AnyValue::Datetime(
                        datetime.and_utc().timestamp_millis(),
                        TimeUnit::Milliseconds,
                        None,
                    )
                })
                .ok_or(DataProcessingError::CastingError {
                    col_name: col_name.to_string(),
                    from: column.dtype().clone(),
                    to: DataType::Datetime(TimeUnit::Milliseconds, None),
                }),
            None => Ok(AnyValue::Null),
        })
        .collect::<Result<Vec<AnyValue>, DataProcessingError>>()?;

    Ok(Column::new(col_name.clone(), datetimes))
}

pub fn is_iso8601_duration(dur_string: &str) -> bool {
    let re = Regex::new(ISO8601_DUR_PATTERN).unwrap();
    re.is_match(dur_string)
}

pub fn polars_column_cast_ambivalent(column: &Column) -> Column {
    let col_name = column.name();
    debug!("Trying to cast column: {col_name}.");

    if column.dtype() != &DataType::String {
        debug!("Ignored {col_name}. Not of string type.");
        return column.clone();
    }

    if let Ok(casted) = cast_to_bool(column) {
        debug!("Casted column: {col_name} to bool.");
        return casted;
    }

    if let Ok(casted) = column.strict_cast(&DataType::Int64) {
        debug!("Casted column: {col_name} to i32.");
        return casted;
    }

    if let Ok(casted) = column.strict_cast(&DataType::Float64) {
        debug!("Casted column: {col_name} to f64.");
        return casted;
    }

    if let Ok(casted) = cast_to_date(column) {
        debug!("Casted column: {col_name} to date.");
        return casted;
    }

    if let Ok(casted) = cast_to_datetime(column) {
        debug!("Casted column: {col_name} to datetime.");
        return casted;
    }

    column.clone()
}

pub fn polars_column_cast_specific(
    column: &Column,
    desired_output_dtype: &OutputDataType,
) -> Result<Column, DataProcessingError> {
    let col_name = column.name();
    debug!("Trying to cast column: {col_name} to datatype: {desired_output_dtype:?}");

    if column.dtype() != &DataType::String {
        debug!("Ignored {col_name}. Not of string type.");
        return Ok(column.clone());
    }

    let failed_parse_err = |dtype: DataType| DataProcessingError::CastingError {
        col_name: col_name.to_string(),
        from: column.dtype().clone(),
        to: dtype,
    };

    match desired_output_dtype {
        OutputDataType::String => Ok(column.clone()),
        OutputDataType::Boolean => cast_to_bool(column).inspect(|_casted| {
            debug!("Casted column: {col_name} to bool.");
        }),
        OutputDataType::Int64 => column
            .strict_cast(&DataType::Int64)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to Int64.");
            })
            .map_err(|_| failed_parse_err(DataType::Int64)),
        OutputDataType::Float64 => column
            .strict_cast(&DataType::Float64)
            .inspect(|_casted| {
                debug!("Casted column: {col_name} to Float64.");
            })
            .map_err(|_| failed_parse_err(DataType::Float64)),
        OutputDataType::Date => cast_to_date(column).inspect(|_casted| {
            debug!("Casted column: {col_name} to Date.");
        }),
        OutputDataType::Datetime => cast_to_datetime(column).inspect(|_casted| {
            debug!("Casted column: {col_name} to Datetime.");
        }),
    }
}

/// A struct for creating columns which have HPO IDs in the header
/// and observation statuses in the cells.
/// The headers of HPO columns will have the format HP:1234567{separator}A
/// where {separator} is some char, which is by default #, and A is the block_id.
/// If block_id = None then the HPO column headers will have the format HP:1234567.
pub struct HpoColMaker {
    separator: char,
}

impl HpoColMaker {
    pub fn new() -> HpoColMaker {
        HpoColMaker { separator: '#' }
    }

    pub fn create_hpo_col(
        &self,
        hpo_id: &str,
        block_id: Option<&str>,
        data: Vec<AnyValue>,
    ) -> Column {
        let header = match block_id {
            None => hpo_id.to_string(),
            Some(block_id) => format!("{}{}{}", hpo_id, self.separator, block_id),
        };
        Column::new(header.into(), data)
    }

    pub fn decode_column_header<'a>(&self, hpo_col: &'a Column) -> (&'a str, Option<&'a str>) {
        let split_col_name: Vec<&str> = hpo_col.name().split(self.separator).collect();
        let hpo_id = split_col_name[0];
        let block_id = split_col_name.get(1).copied();
        (hpo_id, block_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::OutputDataType;
    use crate::transform::utils::{
        HpoColMaker, cast_to_bool, cast_to_date, cast_to_datetime, polars_column_cast_ambivalent,
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
    fn casted_date_col() -> Column {
        use polars::prelude::*;

        Column::new(
            "date_col".into(),
            [
                AnyValue::Date(19358), // 2023-01-01
                AnyValue::Date(19359), // 2023-01-02
                AnyValue::Date(19360), // 2023-01-03
            ],
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
    fn casted_date_col_with_null() -> Column {
        Column::new(
            "date_col".into(),
            [
                AnyValue::Date(19358), // 2023-01-01
                AnyValue::Null,
                AnyValue::Date(19360), // 2023-01-03
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
    fn casted_datetime_col() -> Column {
        Column::new(
            "datetime_col".into(),
            [
                AnyValue::Datetime(1672574400000, TimeUnit::Milliseconds, None),
                AnyValue::Datetime(1672666200000, TimeUnit::Milliseconds, None),
                AnyValue::Datetime(1672760700000, TimeUnit::Milliseconds, None),
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
    fn casted_datetime_col_with_null() -> Column {
        Column::new(
            "datetime_col".into(),
            [
                AnyValue::Datetime(1672574400000, TimeUnit::Milliseconds, None),
                AnyValue::Null,
                AnyValue::Datetime(1672760700000, TimeUnit::Milliseconds, None),
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
    #[case::int(int_col(), DataType::Int64, casted_int_col())]
    #[case::float(float_col(), DataType::Float64, casted_float_col())]
    #[case::bool(bool_col(), DataType::Boolean, casted_bool_col())]
    #[case::bool_with_nulls(bool_col_with_nulls(), DataType::Boolean, casted_bool_col_with_nulls())]
    #[case::date(date_col(), DataType::Date, casted_date_col())]
    #[case::date_with_null(date_col_with_null(), DataType::Date, casted_date_col_with_null())]
    #[case::datetime(
        datetime_col(),
        DataType::Datetime(TimeUnit::Milliseconds, None),
        casted_datetime_col()
    )]
    #[case::datetime_with_null(
        datetime_col_with_null(),
        DataType::Datetime(TimeUnit::Milliseconds, None),
        casted_datetime_col_with_null()
    )]
    fn test_cast_ambivalent(
        #[case] input: Column,
        #[case] expected_dtype: DataType,
        #[case] expected: Column,
    ) {
        let casted_col = polars_column_cast_ambivalent(&input);
        assert_eq!(casted_col.dtype(), &expected_dtype);
        assert_eq!(casted_col, expected);
    }

    #[rstest]
    #[case::string(string_col(), DataType::String)]
    #[case::mixed_bag(mixed_bag_col(), DataType::String)]
    fn test_no_change_ambivalent(#[case] input: Column, #[case] expected_dtype: DataType) {
        let casted_col = polars_column_cast_ambivalent(&input);
        assert_eq!(casted_col.dtype(), &expected_dtype);
        assert_eq!(casted_col, input);
    }

    #[rstest]
    #[case::int(int_col(), OutputDataType::Int64, DataType::Int64, casted_int_col())]
    #[case::float(
        float_col(),
        OutputDataType::Float64,
        DataType::Float64,
        casted_float_col()
    )]
    #[case::bool(
        bool_col(),
        OutputDataType::Boolean,
        DataType::Boolean,
        casted_bool_col()
    )]
    #[case::bool_with_nulls(
        bool_col_with_nulls(),
        OutputDataType::Boolean,
        DataType::Boolean,
        casted_bool_col_with_nulls()
    )]
    #[case::date(date_col(), OutputDataType::Date, DataType::Date, casted_date_col())]
    #[case::date_with_null(
        date_col_with_null(),
        OutputDataType::Date,
        DataType::Date,
        casted_date_col_with_null()
    )]
    #[case::datetime(
        datetime_col(),
        OutputDataType::Datetime,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        casted_datetime_col()
    )]
    #[case::datetime_with_null(
        datetime_col_with_null(),
        OutputDataType::Datetime,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        casted_datetime_col_with_null()
    )]
    fn test_cast_specific(
        #[case] input: Column,
        #[case] output_dtype: OutputDataType,
        #[case] expected_dtype: DataType,
        #[case] expected: Column,
    ) {
        let casted_col = polars_column_cast_specific(&input, &output_dtype).unwrap();
        assert_eq!(casted_col.dtype(), &expected_dtype);
        assert_eq!(casted_col, expected);
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
        assert!(cast_to_bool(&string_col).is_err());
    }

    #[rstest]
    fn test_cast_to_date(date_col: Column) {
        let casted_col = cast_to_date(&date_col).unwrap();
        assert_eq!(casted_col.dtype(), &DataType::Date);
    }

    #[rstest]
    fn test_cast_to_date_err(string_col: Column) {
        assert!(cast_to_date(&string_col).is_err())
    }

    #[rstest]
    fn test_cast_to_datetime(datetime_col: Column) {
        let casted_col = cast_to_datetime(&datetime_col).unwrap();
        assert_eq!(
            casted_col.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
    }

    #[rstest]
    fn test_cast_to_datetime_err(string_col: Column) {
        assert!(cast_to_datetime(&string_col).is_err())
    }

    #[rstest]
    fn test_create_hpo_col() {
        let hpo_col_maker = HpoColMaker::new();

        let hpo_col = hpo_col_maker.create_hpo_col(
            "HP:1234567",
            Some("A"),
            vec![
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(false),
            ],
        );
        let expected_hpo_col = Column::new("HP:1234567#A".into(), vec![true, true, false]);
        assert_eq!(hpo_col, expected_hpo_col);

        let hpo_col2 = hpo_col_maker.create_hpo_col(
            "HP:1234567",
            None,
            vec![
                AnyValue::Boolean(true),
                AnyValue::Boolean(true),
                AnyValue::Boolean(false),
            ],
        );
        let expected_hpo_col2 = Column::new("HP:1234567".into(), vec![true, true, false]);
        assert_eq!(hpo_col2, expected_hpo_col2);
    }

    #[rstest]
    fn test_decode_column_header() {
        let hpo_col_maker = HpoColMaker::new();
        let hpo_col = Column::new("HP:1234567#A".into(), vec![true, true, false]);
        assert_eq!(
            ("HP:1234567", Some("A")),
            hpo_col_maker.decode_column_header(&hpo_col)
        );

        let hpo_col2 = Column::new("HP:1234567".into(), vec![true, true, false]);
        assert_eq!(
            ("HP:1234567", None),
            hpo_col_maker.decode_column_header(&hpo_col2)
        );
    }
}
