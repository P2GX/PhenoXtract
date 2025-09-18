use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use polars::datatypes::AnyValue;
use polars::prelude::Column;

pub fn convert_col_to_string_vec(col: &Column) -> Result<Vec<String>, TransformError> {
    match col.as_series() {
        Some(col_as_series) => Ok(col_as_series
            .iter()
            .map(|val| match val {
                AnyValue::String(s) => s.to_string(),
                _ => val.to_string(),
            })
            .collect::<Vec<String>>()),
        None => Err(StrategyError(format!(
            "Could not convert column {} to a series.",
            col.name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::transform::strategies::utils::convert_col_to_string_vec;
    use polars::prelude::{AnyValue, Column};
    use rstest::rstest;

    #[rstest]
    fn test_convert_string_col_to_string_vec() {
        let strings = vec![
            "pneumonia",
            "Big calvaria",
            "Joint inflammation",
            "Nail psoriasis",
        ];
        let col = Column::new("phenotypic_features".into(), strings.clone());
        assert_eq!(convert_col_to_string_vec(&col).unwrap(), strings);
    }

    #[rstest]
    fn test_convert_int_col_to_string_vec() {
        let ints = vec![123, 456, 9, 10];
        let col = Column::new("age".into(), ints.clone());
        assert_eq!(
            convert_col_to_string_vec(&col).unwrap(),
            vec!["123", "456", "9", "10"]
        );
    }

    #[rstest]
    fn test_convert_float_col_to_string_vec() {
        let floats = vec![123.8, 456.2, 9.1, 10.20];
        let col = Column::new("weight".into(), floats.clone());
        assert_eq!(
            convert_col_to_string_vec(&col).unwrap(),
            vec!["123.8", "456.2", "9.1", "10.2"]
        );
    }

    #[rstest]
    fn test_convert_bool_col_to_string_vec() {
        let bools = vec![true, false, false, true];
        let col = Column::new("smokes".into(), bools.clone());
        assert_eq!(
            convert_col_to_string_vec(&col).unwrap(),
            vec!["true", "false", "false", "true"]
        );
    }

    //you could argue it is a bug with our code, that in a string column like below
    //there is no distinction between the string null and a null cell
    #[rstest]
    fn test_convert_col_with_nulls_to_string_vec() {
        let vec_with_nulls = vec![
            AnyValue::String("Pneumonia"),
            AnyValue::Null,
            AnyValue::Null,
            AnyValue::String("Asthma"),
            AnyValue::String("null"),
        ];
        let col = Column::new("known_conditions".into(), vec_with_nulls.clone());
        assert_eq!(
            convert_col_to_string_vec(&col).unwrap(),
            vec!["Pneumonia", "null", "null", "Asthma", "null"]
        );
    }
}
