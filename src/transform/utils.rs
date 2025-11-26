use crate::constants::ISO8601_DUR_PATTERN;
use crate::transform::error::DataProcessingError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use log::debug;
use polars::datatypes::DataType;
use polars::prelude::{AnyValue, Column, TimeUnit};
use regex::Regex;

pub fn is_iso8601_duration(dur_string: &str) -> bool {
    let re = Regex::new(ISO8601_DUR_PATTERN).unwrap();
    re.is_match(dur_string)
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
    use super::*;
    use rstest::rstest;

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

    #[rstest]
    fn test_is_iso8601_duration() {
        assert!(is_iso8601_duration("P47Y"));
        assert!(is_iso8601_duration("P47Y5M"));
        assert!(is_iso8601_duration("P47Y5M29D"));
        assert!(is_iso8601_duration("P47Y5M29DT8H"));
        assert!(is_iso8601_duration("P47Y5M29DT8H12M"));
        assert!(is_iso8601_duration("P47Y5M29DT8H12M15S"));

        assert!(!is_iso8601_duration("asd"));
        assert!(!is_iso8601_duration("123"));
        assert!(!is_iso8601_duration("47Y"));
    }
}
