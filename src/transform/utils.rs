use crate::constants::ISO8601_DUR_PATTERN;
use crate::transform::data_processing::parsing::{
    try_parse_string_date, try_parse_string_datetime,
};
use crate::transform::error::{CollectorError, PhenopacketBuilderError};
use chrono::{TimeZone, Utc};
use phenopackets::schema::v2::core::Sex;
use phenopackets::schema::v2::core::time_element::Element;
use phenopackets::schema::v2::core::{Age as IndividualAge, TimeElement};
use pivot::hgvs::ChromosomalSex;
use polars::datatypes::DataType;
use polars::prelude::{AnyValue, Column};
use prost_types::Timestamp;
use regex::Regex;
use std::borrow::Cow;

pub(crate) fn is_iso8601_duration(dur_string: &str) -> bool {
    let re = Regex::new(ISO8601_DUR_PATTERN).unwrap();
    re.is_match(dur_string)
}

pub(crate) fn try_parse_timestamp(ts_string: &str) -> Option<Timestamp> {
    try_parse_string_datetime(ts_string)
        .or_else(|| try_parse_string_date(ts_string).and_then(|date| date.and_hms_opt(0, 0, 0)))
        .map(|naive| Utc.from_utc_datetime(&naive))
        .map(|utc_dt| {
            let seconds = utc_dt.timestamp();
            let nanos = utc_dt.timestamp_subsec_nanos() as i32;
            Timestamp { seconds, nanos }
        })
}

pub(crate) fn try_parse_time_element(te_string: &str) -> Option<TimeElement> {
    if let Some(ts) = try_parse_timestamp(te_string) {
        let datetime_te = TimeElement {
            element: Some(Element::Timestamp(ts)),
        };
        return Some(datetime_te);
    }

    if is_iso8601_duration(te_string) {
        let dur_te = TimeElement {
            element: Some(Element::Age(IndividualAge {
                iso8601duration: te_string.to_string(),
            })),
        };
        return Some(dur_te);
    }

    None
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

pub(crate) fn chromosomal_sex_from_str(
    subject_sex: Option<String>,
) -> Result<ChromosomalSex, PhenopacketBuilderError> {
    match subject_sex {
        None => Ok(ChromosomalSex::Unknown),
        Some(sex) => {
            if let Some(pp_sex) = Sex::from_str_name(&sex) {
                match pp_sex {
                    Sex::Male => Ok(ChromosomalSex::XY),
                    Sex::Female => Ok(ChromosomalSex::XX),
                    Sex::OtherSex | Sex::UnknownSex => Ok(ChromosomalSex::Unknown),
                }
            } else {
                Err(PhenopacketBuilderError::ParsingError {
                    what: "Subject Sex".to_string(),
                    value: sex,
                })
            }
        }
    }
}

pub(crate) fn cow_cast(
    col: &'_ Column,
    output_dtype: DataType,
    allowed_datatypes: Vec<DataType>,
) -> Result<Cow<'_, Column>, CollectorError> {
    let col_dtype = col.dtype();
    if col_dtype == &output_dtype {
        Ok(Cow::Borrowed(&col))
    } else if allowed_datatypes.contains(col_dtype) {
        Ok(Cow::Owned(col.cast(&output_dtype)?))
    } else {
        Err(CollectorError::DataTypeError {
            column_name: col.name().to_string(),
            allowed_datatypes,
            found_datatype: col_dtype.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_suite::phenopacket_component_generation::{
        default_age_element, default_iso_age,
    };
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

    #[rstest]
    fn test_chromosomal_sex_from_str() {
        assert_eq!(
            chromosomal_sex_from_str(Some("MALE".to_string())).unwrap(),
            ChromosomalSex::XY
        );
        assert_eq!(
            chromosomal_sex_from_str(Some("FEMALE".to_string())).unwrap(),
            ChromosomalSex::XX
        );
        assert_eq!(
            chromosomal_sex_from_str(Some("UNKNOWN_SEX".to_string())).unwrap(),
            ChromosomalSex::Unknown
        );
        assert_eq!(
            chromosomal_sex_from_str(Some("OTHER_SEX".to_string())).unwrap(),
            ChromosomalSex::Unknown
        );
        assert_eq!(
            chromosomal_sex_from_str(None).unwrap(),
            ChromosomalSex::Unknown
        );
    }

    #[rstest]
    fn test_chromosomal_sex_from_str_err() {
        assert!(chromosomal_sex_from_str(Some("blah".to_string())).is_err());
    }

    #[rstest]
    fn test_parse_time_element_duration() {
        let te = try_parse_time_element(&default_iso_age()).unwrap();
        pretty_assertions::assert_eq!(te, default_age_element());
    }

    #[rstest]
    fn test_parse_time_element_datetime() {
        let te_date = try_parse_time_element("2001-01-29").unwrap();
        pretty_assertions::assert_eq!(
            te_date,
            TimeElement {
                element: Some(Element::Timestamp(Timestamp {
                    seconds: 980726400,
                    nanos: 0,
                })),
            }
        );
        let te_datetime = try_parse_time_element("2015-06-05T09:17:39Z").unwrap();
        pretty_assertions::assert_eq!(
            te_datetime,
            TimeElement {
                element: Some(Element::Timestamp(Timestamp {
                    seconds: 1433495859,
                    nanos: 0,
                })),
            }
        );
    }

    #[rstest]
    #[case("P81D5M13Y")]
    #[case("8D5M13Y")]
    #[case("09:17:39Z")]
    #[case("2020-20-15T09:17:39Z")]
    fn test_parse_time_element_invalid(#[case] date_str: &str) {
        let result = try_parse_time_element(date_str);
        assert!(result.is_none());
    }

    #[rstest]
    fn test_parse_timestamp() {
        let ts_date = try_parse_timestamp("2001-01-29").unwrap();
        pretty_assertions::assert_eq!(
            ts_date,
            Timestamp {
                seconds: 980726400,
                nanos: 0,
            }
        );
        let ts_datetime = try_parse_timestamp("2015-06-05T09:17:39Z").unwrap();
        pretty_assertions::assert_eq!(
            ts_datetime,
            Timestamp {
                seconds: 1433495859,
                nanos: 0,
            }
        );
        let result = try_parse_timestamp("09:17:39Z");
        assert!(result.is_none());
        let result = try_parse_timestamp("2020-20-15T09:17:39Z");
        assert!(result.is_none());
    }
}
