use crate::constants::ISO8601_DUR_PATTERN;
use crate::transform::error::PhenopacketBuilderError;
use phenopackets::schema::v2::core::Sex;
use pivot::hgvs::ChromosomalSex;
use polars::prelude::{AnyValue, Column};
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
}
