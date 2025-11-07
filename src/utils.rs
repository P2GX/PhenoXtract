use crate::constants::{DATE_FORMATS, DATETIME_FORMATS};
use chrono::{NaiveDate, NaiveDateTime};
use log::debug;

fn try_parse<T, F>(date_str: &str, formats: &[&str], parser: F) -> Option<T>
where
    F: Fn(&str, &str) -> Result<T, chrono::ParseError>,
{
    for format in formats {
        match parser(date_str, format) {
            Ok(value) => {
                return Some(value);
            }
            _ => {
                debug!("Failed to cast {date_str} to {format:?}");
                continue;
            }
        }
    }
    None
}

pub fn try_parse_string_date(date_str: &str) -> Option<NaiveDate> {
    try_parse(date_str, DATE_FORMATS, NaiveDate::parse_from_str)
}

pub fn try_parse_string_datetime(date_str: &str) -> Option<NaiveDateTime> {
    try_parse(date_str, DATETIME_FORMATS, NaiveDateTime::parse_from_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rstest::rstest;

    #[rstest]
    fn test_try_parse_date_success() {
        let date = try_parse_string_date("2025-09-04");
        assert_eq!(date, Some(NaiveDate::from_ymd_opt(2025, 9, 4).unwrap()));

        let date = try_parse_string_date("2025.09.04");
        assert_eq!(date, Some(NaiveDate::from_ymd_opt(2025, 9, 4).unwrap()));

        let date = try_parse_string_date("09/04/2025"); // US format
        assert_eq!(date, Some(NaiveDate::from_ymd_opt(2025, 9, 4).unwrap()));

        let date = try_parse_string_date("04-09-2025"); // European format
        assert_eq!(date, Some(NaiveDate::from_ymd_opt(2025, 9, 4).unwrap()));

        let date = try_parse_string_date("04.09.2025"); // European format
        assert_eq!(date, Some(NaiveDate::from_ymd_opt(2025, 9, 4).unwrap()));
    }

    #[rstest]
    fn test_try_parse_date_failure() {
        let date = try_parse_string_date("invalid-date");
        assert_eq!(date, None);

        let date = try_parse_string_date("2025/09/04"); // unsupported format
        assert_eq!(date, None);
    }

    #[rstest]
    fn test_try_parse_datetime_success() {
        let datetime = try_parse_string_datetime("2025-09-04 11:00:59");
        assert_eq!(
            datetime,
            Some(
                NaiveDate::from_ymd_opt(2025, 9, 4)
                    .unwrap()
                    .and_hms_opt(11, 0, 59)
                    .unwrap()
            )
        );

        let datetime = try_parse_string_datetime("2025-09-04T11:00:59");
        assert_eq!(
            datetime,
            Some(
                NaiveDate::from_ymd_opt(2025, 9, 4)
                    .unwrap()
                    .and_hms_opt(11, 0, 59)
                    .unwrap()
            )
        );

        let datetime = try_parse_string_datetime("2025-09-04 11:00:59.123456");
        assert_eq!(
            datetime,
            Some(
                NaiveDate::from_ymd_opt(2025, 9, 4)
                    .unwrap()
                    .and_hms_micro_opt(11, 0, 59, 123456)
                    .unwrap()
            )
        );

        let datetime = try_parse_string_datetime("Thu, 04 Sep 2025 11:00:59 GMT");
        assert_eq!(
            datetime,
            Some(
                NaiveDate::from_ymd_opt(2025, 9, 4)
                    .unwrap()
                    .and_hms_opt(11, 0, 59)
                    .unwrap()
            )
        );

        // ISO 8601 format
        let datetime = try_parse_string_datetime("2025-09-04T11:00:59+00:00");
        assert!(datetime.is_some());
    }

    #[rstest]
    fn test_try_parse_datetime_failure() {
        let datetime = try_parse_string_datetime("not-a-datetime");
        assert_eq!(datetime, None);

        let datetime = try_parse_string_datetime("2025/09/04 11:00:59"); // unsupported format
        assert_eq!(datetime, None);
    }
}
