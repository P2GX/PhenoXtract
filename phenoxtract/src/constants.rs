#![allow(dead_code)]
use polars::prelude::DataType;

pub const DATE_FORMATS: &[&str] = &[
    "%Y",       // just the year (will be interpreted as the 1st of January)
    "%Y-%m-%d", // Date only
    "%Y.%m.%d", // Date only
    "%m/%d/%Y", // US date format
    "%d-%m-%Y", // European date format
    "%d.%m.%Y", // European date format
];

pub const DATETIME_FORMATS: &[&str] = &[
    "%Y",                // just the year (will be interpreted as the first second of 1st of January)
    "%Y-%m-%d %H:%M:%S", // e.g., 2025-09-04 11:00:59
    "%Y-%m-%dT%H:%M:%S", // e.g., 2025-09-04T11:00:59
    "%Y-%m-%d %H:%M:%S%.f", // With fractional seconds
    "%Y-%m-%dT%H:%M:%S%.f", // With fractional seconds
    "%a, %d %b %Y %H:%M:%S GMT", // RFC 822 format
    "%+",                // RFC 3339 / ISO 8601 format
];

pub const ISO8601_DUR_PATTERN: &str = r"^P(\d+Y)?(\d+M)?(\d+D)?(T(\d+H)?(\d+M)?(\d+S)?)?$";

pub(crate) struct PolarsNumericTypes;

impl PolarsNumericTypes {
    const ALL: [DataType; 4] = [
        DataType::Float64,
        DataType::Float32,
        DataType::Int64,
        DataType::Int32,
    ];

    const INTS: [DataType; 2] = [DataType::Int64, DataType::Int32];

    const FLOATS: [DataType; 2] = [DataType::Float64, DataType::Float32];

    pub(crate) const fn all() -> &'static [DataType; 4] {
        &Self::ALL
    }

    pub(crate) const fn ints() -> &'static [DataType; 2] {
        &Self::INTS
    }

    pub(crate) const fn floats() -> &'static [DataType; 2] {
        &Self::FLOATS
    }
}
