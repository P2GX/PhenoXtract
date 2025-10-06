pub const DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d", // Date only
    "%Y.%m.%d", // Date only
    "%m/%d/%Y", // US date format
    "%d-%m-%Y", // European date format
    "%d.%m.%Y", // European date format
];

pub const DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S",         // e.g., 2025-09-04 11:00:59
    "%Y-%m-%dT%H:%M:%S",         // e.g., 2025-09-04T11:00:59
    "%Y-%m-%d %H:%M:%S%.f",      // With fractional seconds
    "%Y-%m-%dT%H:%M:%S%.f",      // With fractional seconds
    "%a, %d %b %Y %H:%M:%S GMT", // RFC 822 format
    "%+",                        // RFC 3339 / ISO 8601 format
];

pub const ISO8601_DUR_PATTERN: &str = r"^P(\d+Y)?(\d+M)?(\d+D)?(T(\d+H)?(\d+M)?(\d+S)?)?$";
