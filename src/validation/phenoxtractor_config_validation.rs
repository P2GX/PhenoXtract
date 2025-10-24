use crate::extract::data_source::DataSource;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::collections::HashSet;
use validator::ValidationError;

pub fn validate_unique_data_sources(sources: &[DataSource]) -> Result<(), ValidationError> {
    let mut unique_identifiers: HashSet<String> = HashSet::new();
    let mut duplicates: Vec<String> = vec![];

    for source in sources {
        let path_buf = match source {
            DataSource::Csv(csv_source) => &csv_source.source,
            DataSource::Excel(excel_source) => &excel_source.source,
        };

        if let Some(path_string) = path_buf.to_str() {
            let owned_path = path_string.to_string();

            if !unique_identifiers.insert(owned_path.clone()) {
                duplicates.push(owned_path.clone());
            }
        } else {
            return Err(ValidationError::new("Unable to convert source to string"));
        }
    }
    fail_validation_on_duplicates(&duplicates, "duplicates", "Found duplicate data sources")
}

#[cfg(test)]
mod tests {
    use super::validate_unique_data_sources;
    use crate::config::table_context::TableContext;
    use crate::extract::csv_data_source::CSVDataSource;
    use crate::extract::data_source::DataSource;
    use crate::extract::excel_data_source::ExcelDatasource;
    use crate::extract::extraction_config::ExtractionConfig;
    use crate::extract::traits::HasSource;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::vec;

    #[fixture]
    fn csv_data_source() -> DataSource {
        DataSource::Csv(CSVDataSource::new(
            PathBuf::from("some/dir/file.csv"),
            None,
            TableContext::new("".to_string(), vec![]),
            ExtractionConfig::new("".to_string(), true, true),
        ))
    }
    #[fixture]
    fn excel_data_source() -> DataSource {
        DataSource::Excel(ExcelDatasource::new(
            PathBuf::from("some/dir/file.csv"),
            vec![TableContext::new("".to_string(), vec![])],
            vec![ExtractionConfig::new("".to_string(), true, true)],
        ))
    }
    #[rstest]
    fn test_validate_unique_data_sources_pass(csv_data_source: DataSource) {
        let mut other = csv_data_source.clone();
        if let DataSource::Csv(other_csv_source) = &mut other {
            let new_path = PathBuf::from_str("some/dir/file_1.csv").unwrap();
            *other_csv_source = other_csv_source.clone().with_source(&new_path);
        }

        let sources = [csv_data_source, other];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_validate_unique_data_sources_fail(csv_data_source: DataSource) {
        let other = csv_data_source.clone();

        let sources = [csv_data_source, other];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_validate_unique_data_sources_fail_mixed(
        csv_data_source: DataSource,
        excel_data_source: DataSource,
    ) {
        let sources = [csv_data_source, excel_data_source];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_validate_unique_data_sources_pass_mixed(
        mut csv_data_source: DataSource,
        excel_data_source: DataSource,
    ) {
        if let DataSource::Csv(csv_source) = &mut csv_data_source {
            let new_path = PathBuf::from_str("some/dir/file_1.csv").unwrap();
            *csv_source = csv_source.clone().with_source(&new_path);
        }

        let sources = [csv_data_source, excel_data_source];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_ok());
    }
}
