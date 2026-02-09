use crate::config::DataSourceConfig;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::collections::HashSet;
use validator::ValidationError;

pub fn validate_unique_data_sources(sources: &[DataSourceConfig]) -> Result<(), ValidationError> {
    let mut unique_identifiers: HashSet<String> = HashSet::new();
    let mut duplicates: Vec<String> = vec![];

    for source in sources {
        let path_buf = match source {
            DataSourceConfig::Csv(csv_config) => &csv_config.source,
            DataSourceConfig::Excel(excel_config) => &excel_config.source,
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
    use crate::config::DataSourceConfig;
    use crate::config::datasource_config::{CsvConfig, ExcelWorkbookConfig};
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use std::vec;

    #[fixture]
    fn csv_config() -> DataSourceConfig {
        DataSourceConfig::Csv(CsvConfig::new(
            PathBuf::from("some/dir/file.csv"),
            None,
            vec![],
            true,
            true,
        ))
    }

    #[fixture]
    fn other_csv_config() -> DataSourceConfig {
        DataSourceConfig::Csv(CsvConfig::new(
            PathBuf::from("some/dir/file_1.csv"),
            None,
            vec![],
            true,
            true,
        ))
    }

    #[fixture]
    fn excel_config() -> DataSourceConfig {
        DataSourceConfig::Excel(ExcelWorkbookConfig::new(
            PathBuf::from("some/dir/file.csv"),
            vec![],
        ))
    }

    #[rstest]
    fn test_validate_unique_data_sources_pass(
        csv_config: DataSourceConfig,
        other_csv_config: DataSourceConfig,
    ) {
        let sources = [csv_config, other_csv_config];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_validate_unique_data_sources_fail(csv_config: DataSourceConfig) {
        let other = csv_config.clone();

        let sources = [csv_config, other];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_validate_unique_data_sources_fail_mixed(
        csv_config: DataSourceConfig,
        excel_config: DataSourceConfig,
    ) {
        let sources = [csv_config, excel_config];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_validate_unique_data_sources_pass_mixed(
        other_csv_config: DataSourceConfig,
        excel_config: DataSourceConfig,
    ) {
        let sources = [other_csv_config, excel_config];

        let result = validate_unique_data_sources(&sources);
        assert!(result.is_ok());
    }
}
