use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use crate::validation::phenoxtractor_config_validation::validate_unique_data_sources;
use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize, Validate, Serialize, Clone)]
pub struct PhenoXtractorConfig {
    #[validate(custom(function = "validate_unique_data_sources"))]
    #[allow(unused)]
    data_sources: Vec<DataSource>,
    #[allow(unused)]
    meta_data: MetaData,
    #[allow(unused)]
    pipeline: Option<PipelineConfig>,
}

impl PhenoXtractorConfig {
    #[allow(dead_code)]
    pub fn load(file_path: PathBuf) -> Result<PhenoXtractorConfig, ConfigError> {
        if let Some(ext) = file_path.extension() {
            let file_format = match ext.to_str() {
                Some("yaml") => Ok(FileFormat::Yaml),
                Some("yml") => Ok(FileFormat::Yaml),
                Some("json") => Ok(FileFormat::Json),
                Some("toml") => Ok(FileFormat::Toml),
                Some("ron") => Ok(FileFormat::Ron),
                _ => Err(ConfigError::NotFound(format!(
                    "File format not supported. File needs to end with .yaml, .json, .toml or .ron. {file_path:?}"
                ))),
            }?;

            let settings = Config::builder()
                .add_source(File::new(file_path.to_str().unwrap(), file_format))
                .build()?;
            let settings_struct: PhenoXtractorConfig = settings.try_deserialize()?;
            Ok(settings_struct)
        } else {
            Err(ConfigError::NotFound(format!(
                "Could not find file extension on path {file_path:?}"
            )))
        }
    }
    #[allow(dead_code)]
    pub fn pipeline_config(&self) -> Option<PipelineConfig> {
        self.pipeline.clone()
    }
    #[allow(dead_code)]
    pub fn data_sources(&self) -> Vec<DataSource> {
        self.data_sources.clone()
    }
    #[allow(dead_code)]
    pub fn meta_data(&self) -> MetaData {
        self.meta_data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::data_source::DataSource;
    use rstest::*;
    use std::fs::File as StdFile;
    use std::io::Write;
    use std::str::FromStr;
    use tempfile::TempDir;

    const YAML_DATA: &[u8] = br#"
    data_sources:
      - type: "csv"
        source: "test/path"
        separator: ","
        context:
          name: "test_table"
          context_in_columns: true


    meta_data:
      created_by: Rouven Reuter
      submitted_by: Magnus Knut Hansen
    "#;

    const TOML_DATA: &[u8] = br#"
    [meta_data]
    created_by = "Rouven Reuter"
    submitted_by = "Magnus Knut Hansen"

    [[data_sources]]
    type = "csv"
    source = "test/path"
    separator = ","
    context = { name = "test_table", context_in_columns = true}
    "#;

    const JSON_DATA: &[u8] = br#"
    {
      "meta_data": {
        "created_by": "Rouven Reuter",
        "submitted_by": "Magnus Knut Hansen"
      },
      "data_sources": [
        {
          "type": "csv",
          "source": "test/path",
          "separator": ",",
          "context": {
            "name": "test_table",
            "context_in_columns": true
          }
        }
      ]
    }
    "#;

    const RON_DATA: &[u8] = br#"
(
    meta_data: (
        created_by: "Rouven Reuter",
        submitted_by: "Magnus Knut Hansen",
    ),
    data_sources: [
        (
            type: "csv",
            source: "test/path",
            separator: ",",
            context: (
                name: "test_table",
                context_in_columns: true
            ),
        ),
    ],
)
"#;
    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    #[case("yaml", YAML_DATA)]
    #[case("yml", YAML_DATA)]
    #[case("toml", TOML_DATA)]
    #[case("json", JSON_DATA)]
    #[case("ron", RON_DATA)]
    fn test_load_config_from_various_formats(
        temp_dir: TempDir,
        #[case] extension: &str,
        #[case] data: &[u8],
    ) {
        let file_path = temp_dir.path().join(format!("config.{extension}"));
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(data).unwrap();

        let mut phenoxtractor_config = PhenoXtractorConfig::load(file_path).unwrap();
        let meta_data = phenoxtractor_config.meta_data;
        let source = phenoxtractor_config.data_sources.pop().unwrap();

        match source {
            DataSource::Csv(data) => {
                assert_eq!(data.separator, Some(','));
                assert_eq!(data.context.name, "test_table");
                assert_eq!(data.source.to_str().unwrap(), "test/path");
            }
            _ => panic!("Wrong data source type. Expected Csv."),
        }

        assert_eq!(meta_data.created_by, Some("Rouven Reuter".to_string()));
        assert_eq!(meta_data.submitted_by, "Magnus Knut Hansen".to_string());
    }

    #[rstest]
    fn test_load_config_unsupported_file_format() {
        let file_path = PathBuf::from_str("test/path/config.exe").unwrap();
        let err = PhenoXtractorConfig::load(file_path);
        assert!(err.is_err());
    }
}
