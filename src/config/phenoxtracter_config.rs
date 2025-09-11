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
        extraction_config:
            name: "test_config"
            has_headers: true
            patients_are_rows: true
        context:
          name: "test_table"


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
    context = { name = "test_table"}
    extraction_config = { name = "test_config", has_headers = true, patients_are_rows = true}
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
            "name": "test_table"
          },
          "extraction_config": {
            "name": "test_config",
            "has_headers": true,
            "patients_are_rows": true
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
            ),
            extraction_config: (
                name: "test_config",
                has_headers: true,
                patients_are_rows: true,
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

    #[rstest]
    fn test_load_complete_config(temp_dir: TempDir) {
        let data: &[u8] = br#"
data_sources:
  - type: "csv"
    source: "./data/example.csv"
    separator: ","
    context:
      name: "TestTable"
      context:
        - identifier: "patient_id"
          id_context: subject_id
          cells:
            context: "hpo_label"
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              "null": "Primary peritoneal carcinoma"
              "neoplasma": 4
              "smoker": true
              "height": 1.85
          linked_to:
          - "visit_table"
          - "demographics_table"
    extraction_config:
      name: "Sheet1"
      has_headers: true
      patients_are_rows: true

  - type: "excel"
    source: "./data/example.excel"
    contexts:
      - name: "Sheet1"
        context:
        - multi_identifier: "lab_result_.*"
          id_context: subject_id
          cells:
            context: "hpo_label"
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              "null": "Primary peritoneal carcinoma"
              "neoplasma": 4
              "smoker": true
              "height": 1.85
      - name: "Sheet2"
        context:
        - multi_identifier:
          - "Col_1"
          - "Col_2"
          - "Col_3"
          id_context: subject_id
          cells:
            context: "hpo_label"
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              "null": "Primary peritoneal carcinoma"
              "neoplasma": 4
              "smoker": true
              "height": 1.85
    extraction_configs:
      - name: "Sheet1"
        has_headers: true
        patients_are_rows: true
      - name: "Sheet2"
        has_headers: true
        patients_are_rows: true

pipeline:
  transform_strategies:
    - "alias_mapping"
    - "fill_null"
  loader: "file_system"

meta_data:
  created_by: Rouven Reuter
  submitted_by: Magnus Knut Hansen
    "#;

        let file_path = PathBuf::from_str("config.yaml").unwrap();
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(data).unwrap();
        let err = PhenoXtractorConfig::load(file_path).unwrap();
        dbg!(err);
    }
}
