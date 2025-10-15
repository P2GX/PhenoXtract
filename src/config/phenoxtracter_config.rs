use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use crate::validation::phenoxtractor_config_validation::validate_unique_data_sources;
use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize, Validate, Serialize, Clone, PartialEq)]
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
    use crate::config::table_context::{
        AliasMap, CellValue, OutputDataType, SeriesContext, TableContext,
    };
    use crate::config::table_context::{Context as PhenopacketContext, Identifier};
    use crate::extract::csv_data_source::CSVDataSource;
    use crate::extract::data_source::DataSource;
    use crate::extract::excel_data_source::ExcelDatasource;
    use crate::extract::extraction_config::ExtractionConfig;
    use rstest::*;
    use std::collections::HashMap;
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
      cohort_name: arkham asylum
    "#;

    const TOML_DATA: &[u8] = br#"
    [meta_data]
    created_by = "Rouven Reuter"
    submitted_by = "Magnus Knut Hansen"
    cohort_name = "arkham asylum"

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
        "submitted_by": "Magnus Knut Hansen",
        "cohort_name": "arkham asylum"
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
        cohort_name: "arkham asylum"
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
          header_context: subject_id
          data_context: hpo_label
          fill_missing: "Zollinger-Ellison syndrome"
          alias_map:
            hash_map:
              "null": "Primary peritoneal carcinoma"
              "M": "Male"
              "102": "High quantity"
              "169.5": "Very high quantity"
              "true": "smoker"
            output_dtype: String
          building_block_id: "block_1"
    extraction_config:
      name: "Sheet1"
      has_headers: true
      patients_are_rows: true

  - type: "excel"
    source: "./data/example.excel"
    contexts:
      - name: "Sheet1"
        context:
          - identifier: "lab_result_.*"
            header_context: subject_id
            data_context: hpo_label
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              hash_map:
                "neoplasma": "4"
                "height": "1.85"
              output_dtype: Float64
      - name: "Sheet2"
        context:
          - identifier:
              - "Col_1"
              - "Col_2"
              - "Col_3"
            header_context: subject_id
            data_context: hpo_label
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              hash_map:
                "smoker": "true"
              output_dtype: Boolean
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
  cohort_name: "Arkham Asylum 2025"
  hpo_version: "latest"
  mondo_version: "latest"
  geno_version: "latest"
    "#;

        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(data).unwrap();
        let config = PhenoXtractorConfig::load(file_path).unwrap();

        let expected_config = PhenoXtractorConfig {
            meta_data: MetaData {
                created_by: Some("Rouven Reuter".to_string()),
                submitted_by: "Magnus Knut Hansen".to_string(),
                cohort_name: "Arkham Asylum 2025".to_string(),
                ..Default::default()
            },
            pipeline: Some(PipelineConfig::new(
                vec!["alias_mapping".to_string(), "fill_null".to_string()],
                "file_system".to_string(),
            )),
            data_sources: vec![
                // First data source: CSV
                DataSource::Csv(CSVDataSource {
                    source: PathBuf::from("./data/example.csv"),
                    separator: Some(','),
                    extraction_config: ExtractionConfig {
                        name: "Sheet1".to_string(),
                        has_headers: true,
                        patients_are_rows: true,
                    },
                    context: TableContext {
                        name: "TestTable".to_string(),
                        context: vec![SeriesContext::new(
                            Identifier::Regex("patient_id".to_string()),
                            PhenopacketContext::SubjectId,
                            PhenopacketContext::HpoLabel,
                            Some(CellValue::String("Zollinger-Ellison syndrome".to_string())),
                            Some(AliasMap::new(
                                HashMap::from([
                                    (
                                        "null".to_string(),
                                        "Primary peritoneal carcinoma".to_string(),
                                    ),
                                    ("M".to_string(), "Male".to_string()),
                                    ("102".to_string(), "High quantity".to_string()),
                                    ("169.5".to_string(), "Very high quantity".to_string()),
                                    ("true".to_string(), "smoker".to_string()),
                                ]),
                                OutputDataType::String,
                            )),
                            Some("block_1".to_string()),
                        )],
                    },
                }),
                // Second data source: Excel
                DataSource::Excel(ExcelDatasource {
                    source: PathBuf::from("./data/example.excel"),
                    extraction_configs: vec![
                        ExtractionConfig {
                            name: "Sheet1".to_string(),
                            has_headers: true,
                            patients_are_rows: true,
                        },
                        ExtractionConfig {
                            name: "Sheet2".to_string(),
                            has_headers: true,
                            patients_are_rows: true,
                        },
                    ],
                    contexts: vec![
                        // Context for "Sheet1"
                        TableContext {
                            name: "Sheet1".to_string(),
                            context: vec![SeriesContext::new(
                                Identifier::Regex("lab_result_.*".to_string()),
                                PhenopacketContext::SubjectId,
                                PhenopacketContext::HpoLabel,
                                Some(CellValue::String("Zollinger-Ellison syndrome".to_string())),
                                Some(AliasMap::new(
                                    HashMap::from([
                                        ("neoplasma".to_string(), "4".to_string()),
                                        ("height".to_string(), "1.85".to_string()),
                                    ]),
                                    OutputDataType::Float64,
                                )),
                                None,
                            )],
                        },
                        // Context for "Sheet2"
                        TableContext {
                            name: "Sheet2".to_string(),
                            context: vec![SeriesContext::new(
                                Identifier::Multi(vec![
                                    "Col_1".to_string(),
                                    "Col_2".to_string(),
                                    "Col_3".to_string(),
                                ]),
                                PhenopacketContext::SubjectId,
                                PhenopacketContext::HpoLabel,
                                Some(CellValue::String("Zollinger-Ellison syndrome".to_string())),
                                Some(AliasMap::new(
                                    HashMap::from([("smoker".to_string(), "true".to_string())]),
                                    OutputDataType::Boolean,
                                )),
                                None,
                            )],
                        },
                    ],
                }),
            ],
        };

        assert_eq!(config, expected_config);
    }
}
