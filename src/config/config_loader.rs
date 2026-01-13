use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub struct ConfigLoader;

impl ConfigLoader {
    pub fn load<'a, T: Serialize + Deserialize<'a>>(file_path: PathBuf) -> Result<T, ConfigError> {
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

            let config = Config::builder()
                .add_source(File::new(file_path.to_str().unwrap(), file_format))
                .build()?;
            let settings_struct: T = config.try_deserialize()?;
            Ok(settings_struct)
        } else {
            Err(ConfigError::NotFound(format!(
                "Could not find file extension on path {file_path:?}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::Context;
    use crate::config::loader_config::LoaderConfig;
    use crate::config::meta_data::MetaData;
    use crate::config::strategy_config::StrategyConfig;
    use crate::config::table_context::Identifier;
    use crate::config::table_context::{
        AliasMap, CellValue, OutputDataType, SeriesContext, TableContext,
    };
    use crate::config::{PhenoXtractorConfig, PipelineConfig};
    use crate::extract::csv_data_source::CSVDataSource;
    use crate::extract::data_source::DataSource;
    use crate::extract::excel_data_source::ExcelDatasource;
    use crate::extract::extraction_config::ExtractionConfig;
    use crate::ontology::OntologyRef;
    use crate::test_suite::config::get_full_config_bytes;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};
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
    pipeline:
      transform_strategies:
        - "alias_map"
        - "multi_hpo_col_expansion"
      loader:
        file_system:
            output_dir: "some/dir"
            create_dir: true
      meta_data:
        created_by: Rouven Reuter
        submitted_by: Magnus Knut Hansen
        cohort_name: "Arkham Asylum 2025"
        hp_ref:
          version: "2025-09-01"
          prefix_id: "HP"
    "#;

    const TOML_DATA: &[u8] = br#"
[[data_sources]]
type = "csv"
source = "test/path"
separator = ","

[data_sources.extraction_config]
name = "test_config"
has_headers = true
patients_are_rows = true

[data_sources.context]
name = "test_table"

[pipeline]
transform_strategies = [
    "alias_map",
    "multi_hpo_col_expansion"
]

[pipeline.loader.file_system]
output_dir = "some/dir"
create_dir = true

[pipeline.meta_data]
created_by = "Rouven Reuter"
submitted_by = "Magnus Knut Hansen"
cohort_name = "Arkham Asylum 2025"

[pipeline.meta_data.hp_ref]
version = "2025-09-01"
prefix_id = "HP"
    "#;

    const JSON_DATA: &[u8] = br#"
{
  "data_sources": [
    {
      "type": "csv",
      "source": "test/path",
      "separator": ",",
      "extraction_config": {
        "name": "test_config",
        "has_headers": true,
        "patients_are_rows": true
      },
      "context": {
        "name": "test_table"
      }
    }
  ],
  "pipeline": {
    "transform_strategies": [
      "alias_map",
      "multi_hpo_col_expansion"
    ],
    "loader": {
      "file_system": {
        "output_dir": "some/dir",
        "create_dir": true
      }
    },
    "meta_data": {
      "created_by": "Rouven Reuter",
      "submitted_by": "Magnus Knut Hansen",
      "cohort_name": "Arkham Asylum 2025",
      "hp_ref": {
        "version": "2025-09-01",
        "prefix_id": "HP"
      }
    }
  }
}
    "#;

    const RON_DATA: &[u8] = br#"
(
  data_sources: [
    (
      type: "csv",
      source: "test/path",
      separator: ",",
      extraction_config: (
        name: "test_config",
        has_headers: true,
        patients_are_rows: true,
      ),
      context: (
        name: "test_table",
      ),
    ),
  ],
  pipeline: (
    transform_strategies: [
      "alias_map",
      "multi_hpo_col_expansion",
    ],
    loader: (
      file_system: (
        output_dir: "some/dir",
        create_dir: true,
      ),
    ),
    meta_data: (
      created_by: "Rouven Reuter",
      submitted_by: "Magnus Knut Hansen",
      cohort_name: "Arkham Asylum 2025",
      hp_ref: (
        version: "2025-09-01",
        prefix_id: "HP",
      ),
    ),
  ),
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

        let mut phenoxtractor_config: PhenoXtractorConfig = ConfigLoader::load(file_path).unwrap();
        let source = phenoxtractor_config.data_sources.pop().unwrap();

        match source {
            DataSource::Csv(data) => {
                assert_eq!(data.separator, Some(','));
                assert_eq!(data.context.name(), "test_table");
                assert_eq!(data.source.to_str().unwrap(), "test/path");
            }
            _ => panic!("Wrong data source type. Expected Csv."),
        }
    }

    #[rstest]
    fn test_load_config_unsupported_file_format() {
        let file_path = PathBuf::from_str("test/path/config.exe").unwrap();
        let err: Result<PhenoXtractorConfig, _> = ConfigLoader::load(file_path);
        assert!(err.is_err());
    }

    #[rstest]
    fn test_load_complete_config(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(get_full_config_bytes().as_slice()).unwrap();
        let config: PhenoXtractorConfig = ConfigLoader::load(file_path).unwrap();

        let expected_config = PhenoXtractorConfig {
            pipeline: PipelineConfig::new(
                MetaData::new(
                    Some("Rouven Reuter"),
                    Some("Magnus Knut Hansen"),
                    "Arkham Asylum 2025",
                    Some(&OntologyRef::hp_with_version("2025-09-01")),
                    Some(&OntologyRef::mondo_with_version("2025-11-04")),
                    Some(&OntologyRef::geno()),
                ),
                vec![
                    StrategyConfig::AliasMap,
                    StrategyConfig::MultiHpoColExpansion,
                ],
                LoaderConfig::FileSystem {
                    output_dir: PathBuf::from("some/dir"),
                    create_dir: true,
                },
            ),
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
                    context: TableContext::new(
                        "TestTable".to_string(),
                        vec![SeriesContext::new(
                            Identifier::Regex("patient_id".to_string()),
                            Context::SubjectId,
                            Context::HpoLabelOrId,
                            Some(CellValue::String("Zollinger-Ellison syndrome".to_string())),
                            Some(AliasMap::new(
                                HashMap::from([
                                    ("null".to_string(), None),
                                    ("M".to_string(), Some("Male".to_string())),
                                    ("102".to_string(), Some("High quantity".to_string())),
                                    ("169.5".to_string(), Some("Very high quantity".to_string())),
                                    ("true".to_string(), Some("smoker".to_string())),
                                ]),
                                OutputDataType::String,
                            )),
                            Some("block_1".to_string()),
                        )],
                    ),
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
                        TableContext::new(
                            "Sheet1".to_string(),
                            vec![SeriesContext::new(
                                Identifier::Regex("lab_result_.*".to_string()),
                                Context::SubjectId,
                                Context::HpoLabelOrId,
                                Some(CellValue::String("Zollinger-Ellison syndrome".to_string())),
                                Some(AliasMap::new(
                                    HashMap::from([
                                        ("neoplasma".to_string(), Some("4".to_string())),
                                        ("height".to_string(), Some("1.85".to_string())),
                                    ]),
                                    OutputDataType::Float64,
                                )),
                                None,
                            )],
                        ),
                        // Context for "Sheet2"
                        TableContext::new(
                            "Sheet2".to_string(),
                            vec![SeriesContext::new(
                                Identifier::Multi(vec![
                                    "Col_1".to_string(),
                                    "Col_2".to_string(),
                                    "Col_3".to_string(),
                                ]),
                                Context::SubjectId,
                                Context::HpoLabelOrId,
                                Some(CellValue::String("Zollinger-Ellison syndrome".to_string())),
                                Some(AliasMap::new(
                                    HashMap::from([(
                                        "smoker".to_string(),
                                        Some("true".to_string()),
                                    )]),
                                    OutputDataType::Boolean,
                                )),
                                None,
                            )],
                        ),
                    ],
                }),
            ],
        };

        assert_eq!(config, expected_config);
    }
}
