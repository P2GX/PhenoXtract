use crate::config::phenoxtracter_config::PhenoXtractorConfig;
use config::{Config, ConfigError, File, FileFormat};
use std::path::PathBuf;

fn load_config(path_buf: PathBuf) -> Result<PhenoXtractorConfig, ConfigError> {
    if let Some(ext) = path_buf.extension() {
        let file_format = match ext.to_str() {
            Some("yaml") => Ok(FileFormat::Yaml),
            Some("yml") => Ok(FileFormat::Yaml),
            Some("json") => Ok(FileFormat::Json),
            Some("toml") => Ok(FileFormat::Toml),
            Some("ron") => Ok(FileFormat::Ron),
            _ => Err(ConfigError::NotFound(format!(
                "File format not supported. yaml, json, toml or ron are supported. {path_buf:?}"
            ))),
        }?;

        let settings = Config::builder()
            .add_source(File::new(path_buf.to_str().unwrap(), file_format))
            .build()?;
        let settings_struct: PhenoXtractorConfig = settings.try_deserialize()?;
        Ok(settings_struct)
    } else {
        Err(ConfigError::NotFound(format!(
            "Could not find file extension on path {path_buf:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::data_source::DataSource;
    use rstest::*;
    use std::fs::File as StdFile;
    use std::io::Write;
    use tempfile::TempDir;

    const YAML_DATA: &[u8] = br#"
    data_sources:
      - type: "csv"
        source: "test/path"
        separator: ","
        table:
          name: "test_table"

    meta_data:
      creator: Rouven Reuter
      submitted_by: Magnus Knut Hansen
    "#;

    const TOML_DATA: &[u8] = br#"
    [meta_data]
    creator = "Rouven Reuter"
    submitted_by = "Magnus Knut Hansen"

    [[data_sources]]
    type = "csv"
    source = "test/path"
    separator = ","
    table = { name = "test_table" }
    "#;

    const JSON_DATA: &[u8] = br#"
    {
      "meta_data": {
        "creator": "Rouven Reuter",
        "submitted_by": "Magnus Knut Hansen"
      },
      "data_sources": [
        {
          "type": "csv",
          "source": "test/path",
          "separator": ",",
          "table": {
            "name": "test_table"
          }
        }
      ]
    }
    "#;

    const RON_DATA: &[u8] = br#"
( // Represents the top-level config struct
    meta_data: (
        creator: "Rouven Reuter",
        submitted_by: "Magnus Knut Hansen",
    ),
    data_sources: [ // A list of data sources
        ( // The Csv data source variant
            type: "csv",
            source: "test/path",
            separator: ",",
            table: (
                name: "test_table",
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
        temp_dir: TempDir, // Injects the temp_dir fixture
        #[case] extension: &str,
        #[case] data: &[u8],
    ) {
        // --- Setup: Create the config file for the current case ---
        let file_path = temp_dir.path().join(format!("config.{}", extension));
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(data).unwrap();

        // --- Test Logic: This part is now reused for every format ---
        let mut phenoxtractor_config = load_config(file_path).unwrap();
        let source = phenoxtractor_config.data_sources.pop().unwrap();

        match source {
            DataSource::Csv(data) => {
                assert_eq!(data.separator.unwrap(), ",");
                assert_eq!(data.table.name, "test_table");
                assert_eq!(data.source.to_str().unwrap(), "test/path");
            }
            _ => panic!("Wrong data source type. Expected Csv."),
        }
    }
}
