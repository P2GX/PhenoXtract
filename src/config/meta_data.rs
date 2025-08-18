use serde::{Deserialize, Serialize};

/// Holds all shared meta data for the phenopackets produced by the pipeline
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct MetaData {
    #[allow(unused)]
    #[serde(default = "default_creator")]
    created_by: Option<String>,
    #[allow(unused)]
    submitted_by: String,
}

impl Default for MetaData {
    fn default() -> MetaData {
        MetaData {
            created_by: default_creator(),
            submitted_by: "".to_string(),
        }
    }
}

fn default_creator() -> Option<String> {
    let version_number = env!("CARGO_PKG_VERSION");
    let package_name = env!("CARGO_PKG_NAME");

    Some(format!("{package_name}-{version_number}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{Config, File, FileFormat};
    use rstest::{fixture, rstest};
    use std::fs::File as StdFile;
    use std::io::Write;
    use tempfile::TempDir;
    #[rstest]
    fn test_default_creator() {
        let default_creator = default_creator();
        assert!(default_creator.is_some());
        let creator = default_creator.unwrap();
        assert!(creator.contains("phenoxtrackt"));
    }

    #[rstest]
    fn test_meta_data_default() {
        let default_meta_data = MetaData::default();
        assert!(default_meta_data.created_by.is_some());
        let creator = default_meta_data.created_by.unwrap();
        assert!(creator.contains("phenoxtrackt"));
    }

    const YAML_DATA: &[u8] = br#"
    submitted_by: Magnus Knut Hansen
    "#;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_meta_data_default_from_config(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("meta_data.yaml");
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(YAML_DATA).unwrap();

        let raw_data = Config::builder()
            .add_source(File::new(file_path.to_str().unwrap(), FileFormat::Yaml))
            .build()
            .unwrap();
        let default_meta_data: MetaData = raw_data.try_deserialize().unwrap();

        assert!(default_meta_data.created_by.is_some());
        let creator = default_meta_data.created_by.unwrap();
        assert!(creator.contains("phenoxtrackt"));
        assert_eq!(default_meta_data.submitted_by, "Magnus Knut Hansen");
    }
}
