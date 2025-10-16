use serde::{Deserialize, Serialize};

/// Holds all shared metadata for the phenopackets
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct MetaData {
    #[allow(unused)]
    #[serde(default = "default_creator")]
    pub created_by: Option<String>,
    #[allow(unused)]
    pub submitted_by: String,
    pub cohort_name: String,
    #[serde(default = "default_ontology_version")]
    pub hpo_version: String,
    #[serde(default = "default_ontology_version")]
    pub mondo_version: String,
    #[serde(default = "default_ontology_version")]
    pub geno_version: String,
}

impl Default for MetaData {
    fn default() -> MetaData {
        MetaData {
            created_by: default_creator(),
            submitted_by: env!("CARGO_PKG_NAME").to_string(),
            cohort_name: "unnamed_cohort".to_string(),
            hpo_version: default_ontology_version(),
            mondo_version: default_ontology_version(),
            geno_version: default_ontology_version(),
        }
    }
}

fn default_creator() -> Option<String> {
    let version_number = env!("CARGO_PKG_VERSION");
    let package_name = env!("CARGO_PKG_NAME");

    Some(format!("{package_name}-{version_number}"))
}

fn default_ontology_version() -> String {
    "latest".to_string()
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
        assert!(creator.contains("phenoxtract"));
    }

    #[rstest]
    fn test_default_ontology_version() {
        let default_creator = default_ontology_version();
        assert_eq!(default_creator, "latest");
    }

    #[rstest]
    fn test_metadata_default_values() {
        let expected_cohort = "unnamed_cohort".to_string();
        let expected_ontology_version = default_ontology_version();

        let expected_package_name = env!("CARGO_PKG_NAME").to_string();
        let expected_creator = Some(format!(
            "{}-{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ));

        let metadata = MetaData::default();

        assert_eq!(metadata.created_by, expected_creator);
        assert_eq!(metadata.submitted_by, expected_package_name);
        assert_eq!(metadata.cohort_name, expected_cohort);
        assert_eq!(metadata.hpo_version, expected_ontology_version);
        assert_eq!(metadata.mondo_version, expected_ontology_version);
        assert_eq!(metadata.geno_version, expected_ontology_version);
    }

    const YAML_DATA: &[u8] = br#"
    submitted_by: Magnus Knut Hansen
    cohort_name: arkham 2025
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
        assert!(creator.contains("phenoxtract"));
        assert_eq!(default_meta_data.submitted_by, "Magnus Knut Hansen");
        assert_eq!(default_meta_data.cohort_name, "arkham 2025");
        assert_eq!(default_meta_data.hpo_version, default_ontology_version());
        assert_eq!(default_meta_data.mondo_version, default_ontology_version());
        assert_eq!(default_meta_data.geno_version, default_ontology_version());
    }
}
