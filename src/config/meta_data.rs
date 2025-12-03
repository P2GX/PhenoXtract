use crate::ontology::OntologyRef;
use serde::{Deserialize, Serialize};

/// Holds all shared metadata for the phenopackets
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct MetaData {
    pub cohort_name: String,
    #[serde(default = "default_creator")]
    pub created_by: String,
    #[serde(default = "default_creator")]
    pub submitted_by: String,
    #[serde(default = "crate::ontology::OntologyRef::hp")]
    pub hp_ref: OntologyRef,
    #[serde(default = "crate::ontology::OntologyRef::mondo")]
    pub mondo_ref: OntologyRef,
    #[serde(default = "crate::ontology::OntologyRef::geno")]
    pub geno_ref: OntologyRef,
}
impl MetaData {
    pub fn new(
        created_by: Option<&str>,
        submitted_by: Option<&str>,
        cohort_name: &str,
        hpo_version: Option<&OntologyRef>,
        mondo_version: Option<&OntologyRef>,
        geno_version: Option<&OntologyRef>,
    ) -> Self {
        Self {
            created_by: match created_by {
                None => default_creator(),
                Some(s) => s.to_owned(),
            },
            submitted_by: match submitted_by {
                None => default_creator(),
                Some(s) => s.to_owned(),
            },
            cohort_name: cohort_name.to_owned(),
            hp_ref: match hpo_version {
                None => OntologyRef::hp(),
                Some(s) => s.to_owned(),
            },
            mondo_ref: match mondo_version {
                None => OntologyRef::mondo(),
                Some(s) => s.to_owned(),
            },
            geno_ref: match geno_version {
                None => OntologyRef::geno(),
                Some(s) => s.to_owned(),
            },
        }
    }
}

impl Default for MetaData {
    fn default() -> MetaData {
        MetaData {
            created_by: default_creator(),
            submitted_by: default_creator(),
            cohort_name: "unnamed_cohort".to_string(),
            hp_ref: OntologyRef::hp(),
            mondo_ref: OntologyRef::mondo(),
            geno_ref: OntologyRef::geno(),
        }
    }
}

fn default_creator() -> String {
    let version_number = env!("CARGO_PKG_VERSION");
    let package_name = env!("CARGO_PKG_NAME");

    format!("{package_name}-{version_number}")
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
        let creator = default_creator;
        assert!(creator.contains("phenoxtract"));
    }

    #[rstest]
    fn test_metadata_default_values() {
        let expected_cohort = "unnamed_cohort".to_string();

        let expected_creator = format!("{}-{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

        let metadata = MetaData::default();

        assert_eq!(metadata.created_by, expected_creator);
        assert_eq!(metadata.submitted_by, expected_creator);
        assert_eq!(metadata.cohort_name, expected_cohort);
        assert_eq!(metadata.hp_ref, OntologyRef::hp());
        assert_eq!(metadata.mondo_ref, OntologyRef::mondo());
        assert_eq!(metadata.geno_ref, OntologyRef::geno());
    }

    const YAML_DATA: &[u8] = br#"
    submitted_by: Magnus Knut Hansen
    cohort_name: arkham 2025
    hp_ref:
      version: "2025-09-01"
      prefix_id: "HP"
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

        let creator = default_meta_data.created_by;
        assert!(creator.contains("phenoxtract"));
        assert_eq!(
            default_meta_data.submitted_by,
            "Magnus Knut Hansen".to_string()
        );
        assert_eq!(default_meta_data.cohort_name, "arkham 2025");
        assert_eq!(
            default_meta_data.hp_ref,
            OntologyRef::hp_with_version("2025-09-01")
        );
        assert_eq!(default_meta_data.mondo_ref, OntologyRef::mondo());
        assert_eq!(default_meta_data.geno_ref, OntologyRef::geno());
    }
}
