use std::collections::HashMap;
use std::env::home_dir;
use std::fs;
use std::path::{Path, PathBuf};
use directories::ProjectDirs;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::Phenopacket;
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use rstest::fixture;
use serde_json::Value;
use tempfile::TempDir;
use phenoxtract::config::table_context::{AliasMap, OutputDataType};
use phenoxtract::ontology::error::RegistryError;

#[fixture]
pub fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temporary directory")
}

#[fixture]
pub fn cohort_name() -> String {
    "my_cohort".to_string()
}

#[fixture]
pub fn vital_status_aliases() -> AliasMap {
    let mut vs_hash_map: HashMap<String, Option<String>> = HashMap::default();
    vs_hash_map.insert("Yes".to_string(), Some("ALIVE".to_string()));
    vs_hash_map.insert("No".to_string(), Some("DECEASED".to_string()));
    AliasMap::new(vs_hash_map, OutputDataType::String)
}

#[fixture]
pub fn no_info_alias() -> AliasMap {
    let mut no_info_hash_map: HashMap<String, Option<String>> = HashMap::default();
    no_info_hash_map.insert("no_info".to_string(), None);
    AliasMap::new(no_info_hash_map, OutputDataType::String)
}

pub fn build_hgnc_test_client(temp_dir: &Path) -> CachedHGNCClient {
    CachedHGNCClient::new(temp_dir.join("test_hgnc_cache"), HGNCClient::default()).unwrap()
}

pub fn build_hgvs_test_client(temp_dir: &Path) -> CachedHGVSClient {
    CachedHGVSClient::new(temp_dir.join("test_hgvs_cache"), HGVSClient::default()).unwrap()
}

pub fn assert_phenopackets(actual: &mut Phenopacket, expected: &mut Phenopacket) {
    remove_created_from_metadata(actual);
    remove_created_from_metadata(expected);

    remove_id_from_variation_descriptor(actual);
    remove_id_from_variation_descriptor(expected);

    remove_version_from_loinc(actual);
    remove_version_from_loinc(expected);

    pretty_assertions::assert_eq!(actual, expected);
}

pub fn remove_created_from_metadata(pp: &mut Phenopacket) {
    if let Some(meta) = &mut pp.meta_data {
        meta.created = None;
    }
}

pub fn remove_id_from_variation_descriptor(pp: &mut Phenopacket) {
    for interpretation in pp.interpretations.iter_mut() {
        if let Some(diagnosis) = &mut interpretation.diagnosis {
            for gi in diagnosis.genomic_interpretations.iter_mut() {
                if let Some(call) = &mut gi.call
                    && let Call::VariantInterpretation(vi) = call
                    && let Some(vi) = &mut vi.variation_descriptor
                {
                    vi.id = "TEST_ID".to_string();
                }
            }
        }
    }
}

pub fn remove_version_from_loinc(pp: &mut Phenopacket) {
    if let Some(metadata) = &mut pp.meta_data {
        let loinc_resource = metadata
            .resources
            .iter_mut()
            .find(|resource| resource.id == "loinc");

        if let Some(loinc_resource) = loinc_resource {
            loinc_resource.version = "-".to_string()
        }
    }
}

// We remove the survival time in the loader. However, the Phenopacket struct can not be constructed if that field is missing.
pub fn ensure_survival_time(pp: &mut Value) {
    #[allow(clippy::collapsible_if)]
    if let Some(individual) = pp.get_mut("subject") {
        if let Some(vital_status_value) = individual.get_mut("vitalStatus") {
            if let Some(vital_status) = vital_status_value.as_object_mut() {
                if vital_status.get("survivalTimeInDays").is_none() {
                    vital_status.insert("survivalTimeInDays".to_string(), Value::Number(0.into()));
                }
            }
        }
    }
}

pub fn load_phenopacket(path: PathBuf) -> Phenopacket {
    let data = fs::read_to_string(path).unwrap();
    let mut expected_pp: Value = serde_json::from_str(&data).unwrap();

    ensure_survival_time(&mut expected_pp);

    serde_json::from_value::<Phenopacket>(expected_pp).unwrap()
}

pub fn ontology_registry_dir() -> Result<PathBuf, RegistryError> {
    let pkg_name = env!("CARGO_PKG_NAME");

    let phenox_cache_dir = if let Some(project_dir) = ProjectDirs::from("", "", pkg_name) {
        project_dir.cache_dir().to_path_buf()
    } else if let Some(home_dir) = home_dir() {
        home_dir.join(pkg_name)
    } else {
        return Err(RegistryError::CantEstablishRegistryDir);
    };

    if !phenox_cache_dir.exists() {
        fs::create_dir_all(&phenox_cache_dir)?;
    }

    let ontology_registry_dir = phenox_cache_dir.join("ontology_registry");

    if !ontology_registry_dir.exists() {
        fs::create_dir_all(&ontology_registry_dir)?;
    }
    Ok(ontology_registry_dir.to_owned())
}