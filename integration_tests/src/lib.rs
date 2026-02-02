use directories::ProjectDirs;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenoxtract::config::table_context::{AliasMap, OutputDataType};
use phenoxtract::ontology::error::RegistryError;
use phenoxtract::ontology::resource_references::ResourceRef;
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use rstest::fixture;
use serde_json::Value;
use std::collections::HashMap;
use std::env::home_dir;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[fixture]
pub fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temporary directory")
}

#[fixture]
pub fn cohort_name() -> String {
    "my_cohort".to_string()
}

#[fixture]
pub fn hp_ref() -> ResourceRef {
    ResourceRef::hp().with_version("2025-09-01")
}

#[fixture]
pub fn mondo_ref() -> ResourceRef {
    ResourceRef::mondo().with_version("2026-01-06")
}

#[fixture]
pub fn uo_ref() -> ResourceRef {
    ResourceRef::uo().with_version("2026-01-09")
}

#[fixture]
pub fn pato_ref() -> ResourceRef {
    ResourceRef::pato().with_version("2025-05-14")
}

#[fixture]
pub fn tests_assets() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets")
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

fn assert_phenopackets(actual: &mut Phenopacket, expected: &mut Phenopacket) {
    remove_created_from_metadata(actual);
    remove_created_from_metadata(expected);

    remove_id_from_variation_descriptor(actual);
    remove_id_from_variation_descriptor(expected);

    remove_version_from_loinc(actual);
    remove_version_from_loinc(expected);

    pretty_assertions::assert_eq!(actual, expected);
}

fn remove_created_from_metadata(pp: &mut Phenopacket) {
    if let Some(meta) = &mut pp.meta_data {
        meta.created = None;
    }
}

fn remove_id_from_variation_descriptor(pp: &mut Phenopacket) {
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

fn remove_version_from_loinc(pp: &mut Phenopacket) {
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
fn ensure_survival_time(pp: &mut Value) {
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

fn load_phenopacket(path: PathBuf) -> Phenopacket {
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

pub fn compare_expected_and_extracted_phenopackets(
    assets_dir: PathBuf,
    output_dir_name: &str,
    expected_dir_name: &str,
) {
    let mut expected_phenopackets: HashMap<String, Phenopacket> =
        fs::read_dir(assets_dir.join(expected_dir_name))
            .unwrap()
            .map(|entry| {
                let phenopacket = load_phenopacket(entry.unwrap().path());
                (phenopacket.id.clone(), phenopacket)
            })
            .collect();

    let output_dir = assets_dir.join(output_dir_name);
    for extracted_pp_file in fs::read_dir(output_dir).unwrap() {
        if let Ok(extracted_pp_file) = extracted_pp_file
            && extracted_pp_file.path().extension() == Some(OsStr::new("json"))
        {
            let mut extracted_pp = load_phenopacket(extracted_pp_file.path());
            let expected_pp = expected_phenopackets.get_mut(&extracted_pp.id).unwrap();

            assert_phenopackets(&mut extracted_pp, expected_pp);
        }
    }
}