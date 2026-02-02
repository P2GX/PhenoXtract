use phenopackets::schema::v2::Phenopacket;
use phenoxtract::Pipeline;
use phenoxtract::config::context::{Context, ContextKind};
use phenoxtract::config::table_context::{
    AliasMap, Identifier, OutputDataType, SeriesContext, TableContext,
};
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::resource_references::ResourceRef;

use directories::ProjectDirs;
use dotenvy::dotenv;
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use phenoxtract::ontology::CachedOntologyFactory;
use phenoxtract::ontology::error::RegistryError;
use phenoxtract::ontology::loinc_client::LoincClient;
use phenoxtract::transform::bidict_library::BiDictLibrary;
use phenoxtract::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use phenoxtract::transform::phenopacket_builder::BuilderMetaData;
use phenoxtract::transform::strategies::traits::Strategy;
use phenoxtract::transform::strategies::{AgeToIso8601Strategy, MappingStrategy};
use phenoxtract::transform::strategies::{AliasMapStrategy, MultiHPOColExpansionStrategy};
use phenoxtract::transform::strategies::{DateToAgeStrategy, OntologyNormaliserStrategy};
use phenoxtract::transform::{PhenopacketBuilder, TransformerModule};
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use rstest::{fixture, rstest};
use std::collections::HashMap;
use std::env::home_dir;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::{env, fs};
use tempfile::TempDir;
use integration_tests::cohort_name;

#[fixture]
fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temporary directory")
}

#[fixture]
fn vital_status_aliases() -> AliasMap {
    let mut vs_hash_map: HashMap<String, Option<String>> = HashMap::default();
    vs_hash_map.insert("Yes".to_string(), Some("ALIVE".to_string()));
    vs_hash_map.insert("No".to_string(), Some("DECEASED".to_string()));
    AliasMap::new(vs_hash_map, OutputDataType::String)
}

#[fixture]
fn csv_context(vital_status_aliases: AliasMap) -> TableContext {
    TableContext::new(
        "CSV_Table".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("patient_id".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_of_birth".to_string()))
                .with_data_context(Context::DateOfBirth),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("age_at_last_encounter".to_string()))
                .with_data_context(Context::AgeAtLastEncounter),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_at_last_encounter".to_string()))
                .with_data_context(Context::DateAtLastEncounter),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("sex".to_string()))
                .with_data_context(Context::SubjectSex),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("vital_status".to_string()))
                .with_data_context(Context::VitalStatus)
                .with_alias_map(Some(vital_status_aliases)),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("age_of_death".to_string()))
                .with_data_context(Context::AgeOfDeath),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_of_death".to_string()))
                .with_data_context(Context::DateOfDeath),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("cause_of_death".to_string()))
                .with_data_context(Context::CauseOfDeath),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("survival_time_in_days".to_string()))
                .with_data_context(Context::SurvivalTimeDays),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("phenotype".to_string()))
                .with_data_context(Context::HpoLabelOrId)
                .with_building_block_id(Some("P1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("multi_hpo".to_string()))
                .with_data_context(Context::MultiHpoId)
                .with_building_block_id(Some("P1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("phenotype_onset_age".to_string()))
                .with_data_context(Context::OnsetAge)
                .with_building_block_id(Some("P1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HP:1234567".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("P2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HP:1234567_onset_date".to_string()))
                .with_data_context(Context::OnsetDate)
                .with_building_block_id(Some("P2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease".to_string()))
                .with_data_context(Context::DiseaseLabelOrId)
                .with_building_block_id(Some("D1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease_onset_age".to_string()))
                .with_data_context(Context::OnsetAge)
                .with_building_block_id(Some("D1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("gene".to_string()))
                .with_data_context(Context::HgncSymbolOrId)
                .with_building_block_id(Some("D1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("hgvs1".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("D1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("hgvs2".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("D1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease2".to_string()))
                .with_data_context(Context::DiseaseLabelOrId)
                .with_building_block_id(Some("D2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease2_onset_date".to_string()))
                .with_data_context(Context::OnsetDate)
                .with_building_block_id(Some("D2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("body_height_cm".to_string()))
                .with_data_context(Context::QuantitativeMeasurement {
                    assay_id: "LOINC:8302-2".to_string(),
                    unit_ontology_id: "UO:0000015".to_string(),
                })
                .with_building_block_id(Some("QUANT_M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex(
                    "body_height_cm_measurement_age".to_string(),
                ))
                .with_data_context(Context::OnsetAge)
                .with_building_block_id(Some("QUANT_M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("nitrate_presence".to_string()))
                .with_data_context(Context::QualitativeMeasurement {
                    assay_id: "LOINC:5802-4".to_string(),
                })
                .with_building_block_id(Some("QUAL_M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex(
                    "nitrate_presence_measurement_date".to_string(),
                ))
                .with_data_context(Context::OnsetDate)
                .with_building_block_id(Some("QUAL_M".to_string())),
        ],
    )
}

fn build_hgnc_test_client(temp_dir: &Path) -> CachedHGNCClient {
    CachedHGNCClient::new(temp_dir.join("test_hgnc_cache"), HGNCClient::default()).unwrap()
}

fn build_hgvs_test_client(temp_dir: &Path) -> CachedHGVSClient {
    CachedHGVSClient::new(temp_dir.join("test_hgvs_cache"), HGVSClient::default()).unwrap()
}

fn assert_phenopackets(actual: &mut Phenopacket, expected: &mut Phenopacket) {
    remove_created_from_metadata(actual);
    remove_created_from_metadata(expected);
    pretty_assertions::assert_eq!(actual, expected);
}

fn remove_created_from_metadata(pp: &mut Phenopacket) {
    if let Some(meta) = &mut pp.meta_data {
        meta.created = None;
    }
}

fn ontology_registry_dir() -> Result<PathBuf, RegistryError> {
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

#[rstest]
fn big_null_test(csv_context: TableContext, temp_dir: TempDir, cohort_name: String,) {
    //Set-up

    let mut onto_factory = CachedOntologyFactory::new(Box::new(FileSystemOntologyRegistry::new(
        ontology_registry_dir().expect("ontology_registry_dir could not be created"),
        BioRegistryMetadataProvider::default(),
        OboLibraryProvider::default(),
    )));

    let pato_ref = ResourceRef::pato().with_version("2025-05-14");
    let pato_dict = Box::new(onto_factory.build_bidict(&pato_ref, None).unwrap());

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets/big_null_test");

    //Configure data sources and contexts
    let csv_path = assets_dir.clone().join("input_data/data.csv");

    let mut data_sources = [DataSource::Csv(CSVDataSource::new(
        csv_path,
        None,
        csv_context,
        ExtractionConfig::new("CSV_Table".to_string(), true, true),
    ))];

    //Configure strategies (a.k.a. transformations)
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(OntologyNormaliserStrategy::new(
            onto_factory.build_bidict(&pato_ref, None).unwrap(),
            ContextKind::QualitativeMeasurement,
        )),
        Box::new(DateToAgeStrategy),
        Box::new(MappingStrategy::default_sex_mapping_strategy()),
        Box::new(AgeToIso8601Strategy::default()),
        Box::new(MultiHPOColExpansionStrategy),
    ];

    //Create the pipeline

    // load variables in .env into environment. This is needed for the default LoincCredentials.
    dotenv().ok();

    let phenopacket_builder = PhenopacketBuilder::new(
        BuilderMetaData::new(cohort_name, "Big Null Test", "Someone"),
        Box::new(build_hgnc_test_client(temp_dir.path())),
        Box::new(build_hgvs_test_client(temp_dir.path())),
        BiDictLibrary::new("HPO", vec![]),
        BiDictLibrary::new("DISEASE", vec![]),
        BiDictLibrary::new("UNIT", vec![]),
        BiDictLibrary::new("ASSAY", vec![Box::new(LoincClient::default())]),
        BiDictLibrary::new("QUAL", vec![pato_dict]),
    );

    let transformer_module = TransformerModule::new(
        strategies,
        CdfCollectorBroker::with_default_collectors(phenopacket_builder),
    );

    let output_dir = assets_dir.join("output_phenopackets");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = Box::new(FileSystemLoader::new(output_dir.clone(), true));

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //Run the pipeline on the data sources
    pipeline.run(&mut data_sources).expect("Pipeline failed");

    //create a phenopacket_ID -> expected phenopacket HashMap
    let expected_phenopackets_files = fs::read_dir(assets_dir.join("expected_phenopackets"))
        .expect("Could not find expected_phenopackets dir");

    let mut expected_phenopackets: HashMap<String, Phenopacket> = HashMap::new();
    for expected_pp_file in expected_phenopackets_files {
        let data = fs::read_to_string(expected_pp_file.unwrap().path())
            .expect("Could not find expected_phenopackets file");
        let expected_pp: Phenopacket =
            serde_json::from_str(&data).expect("Could not load expected phenopacket");

        expected_phenopackets.insert(expected_pp.id.clone(), expected_pp);
    }

    //go through the extracted phenopackets and assert equality with the corresponding expected phenopacket
    for extracted_pp_file in fs::read_dir(output_dir).unwrap() {
        if let Ok(extracted_pp_file) = extracted_pp_file
            && extracted_pp_file.path().extension() == Some(OsStr::new("json"))
        {
            let data = fs::read_to_string(extracted_pp_file.path()).unwrap();
            let mut extracted_pp: Phenopacket = serde_json::from_str(&data).unwrap();

            let expected_pp = expected_phenopackets.get_mut(&extracted_pp.id).unwrap();

            assert_phenopackets(&mut extracted_pp, expected_pp);
        }
    }
}
