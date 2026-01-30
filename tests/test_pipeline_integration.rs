use phenopackets::schema::v2::Phenopacket;
use phenoxtract::Pipeline;
use phenoxtract::config::context::{Context, ContextKind};
use phenoxtract::config::table_context::{
    AliasMap, Identifier, OutputDataType, SeriesContext, TableContext,
};
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::resource_references::ResourceRef;

use directories::ProjectDirs;
use dotenvy::dotenv;
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
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
fn no_info_alias() -> AliasMap {
    let mut no_info_hash_map: HashMap<String, Option<String>> = HashMap::default();
    no_info_hash_map.insert("no_info".to_string(), None);
    AliasMap::new(no_info_hash_map, OutputDataType::String)
}

#[fixture]
fn csv_context(no_info_alias: AliasMap) -> TableContext {
    TableContext::new(
        "CSV_Table".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("0".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Multi(vec!["1".to_string(), "2".to_string()]))
                .with_data_context(Context::HpoLabelOrId)
                .with_alias_map(Some(no_info_alias)),
        ],
    )
}

#[fixture]
fn csv_context_2() -> TableContext {
    TableContext::new(
        "CSV_Table_2".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Patient ID".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HP:0012373".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Rhinorrhea".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Date of onset".to_string()))
                .with_data_context(Context::OnsetDate)
                .with_building_block_id(Some("A".to_string())),
        ],
    )
}

#[fixture]
fn csv_context_3() -> TableContext {
    TableContext::new(
        "CSV_Table_3".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Patient ID".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HPOs".to_string()))
                .with_data_context(Context::MultiHpoId)
                .with_building_block_id(Some("B".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Date of onset".to_string()))
                .with_data_context(Context::OnsetDate)
                .with_building_block_id(Some("B".to_string())),
        ],
    )
}

#[fixture]
fn csv_context_4() -> TableContext {
    TableContext::new(
        "CSV_Table_4".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Patient ID".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("diseases".to_string()))
                .with_data_context(Context::DiseaseLabelOrId)
                .with_building_block_id(Some("C".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease_onset".to_string()))
                .with_data_context(Context::OnsetAge)
                .with_building_block_id(Some("C".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("gene".to_string()))
                .with_data_context(Context::HgncSymbolOrId)
                .with_building_block_id(Some("C".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("hgvs1".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("C".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("hgvs2".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("C".to_string())),
        ],
    )
}

#[fixture]
fn csv_context_5() -> TableContext {
    TableContext::new(
        "CSV_Table_5".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("Patient ID".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("height (cm)".to_string()))
                .with_data_context(Context::QuantitativeMeasurement {
                    assay_id: "LOINC:8302-2".to_string(),
                    unit_ontology_id: "UO:0000015".to_string(),
                })
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("ref_low".to_string()))
                .with_data_context(Context::ReferenceRangeLow)
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("ref_high".to_string()))
                .with_data_context(Context::ReferenceRangeHigh)
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("nitrates in urine".to_string()))
                .with_data_context(Context::QualitativeMeasurement {
                    assay_id: "LOINC:5802-4".to_string(),
                })
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_of_observation".to_string()))
                .with_data_context(Context::OnsetDate)
                .with_building_block_id(Some("M".to_string())),
        ],
    )
}

#[fixture]
fn excel_context(vital_status_aliases: AliasMap) -> Vec<TableContext> {
    vec![
        TableContext::new(
            "basic info".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Patient ID".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Sex".to_string()))
                    .with_data_context(Context::SubjectSex),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Living".to_string()))
                    .with_data_context(Context::VitalStatus)
                    .with_alias_map(Some(vital_status_aliases)),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("DOB".to_string()))
                    .with_data_context(Context::DateOfBirth),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Age of death".to_string()))
                    .with_data_context(Context::AgeOfDeath),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex(
                        "Survival time since diagnosis (days)".to_string(),
                    ))
                    .with_data_context(Context::SurvivalTimeDays),
            ],
        ),
        TableContext::new(
            "conditions".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Patient ID".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Phenotypic Features".to_string()))
                    .with_data_context(Context::HpoLabelOrId)
                    .with_building_block_id(Some("C".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Age of onset".to_string()))
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("C".to_string())),
            ],
        ),
        TableContext::new(
            "more conditions".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Patient ID".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex(r"Phenotypic Features \d+".to_string()))
                    .with_data_context(Context::HpoLabelOrId),
            ],
        ),
    ]
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
fn test_pipeline_integration(
    csv_context: TableContext,
    csv_context_2: TableContext,
    csv_context_3: TableContext,
    csv_context_4: TableContext,
    csv_context_5: TableContext,
    excel_context: Vec<TableContext>,
    temp_dir: TempDir,
) {
    //Set-up
    let cohort_name = "my_cohort";

    let mut onto_factory = CachedOntologyFactory::new(Box::new(FileSystemOntologyRegistry::new(
        ontology_registry_dir().expect("ontology_registry_dir could not be created"),
        BioRegistryMetadataProvider::default(),
        OboLibraryProvider::default(),
    )));

    let hp_ref = ResourceRef::hp().with_version("2025-09-01");
    let mondo_ref = ResourceRef::mondo().with_version("2026-01-06");
    let uo_ref = ResourceRef::uo().with_version("2026-01-09");
    let pato_ref = ResourceRef::pato().with_version("2025-05-14");

    let hpo_dict = Box::new(onto_factory.build_bidict(&hp_ref, None).unwrap());
    let mondo_dict = Box::new(onto_factory.build_bidict(&mondo_ref, None).unwrap());
    let uo_dict = Box::new(onto_factory.build_bidict(&uo_ref, None).unwrap());
    let pato_dict = Box::new(onto_factory.build_bidict(&pato_ref, None).unwrap());

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        PathBuf::from(file!())
            .parent()
            .unwrap()
            .join("assets/integration_test"),
    );

    //Configure data sources and contexts
    let csv_path = assets_dir.clone().join("input_data/csv_data.csv");
    let csv_path_2 = assets_dir.clone().join("input_data/csv_data_2.csv");
    let csv_path_3 = assets_dir.clone().join("input_data/csv_data_3.csv");
    let csv_path_4 = assets_dir.clone().join("input_data/csv_data_4.csv");
    let csv_path_5 = assets_dir.clone().join("input_data/csv_data_5.csv");
    let excel_path = assets_dir.clone().join("input_data/excel_data.xlsx");

    let mut data_sources = [
        DataSource::Csv(CSVDataSource::new(
            csv_path,
            None,
            csv_context,
            ExtractionConfig::new("CSV_Table".to_string(), false, true),
        )),
        DataSource::Csv(CSVDataSource::new(
            csv_path_2,
            None,
            csv_context_2,
            ExtractionConfig::new("CSV_Table_2".to_string(), true, false),
        )),
        DataSource::Csv(CSVDataSource::new(
            csv_path_3,
            None,
            csv_context_3,
            ExtractionConfig::new("CSV_Table_3".to_string(), true, false),
        )),
        DataSource::Csv(CSVDataSource::new(
            csv_path_4,
            None,
            csv_context_4,
            ExtractionConfig::new("CSV_Table_4".to_string(), true, true),
        )),
        DataSource::Csv(CSVDataSource::new(
            csv_path_5,
            None,
            csv_context_5,
            ExtractionConfig::new("CSV_Table_5".to_string(), true, true),
        )),
        DataSource::Excel(ExcelDatasource::new(
            excel_path,
            excel_context,
            vec![
                ExtractionConfig::new("basic info".to_string(), true, true),
                ExtractionConfig::new("conditions".to_string(), true, false),
                ExtractionConfig::new("more conditions".to_string(), true, false),
            ],
        )),
    ];

    //Configure strategies (a.k.a. transformations)
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(OntologyNormaliserStrategy::new(
            onto_factory.build_bidict(&hp_ref, None).unwrap(),
            ContextKind::HpoLabelOrId,
        )),
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
        BuilderMetaData::new(cohort_name, "Integration Test", "Someone"),
        Box::new(build_hgnc_test_client(temp_dir.path())),
        Box::new(build_hgvs_test_client(temp_dir.path())),
        BiDictLibrary::new("HPO", vec![hpo_dict]),
        BiDictLibrary::new("DISEASE", vec![mondo_dict]),
        BiDictLibrary::new("UNIT", vec![uo_dict]),
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
    pipeline.run(&mut data_sources).unwrap();

    //create a phenopacket_ID -> expected phenopacket HashMap
    let expected_phenopackets_files =
        fs::read_dir(assets_dir.join("expected_phenopackets")).unwrap();

    let mut expected_phenopackets: HashMap<String, Phenopacket> = HashMap::new();
    for expected_pp_file in expected_phenopackets_files {
        let data = fs::read_to_string(expected_pp_file.unwrap().path()).unwrap();
        let expected_pp: Phenopacket = serde_json::from_str(&data).unwrap();

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
