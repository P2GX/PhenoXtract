#![allow(clippy::too_many_arguments)]
use phenoxtract::Pipeline;
use phenoxtract::config::context::{Context, ContextKind, TimeElementType};
use phenoxtract::config::table_context::{AliasMap, SeriesContext, TableContext};
use phenoxtract::config::traits::SeriesContextBuilding;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CsvDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::resource_references::ResourceRef;

use dotenvy::dotenv;
use integration_tests::{
    build_hgnc_test_client, build_hgvs_test_client, cohort_name,
    compare_expected_and_extracted_phenopackets, ontology_registry_dir, pato_ref, temp_dir,
    tests_assets, vital_status_aliases,
};
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use phenoxtract::ontology::CachedOntologyFactory;
use phenoxtract::ontology::loinc_client::LoincClient;
use phenoxtract::transform::bidict_library::BiDictLibrary;
use phenoxtract::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use phenoxtract::transform::phenopacket_builder::BuilderMetaData;
use phenoxtract::transform::strategies::traits::Strategy;
use phenoxtract::transform::strategies::{AgeToIso8601Strategy, MappingStrategy};
use phenoxtract::transform::strategies::{AliasMapStrategy, MultiHPOColExpansionStrategy};
use phenoxtract::transform::strategies::{DateToAgeStrategy, OntologyNormaliserStrategy};
use phenoxtract::transform::{PhenopacketBuilder, TransformerModule};
use rstest::{fixture, rstest};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

#[fixture]
fn csv_context(vital_status_aliases: AliasMap) -> TableContext {
    TableContext::new(
        "CSV_Table".to_string(),
        vec![
            SeriesContext::from_identifier("patient_id").with_data_context(Context::SubjectId),
            SeriesContext::from_identifier("date_of_birth").with_data_context(Context::DateOfBirth),
            SeriesContext::from_identifier("age_at_last_encounter")
                .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age)),
            SeriesContext::from_identifier("date_at_last_encounter")
                .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Date)),
            SeriesContext::from_identifier("sex").with_data_context(Context::SubjectSex),
            SeriesContext::from_identifier("vital_status")
                .with_data_context(Context::VitalStatus)
                .with_alias_map(vital_status_aliases),
            SeriesContext::from_identifier("age_of_death")
                .with_data_context(Context::TimeOfDeath(TimeElementType::Age)),
            SeriesContext::from_identifier("date_of_death")
                .with_data_context(Context::TimeOfDeath(TimeElementType::Date)),
            SeriesContext::from_identifier("cause_of_death")
                .with_data_context(Context::CauseOfDeath),
            SeriesContext::from_identifier("survival_time_in_days")
                .with_data_context(Context::SurvivalTimeDays),
            SeriesContext::from_identifier("phenotype")
                .with_data_context(Context::Hpo)
                .with_building_block_id("P1"),
            SeriesContext::from_identifier("multi_hpo")
                .with_data_context(Context::MultiHpoId)
                .with_building_block_id("P1"),
            SeriesContext::from_identifier("phenotype_onset_age")
                .with_data_context(Context::Onset(TimeElementType::Age))
                .with_building_block_id("P1"),
            SeriesContext::from_identifier("HP:1234567")
                .with_header_context(Context::Hpo)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id("P2"),
            SeriesContext::from_identifier("HP:1234567_onset_date")
                .with_data_context(Context::Onset(TimeElementType::Age))
                .with_building_block_id("P2"),
            SeriesContext::from_identifier("disease")
                .with_data_context(Context::Disease)
                .with_building_block_id("D1"),
            SeriesContext::from_identifier("disease_onset_age")
                .with_data_context(Context::Onset(TimeElementType::Age))
                .with_building_block_id("D1"),
            SeriesContext::from_identifier("gene")
                .with_data_context(Context::Hgnc)
                .with_building_block_id("D1"),
            SeriesContext::from_identifier("hgvs1")
                .with_data_context(Context::Hgvs)
                .with_building_block_id("D1"),
            SeriesContext::from_identifier("hgvs2")
                .with_data_context(Context::Hgvs)
                .with_building_block_id("D1"),
            SeriesContext::from_identifier("disease2")
                .with_data_context(Context::Disease)
                .with_building_block_id("D2"),
            SeriesContext::from_identifier("disease2_onset_date")
                .with_data_context(Context::Onset(TimeElementType::Date))
                .with_building_block_id("D2"),
            SeriesContext::from_identifier("body_height_cm")
                .with_data_context(Context::QuantitativeMeasurement {
                    assay_id: "LOINC:8302-2".to_string(),
                    unit_ontology_id: "UO:0000015".to_string(),
                })
                .with_building_block_id("QUANT_M"),
            SeriesContext::from_identifier("body_height_cm_measurement_age")
                .with_data_context(Context::Onset(TimeElementType::Age))
                .with_building_block_id("QUANT_M"),
            SeriesContext::from_identifier("nitrate_presence")
                .with_data_context(Context::QualitativeMeasurement {
                    assay_id: "LOINC:5802-4".to_string(),
                })
                .with_building_block_id("QUAL_M"),
            SeriesContext::from_identifier("nitrate_presence_measurement_date")
                .with_data_context(Context::Onset(TimeElementType::Date))
                .with_building_block_id("QUAL_M"),
        ],
    )
}

#[rstest]
fn big_null_test(
    csv_context: TableContext,
    temp_dir: TempDir,
    pato_ref: ResourceRef,
    tests_assets: PathBuf,
    cohort_name: String,
) {
    // Set up
    let mut onto_factory = CachedOntologyFactory::new(FileSystemOntologyRegistry::new(
        ontology_registry_dir().expect("ontology_registry_dir could not be created"),
        BioRegistryMetadataProvider::default(),
        OboLibraryProvider::default(),
    ));
    let pato_dict = Box::new(onto_factory.build_bidict(&pato_ref, None).unwrap());
    let assets_dir = tests_assets.join("big_null_test");

    //Configure data sources and contexts
    let csv_path = assets_dir.clone().join("input_data/data.csv");

    let mut data_sources = [DataSource::Csv(CsvDataSource::new(
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
        BiDictLibrary::new("PROCEDURE", vec![]),
        BiDictLibrary::new("ANATOMY", vec![]),
        BiDictLibrary::new("TREATMENT", vec![]),
    );

    let transformer_module = TransformerModule::new(
        strategies,
        CdfCollectorBroker::with_default_collectors(phenopacket_builder),
    );

    let output_dir_name = "extracted_phenopackets";
    let output_dir = assets_dir.join(output_dir_name);
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = Box::new(FileSystemLoader::new(output_dir.clone(), true));

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //Run the pipeline on the data sources
    pipeline.run(&mut data_sources).expect("Pipeline failed");

    compare_expected_and_extracted_phenopackets(
        assets_dir,
        output_dir_name,
        "extracted_phenopackets",
    );
}
