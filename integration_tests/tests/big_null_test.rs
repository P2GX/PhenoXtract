#![allow(clippy::too_many_arguments)]
use phenoxtract::Pipeline;
use phenoxtract::config::context::{Context, ContextKind, TimeElementType};
use phenoxtract::config::table_context::{AliasMap, Identifier, SeriesContext, TableContext};
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
            SeriesContext::default()
                .with_identifier(Identifier::Regex("patient_id".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_of_birth".to_string()))
                .with_data_context(Context::DateOfBirth),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("age_at_last_encounter".to_string()))
                .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age)),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_at_last_encounter".to_string()))
                .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Date)),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("sex".to_string()))
                .with_data_context(Context::SubjectSex),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("vital_status".to_string()))
                .with_data_context(Context::VitalStatus)
                .with_alias_map(Some(vital_status_aliases)),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("age_of_death".to_string()))
                .with_data_context(Context::TimeOfDeath(TimeElementType::Age)),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_of_death".to_string()))
                .with_data_context(Context::TimeOfDeath(TimeElementType::Date)),
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
                .with_data_context(Context::Onset(TimeElementType::Age))
                .with_building_block_id(Some("P1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HP:1234567".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("P2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HP:1234567_onset_date".to_string()))
                .with_data_context(Context::Onset(TimeElementType::Age))
                .with_building_block_id(Some("P2".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease".to_string()))
                .with_data_context(Context::DiseaseLabelOrId)
                .with_building_block_id(Some("D1".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease_onset_age".to_string()))
                .with_data_context(Context::Onset(TimeElementType::Age))
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
                .with_data_context(Context::Onset(TimeElementType::Date))
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
                .with_data_context(Context::Onset(TimeElementType::Age))
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
                .with_data_context(Context::Onset(TimeElementType::Date))
                .with_building_block_id(Some("QUAL_M".to_string())),
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
    let mut onto_factory = CachedOntologyFactory::new(Box::new(FileSystemOntologyRegistry::new(
        ontology_registry_dir().expect("ontology_registry_dir could not be created"),
        BioRegistryMetadataProvider::default(),
        OboLibraryProvider::default(),
    )));
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
