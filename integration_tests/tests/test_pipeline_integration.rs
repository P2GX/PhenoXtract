#![allow(clippy::too_many_arguments)]
use dotenvy::dotenv;
use integration_tests::{
    build_hgnc_test_client, build_hgvs_test_client, cohort_name,
    compare_expected_and_extracted_phenopackets, hp_ref, mondo_ref, no_info_alias,
    ontology_registry_dir, pato_ref, temp_dir, tests_assets, uo_ref, vital_status_aliases,
};
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use phenoxtract::Pipeline;
use phenoxtract::config::context::{Boundary, Context, ContextKind, TimeElementType};
use phenoxtract::config::table_context::{AliasMap, Identifier, SeriesContext, TableContext};
use phenoxtract::extract::ExcelDataSource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CsvDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::CachedOntologyFactory;
use phenoxtract::ontology::loinc_client::LoincClient;
use phenoxtract::ontology::resource_references::ResourceRef;
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
                .with_data_context(Context::Onset(TimeElementType::Date))
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
                .with_data_context(Context::Onset(TimeElementType::Date))
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
                .with_data_context(Context::Onset(TimeElementType::Age))
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
                .with_data_context(Context::ReferenceRange(Boundary::Start))
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("ref_high".to_string()))
                .with_data_context(Context::ReferenceRange(Boundary::End))
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("nitrates in urine".to_string()))
                .with_data_context(Context::QualitativeMeasurement {
                    assay_id: "LOINC:5802-4".to_string(),
                })
                .with_building_block_id(Some("M".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("date_of_observation".to_string()))
                .with_data_context(Context::Onset(TimeElementType::Date))
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
                    .with_data_context(Context::TimeOfDeath(TimeElementType::Age)),
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
                    .with_data_context(Context::Onset(TimeElementType::Age))
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

#[rstest]
fn test_pipeline_integration(
    csv_context: TableContext,
    csv_context_2: TableContext,
    csv_context_3: TableContext,
    csv_context_4: TableContext,
    csv_context_5: TableContext,
    excel_context: Vec<TableContext>,
    temp_dir: TempDir,
    hp_ref: ResourceRef,
    mondo_ref: ResourceRef,
    uo_ref: ResourceRef,
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

    let hpo_dict = Box::new(onto_factory.build_bidict(&hp_ref, None).unwrap());
    let mondo_dict = Box::new(onto_factory.build_bidict(&mondo_ref, None).unwrap());
    let uo_dict = Box::new(onto_factory.build_bidict(&uo_ref, None).unwrap());
    let pato_dict = Box::new(onto_factory.build_bidict(&pato_ref, None).unwrap());

    let assets_dir = tests_assets.join("integration_test");

    //Configure data sources and contexts
    let csv_path = assets_dir.clone().join("input_data/csv_data.csv");
    let csv_path_2 = assets_dir.clone().join("input_data/csv_data_2.csv");
    let csv_path_3 = assets_dir.clone().join("input_data/csv_data_3.csv");
    let csv_path_4 = assets_dir.clone().join("input_data/csv_data_4.csv");
    let csv_path_5 = assets_dir.clone().join("input_data/csv_data_5.csv");
    let excel_path = assets_dir.clone().join("input_data/excel_data.xlsx");

    let mut data_sources = [
        DataSource::Csv(CsvDataSource::new(
            csv_path,
            None,
            csv_context,
            ExtractionConfig::new("CSV_Table".to_string(), false, true),
        )),
        DataSource::Csv(CsvDataSource::new(
            csv_path_2,
            None,
            csv_context_2,
            ExtractionConfig::new("CSV_Table_2".to_string(), true, false),
        )),
        DataSource::Csv(CsvDataSource::new(
            csv_path_3,
            None,
            csv_context_3,
            ExtractionConfig::new("CSV_Table_3".to_string(), true, false),
        )),
        DataSource::Csv(CsvDataSource::new(
            csv_path_4,
            None,
            csv_context_4,
            ExtractionConfig::new("CSV_Table_4".to_string(), true, true),
        )),
        DataSource::Csv(CsvDataSource::new(
            csv_path_5,
            None,
            csv_context_5,
            ExtractionConfig::new("CSV_Table_5".to_string(), true, true),
        )),
        DataSource::Excel(ExcelDataSource::new(
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
    pipeline.run(&mut data_sources).unwrap();

    compare_expected_and_extracted_phenopackets(
        assets_dir,
        output_dir_name,
        "extracted_phenopackets",
    );
}
