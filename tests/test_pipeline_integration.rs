use phenopackets::schema::v2::Phenopacket;
use phenoxtract::Pipeline;
use phenoxtract::config::table_context::{
    AliasMap, Context, Identifier, OutputDataType, SeriesContext, TableContext,
};
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::ObolibraryOntologyRegistry;
use phenoxtract::ontology::ontology_bidict::OntologyBiDict;
use phenoxtract::ontology::traits::OntologyRegistry;
use phenoxtract::ontology::utils::init_ontolius;
use phenoxtract::transform::strategies::AliasMapStrategy;
use phenoxtract::transform::strategies::MappingStrategy;
use phenoxtract::transform::strategies::SynonymsToPrimaryTermsStrategy;
use phenoxtract::transform::traits::Strategy;
use phenoxtract::transform::{Collector, PhenopacketBuilder, TransformerModule};
use rstest::{fixture, rstest};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[fixture]
fn vital_status_aliases() -> AliasMap {
    let mut vs_hash_map: HashMap<String, String> = HashMap::default();
    vs_hash_map.insert("Yes".to_string(), "ALIVE".to_string());
    vs_hash_map.insert("No".to_string(), "DECEASED".to_string());
    AliasMap::new(vs_hash_map, OutputDataType::String)
}

#[fixture]
fn csv_context() -> TableContext {
    TableContext::new(
        "CSV_Table".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("0".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("1".to_string()))
                .with_data_context(Context::HpoLabelOrId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("2".to_string()))
                .with_data_context(Context::HpoLabelOrId),
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
                    .with_identifier(Identifier::Regex("Time of death".to_string()))
                    .with_data_context(Context::TimeOfDeath),
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
                    .with_building_block_id(Some("block_1".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Age of onset".to_string()))
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("block_1".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("HP:0012373".to_string()))
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(Some("block_2".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Rhinorrhea".to_string()))
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(Some("block_2".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("Date of onset".to_string()))
                    .with_data_context(Context::OnsetDateTime)
                    .with_building_block_id(Some("block_2".to_string())),
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
fn test_pipeline_integration(csv_context: TableContext, excel_context: Vec<TableContext>) {
    //Set-up
    let cohort_name = "my_cohort";
    let hpo_registry = ObolibraryOntologyRegistry::default_hpo_registry().unwrap();
    let hpo = init_ontolius(hpo_registry.register("2025-09-01").unwrap()).unwrap();
    let hpo_dict = Arc::new(OntologyBiDict::from(hpo));
    let assets_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(PathBuf::from(file!()).parent().unwrap().join("assets"));

    //Configure data sources and contexts
    let csv_path = assets_path.clone().join("csv_data.csv");
    let excel_path = assets_path.clone().join("excel_data.xlsx");

    let mut data_sources = [
        DataSource::Csv(CSVDataSource::new(
            csv_path,
            None,
            csv_context,
            ExtractionConfig::new("CSV_Table".to_string(), false, true),
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
        Box::new(SynonymsToPrimaryTermsStrategy::new(
            hpo_dict.clone(),
            Context::HpoLabel,
        )),
        Box::new(MappingStrategy::default_sex_mapping_strategy()),
    ];

    //Create the pipeline
    let transformer_module = TransformerModule::new(
        strategies,
        Collector::new(PhenopacketBuilder::new(hpo_dict), cohort_name.to_owned()),
    );

    let output_dir = assets_path.join("do_not_push");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = FileSystemLoader::new(output_dir.clone());

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //Run the pipeline on the data sources
    let res = pipeline.run(&mut data_sources);

    res.unwrap();

    let expected_phenopackets_files =
        fs::read_dir(assets_path.join("integration_test_expected_phenopackets")).unwrap();

    let mut expected_phenopackets: HashMap<String, Phenopacket> = HashMap::new();
    for expected_pp_file in expected_phenopackets_files {
        let data = fs::read_to_string(expected_pp_file.unwrap().path()).unwrap();
        let expected_pp: Phenopacket = serde_json::from_str(&data).unwrap();
        expected_phenopackets.insert(expected_pp.id.clone(), expected_pp);
    }

    for extracted_pp_file in fs::read_dir(output_dir).unwrap() {
        let data = fs::read_to_string(extracted_pp_file.unwrap().path()).unwrap();
        let extracted_pp: Phenopacket = serde_json::from_str(&data).unwrap();
        let extracted_pp_id = extracted_pp.id.clone();
        assert_eq!(
            extracted_pp,
            expected_phenopackets.get(&extracted_pp_id).unwrap().clone()
        );
    }
}
