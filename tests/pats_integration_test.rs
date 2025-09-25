use phenoxtract::Pipeline;
use phenoxtract::config::table_context::{
    AliasMap, Context, Identifier, SeriesContext, TableContext,
};
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::GithubOntologyRegistry;
use phenoxtract::ontology::traits::OntologyRegistry;
use phenoxtract::ontology::utils::init_ontolius;
use phenoxtract::transform::strategies::{
    AliasMapStrategy, HPOSynonymsToPrimaryTermsStrategy, SexMappingStrategy,
};
use phenoxtract::transform::traits::Strategy;
use phenoxtract::transform::{Collector, PhenopacketBuilder, TransformerModule};
use rstest::{fixture, rstest};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[fixture]
fn vital_status_aliases() -> AliasMap {
    let mut vs_alias_map: HashMap<String, String> = HashMap::default();
    vs_alias_map.insert("Yes".to_string(), "ALIVE".to_string());
    vs_alias_map.insert("No".to_string(), "DECEASED".to_string());
    AliasMap::ToString(vs_alias_map)
}

#[fixture]
fn csv_context() -> TableContext {
    TableContext::new(
        "CSV_Table".to_string(),
        vec![
            SeriesContext::new(
                Identifier::Regex("0".to_string()),
                Default::default(),
                Context::SubjectId,
                None,
                None,
                vec![],
            ),
            SeriesContext::new(
                Identifier::Regex("1".to_string()),
                Default::default(),
                Context::HpoLabel,
                None,
                None,
                vec![],
            ),
            SeriesContext::new(
                Identifier::Regex("2".to_string()),
                Default::default(),
                Context::HpoLabel,
                None,
                None,
                vec![],
            ),
        ],
    )
}

#[fixture]
fn excel_context(vital_status_aliases: AliasMap) -> Vec<TableContext> {
    vec![
        TableContext::new(
            "basic info".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("Patient ID".to_string()),
                    Default::default(),
                    Context::SubjectId,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("Sex".to_string()),
                    Default::default(),
                    Context::SubjectSex,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("Living".to_string()),
                    Default::default(),
                    Context::VitalStat,
                    None,
                    Some(vital_status_aliases),
                    vec![],
                ),
            ],
        ),
        TableContext::new(
            "conditions".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("Patient ID".to_string()),
                    Default::default(),
                    Context::SubjectId,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("Phenotypic Features".to_string()),
                    Default::default(),
                    Context::HpoLabel,
                    None,
                    None,
                    vec![Identifier::Regex("Age of onset".to_string())],
                ),
                SeriesContext::new(
                    Identifier::Regex("Age of onset".to_string()),
                    Default::default(),
                    Context::OnsetAge,
                    None,
                    None,
                    vec![],
                ),
            ],
        ),
        TableContext::new(
            "more conditions".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("Patient ID".to_string()),
                    Default::default(),
                    Context::SubjectId,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex(r"Phenotypic Features \d+".to_string()),
                    Default::default(),
                    Context::HpoLabel,
                    None,
                    None,
                    vec![],
                ),
            ],
        ),
    ]
}

#[rstest]
fn test_phenoxtract_first_version(csv_context: TableContext, excel_context: Vec<TableContext>) {
    //Set-up
    let cohort_name = "my_cohort";
    let hpo_registry = GithubOntologyRegistry::default_hpo_registry().unwrap();
    let hpo = init_ontolius(hpo_registry.register("v2025-09-01").unwrap()).unwrap();
    let assets_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        PathBuf::from(file!())
            .parent()
            .unwrap()
            .join("assets/pats_test"),
    );

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
        Box::new(HPOSynonymsToPrimaryTermsStrategy::new(hpo.clone())),
        Box::new(SexMappingStrategy::default()),
    ];

    //Create the pipeline
    let transformer_module = TransformerModule::new(
        strategies,
        Collector::new(PhenopacketBuilder::new(hpo), cohort_name.to_owned()),
    );

    let output_dir = assets_path.join("do_not_push");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = FileSystemLoader::new(output_dir.clone());

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //run the pipeline on the data sources
    let res = pipeline.run(&mut data_sources);

    assert!(res.is_ok());
}
