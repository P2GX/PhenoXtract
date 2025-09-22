use phenoxtract::Pipeline;
use phenoxtract::ontology::traits::OntologyRegistry;
use phenoxtract::ontology::utils::init_ontolius;
use phenoxtract::transform::collector::Collector;

use phenoxtract::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::extract::{CSVDataSource, DataSource};
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::GithubOntologyRegistry;
use phenoxtract::transform::TransformerModule;
use phenoxtract::transform::strategies::{
    AliasMapStrategy, HPOSynonymsToPrimaryTermsStrategy, SexMappingStrategy,
};
use phenoxtract::transform::traits::Strategy;
use rstest::rstest;
use std::path::PathBuf;

#[rstest]
fn test_pipeline_integration() {
    // TODO: Load pipeline from config file. Can not be done yet, because strategies are not ready to be loaded from file
    let hpo_registry = GithubOntologyRegistry::default_hpo_registry().unwrap();
    let hpo = init_ontolius(hpo_registry.register("latest").unwrap()).unwrap();

    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(HPOSynonymsToPrimaryTermsStrategy::new(hpo.clone())),
        Box::new(SexMappingStrategy::default()),
    ];

    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let assets_path = manifest_path.join(PathBuf::from(file!()).parent().unwrap().join("assets"));

    let collector = Collector::new(hpo);
    let tm = TransformerModule::new(strategies, collector);
    let loader = FileSystemLoader::new(assets_path.join("do_not_push"));
    let pipeline = Pipeline::new(tm, loader);

    let csv_path = assets_path.clone().join("test_data.csv");
    let excel_path = assets_path.clone().join("test_data.xlsx");

    let mut data_sources = [
        DataSource::Csv(CSVDataSource::new(
            csv_path,
            None,
            TableContext::new(
                "CSVTable".to_string(),
                vec![
                    SeriesContext::new(
                        Identifier::Regex("patient_id".to_string()),
                        Default::default(),
                        Context::SubjectId,
                        None,
                        None,
                        vec![],
                    ),
                    SeriesContext::new(
                        Identifier::Regex("sex".to_string()),
                        Default::default(),
                        Context::SubjectSex,
                        None,
                        None,
                        vec![],
                    ),
                ],
            ),
            ExtractionConfig::new("CSVTable".to_string(), true, false),
        )),
        DataSource::Excel(ExcelDatasource::new(
            excel_path,
            vec![TableContext::new(
                "ExcelTable".to_string(),
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
                ],
            )],
            vec![ExtractionConfig::new("ExcelTable".to_string(), false, true)],
        )),
    ];
    let res = pipeline.run(&mut data_sources);

    assert!(res.is_ok());
}
