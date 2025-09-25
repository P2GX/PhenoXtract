use phenopackets::schema::v2::Phenopacket;
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
use rstest::rstest;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[rstest]
pub fn test_pipeline_integration() {
    // TODO: Load pipeline from config file. Can not be done yet, because strategies are not ready to be loaded from file
    let cohort_name = "test_cohort";
    let hpo_registry = GithubOntologyRegistry::default_hpo_registry().unwrap();
    let hpo = init_ontolius(hpo_registry.register("v2025-09-01").unwrap()).unwrap();

    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(AliasMapStrategy),
        Box::new(HPOSynonymsToPrimaryTermsStrategy::new(hpo.clone())),
        Box::new(SexMappingStrategy::default()),
    ];

    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let assets_path = manifest_path.join(PathBuf::from(file!()).parent().unwrap().join("assets"));

    let collector = Collector::new(PhenopacketBuilder::new(hpo), cohort_name.to_owned());
    let tm = TransformerModule::new(strategies, collector);

    let output_dir = assets_path.join("do_not_push");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = FileSystemLoader::new(output_dir.clone());

    let mut pipeline = Pipeline::new(tm, loader);

    let csv_path = assets_path.clone().join("test_data.csv");
    let excel_path = assets_path.clone().join("test_data.xlsx");

    let mut alias_map_sex_col: HashMap<String, String> = HashMap::default();
    alias_map_sex_col.insert("replace_me".to_string(), "male".to_string());

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
                        Identifier::Regex("SEX".to_string()),
                        Default::default(),
                        Context::SubjectSex,
                        None,
                        Some(AliasMap::ToString(alias_map_sex_col)),
                        vec![],
                    ),
                ],
            ),
            ExtractionConfig::new("CSVTable".to_string(), true, false),
        )),
        DataSource::Excel(ExcelDatasource::new(
            excel_path,
            vec![TableContext::new(
                "HPOLabels".to_string(),
                vec![
                    SeriesContext::new(
                        Identifier::Regex("0".to_string()),
                        Default::default(),
                        Context::SubjectId,
                        None,
                        None,
                        vec![Identifier::Regex("4".to_string())],
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
                        Identifier::Regex("4".to_string()),
                        Default::default(),
                        Context::OnsetAge,
                        None,
                        None,
                        vec![],
                    ),
                ],
            )],
            vec![ExtractionConfig::new("HPOLabels".to_string(), false, true)],
        )),
    ];
    let res = pipeline.run(&mut data_sources);

    assert!(res.is_ok());

    let expected_phenopackets_files =
        fs::read_dir(assets_path.join("integration_test_expected_phenopackets")).unwrap();

    let mut expected_phenopackets: HashMap<String, Phenopacket> = HashMap::new();
    for pp_file in expected_phenopackets_files {
        let data = fs::read_to_string(pp_file.unwrap().path()).unwrap();
        let pp: Phenopacket = serde_json::from_str(&data).unwrap();
        expected_phenopackets.insert(pp.id.clone(), pp);
    }

    for pp_file_dir in fs::read_dir(output_dir).unwrap() {
        let data = fs::read_to_string(pp_file_dir.unwrap().path()).unwrap();
        let pp: Phenopacket = serde_json::from_str(&data).unwrap();
        let pp_id = pp.id.clone();
        #[allow(clippy::dbg_macro)]
        {
            dbg!(&pp);
            dbg!(&expected_phenopackets.get(&pp_id)).unwrap();
        }
        assert_eq!(pp, expected_phenopackets.get(&pp_id).unwrap().clone());
    }
}
