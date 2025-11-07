use phenoxtract::Pipeline;
use phenoxtract::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use phenoxtract::extract::DataSource;
use phenoxtract::extract::ExcelDatasource;
use phenoxtract::extract::extraction_config::ExtractionConfig;
use phenoxtract::load::FileSystemLoader;
use phenoxtract::ontology::resource_references::OntologyRef;

use phenoxtract::error::PipelineError;
use phenoxtract::ontology::traits::HasPrefixId;
use phenoxtract::ontology::{CachedOntologyFactory, HGNCClient};
use phenoxtract::transform::strategies::MultiHPOColExpansionStrategy;
use phenoxtract::transform::strategies::{MappingStrategy, StringCorrectionStrategy};
use phenoxtract::transform::traits::Strategy;
use phenoxtract::transform::{Collector, PhenopacketBuilder, TransformerModule};
use ratelimit::Ratelimiter;
use rstest::{fixture, rstest};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

#[fixture]
fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create temporary directory")
}

#[fixture]
fn excel_context() -> TableContext {
    TableContext::new(
        "TestData".to_string(),
        vec![
            SeriesContext::default()
                .with_identifier(Identifier::Regex("patient_id".to_string()))
                .with_data_context(Context::SubjectId),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("sex".to_string()))
                .with_data_context(Context::SubjectSex),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("gene_symbol".to_string()))
                .with_data_context(Context::HgncSymbolOrId)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("disease".to_string()))
                .with_data_context(Context::OmimLabel)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("dissease_OMIM_id".to_string()))
                .with_data_context(Context::OmimId)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HGVS_1".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HGVS_2".to_string()))
                .with_data_context(Context::Hgvs)
                .with_building_block_id(Some("A".to_string())),
            SeriesContext::default()
                .with_identifier(Identifier::Regex("HPO".to_string()))
                .with_data_context(Context::MultiHpoId),
        ],
    )
}

fn build_hgnc_test_client(temp_dir: &Path) -> HGNCClient {
    let rate_limiter = Ratelimiter::builder(10, Duration::from_secs(1))
        .max_tokens(10)
        .build()
        .expect("Building rate limiter failed");

    HGNCClient::new(
        rate_limiter,
        temp_dir.to_path_buf().join("hgnc_test_cache"),
        "https://rest.genenames.org/".to_string(),
    )
    .unwrap()
}

#[rstest]
fn test_j_data(excel_context: TableContext, temp_dir: TempDir) -> Result<(), PipelineError> {
    //Set-up
    let cohort_name = "j_test_cohort";

    let mut onto_factory = CachedOntologyFactory::default();

    let hpo_dict = onto_factory
        .build_bidict(&OntologyRef::hp_with_version("2025-09-01"), None)
        .unwrap();
    let mondo_dict = onto_factory
        .build_bidict(&OntologyRef::mondo(), None)
        .unwrap();

    let assets_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(PathBuf::from(file!()).parent().unwrap().join("assets"));

    //Configure data source and context
    let excel_path = PathBuf::from("/Users/patrick/Downloads/PhenoXtract/Example_J_Data.xlsx");

    let mut data_sources = [DataSource::Excel(ExcelDatasource::new(
        excel_path,
        vec![excel_context],
        vec![ExtractionConfig::new("TestData".to_string(), true, true)],
    ))];

    //Configure strategies (a.k.a. transformations)
    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(MappingStrategy::default_sex_mapping_strategy()),
        Box::new(StringCorrectionStrategy::new(
            Context::None,
            Context::Hgvs,
            "*".to_string(),
            ":".to_string(),
        )),
        Box::new(StringCorrectionStrategy::new(
            Context::None,
            Context::MultiHpoId,
            "HPO".to_string(),
            "HP".to_string(),
        )),
        Box::new(MultiHPOColExpansionStrategy),
    ];

    //Create the pipeline
    let phenopacket_builder = PhenopacketBuilder::new(
        HashMap::from_iter([
            (hpo_dict.ontology.prefix_id().to_string(), hpo_dict),
            (mondo_dict.ontology.prefix_id().to_string(), mondo_dict),
        ]),
        build_hgnc_test_client(temp_dir.path()),
    );

    let transformer_module = TransformerModule::new(
        strategies,
        Collector::new(phenopacket_builder, cohort_name.to_owned()),
    );

    let output_dir = assets_path.join("test_j_data_do_not_push");
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir).unwrap();
    }
    let loader = FileSystemLoader::new(output_dir.clone());

    let mut pipeline = Pipeline::new(transformer_module, loader);

    //Run the pipeline on the data source
    pipeline.run(&mut data_sources)?;

    Ok(())
}
