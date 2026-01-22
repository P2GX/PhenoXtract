use phenoxtract::Pipeline;
use rstest::rstest;
use std::path::PathBuf;
use dotenvy::dotenv;
use phenoxtract::config::PhenoXtractConfig;

#[rstest]
fn test_i_data(
) {
    dotenv().ok();
    let config_path = PathBuf::from("/Users/patrick/RustroverProjects/PhenoXtrackt/tests/configs/i_data_config.yaml");
    let phenoxtract_config = PhenoXtractConfig::try_from(config_path).unwrap();
    let mut pipeline = Pipeline::try_from(phenoxtract_config.pipeline).unwrap();
    let mut data_sources = phenoxtract_config.data_sources;
    pipeline.run(&mut data_sources).unwrap();
}