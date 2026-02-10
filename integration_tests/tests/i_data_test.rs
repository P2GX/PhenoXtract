use dotenvy::dotenv;
use rstest::rstest;
use std::path::PathBuf;
use phenoxtract::phenoxtract::Phenoxtract;

#[rstest]
fn test_i_data() {
    dotenv().ok();
    let config_path = PathBuf::from(
        "/Users/patrick/RustroverProjects/PhenoXtrackt/integration_tests/tests/configs/i_data_config.yaml",
    );
    let mut phenoxtract = Phenoxtract::try_from(config_path).unwrap();
    phenoxtract.run().unwrap();
}
