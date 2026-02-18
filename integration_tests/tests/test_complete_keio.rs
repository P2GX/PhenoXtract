use dotenvy::dotenv;
use phenoxtract::config::{ConfigLoader, PhenoXtractConfig};
use phenoxtract::phenoxtract::Phenoxtract;
use std::path::PathBuf;

#[test]
fn test_pipeline_integration() {
    dotenv().ok();
    let config: PhenoXtractConfig = ConfigLoader::load(PathBuf::from(
        "/Users/adamgraefe/Documents/git/PhenoXtract/integration_tests/tests/japan_config.yaml",
    ))
    .unwrap();
    let mut px = Phenoxtract::try_from(config).unwrap();
    px.run().unwrap();
}
