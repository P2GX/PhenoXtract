use dotenvy::dotenv;
use phenoxtract::phenoxtract::Phenoxtract;
use rstest::rstest;
use std::path::PathBuf;

#[rstest]
fn test_i_data() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let config_path = PathBuf::from(
        "/Users/rouvenreuter/Documents/Projects/PhenoXtrackt/integration_tests/tests/configs/i_data_config.yaml",
    );
    let mut phenoxtract = Phenoxtract::try_from(config_path).unwrap();
    phenoxtract.run()?;

    Ok(())
}
