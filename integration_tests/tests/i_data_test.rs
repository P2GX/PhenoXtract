use dotenvy::dotenv;
use phenoxtract::phenoxtract::Phenoxtract;
use rstest::rstest;
use std::path::PathBuf;

#[rstest]
fn test_i_data() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let root = env!("CARGO_MANIFEST_DIR");
    let config_path = PathBuf::from("/home/lukas/soft/PhenoXtract/integration_tests/tests/configs/i_data_config.yaml");

    println!("config_path: {:?}", config_path);
    let mut phenoxtract = Phenoxtract::try_from(config_path).unwrap();
    phenoxtract.run()?;

    Ok(())
}
