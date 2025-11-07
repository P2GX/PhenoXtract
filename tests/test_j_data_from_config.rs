use phenoxtract::Pipeline;

use phenoxtract::config::PhenoXtractorConfig;
use phenoxtract::error::PipelineError;
use rstest::rstest;
use std::path::PathBuf;
use std::str::FromStr;

#[rstest]
fn test_j_data_from_config() -> Result<(), PipelineError> {
    let mut config = PhenoXtractorConfig::try_from(
        PathBuf::from_str("/Users/patrick/RustroverProjects/PhenoXtrackt/tests/assets/configs/j_data_config.yaml").unwrap(),
    )
    .unwrap();
    let mut pipeline = Pipeline::try_from(config.pipeline).unwrap();
    pipeline.run(config.data_sources.as_mut_slice())?;

    Ok(())
}
