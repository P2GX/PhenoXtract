use phenoxtract::Pipeline;
use phenoxtract::config::{ConfigLoader, PhenoXtractorConfig};
use rstest::rstest;
use std::path::PathBuf;

#[rstest]
fn test_pipeline_integration() {
    let mut config: PhenoXtractorConfig =
        ConfigLoader::load(PathBuf::from("Path/to/config")).unwrap();
    let pipeline = Pipeline::try_from(config.pipeline);

    match pipeline {
        Ok(mut pipeline) => {
            let pipe_res = pipeline.run(config.data_sources.as_mut_slice());

            match pipe_res {
                Ok(_) => {}
                Err(err) => panic!("{}", err),
            }
        }
        Err(err) => {
            println!("{}", err);
        }
    }
}
