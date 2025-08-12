use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    #[allow(unused)]
    transform_strategies: Vec<String>,
    #[allow(unused)]
    loader: String,
    // String for now, later enum
}
