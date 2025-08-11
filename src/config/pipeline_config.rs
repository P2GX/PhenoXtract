use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    transform_strategies: Vec<String>,
    loader: String,
}