use serde::Deserialize;

/// Represents the configuration for a data processing pipeline.
///
/// This struct holds the necessary information to define how data
/// should be loaded and transformed.
#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    #[allow(unused)]
    /// A list of strategies to transform the data. Each string identifies
    /// a specific transformation to be applied in order.
    /// TODO: String for now, later probably Hashmap<String, config::Value>
    transform_strategies: Vec<String>,

    #[allow(unused)]
    /// The loader responsible for fetching the initial data.
    ///
    /// For now, this is a simple string, but it will likely evolve into
    /// a more complex configuration object.
    /// TODO: String for now, later enum
    loader: String,
}
