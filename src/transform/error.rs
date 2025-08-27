#[derive(Debug)]
pub enum TransformError {
    #[allow(dead_code)]
    BuildingError(String),
    #[allow(dead_code)]
    StrategyError(String),
}
