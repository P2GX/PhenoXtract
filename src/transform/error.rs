#[derive(Debug, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum TransformError {
    #[allow(dead_code)]
    BuildingError(String),
    #[allow(dead_code)]
    StrategyError(String),
    #[allow(dead_code)]
    CollectionError(String),
}
