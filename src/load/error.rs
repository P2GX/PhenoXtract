use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("Can not find storage: {reason}")]
    NoStorage { reason: String },
    #[error("Can not store Phenopacket with ID '{pp_id}', because: {reason}")]
    CantStore { pp_id: String, reason: String },
    #[error("Can not convert Phenopacket with ID '{pp_id}' into '{format}'")]
    ConversionError { pp_id: String, format: String },
}
