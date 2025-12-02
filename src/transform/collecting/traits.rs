use crate::extract::ContextualizedDataFrame;
use crate::transform::PhenopacketBuilder;
use crate::transform::error::CollectorError;
use std::fmt::Debug;

pub trait Collect: Debug {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError>;
}
