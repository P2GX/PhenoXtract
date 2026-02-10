use crate::extract::ContextualizedDataFrame;
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use std::any::Any;
use std::fmt::Debug;

pub trait Collect: Debug {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError>;

    fn as_any(&self) -> &dyn Any;
}
