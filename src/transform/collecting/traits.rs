use crate::extract::ContextualizedDataFrame;
use crate::transform::PhenopacketBuilder;
use crate::transform::error::CollectorError;
use std::any::Any;
use std::fmt::Debug;

pub trait Collect: Debug + AsAny {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdfs: &[ContextualizedDataFrame],
        phenopacket_id: &str,
    ) -> Result<(), CollectorError>;
}

pub trait AsAny {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AsAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
