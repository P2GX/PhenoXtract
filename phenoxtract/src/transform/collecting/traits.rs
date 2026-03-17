use crate::extract::ContextualizedDataFrame;
use crate::transform::error::{CollectorError, GetterError};
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

pub(crate) trait Getter {
    type Item<'a>
    where
        Self: 'a;
    fn get(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError>;
    fn len(&self) -> usize;
}
