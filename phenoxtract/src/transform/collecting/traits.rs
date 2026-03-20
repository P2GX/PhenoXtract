#![allow(dead_code)]
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

    fn check_bounds(&self, idx: usize) -> Result<(), GetterError> {
        if self.len() <= idx {
            return Err(GetterError::OutOfBounds);
        }

        Ok(())
    }
    /// Not meant to be called directly
    fn construct_data(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError>;
    fn len(&self) -> usize;
    fn get(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        self.check_bounds(idx)?;

        self.construct_data(idx)
    }
}
