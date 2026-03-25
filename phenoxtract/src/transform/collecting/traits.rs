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

pub(crate) trait GetRows {
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
    #[doc(hidden)]
    fn construct_data_unchecked(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        self.check_bounds(idx)?;

        self.construct_data_unchecked(idx)
    }
}

/// A trait for conditionally extracting a value from an optional reference.
///
/// `Pluck` allows you to apply a function to the inner value of an `Option<T>`
/// by reference, returning a new `Option<U>`. This is useful when you want to
/// extract a sub-value from a type without consuming or cloning the outer `Option`.
pub(crate) trait Pluck<T> {
    /// Applies `f` to a reference of the inner value, returning the result.
    ///
    /// - Returns `None` if `self` is `None`.
    /// - Returns `None` if `f` returns `None`.
    /// - Returns `Some(U)` if both `self` is `Some` and `f` returns `Some`.
    fn pluck<U, F: FnOnce(&T) -> Option<U>>(&self, f: F) -> Option<U>;
}

impl<T> Pluck<T> for Option<T> {
    fn pluck<U, F: FnOnce(&T) -> Option<U>>(&self, f: F) -> Option<U> {
        self.as_ref().and_then(f)
    }
}
