use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::{CollectorError, DataProcessingError};
use phenopackets::schema::v2::Phenopacket;

#[derive(Debug)]
pub struct CdfBroker {
    phenopacket_builder: PhenopacketBuilder,
    cohort_name: String,
    collectors: Vec<Box<dyn Collect>>,
}

impl CdfBroker {
    pub fn new(
        phenopacket_builder: PhenopacketBuilder,
        cohort_name: String,
        collectors: Vec<Box<dyn Collect>>,
    ) -> Self {
        CdfBroker {
            phenopacket_builder,
            cohort_name,
            collectors,
        }
    }

    pub fn broker(
        &mut self,
        cdfs: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, CollectorError> {
        for cdf in cdfs {
            let subject_id_cols = cdf
                .filter_columns()
                .where_data_context(Filter::Is(&Context::SubjectId))
                .collect();
            if subject_id_cols.len() > 1 {
                return Err(CollectorError::ExpectedSingleColumn {
                    table_name: cdf.context().name().to_string(),
                    context: Context::SubjectId,
                });
            }

            let subject_id_col = subject_id_cols
                .last()
                .ok_or(DataProcessingError::EmptyFilteringError)?;

            let patient_dfs = cdf
                .data()
                .partition_by(vec![subject_id_col.name().as_str()], true)?;

            for patient_df in patient_dfs.iter() {
                let patient_id = patient_df
                    .column(subject_id_col.name())?
                    .get(0)?
                    .str_value();

                let phenopacket_id = format!("{}-{}", self.cohort_name, patient_id);

                let patient_cdf =
                    ContextualizedDataFrame::new(cdf.context().clone(), patient_df.clone())?;

                for collector in &self.collectors {
                    collector.collect(
                        &mut self.phenopacket_builder,
                        &patient_cdf,
                        phenopacket_id.as_str(),
                    )?;
                }
            }
        }

        Ok(self.phenopacket_builder.build())
    }
}

impl PartialEq for CdfBroker {
    fn eq(&self, other: &Self) -> bool {
        self.phenopacket_builder == other.phenopacket_builder
            && self.cohort_name == other.cohort_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{build_test_phenopacket_builder, generate_minimal_cdf};
    use rstest::{fixture, rstest};
    use std::any::Any;
    use std::cell::{Cell, RefCell};
    use std::fmt::Debug;
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[derive(Default, Debug)]
    struct MockCollector {
        pub call_count: Cell<usize>,
        pub seen_pps: RefCell<Vec<String>>,
    }

    impl Collect for MockCollector {
        fn collect(
            &self,
            _: &mut PhenopacketBuilder,
            cdf: &ContextualizedDataFrame,
            phenopacket_id: &str,
        ) -> Result<(), CollectorError> {
            self.call_count.set(self.call_count.get() + 1);
            self.seen_pps.borrow_mut().push(phenopacket_id.to_string());

            let subject_col = cdf
                .filter_columns()
                .where_data_context(Filter::Is(&Context::SubjectId))
                .collect();

            let unique_patient_ids = subject_col.first().unwrap().unique()?;
            assert_eq!(unique_patient_ids.str()?.len(), 1);

            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[rstest]
    fn test_collecting_broker(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());
        let patient_cdf_1 = generate_minimal_cdf(2, 1);
        let patient_cdf_2 = generate_minimal_cdf(1, 5);

        let mut broker = CdfBroker::new(
            builder,
            "cohort-1".to_string(),
            vec![
                Box::new(MockCollector::default()),
                Box::new(MockCollector::default()),
            ],
        );

        broker.broker(vec![patient_cdf_1, patient_cdf_2]).unwrap();

        for collector in broker.collectors {
            if let Some(mock) = collector.as_any().downcast_ref::<MockCollector>() {
                assert_eq!(mock.call_count.get(), 3);
                assert_eq!(mock.seen_pps.borrow().len(), 3);
            }
        }
    }
}
