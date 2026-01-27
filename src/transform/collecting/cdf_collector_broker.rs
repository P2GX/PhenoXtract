use crate::extract::ContextualizedDataFrame;
use crate::transform;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::disease_collector::DiseaseCollector;
use crate::transform::collecting::hpo_in_cells_collector::HpoInCellsCollector;
use crate::transform::collecting::hpo_in_header_collector::HpoInHeaderCollector;
use crate::transform::collecting::individual_collector::IndividualCollector;
use crate::transform::collecting::interpretation_collector::InterpretationCollector;
use crate::transform::collecting::qualitative_measurement_collector::QualitativeMeasurementCollector;
use crate::transform::collecting::quantitative_measurement_collector::QuantitativeMeasurementCollector;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use phenopackets::schema::v2::Phenopacket;
use std::any::Any;
use std::collections::HashMap;

#[derive(Debug)]
pub struct CdfCollectorBroker {
    phenopacket_builder: PhenopacketBuilder,
    collectors: Vec<Box<dyn Collect>>,
}

impl CdfCollectorBroker {
    pub fn new(phenopacket_builder: PhenopacketBuilder, collectors: Vec<Box<dyn Collect>>) -> Self {
        CdfCollectorBroker {
            phenopacket_builder,
            collectors,
        }
    }

    pub fn process(
        &mut self,
        cdfs: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, CollectorError> {
        let mut patient_id_to_dfs: HashMap<String, Vec<ContextualizedDataFrame>> = HashMap::new();

        for cdf in cdfs {
            let subject_id_col = cdf.get_subject_id_col();

            let patient_dfs = cdf
                .data()
                .partition_by(vec![subject_id_col.name().as_str()], true)?;

            for patient_df in patient_dfs.iter() {
                let patient_cdf =
                    ContextualizedDataFrame::new(cdf.context().clone(), patient_df.clone())?;

                let patient_id = patient_cdf.get_subject_id_col().get(0)?.str_value();

                patient_id_to_dfs
                    .entry(patient_id.to_string())
                    .or_default()
                    .push(patient_cdf);
            }
        }

        for (patient_id, patient_cdfs) in patient_id_to_dfs {
            for collector in &mut self.collectors {
                collector.collect(
                    &mut self.phenopacket_builder,
                    &patient_cdfs,
                    patient_id.as_str(),
                )?;
            }
        }

        Ok(self.phenopacket_builder.build())
    }

    pub fn with_default_collectors(phenopacket_builder: PhenopacketBuilder) -> Self {
        CdfCollectorBroker::new(
            phenopacket_builder,
            vec![
                Box::new(IndividualCollector),
                Box::new(HpoInCellsCollector),
                Box::new(HpoInHeaderCollector),
                Box::new(InterpretationCollector),
                Box::new(DiseaseCollector),
                Box::new(QuantitativeMeasurementCollector),
                Box::new(QualitativeMeasurementCollector),
            ],
        )
    }
}
impl PartialEq for CdfCollectorBroker {
    fn eq(&self, other: &Self) -> bool {
        if self.phenopacket_builder != other.phenopacket_builder {
            return false;
        }

        if self.collectors.len() != other.collectors.len() {
            return false;
        }

        let mut self_ids: Vec<_> = self
            .collectors
            .iter()
            .map(|col| transform::collecting::traits::AsAny::as_any(col).type_id())
            .collect();

        let mut other_ids: Vec<_> = other
            .collectors
            .iter()
            .map(|col| transform::collecting::traits::AsAny::as_any(col).type_id())
            .collect();

        self_ids.sort();
        other_ids.sort();

        self_ids == other_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::Context;
    use crate::extract::contextualized_dataframe_filters::Filter;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::transform;
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
            patient_cdfs: &[ContextualizedDataFrame],
            phenopacket_id: &str,
        ) -> Result<(), CollectorError> {
            self.call_count.set(self.call_count.get() + 1);
            self.seen_pps.borrow_mut().push(phenopacket_id.to_string());

            for cdf in patient_cdfs {
                let subject_col = cdf
                    .filter_columns()
                    .where_data_context(Filter::Is(&Context::SubjectId))
                    .collect();
                let unique_patient_ids = subject_col.first().unwrap().unique()?;
                assert_eq!(unique_patient_ids.str()?.len(), 1);
            }

            Ok(())
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn build_test_cdf_broker(temp_dir: TempDir) -> CdfCollectorBroker {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        CdfCollectorBroker::new(
            builder,
            vec![
                Box::new(MockCollector::default()),
                Box::new(MockCollector::default()),
            ],
        )
    }

    #[rstest]
    fn test_process(temp_dir: TempDir) {
        let mut broker = build_test_cdf_broker(temp_dir);

        let patient_cdf_1 = generate_minimal_cdf(2, 2);
        let patient_cdf_2 = generate_minimal_cdf(1, 5);

        broker.process(vec![patient_cdf_1, patient_cdf_2]).unwrap();

        for collector in broker.collectors {
            let mock = transform::collecting::traits::AsAny::as_any(&collector)
                .downcast_ref::<MockCollector>()
                .unwrap();

            assert_eq!(mock.call_count.get(), 2);

            let mut seen = mock.seen_pps.borrow().clone();
            seen.sort();

            let expected = ["P0".to_string(), "P1".to_string()];
            assert_eq!(seen, expected);
            assert_eq!(mock.seen_pps.borrow().len(), 2);
        }
    }
}
