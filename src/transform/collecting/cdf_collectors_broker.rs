use crate::extract::ContextualizedDataFrame;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::disease_collector::DiseaseCollector;
use crate::transform::collecting::hpo_in_cells_collector::HpoInCellsCollector;
use crate::transform::collecting::hpo_in_header_collector::HpoInHeaderCollector;
use crate::transform::collecting::individual_collector::IndividualCollector;
use crate::transform::collecting::interpretation_collector::InterpretationCollector;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use phenopackets::schema::v2::Phenopacket;

#[derive(Debug)]
pub struct CdfCollectorBroker {
    phenopacket_builder: PhenopacketBuilder,
    cohort_name: String,
    collectors: Vec<Box<dyn Collect>>,
}

impl CdfCollectorBroker {
    pub fn new(
        phenopacket_builder: PhenopacketBuilder,
        cohort_name: String,
        collectors: Vec<Box<dyn Collect>>,
    ) -> Self {
        CdfCollectorBroker {
            phenopacket_builder,
            cohort_name,
            collectors,
        }
    }

    pub fn process(
        &mut self,
        cdfs: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, CollectorError> {
        for cdf in cdfs {
            let subject_id_col = cdf.get_subject_id_col();

            let patient_dfs = cdf
                .data()
                .partition_by(vec![subject_id_col.name().as_str()], true)?;

            for patient_df in patient_dfs.iter() {
                let patient_cdf =
                    ContextualizedDataFrame::new(cdf.context().clone(), patient_df.clone())?;

                let patient_id = patient_cdf.get_subject_id_col().get(0)?.str_value();
                let phenopacket_id = self.generate_phenopacket_id(patient_id.as_ref());

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

    pub fn with_default_collectors(
        phenopacket_builder: PhenopacketBuilder,
        cohort_name: String,
    ) -> Self {
        CdfCollectorBroker::new(
            phenopacket_builder,
            cohort_name,
            vec![
                Box::new(IndividualCollector),
                Box::new(HpoInCellsCollector),
                Box::new(HpoInHeaderCollector),
                Box::new(InterpretationCollector),
                Box::new(DiseaseCollector),
            ],
        )
    }

    fn generate_phenopacket_id(&self, patient_id: &str) -> String {
        format!("{}-{}", self.cohort_name, patient_id)
    }
}

impl PartialEq for CdfCollectorBroker {
    fn eq(&self, other: &Self) -> bool {
        self.phenopacket_builder == other.phenopacket_builder
            && self.cohort_name == other.cohort_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::Context;
    use crate::extract::contextualized_dataframe_filters::Filter;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf};
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use rstest::{fixture, rstest};
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
    }

    fn build_test_cdf_broker(temp_dir: TempDir) -> CdfCollectorBroker {
        let builder = build_test_phenopacket_builder(temp_dir.path());
        let cohort_name = "cohort-1";

        CdfCollectorBroker::new(
            builder,
            cohort_name.to_string(),
            vec![
                Box::new(MockCollector::default()),
                Box::new(MockCollector::default()),
            ],
        )
    }

    #[rstest]
    fn test_process(temp_dir: TempDir) {
        let mut broker = build_test_cdf_broker(temp_dir);

        let cohort_name = broker.cohort_name.clone();
        let patient_cdf_1 = generate_minimal_cdf(2, 2);
        let patient_cdf_2 = generate_minimal_cdf(1, 5);

        broker.process(vec![patient_cdf_1, patient_cdf_2]).unwrap();

        for collector in broker.collectors {
            let mock = collector.as_any().downcast_ref::<MockCollector>().unwrap();

            assert_eq!(mock.call_count.get(), 3);

            let mut seen = mock.seen_pps.borrow().clone();
            seen.sort();

            let expected = [
                format!("{}-P0", cohort_name),
                format!("{}-P0", cohort_name),
                format!("{}-P1", cohort_name),
            ];
            assert_eq!(seen, expected);
            assert_eq!(mock.seen_pps.borrow().len(), 3);
        }
    }

    #[rstest]
    fn test_generate_phenopacket_id(temp_dir: TempDir) {
        let broker = build_test_cdf_broker(temp_dir);
        let p_id = default_patient_id();
        let cohort_name = broker.cohort_name.clone();

        assert_eq!(
            broker.generate_phenopacket_id(&p_id),
            format!("{}-{}", cohort_name, p_id)
        );
    }
}
