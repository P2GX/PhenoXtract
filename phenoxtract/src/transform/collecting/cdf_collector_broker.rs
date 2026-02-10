use crate::extract::ContextualizedDataFrame;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::disease_collector::DiseaseCollector;
use crate::transform::collecting::hpo_in_cells_collector::HpoInCellsCollector;
use crate::transform::collecting::hpo_in_header_collector::HpoInHeaderCollector;
use crate::transform::collecting::individual_collector::IndividualCollector;
use crate::transform::collecting::interpretation_collector::InterpretationCollector;
use crate::transform::collecting::medical_actions::medical_procedure_collector::MedicalProcedureCollector;
use crate::transform::collecting::qualitative_measurement_collector::QualitativeMeasurementCollector;
use crate::transform::collecting::quantitative_measurement_collector::QuantitativeMeasurementCollector;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use phenopackets::schema::v2::Phenopacket;
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
                let mut patient_cdf =
                    ContextualizedDataFrame::new(cdf.context().clone(), patient_df.clone())?;

                patient_cdf
                    .builder()
                    .drop_null_cols_alongside_scs()?
                    .build()?;

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
                Box::new(MedicalProcedureCollector),
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

        let self_ids: Vec<_> = self
            .collectors
            .iter()
            .map(|col| col.as_any().type_id())
            .collect();

        let other_ids: Vec<_> = other
            .collectors
            .iter()
            .map(|col| col.as_any().type_id())
            .collect();

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
    use crate::test_suite::mocks::MockCollector;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    fn build_test_cdf_broker(temp_dir: TempDir) -> CdfCollectorBroker {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        let mut mock1 = MockCollector::new();
        let mut mock2 = MockCollector::new();

        mock1
            .expect_collect()
            .returning(|_, patient_cdfs, _phenopacket_id| {
                for cdf in patient_cdfs {
                    let subject_col = cdf
                        .filter_columns()
                        .where_data_context(Filter::Is(&Context::SubjectId))
                        .collect();
                    let unique_patient_ids = subject_col.first().unwrap().unique()?;
                    assert_eq!(unique_patient_ids.str()?.len(), 1);
                }
                Ok(())
            })
            .times(2);

        mock2
            .expect_collect()
            .returning(|_, patient_cdfs, _phenopacket_id| {
                for cdf in patient_cdfs {
                    let subject_col = cdf
                        .filter_columns()
                        .where_data_context(Filter::Is(&Context::SubjectId))
                        .collect();
                    let unique_patient_ids = subject_col.first().unwrap().unique()?;
                    assert_eq!(unique_patient_ids.str()?.len(), 1);
                }
                Ok(())
            })
            .times(2);

        CdfCollectorBroker::new(builder, vec![Box::new(mock1), Box::new(mock2)])
    }

    #[rstest]
    fn test_process(temp_dir: TempDir) {
        let mut broker = build_test_cdf_broker(temp_dir);

        let cdf1 = generate_minimal_cdf(2, 2);
        let cdf2 = generate_minimal_cdf(1, 5);

        broker.process(vec![cdf1, cdf2]).unwrap();

        // The expectations are verified when the mocks are dropped
    }
}
