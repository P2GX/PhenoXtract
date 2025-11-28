use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::{CollectorError, DataProcessingError};
use phenopackets::schema::v2::Phenopacket;

#[derive(Debug)]
pub struct CDFBroker {
    phenopacket_builder: PhenopacketBuilder,
    cohort_name: String,
    collectors: Vec<Box<dyn Collect>>,
}

impl CDFBroker {
    pub fn new(
        phenopacket_builder: PhenopacketBuilder,
        cohort_name: String,
        collectors: Vec<Box<dyn Collect>>,
    ) -> Self {
        CDFBroker {
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

impl PartialEq for CDFBroker {
    fn eq(&self, other: &Self) -> bool {
        self.phenopacket_builder == other.phenopacket_builder
            && self.cohort_name == other.cohort_name
    }
}
