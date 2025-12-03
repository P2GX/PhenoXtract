use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::utils::HpoColMaker;
use log::warn;
use polars::polars_utils::nulls::IsNull;
use std::collections::HashSet;

#[derive(Debug)]
pub struct QuantitativeMeasurementCollector;

impl Collect for QuantitativeMeasurementCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        let patient_id = patient_cdf
            .get_subject_id_col()
            .get(0)
            .expect("Should have one patient id")
            .to_string();

        let quantitative_measurement_scs = patient_cdf
            .filter_series_context()
            .where_data_context_kind(Filter::Is(&ContextKind::QuantitativeMeasurement))
            .collect();

        for quant_measurement_sc in quantitative_measurement_scs {
            let (loinc_id, unit_ontology_id) = quant_measurement_sc
                .get_data_context()
                .try_as_quantitative_measurement()?;

            let quant_measurement_cols = patient_cdf.get_columns(quant_measurement_sc.get_identifier());

            let observation_time_col = patient_cdf.get_single_linked_column(
                quant_measurement_sc.get_building_block_id(),
                &[Context::OnsetAge, Context::OnsetDate],
            )?;


            for quant_measurement_col in quant_measurement_cols {
                let floatified_quant_measurement_col = quant_measurement_col.f64()?;

                for row_idx in 0..floatified_quant_measurement_col.len() {
                    let quant_measurement = floatified_quant_measurement_col.get(row_idx);
                    if let Some(quant_measurement) = quant_measurement {
                        let observation_time = if let Some(observation_time_col) = &observation_time_col {
                            observation_time_col.get(row_idx)
                        } else {
                            None
                        };

                        /*builder.upsert_phenotypic_feature(
                            phenopacket_id,
                            hpo,
                            None,
                            None,
                            None,
                            None,
                            hpo_onset,
                            None,
                            None,
                        )?;*/
                        //todo!
                    }
                }
            }
        }

        Ok(())
    }
}
