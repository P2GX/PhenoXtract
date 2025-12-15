use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;

#[allow(dead_code)]
#[derive(Debug)]
pub struct QuantitativeMeasurementCollector;

impl Collect for QuantitativeMeasurementCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdfs: &[ContextualizedDataFrame],
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let quantitative_measurement_scs = patient_cdf
                .filter_series_context()
                .where_data_context_kind(Filter::Is(&ContextKind::QuantitativeMeasurement))
                .collect();

            for quant_measurement_sc in quantitative_measurement_scs {
                let (loinc_id, unit_ontology_id) = quant_measurement_sc
                    .get_data_context()
                    .try_as_quantitative_measurement()
                    .map_err(|err| CollectorError::ContextError(err.to_string()))?;

                let quant_measurement_cols =
                    patient_cdf.get_columns(quant_measurement_sc.get_identifier());

                let time_observed_col = patient_cdf.get_single_linked_column_as_str(
                    quant_measurement_sc.get_building_block_id(),
                    &[Context::OnsetAge, Context::OnsetDate],
                )?;

                let ref_low_col = patient_cdf.get_single_linked_column_as_float(
                    quant_measurement_sc.get_building_block_id(),
                    &[Context::ReferenceRangeLow],
                )?;

                let ref_high_col = patient_cdf.get_single_linked_column_as_float(
                    quant_measurement_sc.get_building_block_id(),
                    &[Context::ReferenceRangeHigh],
                )?;

                for quant_measurement_col in quant_measurement_cols {
                    let floatified_quant_measurement_col = quant_measurement_col.f64()?;

                    for row_idx in 0..floatified_quant_measurement_col.len() {
                        let quant_measurement = floatified_quant_measurement_col.get(row_idx);
                        if let Some(quant_measurement) = quant_measurement {
                            let time_observed = if let Some(time_observed_col) = &time_observed_col
                            {
                                time_observed_col.get(row_idx)
                            } else {
                                None
                            };
                            let ref_low = if let Some(ref_low_col) = &ref_low_col {
                                ref_low_col.get(row_idx)
                            } else {
                                None
                            };
                            let ref_high = if let Some(ref_high_col) = &ref_high_col {
                                ref_high_col.get(row_idx)
                            } else {
                                None
                            };

                            builder.insert_quantitative_measurement(
                                phenopacket_id,
                                quant_measurement,
                                time_observed,
                                loinc_id,
                                unit_ontology_id,
                                ref_low,
                                ref_high,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
