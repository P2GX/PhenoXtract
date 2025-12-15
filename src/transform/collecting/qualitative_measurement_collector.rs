use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;

#[allow(dead_code)]
#[derive(Debug)]
pub struct QualitativeMeasurementCollector;

impl Collect for QualitativeMeasurementCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdfs: &[ContextualizedDataFrame],
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let qualitative_measurement_scs = patient_cdf
                .filter_series_context()
                .where_data_context_kind(Filter::Is(&ContextKind::QualitativeMeasurement))
                .collect();

            for qual_measurement_sc in qualitative_measurement_scs {
                let (loinc_id, unit_ontology_prefix) = qual_measurement_sc
                    .get_data_context()
                    .try_as_qualitative_measurement()
                    .map_err(|err| CollectorError::ContextError(err.to_string()))?;

                let qual_measurement_cols =
                    patient_cdf.get_columns(qual_measurement_sc.get_identifier());

                let time_observed_col = patient_cdf.get_single_linked_column_as_str(
                    qual_measurement_sc.get_building_block_id(),
                    &[Context::OnsetAge, Context::OnsetDate],
                )?;

                for qual_measurement_col in qual_measurement_cols {
                    let stringified_quant_measurement_col = qual_measurement_col.str()?;

                    for row_idx in 0..stringified_quant_measurement_col.len() {
                        let qual_measurement = stringified_quant_measurement_col.get(row_idx);
                        if let Some(qual_measurement) = qual_measurement {
                            let time_observed = if let Some(time_observed_col) = &time_observed_col
                            {
                                time_observed_col.get(row_idx)
                            } else {
                                None
                            };

                            builder.insert_qualitative_measurement(
                                phenopacket_id,
                                qual_measurement,
                                time_observed,
                                loinc_id,
                                unit_ontology_prefix,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
