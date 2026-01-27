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
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let quantitative_measurement_scs = patient_cdf
                .filter_series_context()
                .where_data_context_kind(Filter::Is(&ContextKind::QuantitativeMeasurement))
                .collect();

            for quant_measurement_sc in quantitative_measurement_scs {
                let (assay_id, unit_ontology_id) = quant_measurement_sc
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
                                patient_id,
                                quant_measurement,
                                time_observed,
                                assay_id,
                                unit_ontology_id,
                                ref_low.zip(ref_high),
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf};
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::config::default_config_meta_data;
    use crate::test_suite::phenopacket_component_generation::{
        default_iso_age, default_phenopacket_id, default_quant_loinc, default_quant_measurement,
        default_quant_value, default_reference_range, default_uo_term, generate_quant_measurement,
    };
    use crate::test_suite::resource_references::{loinc_meta_data_resource, uo_meta_data_resource};
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::MetaData;
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn measurements() -> [f64; 2] {
        [default_quant_value(), 2.2]
    }

    #[fixture]
    fn quant_measurement_cdf() -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let measurements = Series::new("height".into(), measurements());

        let time_observed = Series::new(
            "time_observed".into(),
            &[AnyValue::String(&default_iso_age()), AnyValue::Null],
        );

        let ref_low = Series::new(
            "ref_low".into(),
            &[
                AnyValue::Float64(default_reference_range().0),
                AnyValue::Null,
            ],
        );

        let ref_high = Series::new(
            "ref_high".into(),
            &[
                AnyValue::Float64(default_reference_range().1),
                AnyValue::Null,
            ],
        );

        patient_cdf
            .builder()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("height".into())
                    .with_data_context(Context::QuantitativeMeasurement {
                        assay_id: default_quant_loinc().id,
                        unit_ontology_id: default_uo_term().id,
                    })
                    .with_building_block_id(Some("height_measurement".to_string())),
                vec![measurements.into_column()].as_ref(),
            )
            .unwrap()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("time_observed".into())
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("height_measurement".to_string())),
                vec![time_observed.into_column()].as_ref(),
            )
            .unwrap()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("ref_low".into())
                    .with_data_context(Context::ReferenceRangeLow)
                    .with_building_block_id(Some("height_measurement".to_string())),
                vec![ref_low.into_column()].as_ref(),
            )
            .unwrap()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("ref_high".into())
                    .with_data_context(Context::ReferenceRangeHigh)
                    .with_building_block_id(Some("height_measurement".to_string())),
                vec![ref_high.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap()
            .clone()
    }

    #[rstest]
    fn test_collect_quantitative_measurement(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let patient_id = default_patient_id();
        QuantitativeMeasurementCollector
            .collect(&mut builder, &[quant_measurement_cdf()], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let measurement1 = default_quant_measurement();

        let measurement2 = generate_quant_measurement(
            default_quant_loinc(),
            measurements()[1],
            None,
            default_uo_term().id.as_str(),
            None,
        );

        let mut expected_phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            measurements: vec![measurement1, measurement2],
            meta_data: Some(MetaData {
                resources: vec![loinc_meta_data_resource(), uo_meta_data_resource()],
                submitted_by: default_config_meta_data().submitted_by,
                created_by: default_config_meta_data().created_by,
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
