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
                let loinc_id = qual_measurement_sc
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
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_age_element, default_iso_age, default_phenopacket_id, default_qual_loinc,
        generate_qual_measurement,
    };
    use crate::test_suite::resource_references::{
        loinc_meta_data_resource, pato_meta_data_resource,
    };
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::{MetaData, OntologyClass};
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn pato_present() -> OntologyClass {
        OntologyClass {
            id: "PATO:0000467".to_string(),
            label: "present".to_string(),
        }
    }

    #[fixture]
    fn pato_absent() -> OntologyClass {
        OntologyClass {
            id: "PATO:0000462".to_string(),
            label: "absent".to_string(),
        }
    }

    #[fixture]
    fn qual_measurement_cdf() -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let measurements = Series::new(
            "nitrate in urine".into(),
            vec![pato_present().label, pato_absent().label],
        );

        let time_observed = Series::new(
            "time_observed".into(),
            &[AnyValue::Null, AnyValue::String(&default_iso_age())],
        );

        patient_cdf
            .builder()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("nitrate in urine".into())
                    .with_data_context(Context::QualitativeMeasurement {
                        loinc_id: default_qual_loinc().id,
                    })
                    .with_building_block_id(Some("nitrate_measurement".to_string())),
                vec![measurements.into_column()].as_ref(),
            )
            .unwrap()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("time_observed".into())
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("nitrate_measurement".to_string())),
                vec![time_observed.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap()
            .clone()
    }

    #[rstest]
    fn test_collect_qualitative_measurement(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = default_phenopacket_id();
        QualitativeMeasurementCollector
            .collect(&mut builder, &[qual_measurement_cdf()], &pp_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let measurement1 = generate_qual_measurement(default_qual_loinc(), pato_present(), None);

        let measurement2 = generate_qual_measurement(
            default_qual_loinc(),
            pato_absent(),
            Some(default_age_element()),
        );

        let mut expected_phenopacket = Phenopacket {
            id: pp_id,
            measurements: vec![measurement1, measurement2],
            meta_data: Some(MetaData {
                resources: vec![loinc_meta_data_resource(), pato_meta_data_resource()],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
