use crate::config::context::{Context, ContextKind};
use crate::constants::PolarsNumericTypes;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use crate::transform::utils::cow_cast;
use polars::datatypes::DataType;
use std::any::Any;

#[allow(dead_code)]
#[derive(Debug)]
pub struct QualitativeMeasurementCollector;

impl Collect for QualitativeMeasurementCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let qualitative_measurement_scs = patient_cdf
                .filter_series_context()
                .where_data_context_kind(Filter::Is(&ContextKind::QualitativeMeasurement))
                .collect();

            for qual_measurement_sc in qualitative_measurement_scs {
                let assay_id = qual_measurement_sc
                    .get_data_context()
                    .try_as_qualitative_measurement()
                    .map_err(|err| CollectorError::ContextError(err.to_string()))?;

                let qual_measurement_cols =
                    patient_cdf.get_columns(qual_measurement_sc.get_identifier());

                let time_observed_col = patient_cdf.get_single_linked_column_as_str(
                    qual_measurement_sc.get_building_block_id(),
                    Context::ONSET_VARIANTS,
                )?;

                for qual_measurement_col in qual_measurement_cols {
                    let allowed_datatypes = {
                        let mut v = vec![DataType::String, DataType::Null];
                        v.extend_from_slice(PolarsNumericTypes::ints());
                        v
                    };

                    let casted_qual_col =
                        cow_cast(qual_measurement_col, DataType::String, allowed_datatypes)?;

                    let stringified_qual_measurement_col = casted_qual_col.str()?;

                    for row_idx in 0..stringified_qual_measurement_col.len() {
                        let qual_measurement = stringified_qual_measurement_col.get(row_idx);
                        if let Some(qual_measurement) = qual_measurement {
                            let time_observed = if let Some(time_observed_col) = &time_observed_col
                            {
                                time_observed_col.get(row_idx)
                            } else {
                                None
                            };

                            builder.insert_qualitative_measurement(
                                patient_id,
                                qual_measurement,
                                time_observed,
                                assay_id,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::{Context, TimeElementType};
    use crate::config::table_context::SeriesContext;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf};
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::default_meta_data;
    use crate::test_suite::phenopacket_component_generation::{
        default_iso_age, default_pato_qual_measurement, default_phenopacket_id, default_qual_loinc,
        default_qual_measurement, generate_qual_measurement,
    };
    use crate::test_suite::resource_references::{
        loinc_meta_data_resource, pato_meta_data_resource,
    };
    use crate::test_suite::utils::assert_phenopackets;
    use crate::utils::phenopacket_schema_version;
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
            vec![default_pato_qual_measurement().label, pato_absent().label],
        );

        let time_observed = Series::new(
            "time_observed".into(),
            &[AnyValue::String(&default_iso_age()), AnyValue::Null],
        );

        patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("nitrate in urine".into())
                    .with_data_context(Context::QualitativeMeasurement {
                        assay_id: default_qual_loinc().id,
                    })
                    .with_building_block_id(Some("nitrate_measurement".to_string())),
                vec![measurements.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("time_observed".into())
                    .with_data_context(Context::Onset(TimeElementType::Age))
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
        let patient_id = default_patient_id();
        QualitativeMeasurementCollector
            .collect(&mut builder, &[qual_measurement_cdf()], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let measurement1 = default_qual_measurement();

        let measurement2 = generate_qual_measurement(default_qual_loinc(), pato_absent(), None);

        let mut expected_phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            measurements: vec![measurement1, measurement2],
            meta_data: Some(MetaData {
                phenopacket_schema_version: phenopacket_schema_version(),
                resources: vec![loinc_meta_data_resource(), pato_meta_data_resource()],
                created_by: default_meta_data().created_by,
                submitted_by: default_meta_data().submitted_by,
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
