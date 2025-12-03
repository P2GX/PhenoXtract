use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::utils::HpoColMaker;
use log::warn;
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
            .where_header_context_type(Filter::Is(&Context::QuantitativeMeasurement { loinc_id: "".to_string(), unit_ontology_id: "".to_string() }.to_string()))
            .collect();

        for hpo_sc in hpo_term_in_header_scs {
            let sc_id = hpo_sc.get_identifier();
            let hpo_cols = patient_cdf.get_columns(sc_id);

            let stringified_linked_onset_col = patient_cdf.get_single_linked_column(
                hpo_sc.get_building_block_id(),
                &[Context::OnsetAge, Context::OnsetDate],
            )?;

            for hpo_col in hpo_cols {
                let hpo_id = HpoColMaker::new().decode_column_header(hpo_col).0;

                let boolified_hpo_col = hpo_col.bool()?;

                let mut seen_pairs = HashSet::new();

                for row_idx in 0..boolified_hpo_col.len() {
                    let obs_status = boolified_hpo_col.get(row_idx);
                    let onset = if let Some(onset_col) = &stringified_linked_onset_col {
                        onset_col.get(row_idx)
                    } else {
                        None
                    };
                    seen_pairs.insert((obs_status, onset));
                }

                seen_pairs.remove(&(None, None));

                if seen_pairs.len() == 1 {
                    let (obs_status, onset) = seen_pairs.into_iter().next().unwrap();
                    //if the observation_status is None, no phenotype is upserted
                    //if the observation_status is true, the phenotype is upserted with excluded = None
                    //if the observation_status is false, the phenotype is upserted with excluded = true
                    if let Some(obs_status) = obs_status {
                        let excluded = if obs_status { None } else { Some(true) };
                        builder.upsert_phenotypic_feature(
                            phenopacket_id,
                            hpo_id,
                            None,
                            excluded,
                            None,
                            None,
                            onset,
                            None,
                            None,
                        )?;
                    } else if let Some(onset) = onset {
                        warn!(
                            "Non-null onset {onset} found for null observation status for patient {patient_id}."
                        )
                    }
                } else if seen_pairs.len() > 2 {
                    return Err(CollectorError::ExpectedUniquePhenotypeData {
                        table_name: patient_cdf.context().name().to_string(),
                        patient_id: patient_id.to_string(),
                        phenotype: hpo_id.to_string(),
                    });
                }
            }
        }

        Ok(())
    }
}