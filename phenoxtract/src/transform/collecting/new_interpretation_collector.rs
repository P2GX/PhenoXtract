use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::collecting::traits::Collect;
use crate::transform::collecting::utils::get_single_multiplicity_element;
use crate::transform::error::CollectorError;
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use crate::transform::traits::PhenopacketBuilding;
use std::any::Any;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct NewInterpretationCollector;

impl Collect for NewInterpretationCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        let subject_sex =
            get_single_multiplicity_element(patient_cdfs, &Context::SubjectSex, &Context::None)?;

        // STEP 1: COLLECT DISEASE BB_IDS
        let mut disease_bb_ids = HashSet::new();

        for patient_cdf in patient_cdfs {
            let disease_in_cells_scs = patient_cdf
                .filter_series_context()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::Disease))
                .collect();

            for disease_sc in disease_in_cells_scs {
                let bb_id = disease_sc.get_building_block_id();
                if let Some(bb_id) = bb_id {
                    disease_bb_ids.insert(bb_id);
                }
            }
        }

        // STEP 2: FIGURE OUT HOW MANY CDFS ARE RELEVANT TO EACH DISEASE_BB_ID
        let mut disease_bb_id_to_cdf = HashMap::new();

        for bb_id in disease_bb_ids {
            for patient_cdf in patient_cdfs {
                let relevant_cols = patient_cdf
                    .filter_columns()
                    .where_building_block(Filter::Is(&bb_id))
                    .collect();
                if !relevant_cols.is_empty() {
                    disease_bb_id_to_cdf
                        .entry(bb_id.to_string())
                        .or_insert_with(Vec::new)
                        .push(patient_cdf.context().name().to_string());
                }
            }
        }

        // STEP 3: decide which form of collection to apply

        for (bb_id, cdf_names) in disease_bb_id_to_cdf {
            if cdf_names.len() == 1 {
                let cdf = patient_cdfs
                    .iter()
                    .filter(|cdf| cdf.context().name() == cdf_names[0])
                    .collect::<Vec<&ContextualizedDataFrame>>()[0];
                Self::collect_single_sheet_disease_building_block(
                    builder,
                    cdf,
                    patient_id,
                    bb_id,
                    &subject_sex,
                )?;
            } else {
                Self::collect_multi_sheet_disease_building_block(
                    builder,
                    patient_cdfs,
                    patient_id,
                    bb_id,
                    &subject_sex,
                )?;
            }
        }

        Ok(())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl NewInterpretationCollector {
    fn collect_single_sheet_disease_building_block(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdf: &ContextualizedDataFrame,
        patient_id: &str,
        bb_id: String,
        subject_sex: &Option<String>,
    ) -> Result<(), CollectorError> {
        let disease_cols = patient_cdf
            .filter_columns()
            .where_building_block(Filter::Is(&bb_id))
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::Disease))
            .collect();

        let stringified_linked_hgnc_cols = patient_cdf.get_stringified_cols(
            patient_cdf.get_linked_cols_with_context(Some(&bb_id), &Context::Hgnc, &Context::None),
        )?;
        let stringified_linked_hgvs_cols = patient_cdf.get_stringified_cols(
            patient_cdf.get_linked_cols_with_context(Some(&bb_id), &Context::Hgvs, &Context::None),
        )?;

        for row_idx in 0..patient_cdf.data().height() {
            let genes = stringified_linked_hgnc_cols
                .iter()
                .filter_map(|hgnc_col| hgnc_col.get(row_idx))
                .collect::<Vec<&str>>();
            let variants = stringified_linked_hgvs_cols
                .iter()
                .filter_map(|hgvs_col| hgvs_col.get(row_idx))
                .collect::<Vec<&str>>();

            let gene_variant_data =
                PathogenicGeneVariantData::from_genes_and_variants(genes, variants)
                    .map_err(CollectorError::GeneVariantData)?;

            if matches!(gene_variant_data, PathogenicGeneVariantData::None) {
                continue;
            }

            for disease_col in disease_cols.iter() {
                let stringified_disease_col = disease_col.str()?;

                let disease = stringified_disease_col.get(row_idx);
                if let Some(disease) = disease {
                    builder.upsert_interpretation(
                        patient_id,
                        disease,
                        &gene_variant_data,
                        subject_sex.clone(),
                    )?;
                }
            }
        }

        Ok(())
    }

    fn collect_multi_sheet_disease_building_block(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
        bb_id: String,
        subject_sex: &Option<String>,
    ) -> Result<(), CollectorError> {
        //TODO
        let disease = get_single_multiplicity_element(
            patient_cdfs,
            &Context::Disease,
            &Context::None,
            Some(&bb_id),
        ).map_err(|_|CollectorError::TemporaryError)?;
        
        //TODO I think it's too complicated. Explain to Rouven tomorrow
        
        
        //update collect_single_multiplicity_element so it can collect an SME within a BB.
        //collect the single disease (needs to be just one)
        //Then collect all other genetic info, it will be assumed to be in relation to that disease
        Ok(())
    }
}
