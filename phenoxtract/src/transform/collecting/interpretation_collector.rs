use crate::config::context::Context;
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
pub struct InterpretationCollector;

impl Collect for InterpretationCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        let subject_sex = get_single_multiplicity_element(
            patient_cdfs,
            &Context::SubjectSex,
            &Context::None,
            None,
        )?;

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
                    .where_building_block(Filter::Is(bb_id))
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

impl InterpretationCollector {
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
            .collect_owned_names();

        let stringified_disease_cols = patient_cdf.get_stringified_cols(disease_cols)?;

        let resolve_diseases = |row_idx| {
            let mut diseases = vec![];

            for disease_col in &stringified_disease_cols {
                if let Some(disease) = disease_col.get(row_idx) {
                    diseases.push(disease)
                }
            }
            diseases
        };

        Self::collect_genes_and_variants_in_bb(
            builder,
            patient_cdf,
            patient_id,
            &bb_id,
            subject_sex,
            resolve_diseases,
        )
    }

    fn collect_multi_sheet_disease_building_block(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
        bb_id: String,
        subject_sex: &Option<String>,
    ) -> Result<(), CollectorError> {
        let disease = get_single_multiplicity_element(
            patient_cdfs,
            &Context::Disease,
            &Context::None,
            Some(&bb_id),
        )
        .map_err(|_| CollectorError::InterpretationBlockFormat {
            patient_id: patient_id.to_string(),
            bb_id: bb_id.clone(),
        })?;

        if let Some(disease) = disease {
            let resolve_disease = |_| vec![disease.as_str()];

            for patient_cdf in patient_cdfs {
                Self::collect_genes_and_variants_in_bb(
                    builder,
                    patient_cdf,
                    patient_id,
                    &bb_id,
                    subject_sex,
                    resolve_disease,
                )?;
            }
        }

        Ok(())
    }

    fn collect_genes_and_variants_in_bb<'a, F>(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdf: &'a ContextualizedDataFrame,
        patient_id: &str,
        bb_id: &str,
        subject_sex: &Option<String>,
        mut resolve_disease: F,
    ) -> Result<(), CollectorError>
    where
        F: FnMut(usize) -> Vec<&'a str>,
    {
        let stringified_linked_hgnc_cols = patient_cdf.get_stringified_cols(
            patient_cdf.get_linked_cols_with_context(Some(bb_id), &Context::Hgnc, &Context::None),
        )?;

        let stringified_linked_hgvs_cols = patient_cdf.get_stringified_cols(
            patient_cdf.get_linked_cols_with_context(Some(bb_id), &Context::Hgvs, &Context::None),
        )?;

        for row_idx in 0..patient_cdf.data().height() {
            let genes = stringified_linked_hgnc_cols
                .iter()
                .filter_map(|col| col.get(row_idx))
                .collect::<Vec<&str>>();

            let variants = stringified_linked_hgvs_cols
                .iter()
                .filter_map(|col| col.get(row_idx))
                .collect::<Vec<&str>>();

            let gene_variant_data =
                PathogenicGeneVariantData::from_genes_and_variants(genes, variants)
                    .map_err(CollectorError::GeneVariantData)?;

            if matches!(gene_variant_data, PathogenicGeneVariantData::None) {
                continue;
            }

            for disease in resolve_disease(row_idx) {
                builder.upsert_interpretation(
                    patient_id,
                    disease,
                    &gene_variant_data,
                    subject_sex.clone(),
                )?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableContext;
    use crate::config::table_context::SeriesContext;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf_components};
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::default_meta_data;
    use crate::test_suite::phenopacket_component_generation::{
        default_cohort_id, default_disease_oc, default_phenopacket_id,
    };
    use crate::test_suite::resource_references::{
        geno_meta_data_resource, hgnc_meta_data_resource, mondo_meta_data_resource,
    };
    use crate::test_suite::utils::assert_phenopackets;
    use crate::utils::phenopacket_schema_version;
    use phenopackets::ga4gh::vrsatile::v1::{Expression, GeneDescriptor, VcfRecord};
    use phenopackets::ga4gh::vrsatile::v1::{MoleculeContext, VariationDescriptor};
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::AcmgPathogenicityClassification;
    use phenopackets::schema::v2::core::TherapeuticActionability;
    use phenopackets::schema::v2::core::genomic_interpretation::Call;
    use phenopackets::schema::v2::core::{
        Diagnosis, GenomicInterpretation, Interpretation, MetaData, OntologyClass,
        VariantInterpretation,
    };
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};

    #[fixture]
    fn dysostosis_interpretation() -> Interpretation {
        let cohort_id = default_cohort_id();
        let patient_id = default_patient_id();

        Interpretation {
            id: format!("{}-{}-MONDO:0000359", cohort_id, patient_id).to_string(),
            progress_status: 0,
            diagnosis: Some(Diagnosis {
                disease: Some(default_disease_oc()),
                genomic_interpretations: vec![GenomicInterpretation {
                    subject_or_biosample_id: patient_id,
                    interpretation_status: 0,
                    call: Some(Call::VariantInterpretation(VariantInterpretation {
                        acmg_pathogenicity_classification:
                            AcmgPathogenicityClassification::Pathogenic as i32,
                        therapeutic_actionability: TherapeuticActionability::UnknownActionability
                            as i32,
                        variation_descriptor: Some(VariationDescriptor {
                            id: "c2860CtoT_KIF21A_NM_001173464v1".to_string(),
                            variation: None,
                            label: "".to_string(),
                            description: "".to_string(),
                            gene_context: Some(GeneDescriptor {
                                value_id: "HGNC:19349".to_string(),
                                symbol: "KIF21A".to_string(),
                                description: "".to_string(),
                                alternate_ids: Vec::new(),
                                alternate_symbols: Vec::new(),
                                xrefs: Vec::new(),
                            }),
                            expressions: vec![
                                Expression {
                                    syntax: "hgvs.c".to_string(),
                                    value: "NM_001173464.1:c.2860C>T".to_string(),
                                    version: "".to_string(),
                                },
                                Expression {
                                    syntax: "hgvs.g".to_string(),
                                    value: "NC_000012.12:g.39332405G>A".to_string(),
                                    version: "".to_string(),
                                },
                                Expression {
                                    syntax: "hgvs.p".to_string(),
                                    value: "NP_001166935.1:p.(Arg954Trp)".to_string(),
                                    version: "".to_string(),
                                },
                            ],
                            vcf_record: Some(VcfRecord {
                                genome_assembly: "hg38".to_string(),
                                chrom: "chr12".to_string(),
                                pos: 39332405,
                                id: "".to_string(),
                                r#ref: "G".to_string(),
                                alt: "A".to_string(),
                                qual: "".to_string(),
                                filter: "".to_string(),
                                info: "".to_string(),
                            }),
                            xrefs: vec![],
                            alternate_labels: vec![],
                            extensions: vec![],
                            molecule_context: MoleculeContext::Genomic as i32,
                            structural_type: None,
                            vrs_ref_allele_seq: "".to_string(),
                            allelic_state: Some(OntologyClass {
                                id: "GENO:0000136".to_string(),
                                label: "homozygous".to_string(),
                            }),
                        }),
                    })),
                }],
            }),
            summary: "".to_string(),
        }
    }

    #[rstest]
    fn test_collect_interpretations(dysostosis_interpretation: Interpretation) {
        let (patient_col, patient_sc) = generate_minimal_cdf_components(1, 1);
        let disease_col = Column::new(
            "diseases".into(),
            [AnyValue::String(default_disease_oc().label.as_str())],
        );
        let gene_col = Column::new("gene".into(), [AnyValue::String("KIF21A")]);
        let hgvs_col1 = Column::new(
            "hgvs1".into(),
            [AnyValue::String("NM_001173464.1:c.2860C>T")],
        );
        let hgvs_col2 = Column::new(
            "hgvs2".into(),
            [AnyValue::String("NM_001173464.1:c.2860C>T")],
        );

        let diseases_sc = SeriesContext::from_identifier("diseases".to_string())
            .with_data_context(Context::Disease)
            .with_building_block_id("Block_3");

        let gene_sc = SeriesContext::from_identifier("gene".to_string())
            .with_data_context(Context::Hgnc)
            .with_building_block_id("Block_3");

        let hgvs_sc1 = SeriesContext::from_identifier("hgvs1".to_string())
            .with_data_context(Context::Hgvs)
            .with_building_block_id("Block_3");
        let hgvs_sc2 = SeriesContext::from_identifier("hgvs2".to_string())
            .with_data_context(Context::Hgvs)
            .with_building_block_id("Block_3");

        let patient_cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "test",
                vec![patient_sc, diseases_sc, hgvs_sc1, hgvs_sc2, gene_sc],
            ),
            DataFrame::new(
                patient_col.len(),
                vec![patient_col, disease_col, hgvs_col1, hgvs_col2, gene_col],
            )
            .unwrap(),
        )
        .unwrap();

        let mut builder = build_test_phenopacket_builder();
        let patient_id = default_patient_id();

        InterpretationCollector
            .collect(&mut builder, &[patient_cdf], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            interpretations: vec![dysostosis_interpretation],
            meta_data: Some(MetaData {
                phenopacket_schema_version: phenopacket_schema_version(),
                resources: vec![
                    mondo_meta_data_resource(),
                    hgnc_meta_data_resource(),
                    geno_meta_data_resource(),
                ],
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
