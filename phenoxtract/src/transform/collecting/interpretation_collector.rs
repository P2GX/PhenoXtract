use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::column_filter::ColumnFilterConfig;
use crate::extract::enums::Filter;
use crate::transform::collecting::traits::Collect;
use crate::transform::collecting::utils::{get_single_multiplicity_element, get_str_at_index};
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use polars::prelude::StringChunked;
use std::any::Any;

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
            ColumnFilterConfig::default().where_data_context(Filter::Is(&Context::SubjectSex)),
        )?;

        for patient_cdf in patient_cdfs {
            let disease_bbs: Vec<String> = patient_cdf
                .filter_series_context()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::Disease))
                .where_building_block(Filter::IsSome)
                .collect()
                .into_iter()
                .filter_map(|dsc| dsc.get_building_block_id().map(|s| s.to_string()))
                .collect();

            for disease_building_block in disease_bbs.iter() {
                let is_spread = Self::is_spread(patient_cdfs, disease_building_block);

                if is_spread {
                    Self::collect_multi_sheet_disease_building_block(
                        builder,
                        patient_cdfs,
                        patient_id,
                        disease_building_block,
                        subject_sex.as_deref(),
                    )?;
                } else {
                    Self::collect_single_sheet_disease_building_block(
                        builder,
                        patient_cdf,
                        patient_id,
                        disease_building_block,
                        subject_sex.as_deref(),
                    )?;
                }
            }
        }

        Ok(())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl InterpretationCollector {
    fn is_spread(patient_cdfs: &[ContextualizedDataFrame], disease_bb: &str) -> bool {
        patient_cdfs
            .iter()
            .filter(|cdf| {
                !cdf.filter_series_context()
                    .where_building_block(Filter::Is(disease_bb))
                    .collect()
                    .is_empty()
            })
            .count()
            >= 2
    }

    fn collect_single_sheet_disease_building_block(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdf: &ContextualizedDataFrame,
        patient_id: &str,
        bb_id: &str,
        subject_sex: Option<&str>,
    ) -> Result<(), CollectorError> {
        let disease_cols = patient_cdf
            .filter_columns()
            .where_building_block(Filter::Is(bb_id))
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::Disease))
            .collect_owned_names();

        let disease_col = patient_cdf
            .get_stringified_cols(disease_cols)?
            .first()
            .cloned()
            .unwrap();

        Self::collect_genes_and_variants_in_bb(
            builder,
            patient_cdf,
            patient_id,
            bb_id,
            subject_sex,
            disease_col,
        )
    }

    fn collect_multi_sheet_disease_building_block(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
        bb_id: &str,
        subject_sex: Option<&str>,
    ) -> Result<(), CollectorError> {
        let disease = get_single_multiplicity_element(
            patient_cdfs,
            ColumnFilterConfig::default()
                .where_data_context(Filter::Is(&Context::Disease))
                .where_building_block(Filter::Is(bb_id)),
        )
        .map_err(|_| CollectorError::InterpretationBlockFormat {
            patient_id: patient_id.to_string(),
            bb_id: bb_id.to_string(),
        })?;

        if let Some(disease) = disease {
            for patient_cdf in patient_cdfs {
                let disease_col =
                    StringChunked::from_iter(vec![disease.as_str(); patient_cdf.data().height()]);

                Self::collect_genes_and_variants_in_bb(
                    builder,
                    patient_cdf,
                    patient_id,
                    bb_id,
                    subject_sex,
                    &disease_col,
                )?;
            }
        }

        Ok(())
    }

    fn collect_genes_and_variants_in_bb(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdf: &ContextualizedDataFrame,
        patient_id: &str,
        bb_id: &str,
        subject_sex: Option<&str>,
        disease_col: &StringChunked,
    ) -> Result<(), CollectorError> {
        let linked_hgnc_col =
            patient_cdf.get_single_linked_column_as_str(Some(bb_id), &[Context::Hgnc])?;

        let linked_hgvs_cols = patient_cdf.get_stringified_cols(
            patient_cdf.get_linked_cols_with_context(Some(bb_id), &Context::Hgvs, &Context::None),
        )?;

        if linked_hgvs_cols.len() > 2 {
            return Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: bb_id.to_string(),
                contexts: vec![Context::Hgvs],
                n_found: linked_hgvs_cols.len(),
                n_expected: 2,
            });
        }

        let allele_one_col = linked_hgvs_cols.get(0).copied();
        let allele_two_col = linked_hgvs_cols.get(1).copied();

        for row_idx in 0..patient_cdf.data().height() {
            let gene = get_str_at_index(linked_hgnc_col.as_ref(), row_idx);
            let hgvs1 = get_str_at_index(allele_one_col, row_idx);
            let hgvs2 = get_str_at_index(allele_two_col, row_idx);

            if let Some(disease) = disease_col.get(row_idx) {
                builder.upsert_interpretation(
                    patient_id,
                    disease,
                    gene,
                    hgvs1,
                    hgvs2,
                    subject_sex,
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

    #[fixture]
    fn disease_col() -> Column {
        Column::new(
            "diseases".into(),
            [AnyValue::String(default_disease_oc().label.as_str())],
        )
    }

    #[fixture]
    fn genetics_cols() -> Vec<Column> {
        let gene_col = Column::new("gene".into(), [AnyValue::String("KIF21A")]);
        let hgvs_col1 = Column::new(
            "hgvs1".into(),
            [AnyValue::String("NM_001173464.1:c.2860C>T")],
        );
        let hgvs_col2 = Column::new(
            "hgvs2".into(),
            [AnyValue::String("NM_001173464.1:c.2860C>T")],
        );
        vec![gene_col, hgvs_col1, hgvs_col2]
    }

    #[fixture]
    fn disease_sc() -> SeriesContext {
        SeriesContext::from_identifier("diseases".to_string())
            .with_data_context(Context::Disease)
            .with_building_block_id("D")
    }

    #[fixture]
    fn genetics_scs() -> Vec<SeriesContext> {
        let gene_sc = SeriesContext::from_identifier("gene".to_string())
            .with_data_context(Context::Hgnc)
            .with_building_block_id("D");

        let hgvs_sc1 = SeriesContext::from_identifier("hgvs1".to_string())
            .with_data_context(Context::Hgvs)
            .with_building_block_id("D");
        let hgvs_sc2 = SeriesContext::from_identifier("hgvs2".to_string())
            .with_data_context(Context::Hgvs)
            .with_building_block_id("D");

        vec![gene_sc, hgvs_sc1, hgvs_sc2]
    }

    #[fixture]
    fn phenopacket_with_interpretation(dysostosis_interpretation: Interpretation) -> Phenopacket {
        Phenopacket {
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
        }
    }

    #[rstest]
    fn test_collect_interpretations_single_sheet(
        phenopacket_with_interpretation: Phenopacket,
        disease_col: Column,
        genetics_cols: Vec<Column>,
        disease_sc: SeriesContext,
        genetics_scs: Vec<SeriesContext>,
    ) {
        let (patient_col, patient_sc) = generate_minimal_cdf_components(1, 1);

        let mut cols = vec![patient_col, disease_col];
        let mut scs = vec![patient_sc, disease_sc];
        cols.extend(genetics_cols);
        scs.extend(genetics_scs);

        let patient_cdf = ContextualizedDataFrame::new(
            TableContext::new("disease_table", scs),
            DataFrame::new(1, cols).unwrap(),
        )
        .unwrap();

        let mut builder = build_test_phenopacket_builder();
        let patient_id = default_patient_id();

        InterpretationCollector
            .collect(&mut builder, &[patient_cdf], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(
            &mut phenopackets[0],
            &mut phenopacket_with_interpretation.clone(),
        );
    }

    #[rstest]
    fn test_collect_interpretations_multi_sheet(
        phenopacket_with_interpretation: Phenopacket,
        disease_col: Column,
        genetics_cols: Vec<Column>,
        disease_sc: SeriesContext,
        genetics_scs: Vec<SeriesContext>,
    ) {
        let (patient_col, patient_sc) = generate_minimal_cdf_components(1, 1);

        let disease_cdf_cols = vec![patient_col.clone(), disease_col];
        let disease_cdf_scs = vec![patient_sc.clone(), disease_sc];

        let mut genetics_cdf_cols = vec![patient_col];
        let mut genetics_cdf_scs = vec![patient_sc];

        genetics_cdf_cols.extend(genetics_cols);
        genetics_cdf_scs.extend(genetics_scs);

        let disease_cdf = ContextualizedDataFrame::new(
            TableContext::new("disease_table", disease_cdf_scs),
            DataFrame::new(1, disease_cdf_cols).unwrap(),
        )
        .unwrap();

        let genetics_cdf = ContextualizedDataFrame::new(
            TableContext::new("genetics_table", genetics_cdf_scs),
            DataFrame::new(1, genetics_cdf_cols).unwrap(),
        )
        .unwrap();

        let mut builder = build_test_phenopacket_builder();
        let patient_id = default_patient_id();

        InterpretationCollector
            .collect(&mut builder, &[disease_cdf, genetics_cdf], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(
            &mut phenopackets[0],
            &mut phenopacket_with_interpretation.clone(),
        );
    }

    #[rstest]
    fn test_collect_interpretations_invalid_format_err(disease_sc: SeriesContext) {
        let (patient_col, patient_sc) = generate_minimal_cdf_components(1, 2);

        let disease_col = Column::new(
            "diseases".into(),
            [
                AnyValue::String(default_disease_oc().label.as_str()),
                AnyValue::String("Another disease"),
            ],
        );

        let gene_col = Column::new(
            "gene".into(),
            [AnyValue::String("CLOCK"), AnyValue::String("SHH")],
        );

        let gene_sc = SeriesContext::from_identifier("gene".to_string())
            .with_data_context(Context::Hgnc)
            .with_building_block_id("D");

        let disease_cdf = ContextualizedDataFrame::new(
            TableContext::new("disease_table", vec![patient_sc.clone(), disease_sc]),
            DataFrame::new(2, vec![patient_col.clone(), disease_col]).unwrap(),
        )
        .unwrap();

        let genetics_cdf = ContextualizedDataFrame::new(
            TableContext::new("gene_table", vec![patient_sc, gene_sc]),
            DataFrame::new(2, vec![patient_col, gene_col]).unwrap(),
        )
        .unwrap();

        let mut builder = build_test_phenopacket_builder();
        let patient_id = default_patient_id();

        let result = InterpretationCollector.collect(
            &mut builder,
            &[disease_cdf, genetics_cdf],
            &patient_id,
        );

        assert!(result.is_err());
    }
}
