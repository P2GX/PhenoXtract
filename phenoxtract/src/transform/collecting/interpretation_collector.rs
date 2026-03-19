use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::{
    ColumnFilterConfig, Filter, SeriesContextFilterConfig,
};
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
            SeriesContextFilterConfig::new().where_data_context(Filter::Is(&Context::SubjectSex)),
            ColumnFilterConfig::new(),
        )?;

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

        let mut disease_bb_id_to_cdf: HashMap<String, Vec<String>> = HashMap::new();

        for bb_id in disease_bb_ids {
            for patient_cdf in patient_cdfs {
                let relevant_cols = patient_cdf
                    .filter_columns()
                    .where_building_block(Filter::Is(bb_id))
                    .collect();
                if !relevant_cols.is_empty() {
                    disease_bb_id_to_cdf
                        .entry(bb_id.to_string())
                        .or_default()
                        .push(patient_cdf.context().name().to_string());
                }
            }
        }

        for (bb_id, cdf_names) in disease_bb_id_to_cdf {
            if cdf_names.len() == 1 {
                let cdf = patient_cdfs
                    .iter()
                    .find(|cdf| cdf.context().name() == cdf_names[0])
                    .expect("CDF should exist.");
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

        let resolve_diseases_at_index = |row_idx| {
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
            resolve_diseases_at_index,
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
            SeriesContextFilterConfig::new()
                .where_data_context(Filter::Is(&Context::Disease))
                .where_building_block(Filter::Is(&bb_id)),
            ColumnFilterConfig::new(),
        )
        .map_err(|_| CollectorError::InterpretationBlockFormat {
            patient_id: patient_id.to_string(),
            bb_id: bb_id.clone(),
        })?;

        if let Some(disease) = disease {
            let resolve_diseases_at_index = |_| vec![disease.as_str()];

            for patient_cdf in patient_cdfs {
                Self::collect_genes_and_variants_in_bb(
                    builder,
                    patient_cdf,
                    patient_id,
                    &bb_id,
                    subject_sex,
                    resolve_diseases_at_index,
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
        mut resolve_diseases_at_index: F,
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

            for disease in resolve_diseases_at_index(row_idx) {
                builder.upsert_interpretation(
                    patient_id,
                    disease,
                    &gene_variant_data,
                    subject_sex.as_deref(),
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
            TableContext::new("test", scs),
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
            TableContext::new("test", disease_cdf_scs),
            DataFrame::new(1, disease_cdf_cols).unwrap(),
        )
        .unwrap();

        let genetics_cdf = ContextualizedDataFrame::new(
            TableContext::new("test", genetics_cdf_scs),
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
            TableContext::new("test", vec![patient_sc.clone(), disease_sc]),
            DataFrame::new(2, vec![patient_col.clone(), disease_col]).unwrap(),
        )
        .unwrap();

        let genetics_cdf = ContextualizedDataFrame::new(
            TableContext::new("test", vec![patient_sc, gene_sc]),
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
