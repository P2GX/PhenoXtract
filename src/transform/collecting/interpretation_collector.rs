use crate::config::context::{Context, DISEASE_LABEL_OR_ID_CONTEXTS};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::collecting::utils;
use crate::transform::error::CollectorError;
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use polars::datatypes::StringChunked;
use polars::error::PolarsError;
#[derive(Debug)]
pub struct InterpretationCollector;

impl Collect for InterpretationCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        let disease_in_cells_scs = patient_cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_contexts_are(DISEASE_LABEL_OR_ID_CONTEXTS.as_slice())
            .collect();

        for disease_sc in disease_in_cells_scs {
            let sc_id = disease_sc.get_identifier();
            let bb_id = disease_sc.get_building_block_id();

            let stringified_disease_cols = patient_cdf
                .get_columns(sc_id)
                .iter()
                .map(|col| col.str())
                .collect::<Result<Vec<&StringChunked>, PolarsError>>()?;

            let stringified_linked_hgnc_cols = utils::get_stringified_cols_with_data_context_in_bb(
                patient_cdf,
                bb_id,
                &Context::HgncSymbolOrId,
                &Context::None,
            )?;
            let stringified_linked_hgvs_cols = utils::get_stringified_cols_with_data_context_in_bb(
                patient_cdf,
                bb_id,
                &Context::Hgvs,
                &Context::None,
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

                for stringified_disease_col in stringified_disease_cols.iter() {
                    let disease = stringified_disease_col.get(row_idx);
                    if let Some(disease) = disease {
                        let subject_id = patient_cdf
                            .get_subject_id_col()
                            .str()?
                            .get(0)
                            .expect("subject_id missing");

                        builder.upsert_interpretation(
                            subject_id,
                            phenopacket_id,
                            disease,
                            &gene_variant_data,
                        )?;
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
    use crate::config::TableContext;
    use crate::config::table_context::{Identifier, SeriesContext};
    use crate::skip_in_ci;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf_components};
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_disease_oc, default_phenopacket_id,
    };
    use crate::test_suite::resource_references::{
        geno_meta_data_resource, hgnc_meta_data_resource, mondo_meta_data_resource,
    };
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::ga4gh::vrsatile::v1::{Expression, GeneDescriptor, VcfRecord};
    use phenopackets::ga4gh::vrsatile::v1::{MoleculeContext, VariationDescriptor};
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::AcmgPathogenicityClassification;
    use phenopackets::schema::v2::core::TherapeuticActionability;
    use phenopackets::schema::v2::core::genomic_interpretation::Call;
    use phenopackets::schema::v2::core::{
        Diagnosis, GenomicInterpretation, Interpretation, MetaData, OntologyClass, Resource,
        VariantInterpretation,
    };
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn dysostosis_interpretation() -> Interpretation {
        Interpretation {
            id: "pp_1-MONDO:0000359".to_string(),
            progress_status: 0,
            diagnosis: Some(Diagnosis {
                disease: Some(default_disease_oc()),
                genomic_interpretations: vec![GenomicInterpretation {
                    subject_or_biosample_id: default_patient_id(),
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
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_collect_interpretations(dysostosis_interpretation: Interpretation, temp_dir: TempDir) {
        skip_in_ci!();
        let (patient_col, patient_sc) = generate_minimal_cdf_components(1, 1);
        let disease_col = Column::new(
            "diseases".into(),
            [AnyValue::String(default_disease_oc().label.as_str())],
        );
        let gene_col = Column::new("gene".into(), [AnyValue::String("ALMS1")]);
        let hgvs_col1 = Column::new(
            "hgvs1".into(),
            [AnyValue::String("NM_001173464.1:c.2860C>T")],
        );
        let hgvs_col2 = Column::new(
            "hgvs2".into(),
            [AnyValue::String("NM_001173464.1:c.2860C>T")],
        );

        let diseases_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("diseases".to_string()))
            .with_data_context(Context::MondoLabelOrId)
            .with_building_block_id(Some("Block_3".to_string()));

        let gene_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("gene".to_string()))
            .with_data_context(Context::HgncSymbolOrId)
            .with_building_block_id(Some("Block_3".to_string()));

        let hgvs_sc1 = SeriesContext::default()
            .with_identifier(Identifier::Regex("hgvs1".to_string()))
            .with_data_context(Context::Hgvs)
            .with_building_block_id(Some("Block_3".to_string()));
        let hgvs_sc2 = SeriesContext::default()
            .with_identifier(Identifier::Regex("hgvs2".to_string()))
            .with_data_context(Context::Hgvs)
            .with_building_block_id(Some("Block_3".to_string()));

        let patient_cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "test",
                vec![patient_sc, diseases_sc, hgvs_sc1, hgvs_sc2, gene_sc],
            ),
            DataFrame::new(vec![
                patient_col,
                disease_col,
                hgvs_col1,
                hgvs_col2,
                gene_col,
            ])
            .unwrap(),
        )
        .unwrap();

        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id().to_string();

        InterpretationCollector
            .collect(&mut builder, &patient_cdf, &phenopacket_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: phenopacket_id.to_string(),
            interpretations: vec![dysostosis_interpretation],
            meta_data: Some(MetaData {
                resources: vec![
                    mondo_meta_data_resource(),
                    hgnc_meta_data_resource(),
                    geno_meta_data_resource(),
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
