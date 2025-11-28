use crate::config::context::Context;
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
            .where_data_context_is_disease()
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
            )?;
            let stringified_linked_hgvs_cols = utils::get_stringified_cols_with_data_context_in_bb(
                patient_cdf,
                bb_id,
                &Context::Hgvs,
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
    use crate::test_utils::{
        assert_phenopackets, build_test_phenopacket_builder, generate_patent_cdf_components,
    };
    use phenopackets::ga4gh::vrsatile::v1::GeneDescriptor;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::genomic_interpretation::Call::Gene;
    use phenopackets::schema::v2::core::time_element::Element;
    use phenopackets::schema::v2::core::{
        Age, Diagnosis, Disease, GenomicInterpretation, Interpretation, MetaData, OntologyClass,
        Resource, TimeElement,
    };
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn spondylocostal_dysostosis_term() -> OntologyClass {
        OntologyClass {
            id: "MONDO:0000359".to_string(),
            label: "spondylocostal dysostosis".to_string(),
        }
    }

    #[fixture]
    fn mondo_meta_data_resource() -> Resource {
        Resource {
            id: "mondo".to_string(),
            name: "Mondo Disease Ontology".to_string(),
            url: "http://purl.obolibrary.org/obo/mondo.json".to_string(),
            version: "2025-10-07".to_string(),
            namespace_prefix: "MONDO".to_string(),
            iri_prefix: "http://purl.obolibrary.org/obo/MONDO_$1".to_string(),
        }
    }

    #[fixture]
    fn dysostosis_interpretation(spondylocostal_dysostosis_term: OntologyClass) -> Interpretation {
        Interpretation {
            id: "cohort2019-P002-MONDO:0000359".to_string(),
            progress_status: 0,
            diagnosis: Some(Diagnosis {
                disease: Some(spondylocostal_dysostosis_term),
                genomic_interpretations: vec![GenomicInterpretation {
                    subject_or_biosample_id: "P002".to_string(),
                    interpretation_status: 0,
                    call: Some(Gene(GeneDescriptor {
                        value_id: "HGNC:428".to_string(),
                        symbol: "ALMS1".to_string(),
                        description: "".to_string(),
                        alternate_ids: vec![],
                        alternate_symbols: vec![],
                        xrefs: vec![],
                    })),
                }],
            }),
            summary: "".to_string(),
        }
    }

    #[fixture]
    fn hgnc_meta_data_resource() -> Resource {
        Resource {
            id: "hgnc".to_string(),
            name: "HUGO Gene Nomenclature Committee".to_string(),
            url: "https://w3id.org/biopragmatics/resources/hgnc/2025-10-07/hgnc.ofn".to_string(),
            version: "-".to_string(),
            namespace_prefix: "hgnc".to_string(),
            iri_prefix: "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/$1"
                .to_string(),
        }
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_collect_interpretations(
        dysostosis_interpretation: Interpretation,
        mondo_meta_data_resource: Resource,
        hgnc_meta_data_resource: Resource,
        spondylocostal_dysostosis_term: OntologyClass,
        temp_dir: TempDir,
    ) {
        skip_in_ci!();
        let (patient_col, patient_sc) = generate_patent_cdf_components(1, 1);
        let disease_col = Column::new(
            "diseases".into(),
            [AnyValue::String(
                spondylocostal_dysostosis_term.label.as_str(),
            )],
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
        let phenopacket_id = "pp_1".to_string();

        InterpretationCollector
            .collect(&mut builder, &patient_cdf, &phenopacket_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: phenopacket_id.to_string(),
            interpretations: vec![dysostosis_interpretation],
            meta_data: Some(MetaData {
                resources: vec![mondo_meta_data_resource, hgnc_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
