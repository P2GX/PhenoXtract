#![allow(dead_code)]
#![allow(unused)]
use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::transform::error::PhenopacketBuilderError;
use phenopackets::ga4gh::vrsatile::v1::GeneDescriptor;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::core::{GenomicInterpretation, OntologyClass, VariantInterpretation};
use pivotal::hgnc::{CachedHGNCClient, GeneQuery, HGNCData};
use pivotal::hgvs::{CachedHGVSClient, HGVSData, HGVSError, HgvsVariant};
use std::sync::Arc;

#[derive(Debug)]
pub struct GenomicInterpretationParser {
    hgvs_client: Arc<dyn HGVSData>,
    hgnc_client: Arc<dyn HGNCData>,
}

impl GenomicInterpretationParser {
    pub(crate) fn new(hgvs_client: Arc<dyn HGVSData>, hgnc_client: Arc<dyn HGNCData>) -> Self {
        Self {
            hgvs_client,
            hgnc_client,
        }
    }

    pub(crate) fn new_with_defaults() -> Result<Self, PhenopacketBuilderError> {
        Ok(Self {
            hgvs_client: Arc::new(CachedHGVSClient::new_with_defaults()?),
            hgnc_client: Arc::new(CachedHGNCClient::new_with_defaults()?),
        })
    }

    pub(crate) fn build_genomic_interpretations(
        &self,
        gene: Option<&str>,
        hgvs1: Option<&str>,
        hgvs2: Option<&str>,
        patient_id: &str,
        sex: Option<&str>,
    ) -> Result<(Vec<GenomicInterpretation>, Vec<ResourceRef>), PhenopacketBuilderError> {
        let mut gis = vec![];
        let mut resources = vec![];

        let gene_variant_data = GeneVariantData::from_options(gene, hgvs1, hgvs2);

        match gene_variant_data {
            GeneVariantData::None => {}

            GeneVariantData::Gene(gene) => {
                gis.push(self.genomic_interpretation_from_gene(patient_id, gene));
                resources.push(ResourceRef::hgnc())
            }

            GeneVariantData::SingleVariant(hgvs) => {
                let validated_hgvs = self.hgvs_client.request_and_validate_hgvs(hgvs)?;
                let allelic_state = Self::calculate_allelic_state(sex, &validated_hgvs, None);
                gis.push(Self::genomic_interpretation_from_var(
                    &validated_hgvs,
                    &allelic_state,
                ));
                resources.extend(Self::variant_resources())
            }

            GeneVariantData::VariantPair(h1, h2) => {
                let validated_hgvs1 = self.hgvs_client.request_and_validate_hgvs(h1)?;
                let validated_hgvs2 = self.hgvs_client.request_and_validate_hgvs(h2)?;
                let allelic_state =
                    Self::calculate_allelic_state(sex, &validated_hgvs1, Some(&validated_hgvs2));

                if validated_hgvs1 == validated_hgvs2 {
                    gis.push(Self::genomic_interpretation_from_var(
                        &validated_hgvs1,
                        &allelic_state,
                    ));
                } else {
                    gis.push(Self::genomic_interpretation_from_var(
                        &validated_hgvs1,
                        &allelic_state,
                    ));
                    gis.push(Self::genomic_interpretation_from_var(
                        &validated_hgvs2,
                        &allelic_state,
                    ));
                    resources.extend(Self::variant_resources())
                }
            }
        }

        Ok((gis, resources))
    }

    fn variant_resources() -> Vec<ResourceRef> {
        vec![
            ResourceRef::hgnc(),
            ResourceRef::from("geno").with_version("2025-07-25"),
        ]
    }

    fn calculate_allelic_state(
        sex: Option<&str>,
        hgvs1: &HgvsVariant,
        hgvs2: Option<&HgvsVariant>,
    ) -> OntologyClass {
        todo!()
    }

    fn genomic_interpretation_from_gene(
        &self,
        patient_id: &str,
        gene: &str,
    ) -> GenomicInterpretation {
        let (gene_symbol, gene_id) = self
            .hgnc_client
            .request_gene_identifier_pair(GeneQuery::from(gene))
            .unwrap();
        let gene_descriptor = GeneDescriptor {
            value_id: gene_id,
            symbol: gene_symbol,
            ..Default::default()
        };

        GenomicInterpretation {
            subject_or_biosample_id: patient_id.to_string(),
            call: Some(Call::Gene(gene_descriptor)),
            ..Default::default()
        }
    }

    pub(crate) fn genomic_interpretation_from_var(
        validated_hgvs: &HgvsVariant,
        allelic_state: &OntologyClass,
    ) -> GenomicInterpretation {
        todo!()
    }
}

enum GeneVariantData<'a> {
    None,
    Gene(&'a str),
    SingleVariant(&'a str),
    VariantPair(&'a str, &'a str),
}

impl<'a> GeneVariantData<'a> {
    fn from_options(gene: Option<&'a str>, hgvs1: Option<&'a str>, hgvs2: Option<&'a str>) -> Self {
        match (gene, hgvs1, hgvs2) {
            (None, None, None) => Self::None,
            (Some(g), None, None) => Self::Gene(g),
            (_, Some(h), None) | (_, None, Some(h)) => Self::SingleVariant(h),
            (_, Some(h1), Some(h2)) => Self::VariantPair(h1, h2),
        }
    }
}
