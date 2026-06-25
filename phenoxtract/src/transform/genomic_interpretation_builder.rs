use crate::ontology::resource_references::ResourceRef;
use crate::transform::error::PhenopacketBuilderError;
use phenopackets::ga4gh::vrsatile::v1::GeneDescriptor;
use phenopackets::schema::v2::core::{GenomicInterpretation, OntologyClass, VariantInterpretation};
use pivotal::hgnc::{CachedHGNCClient, HGNCData};
use pivotal::hgvs::{CachedHGVSClient, HGVSData, HgvsVariant};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct GenomicInterpretationBuilder {
    hgvs_client: Arc<dyn HGVSData>,
    hgnc_client: Arc<dyn HGNCData>,
}

impl GenomicInterpretationBuilder {
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
    ) -> (Vec<GenomicInterpretation>, Vec<ResourceRef>) {
        todo!()
    }
    pub(crate) fn get_data_from_pivotal(&self, hgvs: &str) -> HgvsVariant {
        self.hgvs_client.request_and_validate_hgvs(hgvs).unwrap()
    }

    pub(crate) fn build_gene_descriptor(gene: &str) -> GeneDescriptor {
        todo!()
    }

    pub(crate) fn build_variant_interpretation(
        hgvs_variant: HgvsVariant,
        allelic_state: OntologyClass,
    ) -> VariantInterpretation {
        todo!()
    }
}
