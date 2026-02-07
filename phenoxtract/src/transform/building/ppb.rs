use crate::ontology::traits::{HasPrefixId, HasVersion};
use crate::transform::bidict_library::BiDictLibrary;
use crate::transform::building::phenotypic_feature_builder::PhenotypicFeatureBuilder;
use crate::transform::cached_resource_resolver::CachedResourceResolver;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::phenopacket_builder::BuilderMetaData;
use crate::transform::utils::try_parse_time_element;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::PhenotypicFeature;
use pivot::hgnc::HGNCData;
use pivot::hgvs::HGVSData;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DictionaryRegistry {
    pub hpo: BiDictLibrary,
    pub disease: BiDictLibrary,
    pub unit: BiDictLibrary,
    pub assay: BiDictLibrary,
    pub qualitative: BiDictLibrary,
}

#[derive(Debug)]
pub struct BuilderContext {
    meta_data: BuilderMetaData,
    pub hgnc_client: Box<dyn HGNCData>,
    pub hgvs_client: Box<dyn HGVSData>,
    pub dictionary_registry: DictionaryRegistry,
    resource_resolver: CachedResourceResolver,
}

#[derive(Debug)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    pub ctx: BuilderContext,
}

impl PhenopacketBuilder {
    pub fn feature<'a>(
        &'a mut self,
        patient_id: &'a str,
        phenotype: &'a str,
    ) -> PhenotypicFeatureBuilder<'a> {
        PhenotypicFeatureBuilder::new(self, patient_id, phenotype)
    }

    pub(super) fn ensure_resource(
        &mut self,
        patient_id: &str,
        resource_id: &(impl HasPrefixId + HasVersion),
    ) {
        let needs_resource = self
            .get_or_create_phenopacket(patient_id)
            .meta_data
            .as_ref()
            .map(|meta_data| {
                !meta_data.resources.iter().any(|resource| {
                    resource.id.to_lowercase() == resource_id.prefix_id().to_lowercase()
                        && resource.version.to_lowercase() == resource.version.to_lowercase()
                })
            })
            .unwrap_or(true);

        if needs_resource {
            let resource = self
                .ctx
                .resource_resolver
                .resolve(resource_id)
                .expect("Could not resolve resource");

            let phenopacket = self.get_or_create_phenopacket(patient_id);
            phenopacket
                .meta_data
                .get_or_insert_with(Default::default)
                .resources
                .push(resource);
        }
    }

    pub(super) fn get_or_create_phenopacket(&mut self, patient_id: &str) -> &mut Phenopacket {
        let phenopacket_id = self.generate_phenopacket_id(patient_id);
        self.subject_to_phenopacket
            .entry(phenopacket_id.clone())
            .or_insert_with(|| Phenopacket {
                id: phenopacket_id.to_string(),
                ..Default::default()
            })
    }

    pub(super) fn generate_phenopacket_id(&self, patient_id: &str) -> String {
        if patient_id.starts_with(&self.ctx.meta_data.cohort_name) {
            return patient_id.to_string();
        }
        format!("{}-{}", self.ctx.meta_data.cohort_name, patient_id)
    }
}
