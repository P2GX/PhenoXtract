#![allow(clippy::too_many_arguments)]
use crate::ontology::loinc_client::LoincClient;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::{HasPrefixId, HasVersion};
use crate::ontology::{DatabaseRef, OntologyRef};
use crate::transform::cached_resource_resolver::CachedResourceResolver;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::pathogenic_gene_variant_info::PathogenicGeneVariantData;
use crate::transform::utils::chromosomal_sex_from_str;
use crate::transform::utils::{try_parse_time_element, try_parse_timestamp};
use chrono::Utc;
use log::warn;
use phenopackets::ga4gh::vrsatile::v1::GeneDescriptor;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::core::interpretation::ProgressStatus;
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{
    Diagnosis, Disease, GenomicInterpretation, Individual, Interpretation, OntologyClass,
    PhenotypicFeature, Sex, VitalStatus,
};
use pivot::hgnc::{GeneQuery, HGNCData};
use pivot::hgvs::{AlleleCount, HGVSData};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    ontology_bidicts: HashMap<String, Arc<OntologyBiDict>>,
    hgnc_client: Box<dyn HGNCData>,
    hgvs_client: Box<dyn HGVSData>,
    _loinc_client: Option<LoincClient>,
    resource_resolver: CachedResourceResolver,
}

impl PhenopacketBuilder {
    pub fn new(
        ontology_bidicts: HashMap<String, Arc<OntologyBiDict>>,
        hgnc_client: Box<dyn HGNCData>,
        hgvs_client: Box<dyn HGVSData>,
        loinc_client: Option<LoincClient>,
    ) -> Self {
        Self {
            subject_to_phenopacket: HashMap::new(),
            ontology_bidicts,
            hgnc_client,
            hgvs_client,
            _loinc_client: loinc_client,
            resource_resolver: CachedResourceResolver::default(),
        }
    }

    pub(crate) fn build(&self) -> Vec<Phenopacket> {
        let mut phenopackets: Vec<Phenopacket> =
            self.subject_to_phenopacket.values().cloned().collect();
        let now = Utc::now().to_string();

        phenopackets.iter_mut().for_each(|pp| {
            let metadata = pp.meta_data.get_or_insert(Default::default());
            metadata.created = Some(
                try_parse_timestamp(&now)
                    .expect("Failed to parse current timestamp for phenopacket metadata"),
            )
        });

        phenopackets
    }

    pub(crate) fn upsert_individual(
        &mut self,
        phenopacket_id: &str,
        individual_id: &str,
        alternate_ids: Option<&[&str]>,
        date_of_birth: Option<&str>,
        time_at_last_encounter: Option<&str>,
        sex: Option<&str>,
        karyotypic_sex: Option<&str>,
        gender: Option<&str>,
        taxonomy: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError> {
        if alternate_ids.is_some() {
            warn!("alternate_ids - not implemented for individual yet");
        }
        if time_at_last_encounter.is_some() {
            warn!("time_at_last_encounter - not implemented for individual yet");
        }
        if karyotypic_sex.is_some() {
            warn!("karyotypic_sex - not implemented for individual yet");
        }
        if gender.is_some() {
            warn!("gender - not implemented for individual yet");
        }
        if taxonomy.is_some() {
            warn!("taxonomy - not implemented for individual yet");
        }

        let phenopacket = self.get_or_create_phenopacket(phenopacket_id);

        let individual = phenopacket.subject.get_or_insert(Individual::default());
        individual.id = individual_id.to_string();

        if let Some(date_of_birth) = date_of_birth {
            individual.date_of_birth =
                Some(try_parse_timestamp(date_of_birth).ok_or_else(|| {
                    PhenopacketBuilderError::ParsingError {
                        what: "TimeStamp".to_string(),
                        value: date_of_birth.to_string(),
                    }
                })?);
        }

        if let Some(sex) = sex {
            individual.sex = Sex::from_str_name(sex)
                .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                    what: "Sex".to_string(),
                    value: sex.to_string(),
                })?
                .into();
        }
        Ok(())
    }

    pub(crate) fn upsert_vital_status(
        &mut self,
        phenopacket_id: &str,
        status: &str,
        time_of_death: Option<&str>,
        cause_of_death: Option<&str>,
        survival_time_in_days: Option<u32>,
    ) -> Result<(), PhenopacketBuilderError> {
        if cause_of_death.is_some() {
            warn!("cause_of_death - not implemented for vital_status yet");
        }

        let status = Status::from_str_name(status).ok_or(PhenopacketBuilderError::ParsingError {
            what: "vital status".to_string(),
            value: status.to_string(),
        })? as i32;

        let time_of_death = match time_of_death {
            Some(tod_string) => Some(try_parse_time_element(tod_string).ok_or_else(|| {
                PhenopacketBuilderError::ParsingError {
                    what: "TimeElement".to_string(),
                    value: tod_string.to_string(),
                }
            })?),
            None => None,
        };

        let survival_time_in_days = survival_time_in_days.unwrap_or(0);

        let phenopacket = self.get_or_create_phenopacket(phenopacket_id);
        let individual = phenopacket.subject.get_or_insert(Individual::default());

        individual.vital_status = Some(VitalStatus {
            status,
            time_of_death,
            cause_of_death: None,
            survival_time_in_days,
        });
        Ok(())
    }

    /// Upserts a phenotypic feature within a specific phenopacket.
    ///
    /// This function adds or updates a `PhenotypicFeature` for a given phenopacket,
    /// identified by `phenopacket_id`. If the phenopacket does not exist, it will be
    /// created. If a feature with the same `phenotype` ID already exists within the
    /// phenopacket, this function will update it (upsert).
    ///
    /// # Arguments
    ///
    /// * `phenopacket_id` - A `String` that uniquely identifies the target phenopacket.
    /// * `phenotype` - A string slice (`&str`) representing the ontology label or id for the
    ///   phenotype (e.g., `"HP:0000118" or "Phenotypic abnormality"`).
    /// * `description` - An optional free-text description of the feature.
    /// * `excluded` - An optional boolean indicating if the feature is explicitly absent.
    /// * `severity` - An optional `String` describing the severity of the phenotype.
    /// * `modifiers` - An optional `Vec<String>` of terms that modify the phenotype.
    /// * `on_set` - An optional `TimeElement` representing the onset time of the feature.
    /// * `resolution` - An optional `TimeElement` indicating when the feature resolved.
    /// * `evidence` - An optional `Evidence` struct providing support for the feature.
    ///
    /// # Errors
    ///
    /// This function will return an `Err` if the provided `phenotype` term cannot be
    /// resolved into a valid `HpoTerm`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful addition or update of the phenotypic feature.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Assuming `handler` is a mutable instance of the struct containing this method.
    /// let phenopacket_id = "patient-1".to_string();
    /// let phenotype_term = "HP:0000118"; // Corresponds to "Phenotypic abnormality"
    ///
    /// match handler.upsert_phenotypic_feature(
    ///     phenopacket_id,
    ///     phenotype_term,
    ///     None, None, None, None, None, None, None
    /// ) {
    ///     Ok(()) => println!("Successfully upserted the phenotypic feature."),
    ///     Err(e) => eprintln!("Error upserting feature: {}", e)
    /// }
    /// ```
    pub(crate) fn upsert_phenotypic_feature(
        &mut self,
        phenopacket_id: &str,
        phenotype: &str,
        description: Option<&str>,
        excluded: Option<bool>,
        severity: Option<&str>,
        modifiers: Option<Vec<&str>>,
        onset: Option<&str>,
        resolution: Option<&str>,
        evidence: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError> {
        if severity.is_some() {
            warn!("severity phenotypic feature not implemented yet");
        }
        if modifiers.is_some() {
            warn!("modifiers phenotypic feature not implemented yet");
        }
        if resolution.is_some() {
            warn!("resolution phenotypic feature not implemented yet");
        }
        if evidence.is_some() {
            warn!("evidence phenotypic feature not implemented yet");
        }

        let phenotype = self.query_hpo_identifiers(phenotype)?;
        let feature = self.get_or_create_phenotypic_feature(phenopacket_id, phenotype);

        if let Some(desc) = description {
            feature.description = desc.to_string();
        }

        if let Some(excluded) = excluded {
            feature.excluded = excluded;
        }

        if let Some(onset) = onset {
            let onset_te = try_parse_time_element(onset).ok_or_else(|| {
                PhenopacketBuilderError::ParsingError {
                    what: "TimeElement".to_string(),
                    value: onset.to_string(),
                }
            })?;
            feature.onset = Some(onset_te);
        }
        self.ensure_resource(
            phenopacket_id,
            &self
                .ontology_bidicts
                .get(OntologyRef::HPO_PREFIX)
                .ok_or_else(|| {
                    PhenopacketBuilderError::MissingBiDict(OntologyRef::HPO_PREFIX.to_string())
                })?
                .ontology
                .clone(),
        );
        Ok(())
    }

    pub(crate) fn upsert_interpretation(
        &mut self,
        patient_id: &str,
        phenopacket_id: &str,
        disease: &str,
        gene_variant_data: &PathogenicGeneVariantData,
        subject_sex: Option<String>,
    ) -> Result<(), PhenopacketBuilderError> {
        let mut genomic_interpretations: Vec<GenomicInterpretation> = vec![];

        let (term, res_ref) = self.query_disease_identifiers(disease)?;
        self.ensure_resource(phenopacket_id, &res_ref);

        if let PathogenicGeneVariantData::CausativeGene(gene) = gene_variant_data {
            let (symbol, id) = self
                .hgnc_client
                .request_gene_identifier_pair(GeneQuery::from(gene.as_str()))?;
            self.ensure_resource(phenopacket_id, &DatabaseRef::hgnc());

            let gi = GenomicInterpretation {
                subject_or_biosample_id: patient_id.to_string(),
                call: Some(Call::Gene(GeneDescriptor {
                    value_id: id.clone(),
                    symbol: symbol.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            };
            genomic_interpretations.push(gi);
        }

        if matches!(
            gene_variant_data,
            PathogenicGeneVariantData::SingleVariant { .. }
                | PathogenicGeneVariantData::HomozygousVariant { .. }
                | PathogenicGeneVariantData::CompoundHeterozygousVariantPair { .. }
        ) {
            let chromosomal_sex = chromosomal_sex_from_str(subject_sex)?;

            for var in gene_variant_data.get_vars() {
                let validated_hgvs = self.hgvs_client.request_and_validate_hgvs(var)?;
                self.ensure_resource(phenopacket_id, &DatabaseRef::hgnc());
                self.ensure_resource(
                    phenopacket_id,
                    &OntologyRef::geno().with_version("2025-07-25"),
                );

                if let Some(gene) = gene_variant_data.get_gene() {
                    validated_hgvs.validate_against_gene(gene)?;
                }

                let vi = validated_hgvs.create_variant_interpretation(
                    AlleleCount::try_from(gene_variant_data.get_allelic_count() as u8)?,
                    &chromosomal_sex,
                )?;

                let gi = GenomicInterpretation {
                    subject_or_biosample_id: patient_id.to_string(),
                    call: Some(Call::VariantInterpretation(vi)),
                    ..Default::default()
                };

                genomic_interpretations.push(gi);
            }
        }

        let interpretation_id = format!("{}-{}", phenopacket_id, term.id);

        let interpretation =
            self.get_or_create_interpretation(phenopacket_id, interpretation_id.as_str());

        interpretation.progress_status = ProgressStatus::UnknownProgress.into();

        interpretation.diagnosis = Some(Diagnosis {
            disease: Some(term),
            genomic_interpretations,
        });

        Ok(())
    }

    pub(crate) fn insert_disease(
        &mut self,
        phenopacket_id: &str,
        disease: &str,
        excluded: Option<bool>,
        onset: Option<&str>,
        resolution: Option<&str>,
        disease_stage: Option<&[&str]>,
        clinical_tnm_finding: Option<&[&str]>,
        primary_site: Option<&str>,
        laterality: Option<&str>,
    ) -> Result<(), PhenopacketBuilderError> {
        if excluded.is_some() {
            warn!("excluded disease not implemented yet");
        }
        if resolution.is_some() {
            warn!("resolution disease not implemented yet");
        }
        if disease_stage.is_some() {
            warn!("disease stage of disease not implemented yet");
        }
        if clinical_tnm_finding.is_some() {
            warn!("clinical_tnm_finding disease not implemented yet");
        }
        if primary_site.is_some() {
            warn!("primary_site disease not implemented yet");
        }
        if laterality.is_some() {
            warn!("laterality disease not implemented yet");
        }

        let (disease_term, res_ref) = self.query_disease_identifiers(disease)?;

        let mut disease_element = Disease {
            term: Some(disease_term),
            ..Default::default()
        };

        if let Some(onset) = onset {
            let onset_te = try_parse_time_element(onset).ok_or_else(|| {
                PhenopacketBuilderError::ParsingError {
                    what: "TimeElement".to_string(),
                    value: onset.to_string(),
                }
            })?;
            disease_element.onset = Some(onset_te);
        }

        let pp = self.get_or_create_phenopacket(phenopacket_id);

        pp.diseases.push(disease_element);

        self.ensure_resource(phenopacket_id, &res_ref);

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn insert_quantitative_measurement(
        &mut self,
        _phenopacket_id: &str,
        _quant_measurement: f64,
        _time_observed: Option<&str>,
        _loinc_id: &str,
        _unit_ontology_id: &str,
        _reference_range_low: Option<f64>,
        _reference_range_high: Option<f64>,
    ) -> Result<(), PhenopacketBuilderError> {
        // todo!

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn insert_qualitative_measurement(
        &mut self,
        _phenopacket_id: &str,
        _qual_measurement: &str,
        _time_observed: Option<&str>,
        _loinc_id: &str,
        _unit_ontology_prefix: &str,
    ) -> Result<(), PhenopacketBuilderError> {
        // todo!

        Ok(())
    }

    fn get_or_create_phenopacket(&mut self, phenopacket_id: &str) -> &mut Phenopacket {
        self.subject_to_phenopacket
            .entry(phenopacket_id.to_string())
            .or_insert_with(|| Phenopacket {
                id: phenopacket_id.to_string(),
                ..Default::default()
            })
    }

    fn get_or_create_phenotypic_feature(
        &mut self,
        phenopacket_id: &str,
        phenotype: OntologyClass,
    ) -> &mut PhenotypicFeature {
        let pp = self.get_or_create_phenopacket(phenopacket_id);
        let pf_index = pp.phenotypic_features.iter().position(|feature| {
            if let Some(t) = &feature.r#type {
                t.id == phenotype.id
            } else {
                false
            }
        });

        match pf_index {
            None => {
                let new_feature = PhenotypicFeature {
                    r#type: Some(phenotype),
                    ..Default::default()
                };
                pp.phenotypic_features.push(new_feature);
                pp.phenotypic_features.last_mut().unwrap()
            }
            Some(index) => &mut pp.phenotypic_features[index],
        }
    }
    fn get_or_create_interpretation(
        &mut self,
        phenopacket_id: &str,
        interpretation_id: &str,
    ) -> &mut Interpretation {
        let pp = self.get_or_create_phenopacket(phenopacket_id);
        let interpretation_index = pp
            .interpretations
            .iter()
            .position(|inter| inter.id == interpretation_id);

        match interpretation_index {
            Some(pos) => &mut pp.interpretations[pos],
            None => {
                pp.interpretations.push(Interpretation {
                    id: interpretation_id.to_string(),
                    progress_status: ProgressStatus::InProgress.into(),
                    ..Default::default()
                });
                pp.interpretations.last_mut().unwrap()
            }
        }
    }

    pub(crate) fn query_disease_identifiers(
        &self,
        query: &str,
    ) -> Result<(OntologyClass, ResourceRef), PhenopacketBuilderError> {
        for prefix in [
            // TODO: add 'DatabaseRef::OMIM_PREFIX,', when OMIM is part of the project
            OntologyRef::MONDO_PREFIX,
        ] {
            let bi_dict = self
                .ontology_bidicts
                .get(prefix)
                .expect("Disease prefix was missing from Ontology Bidicts.");
            let Some(term) = bi_dict.get(query) else {
                continue;
            };

            let corresponding_label_or_id = bi_dict.get(term).unwrap_or_else(|| {
                panic!(
                    "Bidirectional dictionary '{}' inconsistency: missing reverse mapping",
                    bi_dict.ontology.clone().into_inner()
                )
            });

            let (label, id) = if bi_dict.is_primary_label(term) {
                (term, corresponding_label_or_id)
            } else {
                (corresponding_label_or_id, term)
            };

            return Ok((
                OntologyClass {
                    id: id.to_string(),
                    label: label.to_string(),
                },
                bi_dict.ontology.clone().into_inner(),
            ));
        }

        Err(PhenopacketBuilderError::ParsingError {
            what: "disease query".to_string(),
            value: query.to_string(),
        })
    }
    fn query_hpo_identifiers(
        &self,
        hpo_query: &str,
    ) -> Result<OntologyClass, PhenopacketBuilderError> {
        let hpo_dict = self
            .ontology_bidicts
            .get(OntologyRef::HPO_PREFIX)
            .ok_or_else(|| {
                PhenopacketBuilderError::MissingBiDict(OntologyRef::HPO_PREFIX.to_string())
            })?;

        hpo_dict
            .get(hpo_query)
            .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                what: "hpo query".to_string(),
                value: hpo_query.to_string(),
            })
            .map(|found| {
                let corresponding_label_or_id = hpo_dict
                    .get(found)
                    .unwrap_or_else(|| panic!("Could not find hpo label or id from {}", found));
                let (label, id) = if hpo_dict.is_primary_label(found) {
                    (found.to_string(), corresponding_label_or_id.to_string())
                } else {
                    (corresponding_label_or_id.to_string(), found.to_string())
                };
                Ok(OntologyClass { id, label })
            })?
    }

    fn ensure_resource(
        &mut self,
        phenopacket_id: &str,
        resource_id: &(impl HasPrefixId + HasVersion),
    ) {
        let needs_resource = self
            .get_or_create_phenopacket(phenopacket_id)
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
                .resource_resolver
                .resolve(resource_id)
                .expect("Could not resolve resource");

            let phenopacket = self.get_or_create_phenopacket(phenopacket_id);
            phenopacket
                .meta_data
                .get_or_insert_with(Default::default)
                .resources
                .push(resource);
        }
    }
}

impl PartialEq for PhenopacketBuilder {
    fn eq(&self, other: &Self) -> bool {
        self.subject_to_phenopacket == other.subject_to_phenopacket
            && self.ontology_bidicts.len() == other.ontology_bidicts.len()
            && self.ontology_bidicts.iter().all(|(key, value)| {
                other
                    .ontology_bidicts
                    .get(key)
                    .map(|other_value| Arc::ptr_eq(value, other_value) || **value == **other_value)
                    .unwrap_or(false)
            })
            && self.resource_resolver == other.resource_resolver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::DatabaseRef;
    use crate::test_suite::cdf_generation::default_patient_id;
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_age_element, default_disease, default_disease_oc, default_iso_age,
        default_phenopacket_id, default_phenotype_oc, default_timestamp, default_timestamp_element,
        generate_phenotype,
    };
    use crate::test_suite::resource_references::mondo_meta_data_resource;
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::ga4gh::vrsatile::v1::Expression;
    use phenopackets::schema::v2::core::{MetaData, Resource};
    use pretty_assertions::assert_eq;
    use prost_types::Timestamp;
    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_build(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let patient_id = default_patient_id();

        let phenopacket = Phenopacket {
            id: default_phenopacket_id().clone(),
            subject: Some(Individual {
                id: patient_id.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        builder
            .subject_to_phenopacket
            .insert(default_phenopacket_id().clone(), phenopacket);

        let builds = builder.build();
        let build_pp = builds.first().unwrap();

        assert_eq!(build_pp.id, default_phenopacket_id());
        assert_eq!(
            build_pp.subject,
            Some(Individual {
                id: patient_id.to_string(),
                ..Default::default()
            })
        );

        if let Some(mm) = &build_pp.meta_data {
            assert!(mm.created.is_some());
        } else {
            panic!("Meta data was None, when it should have been Some")
        }
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_success(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenotype = default_phenotype_oc();
        let pp_id = default_phenopacket_id();

        builder
            .upsert_phenotypic_feature(
                pp_id.as_str(),
                &phenotype.label.to_string(),
                None,
                None,
                None,
                None,
                Some(default_iso_age().as_str()),
                None,
                None,
            )
            .unwrap();

        assert!(builder.subject_to_phenopacket.contains_key(&pp_id));

        let phenopacket = builder
            .subject_to_phenopacket
            .get(&default_phenopacket_id())
            .unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        let ontology_class = feature.r#type.as_ref().unwrap();
        assert_eq!(ontology_class.id, phenotype.id);
        assert_eq!(ontology_class.label, phenotype.label);

        assert!(feature.onset.is_some());
        let feature_onset = feature.onset.as_ref().unwrap();
        assert_eq!(feature_onset, &default_age_element());
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_invalid_term(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let result = builder.upsert_phenotypic_feature(
            default_phenopacket_id().as_str(),
            "invalid_term",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert!(result.is_err());
    }

    #[rstest]
    fn test_multiple_phenotypic_features_same_phenopacket(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenotype = default_phenotype_oc();
        let pp_id = default_phenopacket_id();

        builder
            .upsert_phenotypic_feature(
                pp_id.as_str(),
                &phenotype.id.to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        builder
            .upsert_phenotypic_feature(
                pp_id.as_str(),
                "HP:0000234",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&pp_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_different_phenopacket_ids(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let id1 = "pp_001".to_string();
        let id2 = "pp_002".to_string();

        builder
            .upsert_phenotypic_feature(
                id1.as_str(),
                &default_phenotype_oc().id.to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        builder
            .upsert_phenotypic_feature(
                id2.as_str(),
                &default_phenotype_oc().id.to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        assert!(builder.subject_to_phenopacket.contains_key(&id1));
        assert!(builder.subject_to_phenopacket.contains_key(&id2));
        assert_eq!(builder.subject_to_phenopacket.len(), 2);
    }

    #[rstest]
    fn test_update_phenotypic_features(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = default_phenopacket_id();

        let existing_phenopacket = Phenopacket {
            id: pp_id.clone(),
            subject: None,
            phenotypic_features: vec![generate_phenotype("HP:0000001", None)],
            measurements: vec![],
            biosamples: vec![],
            interpretations: vec![],
            diseases: vec![],
            medical_actions: vec![],
            files: vec![],
            meta_data: None,
        };
        builder
            .subject_to_phenopacket
            .insert(default_phenopacket_id().clone(), existing_phenopacket);

        builder
            .upsert_phenotypic_feature(
                pp_id.as_str(),
                &default_phenotype_oc().id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder
            .subject_to_phenopacket
            .get(&default_phenopacket_id())
            .unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_update_onset_of_phenotypic_feature(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = default_phenopacket_id();

        builder
            .upsert_phenotypic_feature(
                pp_id.as_str(),
                &default_phenotype_oc().id.to_string(),
                None,
                None,
                None,
                None,
                Some(default_iso_age().as_str()),
                None,
                None,
            )
            .unwrap();

        // Update the same feature
        builder
            .upsert_phenotypic_feature(
                pp_id.as_str(),
                &default_phenotype_oc().id.to_string(),
                None,
                None,
                None,
                None,
                Some(default_timestamp().to_string().as_str()),
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&pp_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        assert!(feature.onset.is_some());
        let feature_onset = feature.onset.as_ref().unwrap();
        assert_eq!(feature_onset, &default_timestamp_element());
    }

    #[fixture]
    fn basic_pp_with_disease_info() -> Phenopacket {
        let disease = default_disease_oc();
        let pp_id = default_phenopacket_id();

        Phenopacket {
            id: pp_id.to_string(),
            interpretations: vec![Interpretation {
                id: format!("{}-{}", pp_id, disease.id),
                progress_status: ProgressStatus::UnknownProgress.into(),
                diagnosis: Some(Diagnosis {
                    disease: Some(disease),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            meta_data: Some(MetaData {
                resources: vec![mondo_meta_data_resource()],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[rstest]
    fn test_upsert_interpretation_no_variants_no_genes(
        basic_pp_with_disease_info: Phenopacket,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let disease_id = default_disease_oc().id.clone();

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &default_phenopacket_id(),
                &disease_id,
                &PathogenicGeneVariantData::None,
                Some("MALE".to_string()),
            )
            .unwrap();

        assert_eq!(
            &basic_pp_with_disease_info,
            builder.subject_to_phenopacket.values().next().unwrap()
        );
    }

    #[rstest]
    fn test_upsert_interpretation_homozygous_variant(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();
        let disease_id = default_disease_oc().id.clone();

        let homozygous_variant = PathogenicGeneVariantData::HomozygousVariant {
            gene: Some("KIF21A".to_string()),
            var: "NM_001173464.1:c.2860C>T".to_string(),
        };

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &disease_id,
                &homozygous_variant,
                Some("FEMALE".to_string()),
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        let pp_interpretation = &pp.interpretations[0];

        assert_eq!(
            pp_interpretation
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            1
        );

        let pp_gi = &pp_interpretation
            .clone()
            .diagnosis
            .unwrap()
            .genomic_interpretations[0];

        match pp_gi.clone().call.unwrap() {
            Call::Gene(_) => {
                panic!("Call should be a VariantInterpretation!")
            }
            Call::VariantInterpretation(vi) => {
                let vd = vi.variation_descriptor.unwrap();
                assert_eq!(vd.allelic_state.unwrap().label, "homozygous");
                assert_eq!(vd.gene_context.unwrap().symbol, "KIF21A");
                let coding_expressions = vd
                    .expressions
                    .iter()
                    .filter(|exp| exp.syntax == "hgvs.c")
                    .collect::<Vec<&Expression>>();
                assert_eq!(coding_expressions.len(), 1);
                assert_eq!(coding_expressions[0].value, "NM_001173464.1:c.2860C>T");
            }
        }
    }

    #[rstest]
    fn test_upsert_interpretation_heterozygous_variant_pair(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();
        let disease_id = default_disease_oc().id.clone();

        let compound_heterozygous_pair =
            PathogenicGeneVariantData::CompoundHeterozygousVariantPair {
                gene: Some("H19".to_string()),
                var1: "NR_002196.1:n.601G>T".to_string(),
                var2: "NR_002196.1:n.602C>T".to_string(),
            };

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &disease_id,
                &compound_heterozygous_pair,
                Some("FEMALE".to_string()),
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        let pp_interpretation = &pp.interpretations[0];

        assert_eq!(
            pp_interpretation
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            2
        );

        let pp_gis = &pp_interpretation
            .clone()
            .diagnosis
            .unwrap()
            .genomic_interpretations;

        for pp_gi in pp_gis {
            match pp_gi.clone().call.unwrap() {
                Call::Gene(_) => {
                    panic!("Call should be a VariantInterpretation!")
                }
                Call::VariantInterpretation(vi) => {
                    let vd = vi.variation_descriptor.unwrap();
                    assert_eq!(vd.allelic_state.unwrap().label, "heterozygous");
                    assert_eq!(vd.gene_context.unwrap().symbol, "H19");
                    let non_coding_expressions = vd
                        .expressions
                        .iter()
                        .filter(|exp| exp.syntax == "hgvs.n")
                        .collect::<Vec<&Expression>>();
                    assert_eq!(non_coding_expressions.len(), 1);
                }
            }
        }
    }

    #[rstest]
    fn test_upsert_interpretation_autosomal_heterozygous_variant(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();
        let disease_id = default_disease_oc().id.clone();

        let heterozygous_variant = PathogenicGeneVariantData::SingleVariant {
            gene: Some("KIF21A".to_string()),
            var: "NM_001173464.1:c.2860C>T".to_string(),
        };

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &disease_id,
                &heterozygous_variant,
                None,
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        let pp_interpretation = &pp.interpretations[0];

        assert_eq!(
            pp_interpretation
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            1
        );

        let pp_gi = &pp_interpretation
            .clone()
            .diagnosis
            .unwrap()
            .genomic_interpretations[0];

        match pp_gi.clone().call.unwrap() {
            Call::Gene(_) => {
                panic!("Call should be a VariantInterpretation!")
            }
            Call::VariantInterpretation(vi) => {
                let vd = vi.variation_descriptor.unwrap();
                assert_eq!(vd.allelic_state.unwrap().label, "heterozygous");
                assert_eq!(vd.gene_context.unwrap().symbol, "KIF21A");
                let coding_expressions = vd
                    .expressions
                    .iter()
                    .filter(|exp| exp.syntax == "hgvs.c")
                    .collect::<Vec<&Expression>>();
                assert_eq!(coding_expressions.len(), 1);
                assert_eq!(coding_expressions[0].value, "NM_001173464.1:c.2860C>T");
            }
        }
    }

    #[rstest]
    fn test_upsert_interpretation_hemizygous_x_variant(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();
        let disease_id = default_disease_oc().id.clone();

        let single_variant = PathogenicGeneVariantData::SingleVariant {
            gene: None,
            var: "NM_000132.4:c.3637A>T".to_string(),
        };

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &disease_id,
                &single_variant,
                Some("MALE".to_string()),
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        let pp_interpretation = &pp.interpretations[0];

        assert_eq!(
            pp_interpretation
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            1
        );

        let pp_gi = &pp_interpretation
            .clone()
            .diagnosis
            .unwrap()
            .genomic_interpretations[0];

        match pp_gi.clone().call.unwrap() {
            Call::Gene(_) => {
                panic!("Call should be a VariantInterpretation!")
            }
            Call::VariantInterpretation(vi) => {
                let vd = vi.variation_descriptor.unwrap();
                assert_eq!(vd.allelic_state.unwrap().label, "hemizygous");
                assert_eq!(vd.gene_context.unwrap().symbol, "F8");
                let coding_expressions = vd
                    .expressions
                    .iter()
                    .filter(|exp| exp.syntax == "hgvs.c")
                    .collect::<Vec<&Expression>>();
                assert_eq!(coding_expressions.len(), 1);
                assert_eq!(coding_expressions[0].value, "NM_000132.4:c.3637A>T");
            }
        }
    }

    #[rstest]
    fn test_upsert_interpretation_heterozygous_x_variant(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();
        let disease_id = default_disease_oc().id.clone();

        let single_variant = PathogenicGeneVariantData::SingleVariant {
            gene: None,
            var: "NM_000132.4:c.3637A>T".to_string(),
        };

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &disease_id,
                &single_variant,
                Some("FEMALE".to_string()),
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        let pp_interpretation = &pp.interpretations[0];

        assert_eq!(
            pp_interpretation
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            1
        );

        let pp_gi = &pp_interpretation
            .clone()
            .diagnosis
            .unwrap()
            .genomic_interpretations[0];

        match pp_gi.clone().call.unwrap() {
            Call::Gene(_) => {
                panic!("Call should be a VariantInterpretation!")
            }
            Call::VariantInterpretation(vi) => {
                let vd = vi.variation_descriptor.unwrap();
                assert_eq!(vd.allelic_state.unwrap().label, "heterozygous");
                assert_eq!(vd.gene_context.unwrap().symbol, "F8");
                let coding_expressions = vd
                    .expressions
                    .iter()
                    .filter(|exp| exp.syntax == "hgvs.c")
                    .collect::<Vec<&Expression>>();
                assert_eq!(coding_expressions.len(), 1);
                assert_eq!(coding_expressions[0].value, "NM_000132.4:c.3637A>T");
            }
        }
    }

    #[rstest]
    fn test_upsert_interpretation_update(
        basic_pp_with_disease_info: Phenopacket,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();

        let existing_pp = basic_pp_with_disease_info;
        builder
            .subject_to_phenopacket
            .insert(phenopacket_id.to_string(), existing_pp.clone());

        let heterozygous_variant = PathogenicGeneVariantData::SingleVariant {
            gene: Some("KIF21A".to_string()),
            var: "NM_001173464.1:c.2860C>T".to_string(),
        };

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &default_disease_oc().label,
                &heterozygous_variant,
                None,
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        assert_eq!(
            pp.interpretations[0]
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            1
        );
    }

    #[rstest]
    fn test_upsert_interpretation_single_gene(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();
        let disease_id = default_disease_oc().id.clone();

        let gene_data = PathogenicGeneVariantData::CausativeGene("KIF21A".to_string());

        builder
            .upsert_interpretation(
                &default_patient_id(),
                &phenopacket_id,
                &disease_id,
                &gene_data,
                None,
            )
            .unwrap();

        let pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_eq!(pp.interpretations.len(), 1);

        let pp_interpretation = &pp.interpretations[0];

        assert_eq!(
            pp_interpretation
                .clone()
                .diagnosis
                .unwrap()
                .genomic_interpretations
                .len(),
            1
        );

        let pp_gi = &pp_interpretation
            .clone()
            .diagnosis
            .unwrap()
            .genomic_interpretations[0];

        match pp_gi.clone().call.unwrap() {
            Call::Gene(gd) => {
                assert_eq!(gd.symbol.clone(), "KIF21A");
            }
            Call::VariantInterpretation(_) => {
                panic!("Call should be a GeneDescriptor!")
            }
        }
    }

    #[rstest]
    fn test_insert_disease(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = default_phenopacket_id();
        let disease = default_disease_oc();
        let onset_age = default_iso_age();

        builder
            .insert_disease(
                &phenopacket_id,
                &disease.id,
                None,
                Some(&onset_age),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let expected_pp = &mut Phenopacket {
            id: phenopacket_id.to_string(),
            diseases: vec![Disease {
                term: Some(disease),
                onset: Some(default_age_element()),
                ..Default::default()
            }],
            meta_data: Some(MetaData {
                resources: vec![mondo_meta_data_resource()],
                ..Default::default()
            }),
            ..Default::default()
        };

        let built_pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_phenopackets(expected_pp, &mut built_pp.clone());
    }

    #[rstest]
    fn test_insert_same_disease_twice(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = default_phenopacket_id();
        let disease = default_disease_oc();

        builder
            .insert_disease(
                &phenopacket_id,
                &disease.id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        builder
            .insert_disease(
                &phenopacket_id,
                &disease.id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let expected_pp = &mut Phenopacket {
            id: phenopacket_id.to_string(),
            diseases: vec![default_disease(), default_disease()],
            meta_data: Some(MetaData {
                resources: vec![mondo_meta_data_resource()],
                ..Default::default()
            }),
            ..Default::default()
        };

        let built_pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_phenopackets(expected_pp, &mut built_pp.clone());
    }

    #[rstest]
    fn test_upsert_individual(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = default_phenopacket_id();
        let individual_id = default_patient_id();

        builder
            .upsert_individual(
                &phenopacket_id,
                &individual_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();
        assert_eq!(individual.id, individual_id.clone());
        assert_eq!(individual.sex, 0);
        assert_eq!(individual.vital_status, None);

        // Test upserting the other entries
        builder
            .upsert_individual(
                &phenopacket_id,
                &individual_id,
                None,
                Some("2001-01-29"), //TODO
                None,
                Some("MALE"),
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(individual.sex, Sex::Male as i32);
        assert_eq!(
            individual.date_of_birth,
            Some(Timestamp {
                seconds: 980726400,
                nanos: 0,
            })
        );
    }

    #[rstest]
    fn test_upsert_vital_status(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = default_phenopacket_id();

        builder
            .upsert_vital_status(
                &phenopacket_id,
                "ALIVE",
                Some(&default_iso_age()),
                None,
                Some(322),
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(
            individual.vital_status,
            Some(VitalStatus {
                status: Status::Alive.into(),
                time_of_death: Some(default_age_element()),
                cause_of_death: None,
                survival_time_in_days: 322,
            })
        );
    }

    #[rstest]
    fn test_query_hpo_identifiers_with_valid_label(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        // Known HPO label from test_utils::HPO_DICT: "Seizure" <-> "HP:0001250"
        let phenotype = default_phenotype_oc();
        let result = builder.query_hpo_identifiers(&phenotype.label).unwrap();

        assert_eq!(result.label, phenotype.label);
        assert_eq!(result.id, phenotype.id);
    }

    #[rstest]
    fn test_query_hpo_identifiers_with_valid_id(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        let phenotype = default_phenotype_oc();
        let result = builder.query_hpo_identifiers(&phenotype.id).unwrap();

        assert_eq!(result.label, phenotype.label);
        assert_eq!(result.id, phenotype.id);
    }

    #[rstest]
    fn test_query_hpo_identifiers_invalid_query(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        let result = builder.query_hpo_identifiers("NonexistentTerm");

        assert!(result.is_err());
    }

    #[rstest]
    fn test_get_or_create_phenopacket(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = default_phenopacket_id();

        builder.get_or_create_phenopacket(&phenopacket_id);
        let pp = builder.get_or_create_phenopacket(&phenopacket_id);

        assert_eq!(pp.id, phenopacket_id);
        assert_eq!(builder.subject_to_phenopacket.len(), 1);
    }

    #[rstest]
    fn test_ensure_resource(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = "test_id".to_string();

        builder.ensure_resource(&pp_id, &DatabaseRef::omim());

        let pp = builder.build().first().unwrap().clone();
        let omim_resrouce = pp.meta_data.as_ref().unwrap().resources.first().unwrap();

        let expected_resource = Resource {
            id: "omim".to_string(),
            name: "Online Mendelian Inheritance in Man".to_string(),
            url: "https://omim.org/".to_string(),
            version: "-".to_string(),
            namespace_prefix: "omim".to_string(),
            iri_prefix: "https://omim.org/MIM:$1".to_string(),
        };
        assert_eq!(omim_resrouce, &expected_resource);
    }
}
