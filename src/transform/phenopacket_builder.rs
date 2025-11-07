#![allow(clippy::too_many_arguments)]
use crate::constants::ISO8601_DUR_PATTERN;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::{HasPrefixId, HasVersion};
use crate::ontology::{DatabaseRef, HGNCClient, OntologyRef};
use crate::transform::cached_resource_resolver::CachedResourceResolver;
use crate::transform::error::PhenopacketBuilderError;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use chrono::{TimeZone, Utc};
use ga4ghphetools::dto::hgvs_variant::HgvsVariant;
use ga4ghphetools::variant::validate_one_hgvs_variant;
use log::warn;
use phenopackets::ga4gh::vrsatile::v1::{
    Expression, GeneDescriptor, MoleculeContext, VariationDescriptor, VcfRecord,
};
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use phenopackets::schema::v2::core::time_element::Element::{Age, Timestamp};
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{
    AcmgPathogenicityClassification, Age as IndividualAge, Diagnosis, Disease,
    GenomicInterpretation, Individual, Interpretation, OntologyClass, PhenotypicFeature, Sex,
    TherapeuticActionability, TimeElement, VariantInterpretation, VitalStatus,
};
use prost_types::Timestamp as TimestampProtobuf;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    ontology_bidicts: HashMap<String, Arc<OntologyBiDict>>,
    hgnc_client: HGNCClient,
    resource_resolver: CachedResourceResolver,
}

impl PhenopacketBuilder {
    pub fn new(
        ontology_bidicts: HashMap<String, Arc<OntologyBiDict>>,
        hgnc_client: HGNCClient,
    ) -> PhenopacketBuilder {
        PhenopacketBuilder {
            subject_to_phenopacket: Default::default(),
            ontology_bidicts,
            hgnc_client,
            resource_resolver: Default::default(),
        }
    }

    pub fn build(&self) -> Vec<Phenopacket> {
        let mut phenopackets: Vec<Phenopacket> =
            self.subject_to_phenopacket.values().cloned().collect();
        let now = Utc::now().to_string();

        phenopackets.iter_mut().for_each(|pp| {
            let metadata = pp.meta_data.get_or_insert(Default::default());
            metadata.created = Some(
                Self::try_parse_timestamp(&now)
                    .expect("Failed to parse current timestamp for phenopacket metadata"),
            )
        });

        phenopackets
    }
    #[allow(dead_code)]
    pub fn build_for_id(&self, phenopacket_id: String) -> Option<Phenopacket> {
        self.subject_to_phenopacket.get(&phenopacket_id).cloned()
    }

    pub fn upsert_individual(
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
            individual.date_of_birth = Some(Self::try_parse_timestamp(date_of_birth)?);
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

    pub fn upsert_vital_status(
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
            Some(tod_string) => Some(Self::try_parse_time_element(tod_string)?),
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
    ///     Err(e) => eprintln!("Error upserting feature: {}", e),
    /// }
    /// ```
    #[allow(dead_code)]
    pub fn upsert_phenotypic_feature(
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
            let onset_te = Self::try_parse_time_element(onset)?;
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

    /// Valid configurations of genes and variants:
    ///
    /// - **No genes or variants.**
    ///
    /// - **A single gene with no variants.**
    ///   The gene will be added as a `GenomicInterpretation`.
    ///
    /// - **A single gene with a pair of identical, homozygous variants.**
    ///   This will be added as a single *homozygous* `GenomicInterpretation`.
    ///
    /// - **A single gene with a pair of distinct, heterozygous variants.**
    ///   These will be added as separate *heterozygous* `GenomicInterpretation`s.
    ///
    /// - **A single gene with a single heterozygous variant.**
    ///   This will be added as a single *heterozygous* `GenomicInterpretation`.
    ///
    /// All other configurations are invalid.
    pub fn upsert_interpretation(
        &mut self,
        patient_id: &str,
        phenopacket_id: &str,
        disease: &str,
        genes: &[&str],
        variants: &[&str],
    ) -> Result<(), PhenopacketBuilderError> {
        let (term, res_ref) = self.query_disease_identifiers(disease)?;
        self.ensure_resource(phenopacket_id, &res_ref);

        let mut genomic_interpretations: Vec<GenomicInterpretation> = vec![];

        let no_gene_variant_info = variants.is_empty() && genes.is_empty();
        let valid_only_gene_info = variants.is_empty() && genes.len() == 1;
        let valid_homozygous_variant =
            variants.len() == 2 && variants[0] == variants[1] && genes.len() == 1;
        let valid_heterozygous_variant_pair =
            variants.len() == 2 && variants[0] != variants[1] && genes.len() == 1;
        let valid_heterozygous_variant = variants.len() == 1 && genes.len() == 1;
        let valid_configuration = no_gene_variant_info
            || valid_only_gene_info
            || valid_homozygous_variant
            || valid_heterozygous_variant_pair
            || valid_heterozygous_variant;

        if !valid_configuration {
            return Err(PhenopacketBuilderError::InvalidGeneVariantConfiguration { disease: disease.to_string(), invalid_configuration: "There can be only between 1 and 2 variants. If there are variants, a gene must be specified.".to_string() });
        }

        if !no_gene_variant_info {
            let (gene_symbol, hgnc_id) = self.get_gene_data_from_hgnc(phenopacket_id, genes[0])?;

            if valid_only_gene_info {
                let gi = GenomicInterpretation {
                    subject_or_biosample_id: patient_id.to_string(),
                    call: Some(Call::Gene(GeneDescriptor {
                        value_id: hgnc_id.clone(),
                        symbol: gene_symbol.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                genomic_interpretations.push(gi);
            }

            if valid_homozygous_variant {
                let (gene_symbol, hgnc_id) =
                    self.get_gene_data_from_hgnc(phenopacket_id, genes[0])?;
                let variant = variants[0];
                let gi = Self::get_genomic_interpretation_from_data(
                    patient_id,
                    variant,
                    gene_symbol.as_str(),
                    hgnc_id.as_str(),
                    2,
                )?;
                genomic_interpretations.push(gi);
            }

            if valid_heterozygous_variant_pair {
                for variant in variants {
                    let gi = Self::get_genomic_interpretation_from_data(
                        patient_id,
                        variant,
                        gene_symbol.as_str(),
                        hgnc_id.as_str(),
                        1,
                    )?;
                    genomic_interpretations.push(gi);
                }
            }

            if valid_heterozygous_variant {
                let variant = variants[0];
                let gi = Self::get_genomic_interpretation_from_data(
                    patient_id,
                    variant,
                    gene_symbol.as_str(),
                    hgnc_id.as_str(),
                    1,
                )?;
                genomic_interpretations.push(gi);
            }
        }

        let interpretation_id = format!("{}-{}", phenopacket_id, term.id);

        let interpretation =
            self.get_or_create_interpretation(phenopacket_id, interpretation_id.as_str());

        interpretation.progress_status = 0; //UNKNOWN_PROGRESS

        interpretation.diagnosis = Some(Diagnosis {
            disease: Some(term),
            genomic_interpretations,
        });

        Ok(())
    }

    /// Valid configurations of genes and variants:
    ///
    /// - **No genes or variants.**
    ///
    /// - **A single gene with no variants.**
    ///   The gene will be added as a `GenomicInterpretation`.
    ///
    /// - **A single gene with a pair of identical, homozygous variants.**
    ///   This will be added as a single *homozygous* `GenomicInterpretation`.
    ///
    /// - **A single gene with a pair of distinct, heterozygous variants.**
    ///   These will be added as separate *heterozygous* `GenomicInterpretation`s.
    ///
    /// - **A single gene with a single heterozygous variant.**
    ///   This will be added as a single *heterozygous* `GenomicInterpretation`.
    ///
    /// All other configurations are invalid.
    pub fn upsert_omim_interpretation(
        &mut self,
        patient_id: &str,
        phenopacket_id: &str,
        omim_id: &str,
        omim_label: &str,
        genes: &[&str],
        variants: &[&str],
    ) -> Result<(), PhenopacketBuilderError> {
        let omim_term = OntologyClass {
            id: omim_id.to_string(),
            label: omim_label.to_string(),
        };

        let mut genomic_interpretations: Vec<GenomicInterpretation> = vec![];

        let no_gene_variant_info = variants.is_empty() && genes.is_empty();
        let valid_only_gene_info = variants.is_empty() && genes.len() == 1;
        let valid_homozygous_variant =
            variants.len() == 2 && variants[0] == variants[1] && genes.len() == 1;
        let valid_heterozygous_variant_pair =
            variants.len() == 2 && variants[0] != variants[1] && genes.len() == 1;
        let valid_heterozygous_variant = variants.len() == 1 && genes.len() == 1;
        let valid_configuration = no_gene_variant_info
            || valid_only_gene_info
            || valid_homozygous_variant
            || valid_heterozygous_variant_pair
            || valid_heterozygous_variant;

        if !valid_configuration {
            return Err(PhenopacketBuilderError::InvalidGeneVariantConfiguration { disease: omim_id.to_string(), invalid_configuration: "There can be only between 1 and 2 variants. If there are variants, a gene must be specified.".to_string() });
        }

        self.ensure_resource(phenopacket_id, &DatabaseRef::omim());

        if !no_gene_variant_info {
            let (gene_symbol, hgnc_id) = self.get_gene_data_from_hgnc(phenopacket_id, genes[0])?;

            if valid_only_gene_info {
                let gi = GenomicInterpretation {
                    subject_or_biosample_id: patient_id.to_string(),
                    call: Some(Call::Gene(GeneDescriptor {
                        value_id: hgnc_id.clone(),
                        symbol: gene_symbol.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                genomic_interpretations.push(gi);
            }

            if valid_homozygous_variant {
                let (gene_symbol, hgnc_id) =
                    self.get_gene_data_from_hgnc(phenopacket_id, genes[0])?;
                let variant = variants[0];
                let gi = Self::get_genomic_interpretation_from_data(
                    patient_id,
                    variant,
                    gene_symbol.as_str(),
                    hgnc_id.as_str(),
                    2,
                )?;
                genomic_interpretations.push(gi);
            }

            if valid_heterozygous_variant_pair {
                for variant in variants {
                    let gi = Self::get_genomic_interpretation_from_data(
                        patient_id,
                        variant,
                        gene_symbol.as_str(),
                        hgnc_id.as_str(),
                        1,
                    )?;
                    genomic_interpretations.push(gi);
                }
            }

            if valid_heterozygous_variant {
                let variant = variants[0];
                let gi = Self::get_genomic_interpretation_from_data(
                    patient_id,
                    variant,
                    gene_symbol.as_str(),
                    hgnc_id.as_str(),
                    1,
                )?;
                genomic_interpretations.push(gi);
            }
        }

        let interpretation_id = format!("{}-{}", phenopacket_id, omim_term.id);

        let interpretation =
            self.get_or_create_interpretation(phenopacket_id, interpretation_id.as_str());

        interpretation.progress_status = 0; //UNKNOWN_PROGRESS

        interpretation.diagnosis = Some(Diagnosis {
            disease: Some(omim_term),
            genomic_interpretations,
        });

        Ok(())
    }

    pub fn insert_disease(
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
            let onset_te = Self::try_parse_time_element(onset)?;
            disease_element.onset = Some(onset_te);
        }

        let pp = self.get_or_create_phenopacket(phenopacket_id);

        pp.diseases.push(disease_element);

        self.ensure_resource(phenopacket_id, &res_ref);

        Ok(())
    }

    pub fn insert_omim_disease(
        &mut self,
        phenopacket_id: &str,
        omim_id: &str,
        omim_label: &str,
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

        self.ensure_resource(phenopacket_id, &DatabaseRef::omim());

        let omim_term = OntologyClass {
            id: omim_id.to_string(),
            label: omim_label.to_string(),
        };

        let mut disease_element = Disease {
            term: Some(omim_term),
            ..Default::default()
        };

        if let Some(onset) = onset {
            let onset_te = Self::try_parse_time_element(onset)?;
            disease_element.onset = Some(onset_te);
        }

        let pp = self.get_or_create_phenopacket(phenopacket_id);

        pp.diseases.push(disease_element);

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
                    progress_status: 1, // IN_PROGRESS
                    ..Default::default()
                });
                pp.interpretations.last_mut().unwrap()
            }
        }
    }

    pub fn query_disease_identifiers(
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

    fn try_parse_time_element(te_string: &str) -> Result<TimeElement, PhenopacketBuilderError> {
        //try to parse the string as a datetime
        if let Ok(ts) = Self::try_parse_timestamp(te_string) {
            let datetime_te = TimeElement {
                element: Some(Timestamp(ts)),
            };
            return Ok(datetime_te);
        }

        let re = Regex::new(ISO8601_DUR_PATTERN).unwrap();
        let is_iso8601_dur = re.is_match(te_string);
        if is_iso8601_dur {
            let age_te = TimeElement {
                element: Some(Age(IndividualAge {
                    iso8601duration: te_string.to_string(),
                })),
            };
            return Ok(age_te);
        }

        Err(PhenopacketBuilderError::ParsingError {
            what: "TimeElement".to_string(),
            value: te_string.to_string(),
        })
    }

    fn try_parse_timestamp(ts_string: &str) -> Result<TimestampProtobuf, PhenopacketBuilderError> {
        let utc_dt = try_parse_string_datetime(ts_string)
            .or_else(|| try_parse_string_date(ts_string).and_then(|date| date.and_hms_opt(0, 0, 0)))
            .map(|naive| Utc.from_utc_datetime(&naive))
            .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                what: "Timestamp".to_string(),
                value: ts_string.to_string(),
            })?;

        let seconds = utc_dt.timestamp();
        let nanos = utc_dt.timestamp_subsec_nanos() as i32;
        Ok(TimestampProtobuf { seconds, nanos })
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

    fn get_gene_data_from_hgnc(
        &mut self,
        phenopacket_id: &str,
        gene: &str,
    ) -> Result<(String, String), PhenopacketBuilderError> {
        let hgnc_response = self.hgnc_client.fetch_gene_data(gene)?;
        let returned_symbol_id_pairs = hgnc_response.symbol_id_pair();
        let (symbol_ref, id_ref) = returned_symbol_id_pairs.first().ok_or_else(|| {
            PhenopacketBuilderError::HgncGenePair(format!(
                "No (gene_symbol, hgnc_id) pair found via HGNC for gene {gene}."
            ))
        })?;

        self.ensure_resource(
            phenopacket_id,
            &DatabaseRef::hgnc());

        Ok((symbol_ref.to_string(), id_ref.to_string()))
    }

    //Taken from ga4ghphetools
    fn get_hgvs_variant_interpretation(
        hgvs: &HgvsVariant,
        allele_count: usize,
    ) -> VariantInterpretation {
        let gene_ctxt = GeneDescriptor {
            value_id: hgvs.hgnc_id().to_string(),
            symbol: hgvs.symbol().to_string(),
            description: String::default(),
            alternate_ids: vec![],
            alternate_symbols: vec![],
            xrefs: vec![],
        };
        let vcf_record = VcfRecord {
            genome_assembly: hgvs.assembly().to_string(),
            chrom: hgvs.chr().to_string(),
            pos: hgvs.position() as u64,
            id: String::default(),
            r#ref: hgvs.ref_allele().to_string(),
            alt: hgvs.alt_allele().to_string(),
            qual: String::default(),
            filter: String::default(),
            info: String::default(),
        };

        let hgvs_c = Expression {
            syntax: "hgvs.c".to_string(),
            value: format!("{}:{}", hgvs.transcript(), hgvs.hgvs()),
            version: String::default(),
        };
        let mut expression_list = vec![hgvs_c];
        let hgvs_g = Expression {
            syntax: "hgvs.g".to_string(),
            value: hgvs.g_hgvs().to_string(),
            version: String::default(),
        };
        expression_list.push(hgvs_g);
        if let Some(hgsvp) = hgvs.p_hgvs() {
            let hgvs_p = Expression {
                syntax: "hgvs.p".to_string(),
                value: hgsvp,
                version: String::default(),
            };
            expression_list.push(hgvs_p);
        };
        let allelic_state = Self::get_allele_term(allele_count, hgvs.is_x_chromosomal());
        let vdesc = VariationDescriptor {
            id: hgvs.variant_key().to_string(),
            variation: None,
            label: String::default(),
            description: String::default(),
            gene_context: Some(gene_ctxt),
            expressions: expression_list,
            vcf_record: Some(vcf_record),
            xrefs: vec![],
            alternate_labels: vec![],
            extensions: vec![],
            molecule_context: MoleculeContext::Genomic.into(),
            structural_type: None,
            vrs_ref_allele_seq: String::default(),
            allelic_state: Some(allelic_state),
        };
        VariantInterpretation {
            acmg_pathogenicity_classification: AcmgPathogenicityClassification::Pathogenic.into(),
            therapeutic_actionability: TherapeuticActionability::UnknownActionability.into(),
            variation_descriptor: Some(vdesc),
        }
    }

    //Taken from ga4ghphetools
    fn get_allele_term(allele_count: usize, is_x: bool) -> OntologyClass {
        if allele_count == 2 {
            OntologyClass {
                id: "GENO:0000136".to_string(),
                label: "homozygous".to_string(),
            }
        } else if is_x {
            OntologyClass {
                id: "GENO:0000134".to_string(),
                label: "hemizygous".to_string(),
            }
        } else {
            OntologyClass {
                id: "GENO:0000135".to_string(),
                label: "heterozygous".to_string(),
            }
        }
    }

    fn get_genomic_interpretation_from_data(
        patient_id: &str,
        variant: &str,
        gene_symbol: &str,
        hgnc_id: &str,
        allele_count: usize,
    ) -> Result<GenomicInterpretation, PhenopacketBuilderError> {
        let split_var = variant.split(':').collect::<Vec<&str>>();
        let transcript = split_var[0];
        let allele = split_var[1];

        let hgvs_variant = validate_one_hgvs_variant(gene_symbol, hgnc_id, transcript, allele)
            .map_err(PhenopacketBuilderError::VariantValidation)?;

        let variant_interpretation =
            Self::get_hgvs_variant_interpretation(&hgvs_variant, allele_count);

        Ok(GenomicInterpretation {
            subject_or_biosample_id: patient_id.to_string(),
            call: Some(Call::VariantInterpretation(variant_interpretation)),
            ..Default::default()
        })
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
    use crate::ontology::resource_references::ResourceRef;
    use crate::test_utils::{assert_phenopackets, build_test_phenopacket_builder};
    use phenopackets::schema::v2::core::time_element::Element::Age;
    use phenopackets::schema::v2::core::{Age as age_struct, MetaData, Resource};
    use pretty_assertions::assert_eq;
    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    fn phenopacket_id() -> String {
        "cohort_patient_001".to_string()
    }

    #[fixture]
    fn valid_phenotype() -> String {
        "HP:0001166".to_string()
    }

    #[fixture]
    fn another_phenotype() -> String {
        "Microcephaly".to_string()
    }

    #[fixture]
    fn onset_age() -> Option<&'static str> {
        Some("P48Y4M21D")
    }
    #[fixture]
    fn mondo_resource() -> Resource {
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
    fn onset_age_te() -> Option<TimeElement> {
        Some(TimeElement {
            element: Some(Age(age_struct {
                iso8601duration: "P48Y4M21D".to_string(),
            })),
        })
    }

    #[fixture]
    fn onset_timestamp() -> Option<&'static str> {
        Some("2005-10-01T12:34:56Z")
    }

    #[fixture]
    fn onset_timestamp_te() -> Option<TimeElement> {
        Some(TimeElement {
            element: Some(Timestamp(TimestampProtobuf {
                seconds: 1128170096,
                nanos: 0,
            })),
        })
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_build(phenopacket_id: String, temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket = Phenopacket {
            id: phenopacket_id.clone(),
            subject: Some(Individual {
                id: "subject_1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        builder
            .subject_to_phenopacket
            .insert(phenopacket_id.clone(), phenopacket);

        let builds = builder.build();
        let build_pp = builds.first().unwrap();

        assert_eq!(build_pp.id, phenopacket_id);
        assert_eq!(
            build_pp.subject,
            Some(Individual {
                id: "subject_1".to_string(),
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
    fn test_upsert_phenotypic_feature_success(
        phenopacket_id: String,
        valid_phenotype: String,
        onset_age: Option<&str>,
        onset_age_te: Option<TimeElement>,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                onset_age,
                None,
                None,
            )
            .unwrap();

        assert!(builder.subject_to_phenopacket.contains_key(&phenopacket_id));

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        let ontology_class = feature.r#type.as_ref().unwrap();
        assert_eq!(ontology_class.id, "HP:0001166");
        assert_eq!(ontology_class.label, "Arachnodactyly");

        assert!(feature.onset.is_some());
        let feature_onset = feature.onset.as_ref().unwrap();
        assert_eq!(feature_onset, &onset_age_te.unwrap());
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_invalid_term(phenopacket_id: String, temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
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
    fn test_multiple_phenotypic_features_same_phenopacket(
        phenopacket_id: String,
        valid_phenotype: String,
        another_phenotype: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
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
                phenopacket_id.as_str(),
                &another_phenotype,
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
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_different_phenopacket_ids(valid_phenotype: String, temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let id1 = "pp_001".to_string();
        let id2 = "pp_002".to_string();

        builder
            .upsert_phenotypic_feature(
                id1.as_str(),
                &valid_phenotype,
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
                &valid_phenotype,
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
    fn test_update_phenotypic_features(
        phenopacket_id: String,
        valid_phenotype: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let existing_phenopacket = Phenopacket {
            id: phenopacket_id.clone(),
            subject: None,
            phenotypic_features: vec![PhenotypicFeature {
                description: "".to_string(),
                r#type: Some(OntologyClass {
                    id: "HP:0000001".to_string(),
                    label: "All".to_string(),
                }),
                excluded: false,
                severity: None,
                modifiers: vec![],
                onset: None,
                resolution: None,
                evidence: vec![],
            }],
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
            .insert(phenopacket_id.clone(), existing_phenopacket);

        // Add another feature
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
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
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_update_onset_of_phenotypic_feature(
        phenopacket_id: String,
        onset_age: Option<&str>,
        onset_timestamp: Option<&str>,
        onset_timestamp_te: Option<TimeElement>,
        valid_phenotype: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                onset_age,
                None,
                None,
            )
            .unwrap();

        // Update the same feature
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                onset_timestamp,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        assert!(feature.onset.is_some());
        let feature_onset = feature.onset.as_ref().unwrap();
        assert_eq!(feature_onset, &onset_timestamp_te.unwrap());
    }

    #[fixture]
    fn basic_pp_with_disease_info(mondo_resource: Resource) -> Phenopacket {
        Phenopacket {
            id: "pp_001".to_string(),
            interpretations: vec![Interpretation {
                id: "pp_001-MONDO:0012145".to_string(),
                progress_status: 0, // UNKNOWN_PROGRESS
                diagnosis: Some(Diagnosis {
                    disease: Some(OntologyClass {
                        id: "MONDO:0012145".to_string(),
                        label: "macular degeneration, age-related, 3".to_string(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            meta_data: Some(MetaData {
                resources: vec![mondo_resource],
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
        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";

        builder
            .upsert_interpretation("P006", phenopacket_id, disease_id, &[], &[])
            .unwrap();

        assert_eq!(
            &basic_pp_with_disease_info,
            builder.subject_to_phenopacket.values().next().unwrap()
        );
    }

    #[rstest]
    fn test_upsert_interpretation_homozygous_variant(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";

        builder
            .upsert_interpretation(
                "P006",
                phenopacket_id,
                disease_id,
                &["KIF21A"],
                &["NM_001173464.1:c.2860C>T", "NM_001173464.1:c.2860C>T"],
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
        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";

        builder
            .upsert_interpretation(
                "P006",
                phenopacket_id,
                disease_id,
                &["ALMS1"],
                &[
                    "NM_015120.4:c.6960_6963delACAG",
                    "NM_015120.4:c.11031_11032delGA",
                ],
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
                    assert_eq!(vd.gene_context.unwrap().symbol, "ALMS1");
                    let coding_expressions = vd
                        .expressions
                        .iter()
                        .filter(|exp| exp.syntax == "hgvs.c")
                        .collect::<Vec<&Expression>>();
                    assert_eq!(coding_expressions.len(), 1);
                }
            }
        }
    }

    #[rstest]
    fn test_upsert_interpretation_heterozygous_variant(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";

        builder
            .upsert_interpretation(
                "P006",
                phenopacket_id,
                disease_id,
                &["KIF21A"],
                &["NM_001173464.1:c.2860C>T"],
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
    fn test_upsert_interpretation_update(
        basic_pp_with_disease_info: Phenopacket,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = "pp_001";

        let existing_pp = basic_pp_with_disease_info;
        builder
            .subject_to_phenopacket
            .insert(phenopacket_id.to_string(), existing_pp.clone());

        builder
            .upsert_interpretation(
                "P006",
                phenopacket_id,
                "macular degeneration, age-related, 3",
                &["KIF21A"],
                &["NM_001173464.1:c.2860C>T"],
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
        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";

        builder
            .upsert_interpretation("P006", phenopacket_id, disease_id, &["KIF21A"], &[])
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
    fn test_upsert_interpretation_invalid_configuration_err(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";
        // multiple genes
        assert!(
            builder
                .upsert_interpretation(
                    "P006",
                    phenopacket_id,
                    disease_id,
                    &["KIF21A", "CLOCK"],
                    &["NM_001173464.1:c.2860C>T"]
                )
                .is_err()
        );
        // too many variants
        assert!(
            builder
                .upsert_interpretation(
                    "P006",
                    phenopacket_id,
                    disease_id,
                    &["KIF21A"],
                    &[
                        "NM_001173464.1:c.2860C>T",
                        "NM_001173464.1:c.2860C>T",
                        "NM_001173464.1:c.2860C>T"
                    ]
                )
                .is_err()
        );
        // no genes
        assert!(
            builder
                .upsert_interpretation(
                    "P006",
                    phenopacket_id,
                    disease_id,
                    &[],
                    &["NM_001173464.1:c.2860C>T"]
                )
                .is_err()
        );
    }

    #[rstest]
    fn test_insert_disease(mondo_resource: Resource, temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0012145";
        let onset_age = "P3Y4M";

        builder
            .insert_disease(
                phenopacket_id,
                disease_id,
                None,
                Some(onset_age),
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
                term: Some(OntologyClass {
                    id: disease_id.to_string(),
                    label: "macular degeneration, age-related, 3".to_string(),
                }),
                onset: Some(TimeElement {
                    element: Some(Age(age_struct {
                        iso8601duration: "P3Y4M".to_string(),
                    })),
                }),
                ..Default::default()
            }],
            meta_data: Some(MetaData {
                resources: vec![mondo_resource],
                ..Default::default()
            }),
            ..Default::default()
        };

        let built_pp = builder.subject_to_phenopacket.values().next().unwrap();

        assert_phenopackets(expected_pp, &mut built_pp.clone());
    }

    #[rstest]
    fn test_insert_same_disease_twice(mondo_resource: Resource, temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = "pp_001";
        let disease_id = "MONDO:0008258";

        builder
            .insert_disease(
                phenopacket_id,
                disease_id,
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
                phenopacket_id,
                disease_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let platelet_defect_disease_no_onset = Disease {
            term: Some(OntologyClass {
                id: "MONDO:0008258".to_string(),
                label: "platelet signal processing defect".to_string(),
            }),
            ..Default::default()
        };

        let expected_pp = &mut Phenopacket {
            id: phenopacket_id.to_string(),
            diseases: vec![
                platelet_defect_disease_no_onset.clone(),
                platelet_defect_disease_no_onset.clone(),
            ],
            meta_data: Some(MetaData {
                resources: vec![mondo_resource],
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

        let phenopacket_id = "pp_001";
        let individual_id = "individual_001";

        // Test just upserting the individual id
        builder
            .upsert_individual(
                phenopacket_id,
                individual_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();
        assert_eq!(individual.id, individual_id);
        assert_eq!(individual.sex, 0);
        assert_eq!(individual.vital_status, None);

        // Test upserting the other entries
        builder
            .upsert_individual(
                phenopacket_id,
                individual_id,
                None,
                Some("2001-01-29"),
                None,
                Some("MALE"),
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(individual.sex, Sex::Male as i32);
        assert_eq!(
            individual.date_of_birth,
            Some(TimestampProtobuf {
                seconds: 980726400,
                nanos: 0,
            })
        );
    }

    #[rstest]
    fn test_upsert_vital_status(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let phenopacket_id = "pp_001";

        builder
            .upsert_vital_status(phenopacket_id, "ALIVE", Some("P81Y5M13D"), None, Some(322))
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(
            individual.vital_status,
            Some(VitalStatus {
                status: 1,
                time_of_death: Some(TimeElement {
                    element: Some(Age(IndividualAge {
                        iso8601duration: "P81Y5M13D".to_string()
                    }))
                }),
                cause_of_death: None,
                survival_time_in_days: 322,
            })
        );
    }

    #[rstest]
    fn test_query_hpo_identifiers_with_valid_label(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        // Known HPO label from test_utils::HPO_DICT: "Seizure" <-> "HP:0001250"
        let result = builder.query_hpo_identifiers("Seizure").unwrap();

        assert_eq!(result.label, "Seizure");
        assert_eq!(result.id, "HP:0001250");
    }

    #[rstest]
    fn test_query_hpo_identifiers_with_valid_id(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        let result = builder.query_hpo_identifiers("HP:0001250").unwrap();

        assert_eq!(result.label, "Seizure");
        assert_eq!(result.id, "HP:0001250");
    }

    #[rstest]
    fn test_query_hpo_identifiers_invalid_query(temp_dir: TempDir) {
        let builder = build_test_phenopacket_builder(temp_dir.path());

        let result = builder.query_hpo_identifiers("NonexistentTerm");

        assert!(result.is_err());
    }

    #[rstest]
    fn test_parse_time_element_duration() {
        let te = PhenopacketBuilder::try_parse_time_element("P81Y5M13D").unwrap();
        assert_eq!(
            te,
            TimeElement {
                element: Some(Age(IndividualAge {
                    iso8601duration: "P81Y5M13D".to_string()
                }))
            }
        );
    }

    #[rstest]
    fn test_parse_time_element_datetime() {
        let te_date = PhenopacketBuilder::try_parse_time_element("2001-01-29").unwrap();
        assert_eq!(
            te_date,
            TimeElement {
                element: Some(Timestamp(TimestampProtobuf {
                    seconds: 980726400,
                    nanos: 0,
                })),
            }
        );
        let te_datetime =
            PhenopacketBuilder::try_parse_time_element("2015-06-05T09:17:39Z").unwrap();
        assert_eq!(
            te_datetime,
            TimeElement {
                element: Some(Timestamp(TimestampProtobuf {
                    seconds: 1433495859,
                    nanos: 0,
                })),
            }
        );
    }

    #[rstest]
    #[case("P81D5M13Y")]
    #[case("8D5M13Y")]
    #[case("09:17:39Z")]
    #[case("2020-20-15T09:17:39Z")]
    fn test_parse_time_element_invalid(#[case] date_str: &str) {
        let result = PhenopacketBuilder::try_parse_time_element(date_str);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_parse_timestamp() {
        let ts_date = PhenopacketBuilder::try_parse_timestamp("2001-01-29").unwrap();
        assert_eq!(
            ts_date,
            TimestampProtobuf {
                seconds: 980726400,
                nanos: 0,
            }
        );
        let ts_datetime = PhenopacketBuilder::try_parse_timestamp("2015-06-05T09:17:39Z").unwrap();
        assert_eq!(
            ts_datetime,
            TimestampProtobuf {
                seconds: 1433495859,
                nanos: 0,
            }
        );
        let result = PhenopacketBuilder::try_parse_timestamp("09:17:39Z");
        assert!(result.is_err());
        let result = PhenopacketBuilder::try_parse_timestamp("2020-20-15T09:17:39Z");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_get_or_create_phenopacket(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = "pp_001";
        builder.get_or_create_phenopacket(phenopacket_id);
        let pp = builder.get_or_create_phenopacket(phenopacket_id);
        assert_eq!(pp.id, phenopacket_id);
        assert_eq!(builder.subject_to_phenopacket.len(), 1);
    }

    #[rstest]
    fn test_ensure_resource(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = "test_id".to_string();

        builder.ensure_resource(
            &pp_id,
            &ResourceRef::new("omim".to_string(), "latest".to_string()),
        );

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

    #[rstest]
    fn test_get_gene_data_from_hgnc(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = "test_id";

        let hgnc_data = builder.get_gene_data_from_hgnc(pp_id, "CLOCK").unwrap();

        assert_eq!(hgnc_data, ("CLOCK".to_string(), "HGNC:2082".to_string()));
    }

    #[rstest]
    fn test_get_genomic_interpretation_from_data() {
        let pp_gi = PhenopacketBuilder::get_genomic_interpretation_from_data(
            "P006",
            "NM_001173464.1:c.2860C>T",
            "KIF21A",
            "HGNC:19349",
            2,
        )
        .unwrap();

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
    fn test_disease_query_priority(temp_dir: TempDir) {
        //TDOD: Finish when omim is implemented
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let disease = "a sever disease, you do not want to have".to_string();
        let omim_id = "OMIM:0099";
        let mondo_ref = OntologyRef::mondo();
        let _omim_ref = DatabaseRef::omim();
        let label_to_id_mondo =
            HashMap::from_iter([(disease.to_string(), "MONDO:0032".to_string())]);

        let _label_to_id_omim: HashMap<String, String> =
            HashMap::from_iter([(disease.to_string(), omim_id.to_string())]);
        let custom_ontology_dicts: HashMap<String, Arc<OntologyBiDict>> = HashMap::from_iter([
            (
                mondo_ref.prefix_id().to_string(),
                Arc::new(OntologyBiDict::new(
                    OntologyRef::mondo(),
                    label_to_id_mondo.clone(),
                    HashMap::from_iter(
                        label_to_id_mondo
                            .iter()
                            .map(|(key, value)| (value.clone(), key.clone())),
                    ),
                    Default::default(),
                )),
            ),
            /*(omim_ref.prefix_id()
            ,BiDict::new(
                DatabaseRef::omim(None),
                label_to_id_omim.clone(),
                 HashMap::from_iter(
                        label_to_id_omim
                            .iter()
                            .map(|(key, value)| (value.clone(), key.clone())),
                    ),
                Default::default(),)
            ),*/
        ]);
        builder.ontology_bidicts = custom_ontology_dicts;

        let (onto_class, _resource_ref) = builder.query_disease_identifiers(&disease).unwrap();

        assert_eq!(onto_class.label, disease);
        //assert_eq!(onto_class.id, omim_id);

        //assert_eq!(resource_ref.prefix_id(), omim_ref.prefix_id());
    }
}
