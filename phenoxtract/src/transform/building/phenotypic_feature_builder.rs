use crate::transform::building::ppb::PhenopacketBuilder;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::utils::try_parse_time_element;
use phenopackets::schema::v2::core::PhenotypicFeature;

pub struct PhenotypicFeatureBuilder<'a> {
    pp_builder: &'a mut PhenopacketBuilder,
    patient_id: &'a str,
    phenotype: &'a str,
    description: Option<&'a str>,
    excluded: Option<bool>,
    severity: Option<&'a str>,
    modifiers: Vec<&'a str>,
    onset: Option<&'a str>,
    resolution: Option<&'a str>,
    evidence: Option<&'a str>,
}
impl<'a> PhenotypicFeatureBuilder<'a> {
    pub fn new(
        pp_builder: &'a mut PhenopacketBuilder,
        patient_id: &'a str,
        phenotype: &'a str,
    ) -> Self {
        Self {
            pp_builder,
            patient_id,
            phenotype,
            description: None,
            excluded: None,
            severity: None,
            modifiers: vec![],
            onset: None,
            resolution: None,
            evidence: None,
        }
    }

    pub fn description(mut self, desc: &'a str) -> Self {
        self.description = Some(desc);
        self
    }

    pub fn excluded(mut self) -> Self {
        self.excluded = Some(true);
        self
    }

    pub fn severity(mut self, severity: &'a str) -> Self {
        self.severity = Some(severity);
        self
    }

    pub fn modifiers(mut self, modifiers: Vec<&'a str>) -> Self {
        self.modifiers = modifiers;
        self
    }
    pub fn modifier(mut self, modifiers: &'a str) -> Self {
        self.modifiers.push(modifiers);
        self
    }

    pub fn onset(mut self, onset: &'a str) -> Self {
        self.onset = Some(onset);
        self
    }
    pub fn resolution(mut self, resolution: &'a str) -> Self {
        self.resolution = Some(resolution);
        self
    }

    pub fn evidence(mut self, evidence: &'a str) -> Self {
        self.evidence = Some(evidence);
        self
    }

    pub fn apply(self) -> Result<(), PhenopacketBuilderError> {
        if self.pp_builder.ctx.dictionary_registry.hpo.is_empty() {
            return Err(PhenopacketBuilderError::MissingBiDict {
                bidict_type: "HPO".to_string(),
            });
        }

        let (hpo_term, hpo_ref) = self
            .pp_builder
            .ctx
            .dictionary_registry
            .hpo
            .query_bidicts(self.phenotype)
            .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                what: "HPO term".to_string(),
                value: self.phenotype.to_string(),
            })?;

        let mut feature = PhenotypicFeature {
            r#type: Some(hpo_term),
            ..Default::default()
        };

        if let Some(desc) = self.description {
            feature.description = desc.to_string();
        }

        if let Some(excluded) = self.excluded {
            feature.excluded = excluded;
        }

        if let Some(onset) = self.onset {
            let onset_te = try_parse_time_element(onset).ok_or_else(|| {
                PhenopacketBuilderError::ParsingError {
                    what: "TimeElement".to_string(),
                    value: onset.to_string(),
                }
            })?;
            feature.onset = Some(onset_te);
        }

        self.pp_builder.ensure_resource(self.patient_id, &hpo_ref);
        Ok(())
    }
}
