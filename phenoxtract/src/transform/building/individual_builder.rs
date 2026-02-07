use crate::transform::building::ppb::PhenopacketBuilder;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::utils::{try_parse_time_element, try_parse_timestamp};
use log::warn;
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{Individual, Sex, VitalStatus};

pub(crate) struct VitalStatusBuilder<'a> {
    individual_builder: IndividualBuilder<'a>,
    status: &'a str,
    time_of_death: Option<&'a str>,
    cause_of_death: Option<&'a str>,
    survival_time_in_days: Option<u32>,
}

impl<'a> VitalStatusBuilder<'a> {
    fn new(individual_builder: IndividualBuilder<'a>, status: &'a str) -> Self {
        Self {
            individual_builder,
            status,
            time_of_death: None,
            cause_of_death: None,
            survival_time_in_days: None,
        }
    }

    pub fn time_of_death(mut self, time_of_death: &'a str) -> Self {
        self.time_of_death = Some(time_of_death);
        self
    }

    pub fn cause_of_death(mut self, cause_of_death: &'a str) -> Self {
        self.cause_of_death = Some(cause_of_death);
        self
    }
    pub fn survival_time_in_days(mut self, survival_time_in_days: u32) -> Self {
        self.survival_time_in_days = Some(survival_time_in_days);
        self
    }

    pub fn apply(mut self) -> Result<IndividualBuilder<'a>, PhenopacketBuilderError> {
        let status =
            Status::from_str_name(self.status).ok_or(PhenopacketBuilderError::ParsingError {
                what: "vital status".to_string(),
                value: self.status.to_string(),
            })? as i32;

        let time_of_death = match self.time_of_death {
            Some(tod_string) => Some(try_parse_time_element(tod_string).ok_or_else(|| {
                PhenopacketBuilderError::ParsingError {
                    what: "TimeElement".to_string(),
                    value: tod_string.to_string(),
                }
            })?),
            None => None,
        };

        let cause_of_death = match self.cause_of_death {
            Some(cause_of_death) => {
                let (disease_term, disease_ref) = self
                    .individual_builder
                    .pp_builder
                    .ctx
                    .dictionary_registry
                    .disease
                    .query_bidicts(cause_of_death)
                    .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                        what: "disease term".to_string(),
                        value: cause_of_death.to_string(),
                    })?;
                self.individual_builder
                    .pp_builder
                    .ensure_resource(self.individual_builder.patient_id, &disease_ref);
                Some(disease_term)
            }
            None => None,
        };

        let survival_time_in_days = self.survival_time_in_days.unwrap_or(0);

        self.individual_builder.vital_status = Some(VitalStatus {
            status,
            time_of_death,
            cause_of_death,
            survival_time_in_days,
        });
        Ok(self.individual_builder)
    }
}

pub(crate) struct IndividualBuilder<'a> {
    pp_builder: &'a mut PhenopacketBuilder,
    patient_id: &'a str,
    alternate_ids: Vec<&'a str>,
    date_of_birth: Option<&'a str>,
    time_at_last_encounter: Option<&'a str>,
    sex: Option<&'a str>,
    karyotypic_sex: Option<&'a str>,
    gender: Option<&'a str>,
    taxonomy: Option<&'a str>,
    vital_status: Option<VitalStatus>,
}

impl<'a> IndividualBuilder<'a> {
    pub fn new(pp_builder: &'a mut PhenopacketBuilder, patient_id: &'a str) -> Self {
        Self {
            pp_builder,
            patient_id,
            alternate_ids: vec![],
            date_of_birth: None,
            time_at_last_encounter: None,
            sex: None,
            karyotypic_sex: None,
            gender: None,
            taxonomy: None,
            vital_status: None,
        }
    }

    pub fn vital_status(self, status: &'a str) -> VitalStatusBuilder<'a> {
        VitalStatusBuilder::new(self, status)
    }

    pub fn alternate_ids(mut self, ids: Vec<&'a str>) -> Self {
        self.alternate_ids = ids;
        self
    }

    pub fn date_of_birth(mut self, date: &'a str) -> Self {
        self.date_of_birth = Some(date);
        self
    }

    pub fn time_at_last_encounter(mut self, time: &'a str) -> Self {
        self.time_at_last_encounter = Some(time);
        self
    }

    pub fn sex(mut self, s: &'a str) -> Self {
        self.sex = Some(s);
        self
    }

    pub fn karyotypic_sex(mut self, s: &'a str) -> Self {
        self.karyotypic_sex = Some(s);
        self
    }
    pub fn gender(mut self, s: &'a str) -> Self {
        self.gender = Some(s);
        self
    }

    pub fn taxonomy(mut self, s: &'a str) -> Self {
        self.taxonomy = Some(s);
        self
    }

    pub fn apply(self) -> Result<(), PhenopacketBuilderError> {
        if !self.alternate_ids.is_empty() {
            warn!("alternate_ids - not implemented for individual yet");
        }
        if self.karyotypic_sex.is_some() {
            warn!("karyotypic_sex - not implemented for individual yet");
        }
        if self.gender.is_some() {
            warn!("gender - not implemented for individual yet");
        }
        if self.taxonomy.is_some() {
            warn!("taxonomy - not implemented for individual yet");
        }

        let phenopacket = self.pp_builder.get_or_create_phenopacket(self.patient_id);

        let individual = phenopacket.subject.get_or_insert(Individual::default());
        individual.id = self.patient_id.to_string();

        if let Some(date_of_birth) = self.date_of_birth {
            individual.date_of_birth =
                Some(try_parse_timestamp(date_of_birth).ok_or_else(|| {
                    PhenopacketBuilderError::ParsingError {
                        what: "TimeStamp".to_string(),
                        value: date_of_birth.to_string(),
                    }
                })?);
        }

        if let Some(sex) = self.sex {
            individual.sex = Sex::from_str_name(sex)
                .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                    what: "Sex".to_string(),
                    value: sex.to_string(),
                })?
                .into();
        }

        if let Some(time_str) = self.time_at_last_encounter {
            let time_te = try_parse_time_element(time_str).ok_or_else(|| {
                PhenopacketBuilderError::ParsingError {
                    what: "Time At Last Encounter".to_string(),
                    value: time_str.to_string(),
                }
            })?;
            individual.time_at_last_encounter = Some(time_te);
        }

        if let Some(vital_status) = self.vital_status {
            individual.vital_status = Some(vital_status);
        }

        Ok(())
    }
}
