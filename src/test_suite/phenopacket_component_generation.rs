use crate::test_suite::cdf_generation::default_patient_id;
use crate::test_suite::ontology_mocking::{HPO_DICT, MONDO_BIDICT};
use crate::transform::data_processing::parsing::try_parse_string_datetime;
use phenopackets::schema::v2::core::time_element::Element;
use phenopackets::schema::v2::core::{Age, Disease, OntologyClass, PhenotypicFeature, TimeElement};
use prost_types::Timestamp;

pub(crate) fn default_cohort_id() -> String {
    "Cohort-1".to_string()
}

pub(crate) fn default_phenopacket_id() -> String {
    let patient_id = default_patient_id();
    let cohort_id = default_cohort_id();
    format!("{}-{}", cohort_id, patient_id)
}

#[allow(dead_code)]
pub(crate) fn generate_disease(id: &str, onset: Option<TimeElement>) -> Disease {
    let label = MONDO_BIDICT
        .get(id)
        .expect("Not MONDO label found for id in bidict");

    Disease {
        term: Some(OntologyClass {
            id: id.to_string(),
            label: label.to_string(),
        }),
        onset,
        ..Default::default()
    }
}

pub(crate) fn default_disease_oc() -> OntologyClass {
    OntologyClass {
        id: "MONDO:0000359".to_string(),
        label: "spondylocostal dysostosis".to_string(),
    }
}

pub(crate) fn default_disease() -> Disease {
    Disease {
        term: Some(default_disease_oc()),
        ..Default::default()
    }
}
pub(crate) fn default_disease_with_age_onset() -> Disease {
    let mut default_disease = default_disease();
    default_disease.onset = Some(default_age_element());

    default_disease
}

pub(crate) fn default_age_element() -> TimeElement {
    TimeElement {
        element: Some(Element::Age(Age {
            iso8601duration: default_iso_age(),
        })),
    }
}

pub(crate) fn default_iso_age() -> String {
    "P10Y4M21D".to_string()
}

pub(crate) fn default_timestamp() -> Timestamp {
    let dt = try_parse_string_datetime("2005-10-01T12:34:56Z").unwrap();

    Timestamp {
        seconds: dt.and_utc().timestamp(),
        nanos: dt.and_utc().timestamp_subsec_nanos() as i32,
    }
}

pub(crate) fn default_timestamp_element() -> TimeElement {
    TimeElement {
        element: Some(Element::Timestamp(default_timestamp())),
    }
}

pub(crate) fn generate_phenotype(id: &str, onset: Option<TimeElement>) -> PhenotypicFeature {
    let label = HPO_DICT
        .get(id)
        .expect("Not HP label found for id in bidict");

    PhenotypicFeature {
        r#type: Some(OntologyClass {
            id: id.to_string(),
            label: label.to_string(),
        }),
        onset,
        ..Default::default()
    }
}
#[allow(dead_code)]
pub(crate) fn generate_phenotype_oc(id: &str) -> OntologyClass {
    let label = HPO_DICT
        .get(id)
        .expect("Not HP label found for id in bidict");

    OntologyClass {
        id: id.to_string(),
        label: label.to_string(),
    }
}

pub(crate) fn default_phenotype() -> PhenotypicFeature {
    PhenotypicFeature {
        r#type: Some(default_phenotype_oc()),
        ..Default::default()
    }
}

#[allow(dead_code)]
pub(crate) fn default_phenotype_with_age_onset() -> PhenotypicFeature {
    let mut default = default_phenotype();
    default.onset = Some(default_age_element());
    default
}

pub(crate) fn default_phenotype_oc() -> OntologyClass {
    OntologyClass {
        id: "HP:0041249".to_string(),
        label: "Fractured nose".to_string(),
    }
}
