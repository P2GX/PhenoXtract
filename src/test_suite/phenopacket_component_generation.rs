use crate::ontology::traits::BiDict;
use crate::test_suite::cdf_generation::default_patient_id;
use crate::test_suite::ontology_mocking::{HPO_DICT, MONDO_BIDICT, UO_DICT};
use chrono::{NaiveDate, NaiveDateTime};
use phenopackets::schema::v2::core::measurement::MeasurementValue;
use phenopackets::schema::v2::core::time_element::Element;
use phenopackets::schema::v2::core::value::Value;
use phenopackets::schema::v2::core::{
    Age, Disease, Measurement, OntologyClass, PhenotypicFeature, Quantity, ReferenceRange,
    TimeElement, Value as ValueStruct,
};
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

pub(crate) fn default_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2005, 10, 1).unwrap()
}

pub(crate) fn default_datetime() -> NaiveDateTime {
    default_date().and_hms_opt(12, 34, 56).unwrap()
}

/// Corresponds to the default datetime, not the default date
pub(crate) fn default_timestamp() -> Timestamp {
    let dt = default_datetime();
    Timestamp {
        seconds: dt.and_utc().timestamp(),
        nanos: dt.and_utc().timestamp_subsec_nanos() as i32,
    }
}

/// Corresponds to the default datetime, not the default date
pub(crate) fn default_timestamp_element() -> TimeElement {
    TimeElement {
        element: Some(Element::Timestamp(default_timestamp())),
    }
}

pub(crate) fn generate_phenotype(id: &str, onset: Option<TimeElement>) -> PhenotypicFeature {
    let label = HPO_DICT
        .get(id)
        .expect("No HP label found for id in bidict");

    PhenotypicFeature {
        r#type: Some(OntologyClass {
            id: id.to_string(),
            label: label.to_string(),
        }),
        onset,
        ..Default::default()
    }
}

pub(crate) fn default_quant_loinc() -> OntologyClass {
    OntologyClass {
        id: "LOINC:8302-2".to_string(),
        label: "Body height".to_string(),
    }
}

pub(crate) fn default_qual_loinc() -> OntologyClass {
    OntologyClass {
        id: "LOINC:5802-4".to_string(),
        label: "Nitrite [Presence] in Urine by Test strip".to_string(),
    }
}

pub(crate) fn default_uo_term() -> OntologyClass {
    OntologyClass {
        id: "UO:0000015".to_string(),
        label: "centimeter".to_string(),
    }
}

pub(crate) fn default_reference_range() -> (f64, f64) {
    (0.0, 3.3)
}

pub(crate) fn default_quant_value() -> f64 {
    1.1
}

pub(crate) fn default_pato_qual_measurement() -> OntologyClass {
    OntologyClass {
        id: "PATO:0000467".to_string(),
        label: "present".to_string(),
    }
}

pub(crate) fn default_quant_measurement() -> Measurement {
    generate_quant_measurement(
        default_quant_loinc(),
        default_quant_value(),
        Some(default_age_element()),
        default_uo_term().id.as_str(),
        Some(default_reference_range()),
    )
}

pub(crate) fn default_qual_measurement() -> Measurement {
    generate_qual_measurement(
        default_qual_loinc(),
        default_pato_qual_measurement(),
        Some(default_age_element()),
    )
}

pub(crate) fn generate_quant_measurement(
    loinc_term: OntologyClass,
    quant_measurement: f64,
    time_observed: Option<TimeElement>,
    unit_id: &str,
    reference_range: Option<(f64, f64)>,
) -> Measurement {
    let unit_label = UO_DICT
        .get(unit_id)
        .expect("No UO label found for id in bidict");

    let unit_ontology_term = OntologyClass {
        id: unit_id.to_string(),
        label: unit_label.to_string(),
    };

    let mut quantity = Quantity {
        unit: Some(unit_ontology_term.clone()),
        value: quant_measurement,
        ..Default::default()
    };

    if let Some(reference_range) = reference_range {
        quantity.reference_range = Some(ReferenceRange {
            unit: Some(unit_ontology_term),
            low: reference_range.0,
            high: reference_range.1,
        });
    };

    Measurement {
        assay: Some(loinc_term),
        measurement_value: Some(MeasurementValue::Value(ValueStruct {
            value: Some(Value::Quantity(quantity)),
        })),
        time_observed,
        ..Default::default()
    }
}

pub(crate) fn generate_qual_measurement(
    loinc_term: OntologyClass,
    qual_measurement_term: OntologyClass,
    time_observed: Option<TimeElement>,
) -> Measurement {
    Measurement {
        assay: Some(loinc_term),
        measurement_value: Some(MeasurementValue::Value(ValueStruct {
            value: Some(Value::OntologyClass(qual_measurement_term)),
        })),
        time_observed,
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
