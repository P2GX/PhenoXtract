use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::traits::ValidatePhenopacket;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::OntologyClass;
use regex::Regex;
use serde_json::Value;

/// Validator for ensuring ontology term identifiers conform to CURIE format.
///
/// This validator recursively traverses a phenopacket structure to find all
/// `OntologyClass` instances and verifies that their IDs follow the CURIE
/// (Compact URI) format: `PREFIX:LocalID`, where PREFIX consists of uppercase
/// letters, numbers, and underscores starting with a letter, and LocalID
/// consists of alphanumeric characters and underscores.
#[derive(Debug, Default)]
pub(crate) struct CurieValidator;

impl ValidatePhenopacket for CurieValidator {
    /// Validates that all ontology class identifiers in a phenopacket are valid CURIEs.
    ///
    /// This method serializes the phenopacket to JSON and recursively searches for
    /// ontology class objects, checking each one against the CURIE format regex:
    /// `^[A-Z][A-Z0-9_]+:[A-Za-z0-9_]+$`
    ///
    /// # Arguments
    ///
    /// * `phenopacket` - The phenopacket to validate
    /// * `report` - Mutable reference to a lint report where violations are recorded
    ///
    /// # Panics
    ///
    /// Panics if the phenopacket cannot be serialized to JSON
    fn validate(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        let value = serde_json::to_value(phenopacket)
            .unwrap_or_else(|_| panic!("Could not serialize phenopacket {}", phenopacket.id));
        Self::inner_validate(value, report);
    }
}
impl CurieValidator {
    fn inner_validate(value: Value, report: &mut LintReport) {
        if let Some(ont_class) = Self::get_ontology_class_from_value(&value) {
            let regex = Regex::new("^[A-Z][A-Z0-9_]+:[A-Za-z0-9_]+$").unwrap();
            if !regex.is_match(&ont_class.id) {
                report.push_violation(LintingViolations::NotACurieID(ont_class));
            }
        }

        match value {
            Value::Object(map) => {
                for (_key, sub_value) in map {
                    Self::inner_validate(sub_value, report);
                }
            }
            Value::Array(arr) => {
                for sub_value in arr {
                    Self::inner_validate(sub_value, report);
                }
            }
            _ => {}
        }
    }

    fn get_ontology_class_from_value(value: &Value) -> Option<OntologyClass> {
        if let Value::Object(map) = &value
            && map.keys().len() == 2
            && map.contains_key("label")
            && map.contains_key("id")
            && let Ok(ont_class) = serde_json::from_value::<OntologyClass>(value.clone())
        {
            Some(ont_class)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::linter::linting_report::LintReport;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::{Diagnosis, Interpretation, OntologyClass};
    use rstest::rstest;

    #[rstest]
    fn test_valid_curie_format() {
        let mut report = LintReport::new();
        let phenopacket = Phenopacket {
            id: "test-phenopacket".to_string(),
            interpretations: vec![Interpretation {
                diagnosis: Some(Diagnosis {
                    disease: Some(OntologyClass {
                        id: "MONDO:0000359".to_string(),
                        label: "spondylocostal dysostosis".to_string(),
                    }),
                    genomic_interpretations: vec![],
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        CurieValidator.validate(&phenopacket, &mut report);
        assert!(report.into_violations().is_empty());
    }

    #[rstest]
    fn test_invalid_curie_format() {
        let mut report = LintReport::new();
        let phenopacket = Phenopacket {
            id: "test-phenopacket".to_string(),
            interpretations: vec![Interpretation {
                diagnosis: Some(Diagnosis {
                    disease: Some(OntologyClass {
                        id: "666".to_string(),
                        label: "spondylocostal dysostosis".to_string(),
                    }),
                    genomic_interpretations: vec![],
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        CurieValidator.validate(&phenopacket, &mut report);
        assert!(!report.into_violations().is_empty());
    }
}
