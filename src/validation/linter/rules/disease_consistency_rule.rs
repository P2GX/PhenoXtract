use crate::validation::linter::enums::{FixAction, LintingViolations};
use crate::validation::linter::linting_report::{LintReport, LintReportInfo};
use crate::validation::linter::traits::RuleCheck;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::OntologyClass;

#[derive(Default)]
struct DiseaseConsistencyRule;

impl RuleCheck for DiseaseConsistencyRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        let inter_diseases: Vec<OntologyClass> = phenopacket
            .interpretations
            .iter()
            .filter_map(|inter| {
                inter
                    .diagnosis
                    .as_ref()
                    .and_then(|diagnosis| diagnosis.disease.clone())
            })
            .collect();
        let diseases: Vec<OntologyClass> = phenopacket
            .diseases
            .iter()
            .filter_map(|d| d.term.clone())
            .collect();

        let mut seen: Vec<&OntologyClass> = vec![];
        for inter_disease in inter_diseases.iter() {
            if !diseases.contains(inter_disease) && !seen.contains(&inter_disease) {
                report.push_info(LintReportInfo::new(
                    LintingViolations::DiseaseConsistency(inter_disease.clone()),
                    Some(FixAction::Duplicate),
                ));
                seen.push(inter_disease)
            }
        }
    }

    fn rule_id(&self) -> &'static str {
        "INTER001"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::linter::linting_report::LintReport;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::{Diagnosis, Disease, Interpretation, OntologyClass};

    fn create_ontology_class(id: &str, label: &str) -> OntologyClass {
        OntologyClass {
            id: id.to_string(),
            label: label.to_string(),
        }
    }

    fn create_disease(term: OntologyClass) -> Disease {
        Disease {
            term: Some(term),
            ..Default::default()
        }
    }

    fn create_interpretation(disease: Option<OntologyClass>) -> Interpretation {
        Interpretation {
            diagnosis: disease.map(|d| Diagnosis {
                disease: Some(d),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_no_interpretations_no_diseases() {
        let phenopacket = Phenopacket::default();
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        assert!(report.into_violations().is_empty());
    }

    #[test]
    fn test_validate_interpretation_matches_disease() {
        let disease_term = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let disease = create_disease(disease_term.clone());
        let interpretation = create_interpretation(Some(disease_term));

        let phenopacket = Phenopacket {
            diseases: vec![disease],
            interpretations: vec![interpretation],
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        assert!(report.into_violations().is_empty());
    }

    #[test]
    fn test_validate_interpretation_disease_mismatch() {
        let disease_term = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let interpretation_term = create_ontology_class("MONDO:0005148", "Diabetes");

        let disease = create_disease(disease_term);
        let interpretation = create_interpretation(Some(interpretation_term.clone()));

        let phenopacket = Phenopacket {
            diseases: vec![disease],
            interpretations: vec![interpretation],
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        let violations = report.into_violations();
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            LintingViolations::DiseaseConsistency(term) if term.id == "MONDO:0005148"
        ));
    }

    #[test]
    fn test_validate_multiple_interpretations_all_match() {
        let disease1 = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let disease2 = create_ontology_class("MONDO:0005148", "Diabetes");

        let diseases = vec![
            create_disease(disease1.clone()),
            create_disease(disease2.clone()),
        ];
        let interpretations = vec![
            create_interpretation(Some(disease1)),
            create_interpretation(Some(disease2)),
        ];

        let phenopacket = Phenopacket {
            diseases,
            interpretations,
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        assert!(report.into_violations().is_empty());
    }

    #[test]
    fn test_validate_multiple_interpretations_some_mismatch() {
        let disease1 = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let disease2 = create_ontology_class("MONDO:0005148", "Diabetes");
        let disease3 = create_ontology_class("MONDO:0005015", "Hypertension");

        let diseases = vec![
            create_disease(disease1.clone()),
            create_disease(disease2.clone()),
        ];
        let interpretations = vec![
            create_interpretation(Some(disease1)),
            create_interpretation(Some(disease3.clone())),
        ];

        let phenopacket = Phenopacket {
            diseases,
            interpretations,
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);
        let violations = report.into_violations();
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            LintingViolations::DiseaseConsistency(term) if term.id == "MONDO:0005015"
        ));
    }

    #[test]
    fn test_validate_interpretation_without_diagnosis() {
        let disease_term = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let disease = create_disease(disease_term);

        let interpretation = Interpretation {
            diagnosis: None,
            ..Default::default()
        };

        let phenopacket = Phenopacket {
            diseases: vec![disease],
            interpretations: vec![interpretation],
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        assert!(report.into_violations().is_empty());
    }

    #[test]
    fn test_validate_interpretation_with_diagnosis_but_no_disease() {
        let disease_term = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let disease = create_disease(disease_term);

        let interpretation = Interpretation {
            diagnosis: Some(Diagnosis {
                disease: None,
                ..Default::default()
            }),
            ..Default::default()
        };

        let phenopacket = Phenopacket {
            diseases: vec![disease],
            interpretations: vec![interpretation],
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        assert!(report.into_violations().is_empty());
    }

    #[test]
    fn test_validate_disease_without_term() {
        let interpretation_term = create_ontology_class("MONDO:0007254", "Breast Cancer");

        let disease = Disease {
            term: None,
            ..Default::default()
        };
        let interpretation = create_interpretation(Some(interpretation_term.clone()));

        let phenopacket = Phenopacket {
            diseases: vec![disease],
            interpretations: vec![interpretation],
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);
        let violations = report.into_violations();

        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            LintingViolations::DiseaseConsistency(term) if term.id == "MONDO:0007254"
        ));
    }

    #[test]
    fn test_validate_multiple_mismatches() {
        let disease1 = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let interpretation1 = create_ontology_class("MONDO:0005148", "Diabetes");
        let interpretation2 = create_ontology_class("MONDO:0005015", "Hypertension");

        let diseases = vec![create_disease(disease1)];
        let interpretations = vec![
            create_interpretation(Some(interpretation1.clone())),
            create_interpretation(Some(interpretation2.clone())),
        ];

        let phenopacket = Phenopacket {
            diseases,
            interpretations,
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule::default().check(&phenopacket, &mut report);

        assert_eq!(report.into_violations().len(), 2);
    }

    #[test]
    fn test_validate_duplicate_interpretation_diseases() {
        let disease_term = create_ontology_class("MONDO:0007254", "Breast Cancer");
        let disease = create_disease(disease_term.clone());

        let interpretations = vec![
            create_interpretation(Some(disease_term.clone())),
            create_interpretation(Some(disease_term)),
        ];

        let phenopacket = Phenopacket {
            diseases: vec![disease],
            interpretations,
            ..Default::default()
        };
        let mut report = LintReport::default();

        DiseaseConsistencyRule.check(&phenopacket, &mut report);

        assert!(report.into_violations().is_empty());
    }
}
