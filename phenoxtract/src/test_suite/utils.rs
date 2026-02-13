use crate::ontology::resource_references::KnownResourcePrefixes;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::genomic_interpretation::Call;
use std::path::PathBuf;

#[macro_export]
macro_rules! skip_in_ci {
    ($test_name:expr) => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", $test_name);
            return;
        }
    };
    () => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", module_path!());
            return;
        }
    };
}

pub(crate) fn assert_phenopackets(actual: &mut Phenopacket, expected: &mut Phenopacket) {
    remove_created_from_metadata(actual);
    remove_created_from_metadata(expected);

    remove_id_from_variation_descriptor(actual);
    remove_id_from_variation_descriptor(expected);

    remove_version_from_loinc(actual);
    remove_version_from_loinc(expected);

    pretty_assertions::assert_eq!(actual, expected);
}

fn remove_created_from_metadata(pp: &mut Phenopacket) {
    if let Some(meta) = &mut pp.meta_data {
        meta.created = None;
    }
}

fn remove_id_from_variation_descriptor(pp: &mut Phenopacket) {
    for interpretation in pp.interpretations.iter_mut() {
        if let Some(diagnosis) = &mut interpretation.diagnosis {
            for gi in diagnosis.genomic_interpretations.iter_mut() {
                if let Some(call) = &mut gi.call
                    && let Call::VariantInterpretation(vi) = call
                    && let Some(vi) = &mut vi.variation_descriptor
                {
                    vi.id = "TEST_ID".to_string();
                }
            }
        }
    }
}

fn remove_version_from_loinc(pp: &mut Phenopacket) {
    if let Some(metadata) = &mut pp.meta_data {
        let loinc_resource = metadata.resources.iter_mut().find(|resource| {
            resource.id == KnownResourcePrefixes::LOINC.to_string().to_lowercase()
        });

        if let Some(loinc_resource) = loinc_resource {
            loinc_resource.version = "-".to_string()
        }
    }
}

pub(crate) fn test_ontology_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("ontologies")
}
